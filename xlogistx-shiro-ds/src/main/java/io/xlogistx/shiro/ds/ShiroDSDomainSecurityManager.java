package io.xlogistx.shiro.ds;

import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.APIKeyAuthenticationToken;
import io.xlogistx.shiro.authc.CredentialsInfoMatcher;
import io.xlogistx.shiro.authc.DomainUsernamePasswordToken;
import io.xlogistx.shiro.authc.JWTAuthenticationToken;
import io.xlogistx.shiro.mgt.ShiroSecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.cache.CacheManager;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.mgt.RealmSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.security.JWTProvider;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.server.util.UUID7;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.*;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.ExceptionReason.Reason;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * {@link DomainSecurityManager} that persists through an {@link APIDataStore} and authenticates,
 * authorizes and caches through Apache Shiro.
 *
 * <p><b>Persistence.</b> Same entities and tables as {@code DomainSecurityManagerDefault}:
 * {@link SubjectIdentifier}, {@link PrincipalIdentifier}, credential rows ({@link CIPassword},
 * {@link SubjectAPIKey}, plus whatever {@link #addCredentialType(Class)} registers), the
 * permission / role / role-group catalog and the three grant tables, all linked by
 * {@code subject_guid}. Compared with the default it adds: a join-or-begin transaction helper so
 * every multi-row operation is atomic and composes with a caller's transaction; ACTIVE status
 * stamped on new principals and credentials; an ownership check on in-place credential updates;
 * loud failure on unsupported credential types; a row lock that closes the last-principal race;
 * complete password replacement; an explicit API-key cascade on subject delete; and principal IDs
 * normalized through {@link SecConst.SubjectIDFilter} on write and lookup (trimmed, lower-cased,
 * validated), matching what the Shiro token does to the username.</p>
 *
 * <p><b>Authentication.</b> Three credential kinds, one realm: principal ID + password
 * ({@link #login}), raw API key ({@link #loginApiKey}) and JWT bearer token signed with an API
 * key's secret ({@link #loginJWT}; mint one with {@link #mintJWT}). Each builds a Shiro token and
 * runs it through the {@link ShiroSecurityManager}'s authenticator: the {@link DSAuthorizingRealm}
 * loads the subject and credential and applies the status rules, {@link CredentialsInfoMatcher}
 * compares. Nothing is bound to the thread and no session is created, so these calls are safe for
 * "verify the current password" flows. {@link #loginSubject}, {@link #loginSubjectApiKey} and
 * {@link #loginSubjectJWT} are the calls that produce a bound Shiro {@link Subject} with a
 * session; {@link #logout()} ends it.</p>
 *
 * <p><b>Authorization.</b> A bound subject can ask {@code isPermitted(...)} / {@code hasRole(...)}:
 * grants are flattened into permission strings and role names by {@link GrantFlattener} and cached
 * per subject GUID. Every mutation that changes what a subject may do evicts the affected entries.
 * With {@link #setEnforcePermissions(boolean)} on, catalog and subject mutations additionally
 * require the bound subject to hold the matching {@link SecurityModel} permission (off by default
 * so CLI tools and first-run bootstrap work with nobody logged in).</p>
 *
 * <p><b>Wiring.</b> Two modes. <i>Self-managed</i>: {@link #ShiroDSDomainSecurityManager(APIDataStore)}
 * builds its own {@link ShiroSecurityManager} with one {@link DSAuthorizingRealm};
 * {@link #installAsGlobal()} publishes it through {@code SecurityUtils}. <i>Attached</i>: the realm is
 * declared in {@code shiro.ini} (it has a no-arg constructor) and the security manager comes from
 * the INI; the manager is created with {@link #attach(DSAuthorizingRealm, APIDataStore)} or found
 * with {@link #fromGlobal()} after {@code SecurityUtils.setSecurityManager(...)}, and every Shiro call
 * goes through the global security manager. In attached mode {@link #getShiroSecurityManager()} is
 * {@code null} and {@link #installAsGlobal()} throws.</p>
 */
public class ShiroDSDomainSecurityManager
        implements DomainSecurityManager {

    public static final LogWrapper log = new LogWrapper(ShiroDSDomainSecurityManager.class).setEnabled(false);
    public static final String REALM_NAME = "shiro-ds";

    private static final String INVALID_CREDENTIALS = "Invalid credentials";
    private static final String INVALID_KEY = "Invalid key";
    private static final String INVALID_TOKEN = "Invalid token";

    private volatile APIDataStore<?, ?> dataStore;
    private final Set<Class<?>> credentialCollections = ConcurrentHashMap.newKeySet();
    private final DSAuthorizingRealm realm;
    private final CredentialsInfoMatcher credentialsMatcher;
    /** Own security manager; {@code null} when attached to an externally configured one. */
    private final ShiroSecurityManager shiroSecurityManager;
    private volatile boolean enforcePermissions = false;

    /** Backed by the given store, with an in-memory authorization cache. */
    public ShiroDSDomainSecurityManager(APIDataStore<?, ?> dataStore) {
        this(dataStore, null);
    }

    /**
     * @param dataStore    persistence for every entity
     * @param cacheManager Shiro cache manager for authorization info, or {@code null} for an
     *                     in-memory {@link MemoryConstrainedCacheManager}
     */
    public ShiroDSDomainSecurityManager(APIDataStore<?, ?> dataStore, CacheManager cacheManager) {
        this(dataStore, cacheManager, null);
    }

    private ShiroDSDomainSecurityManager(APIDataStore<?, ?> dataStore, CacheManager cacheManager, DSAuthorizingRealm externalRealm) {
        SUS.checkIfNulls("dataStore can't be null", dataStore);
        this.dataStore = dataStore;
        credentialCollections.add(CIPassword.class);
        credentialCollections.add(SubjectAPIKey.class);

        if (externalRealm != null) {
            realm = externalRealm;
            if (realm.getCredentialsMatcher() instanceof CredentialsInfoMatcher) {
                credentialsMatcher = (CredentialsInfoMatcher) realm.getCredentialsMatcher();
            } else {
                credentialsMatcher = new CredentialsInfoMatcher();
                realm.setCredentialsMatcher(credentialsMatcher);
            }
            realm.setDomainSecurityManager(this);
            shiroSecurityManager = null;
            return;
        }

        credentialsMatcher = new CredentialsInfoMatcher();
        realm = new DSAuthorizingRealm(this); // no-arg defaults: name, caching flags
        realm.setCredentialsMatcher(credentialsMatcher);

        shiroSecurityManager = new ShiroSecurityManager();
        shiroSecurityManager.setMainThreadBlocked(false);
        shiroSecurityManager.setCacheManager(cacheManager != null ? cacheManager : new MemoryConstrainedCacheManager());
        shiroSecurityManager.setRealm(realm);
    }

    /**
     * Attach a manager to a realm that was built elsewhere (typically by {@code shiro.ini}). The
     * realm's own matcher is kept when it is a {@link CredentialsInfoMatcher}. Shiro calls made by the
     * returned manager go through {@code SecurityUtils.getSecurityManager()}, which must therefore be
     * the security manager that owns {@code realm}.
     *
     * @throws IllegalStateException if the realm is already bound to another manager
     */
    public static ShiroDSDomainSecurityManager attach(DSAuthorizingRealm realm, APIDataStore<?, ?> dataStore) {
        SUS.checkIfNulls("realm can't be null", realm);
        synchronized (realm) {
            if (realm.isBound()) {
                ShiroDSDomainSecurityManager bound = realm.getDomainSecurityManager();
                if (bound.dataStore == dataStore) {
                    return bound;
                }
                throw new IllegalStateException("Realm '" + realm.getName() + "' is already bound to another manager");
            }
            return new ShiroDSDomainSecurityManager(dataStore, null, realm);
        }
    }

    /**
     * The manager behind the {@link DSAuthorizingRealm} of the global security manager
     * ({@code SecurityUtils}). An unbound realm resolves its store from {@link ResourceManager}
     * (see {@link DSAuthorizingRealm#setDataStoreResource}).
     *
     * @throws IllegalStateException when no global security manager is set, it holds no
     *                               {@link DSAuthorizingRealm}, or the realm cannot resolve a store
     */
    public static ShiroDSDomainSecurityManager fromGlobal() {
        return globalRealm().getDomainSecurityManager();
    }

    /** Like {@link #fromGlobal()}, but attaches {@code dataStore} when the realm is still unbound. */
    public static ShiroDSDomainSecurityManager fromGlobal(APIDataStore<?, ?> dataStore) {
        DSAuthorizingRealm realm = globalRealm();
        return realm.isBound() ? realm.getDomainSecurityManager() : attach(realm, dataStore);
    }

    private static DSAuthorizingRealm globalRealm() {
        SecurityManager sm;
        try {
            sm = SecurityUtils.getSecurityManager();
        } catch (UnavailableSecurityManagerException e) {
            throw new IllegalStateException("No global Shiro SecurityManager: load shiro.ini and call SecurityUtils.setSecurityManager first", e);
        }
        if (sm instanceof RealmSecurityManager) {
            Collection<Realm> realms = ((RealmSecurityManager) sm).getRealms();
            if (realms != null) {
                for (Realm r : realms) {
                    if (r instanceof DSAuthorizingRealm) {
                        return (DSAuthorizingRealm) r;
                    }
                }
            }
        }
        throw new IllegalStateException("Global Shiro SecurityManager " + sm.getClass().getName() + " holds no DSAuthorizingRealm");
    }

    // ------------------------------------------------------------------
    // Shiro wiring
    // ------------------------------------------------------------------

    /** Own security manager, or {@code null} when attached to an externally configured one. */
    public ShiroSecurityManager getShiroSecurityManager() {
        return shiroSecurityManager;
    }

    /** The security manager Shiro calls go through: own, or the global one when attached. */
    public SecurityManager getSecurityManager() {
        return securityManager();
    }

    /** {@code true} when this manager owns its security manager; {@code false} when attached to one built elsewhere. */
    public boolean isSelfManaged() {
        return shiroSecurityManager != null;
    }

    private SecurityManager securityManager() {
        if (shiroSecurityManager != null) {
            return shiroSecurityManager;
        }
        try {
            return SecurityUtils.getSecurityManager();
        } catch (UnavailableSecurityManagerException e) {
            throw new IllegalStateException("Attached to an externally configured realm but no global Shiro SecurityManager is set", e);
        }
    }

    public DSAuthorizingRealm getRealm() {
        return realm;
    }

    /** The matcher, for JWT policy knobs (clock skew, timestamp window, replay cache). */
    public CredentialsInfoMatcher getCredentialsMatcher() {
        return credentialsMatcher;
    }

    /**
     * Make this manager's own Shiro security manager the JVM-wide default ({@link SecurityUtils}).
     *
     * @throws IllegalStateException in attached mode: the INI-built security manager is the one to install
     */
    public ShiroDSDomainSecurityManager installAsGlobal() {
        if (shiroSecurityManager == null) {
            throw new IllegalStateException("Attached to an externally configured SecurityManager; install that one with SecurityUtils.setSecurityManager");
        }
        SecurityUtils.setSecurityManager(shiroSecurityManager);
        return this;
    }

    public boolean isEagerAuthorization() {
        return realm.isEagerAuthorization();
    }

    /** Load and cache roles/permissions at login instead of on the first authorization check (see {@link DSAuthorizingRealm#setEagerAuthorization}). */
    public ShiroDSDomainSecurityManager setEagerAuthorization(boolean eager) {
        realm.setEagerAuthorization(eager);
        return this;
    }

    public boolean isEnforcePermissions() {
        return enforcePermissions;
    }

    /** When on, mutations require the thread-bound Shiro subject to hold the matching permission. */
    public ShiroDSDomainSecurityManager setEnforcePermissions(boolean enforcePermissions) {
        this.enforcePermissions = enforcePermissions;
        return this;
    }

    /**
     * Full Shiro login: authenticates, creates a session, binds the resulting {@link Subject} to the
     * calling thread and returns it. Use {@link #logout()} to end it.
     *
     * @throws SecurityException on any authentication failure (generic message)
     */
    public Subject loginSubject(String principalID, String password, String domainID, String appID)
            throws SecurityException {
        if (SUS.isEmpty(principalID) || password == null) {
            throw new SecurityException(INVALID_CREDENTIALS);
        }
        return bindSubject(new DomainUsernamePasswordToken(principalID, password, false, null, domainID, appID), INVALID_CREDENTIALS);
    }

    /**
     * Full Shiro login with a raw API key (see {@link #loginApiKey}): session, thread binding,
     * returned {@link Subject}. Domain / app default to the key's own scope when {@code null}.
     */
    public Subject loginSubjectApiKey(String key, String domainID, String appID) throws SecurityException {
        if (SUS.isEmpty(key)) {
            throw new SecurityException(INVALID_KEY);
        }
        return bindSubject(new APIKeyAuthenticationToken(key, domainID, appID, null), INVALID_KEY);
    }

    /**
     * Full Shiro login with a JWT bearer token (see {@link #loginJWT}): session, thread binding,
     * returned {@link Subject}. The Shiro principal collection also carries the key ID
     * ({@code DomainPrincipalCollection.getJWSubjectID()}).
     *
     * @param host caller host for the Shiro token, or {@code null}
     */
    public Subject loginSubjectJWT(String compactJWT, String host) throws SecurityException {
        return bindSubject(jwtToken(compactJWT, host), INVALID_TOKEN);
    }

    /** Build a subject, log it in with {@code token}, bind it to the calling thread. */
    private Subject bindSubject(AuthenticationToken token, String failureMessage) {
        Subject subject = new Subject.Builder(securityManager()).buildSubject();
        try {
            subject.login(token);
        } catch (AuthenticationException e) {
            logAuthFailure(token, e);
            throw new SecurityException(failureMessage);
        }
        ThreadContext.bind(subject);
        return subject;
    }

    /** Log out and unbind the Shiro subject bound to the calling thread, if any. */
    public void logout() {
        Subject subject = ThreadContext.getSubject();
        if (subject != null) {
            try {
                subject.logout();
            } finally {
                ThreadContext.unbindSubject();
            }
        }
    }

    // ------------------------------------------------------------------
    // login
    // ------------------------------------------------------------------

    @Override
    public SubjectIdentifier login(String principalID, String credential) throws SecurityException {
        if (SUS.isEmpty(principalID) || credential == null) {
            throw new SecurityException(INVALID_CREDENTIALS);
        }
        return authenticate(new DomainUsernamePasswordToken(principalID, credential, false, null, null, null), INVALID_CREDENTIALS);
    }

    @Override
    public SubjectIdentifier loginApiKey(String key) throws SecurityException {
        if (SUS.isEmpty(key)) {
            throw new SecurityException(INVALID_KEY);
        }
        return authenticate(new APIKeyAuthenticationToken(key), INVALID_KEY);
    }

    /**
     * Authenticate a JWT bearer token (compact form) signed with a {@link SubjectAPIKey}'s secret:
     * the {@code sub} claim names the key ID ({@link SubjectAPIKey#getSubjectID()}), the HMAC is
     * checked with the key's bytes, {@code exp} / {@code nbf} / scope / status rules apply (see
     * {@link CredentialsInfoMatcher}). Authenticator only: no subject, session or thread binding.
     *
     * @return the owning subject
     * @throws SecurityException on any failure (generic message; the cause is logged at INFO)
     */
    public SubjectIdentifier loginJWT(String compactJWT) throws SecurityException {
        return authenticate(jwtToken(compactJWT, null), INVALID_TOKEN);
    }

    /** Parse (without verifying) a compact JWT into a Shiro token; any defect becomes {@link SecurityException}. */
    private static JWTAuthenticationToken jwtToken(String compactJWT, String host) {
        if (SUS.isEmpty(compactJWT)) {
            throw new SecurityException(INVALID_TOKEN);
        }
        String trimmed = compactJWT.trim();
        try {
            JWT jwt = SecUtil.parseJWT(trimmed);
            return new JWTAuthenticationToken(new JWTToken(jwt, trimmed), host);
        } catch (Exception e) {
            if (log.isEnabled()) log.getLogger().log(Level.INFO, "unparseable JWT: " + e);
            throw new SecurityException(INVALID_TOKEN);
        }
    }

    /**
     * Mint a compact JWT that {@link #loginJWT} will accept for {@code sak}: {@code sub} = the key
     * ID, domain / app claims = the key's scope (when set), {@code iat} = now, a fresh nonce, and
     * {@code exp} = now + {@code ttlMillis} when positive. Signed with the key's secret bytes.
     *
     * @param algo HMAC algorithm, {@code null} for HS256
     * @throws IllegalArgumentException when the key has no key ID or no secret
     */
    public static String mintJWT(SubjectAPIKey sak, CryptoConst.JWTAlgo algo, long ttlMillis) {
        SUS.checkIfNulls("API key can't be null", sak);
        if (SUS.isEmpty(sak.getSubjectID())) {
            throw new IllegalArgumentException("API key has no key ID (principalID)");
        }
        byte[] secret = sak.getAPIKeyAsBytes();
        if (secret == null || secret.length == 0) {
            throw new IllegalArgumentException("API key has no secret");
        }
        String domainID = sak.getAppID() != null ? sak.getAppID().getDomainID() : null;
        String appID = sak.getAppID() != null ? sak.getAppID().getAppID() : null;
        JWT jwt = JWT.createJWT(algo != null ? algo : CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), domainID, appID);
        if (ttlMillis > 0) {
            jwt.getPayload().setExpirationTime(new Date(System.currentTimeMillis() + ttlMillis));
        }
        return jwt.hash(secret, JWTProvider.SINGLETON);
    }

    /**
     * Checks a password against the principal's stored {@link CIPassword} without logging in:
     * nothing is bound, no session is created and the Shiro authenticator is not involved.
     *
     * <p>Meant for the "prove you know the current password" step of a password-reset flow, which
     * {@link #login} cannot serve because it denies every subject that is not ACTIVE. The subject
     * may be ACTIVE or {@link SecConst.SecStatus#PENDING_RESET_PASSWORD}; the principal and the
     * password credential must be ACTIVE or unset, as for {@link #login}.</p>
     *
     * @return {@code true} only if the principal resolves, the status rules above hold and the
     *         password matches; {@code false} for every other case (never throws for bad input)
     */
    public boolean verifyPassword(String principalID, String password) {
        if (SUS.isEmpty(principalID) || password == null) {
            return false;
        }
        PrincipalIdentifier principal = lookupPrincipalID(principalID);
        if (principal == null || !DSAuthorizingRealm.isActiveOrUnset(principal.getStatus())) {
            return logVerifyFailure(principalID, "unknown or inactive principal");
        }
        SubjectIdentifier subject = SUS.isEmpty(principal.getSubjectGUID()) ? null : lookupSubjectByGUID(principal.getSubjectGUID());
        if (subject == null) {
            return logVerifyFailure(principalID, "unknown subject");
        }
        SecConst.SecStatus status = subject.getSubjectStatus();
        if (status != SecConst.SecStatus.ACTIVE && status != SecConst.SecStatus.PENDING_RESET_PASSWORD) {
            return logVerifyFailure(principalID, "subject status " + status);
        }
        CIPassword stored = null;
        for (CredentialInfo ci : lookupCredentialsBySubjectGUID(subject.getGUID(), CredentialInfo.Type.PASSWORD)) {
            if (ci instanceof CIPassword) {
                stored = (CIPassword) ci;
                break;
            }
        }
        if (stored == null || !DSAuthorizingRealm.isActiveOrUnset(stored.getCredentialStatus())) {
            return logVerifyFailure(principalID, "missing or inactive password credential");
        }
        try {
            return SecUtil.isPasswordValid(stored, password) || logVerifyFailure(principalID, "password mismatch");
        } catch (RuntimeException e) {
            return logVerifyFailure(principalID, "unreadable password credential: " + e);
        }
    }

    private static boolean logVerifyFailure(String principalID, String why) {
        if (log.isEnabled()) log.getLogger().log(Level.INFO, "verifyPassword failed for " + principalID + ": " + why);
        return false;
    }

    /** Authenticator-only path: no subject, no session, no thread binding. */
    private SubjectIdentifier authenticate(AuthenticationToken token, String failureMessage) {
        AuthenticationInfo info;
        try {
            info = securityManager().authenticate(token);
        } catch (AuthenticationException e) {
            logAuthFailure(token, e);
            throw new SecurityException(failureMessage);
        }
        String subjectGUID = info != null ? DSAuthorizingRealm.subjectGUIDOf(info.getPrincipals()) : null;
        SubjectIdentifier subject = subjectGUID != null ? lookupSubjectByGUID(subjectGUID) : null;
        if (subject == null) {
            throw new SecurityException(failureMessage);
        }
        return subject;
    }

    private static void logAuthFailure(AuthenticationToken token, AuthenticationException e) {
        if (log.isEnabled()) {
            Throwable cause = e.getCause();
            log.getLogger().log(Level.INFO, "authentication failed for " + token + ": " + e
                    + (cause != null ? " caused by " + cause : ""), cause);
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private APIDataStore<?, ?> ds() {
        APIDataStore<?, ?> ret = dataStore;
        if (ret == null) {
            throw new IllegalStateException("No data store set");
        }
        return ret;
    }

    /**
     * Run {@code body} inside the ambient transaction if one is active on this thread, otherwise
     * inside a new one that is committed on success and rolled back on any throwable.
     */
    private <T> T inTransaction(Supplier<T> body) {
        APIDataStore<?, ?> ds = ds();
        if (ds.isTransactionActive()) {
            return body.get();
        }
        ds.beginTransaction();
        boolean ok = false;
        try {
            T ret = body.get();
            ok = true;
            return ret;
        } finally {
            if (ok) {
                ds.endTransaction();
            } else {
                ds.abortTransaction();
            }
        }
    }

    private static <V> V first(List<V> list) {
        return (list == null || list.isEmpty()) ? null : list.get(0);
    }

    private static QueryMatch<String> eq(GetName field, String value) {
        return new QueryMatch<>(field.getName(), value, RelationalOperator.EQUAL);
    }

    /**
     * Normalizes a principal ID for storage and comparison with {@link SecConst.SubjectIDFilter}
     * (trimmed, lower-cased, no invisible characters, minimum length unless it is an email).
     *
     * @return the normalized ID, or {@code null} if the filter rejects it (a rejected ID can match
     *         no stored row, so lookups treat it as unknown)
     */
    static String normalizePrincipal(String principalID) {
        if (principalID == null) {
            return null;
        }
        try {
            return SecConst.SubjectIDFilter.SINGLETON.validate(principalID);
        } catch (NullPointerException | IllegalArgumentException e) {
            return null;
        }
    }

    /** Same as {@link #normalizePrincipal} but surfaces the filter's reason as a {@link SecurityException}. */
    static String requirePrincipal(String principalID) {
        try {
            return SecConst.SubjectIDFilter.SINGLETON.validate(principalID);
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new SecurityException("Invalid principal ID: " + e.getMessage(), e);
        }
    }

    private static boolean appIDMatches(AuthzInfo info, String appID) {
        String owned = info.getAppIdDAO() != null ? info.getAppIdDAO().getAppID() : null;
        return SharedStringUtil.equals(owned, appID, true);
    }

    private PrincipalIdentifier resolvePrincipal(String principalID) {
        String pid = normalizePrincipal(principalID);
        if (SUS.isEmpty(pid)) {
            return null;
        }
        return first(ds().search(PrincipalIdentifier.NVC_PRINCIPAL_IDENTIFIER, null,
                new QueryMatch<>(RelationalOperator.EQUAL, pid, PrincipalIdentifier.Param.PRINCIPAL_ID)));
    }

    private String resolveSubjectGUID(String principalID) {
        PrincipalIdentifier principal = resolvePrincipal(principalID);
        return principal != null ? principal.getSubjectGUID() : null;
    }

    private int countPrincipals(String subjectGUID) {
        return ds().search(PrincipalIdentifier.NVC_PRINCIPAL_IDENTIFIER, APIDataStore.fieldNames(MetaToken.GUID),
                eq(MetaToken.SUBJECT_GUID, subjectGUID)).size();
    }

    private <V extends NVEntity> List<V> byName(NVConfigEntity nvce, String name) {
        return ds().search(nvce, null, eq(MetaToken.NAME, name));
    }

    /** Registered credential classes plus {@link SubjectAPIKey}, which login always honours. */
    private Set<Class<?>> credentialClasses() {
        Set<Class<?>> ret = new LinkedHashSet<>(credentialCollections);
        ret.add(SubjectAPIKey.class);
        return ret;
    }

    // ------------------------------------------------------------------
    // permission enforcement (opt-in)
    // ------------------------------------------------------------------

    private static Subject boundSubject() {
        return ThreadContext.getSubject();
    }

    /** Require the bound subject to hold {@code permission}; no-op unless enforcement is on. */
    private void enforce(String permission) {
        if (!enforcePermissions) {
            return;
        }
        Subject subject = boundSubject();
        if (subject == null || !subject.isAuthenticated()) {
            throw new AccessException("Authentication required for " + permission, Reason.UNAUTHORIZED);
        }
        ShiroUtil.checkPermissions(subject, permission);
    }

    /** Like {@link #enforce}, but a subject acting on itself is always allowed. */
    private void enforceSelfOr(String subjectGUID, String permission) {
        if (!enforcePermissions) {
            return;
        }
        Subject subject = boundSubject();
        if (subject != null && subject.isAuthenticated() && subjectGUID != null
                && subjectGUID.equals(DSAuthorizingRealm.subjectGUIDOf(subject.getPrincipals()))) {
            return;
        }
        enforce(permission);
    }

    // ------------------------------------------------------------------
    // lookups by GUID (used by the realm and the flattener)
    // ------------------------------------------------------------------

    public SubjectIdentifier lookupSubjectByGUID(String subjectGUID) {
        return SUS.isEmpty(subjectGUID) ? null : first(ds().searchByID(SubjectIdentifier.NVC_SUBJECT_IDENTIFIER, subjectGUID));
    }

    public SubjectAPIKey lookupSubjectAPIKey(String key) {
        return SUS.isEmpty(key) ? null : first(ds().search(SubjectAPIKey.NVC_SUBJECT_API_KEY, null,
                eq(SubjectAPIKey.Param.API_KEY.getNVConfig(), key)));
    }

    /** API key by its key ID ({@link SubjectAPIKey#getSubjectID()}, the JWT {@code sub} claim). */
    public SubjectAPIKey lookupSubjectAPIKeyByID(String keyID) {
        return SUS.isEmpty(keyID) ? null : first(ds().search(SubjectAPIKey.NVC_SUBJECT_API_KEY, null,
                eq(SubjectAPIKey.Param.PRINCIPAL_ID.getNVConfig(), keyID)));
    }

    public PermissionInfo lookupPermissionByGUID(String guid) {
        return SUS.isEmpty(guid) ? null : first(ds().searchByID(PermissionInfo.NVC_PERMISSION_INFO, guid));
    }

    public RoleInfo lookupRoleByGUID(String guid) {
        return SUS.isEmpty(guid) ? null : first(ds().searchByID(RoleInfo.NVC_ROLE_INFO, guid));
    }

    public RoleGroupInfo lookupRoleGroupByGUID(String guid) {
        return SUS.isEmpty(guid) ? null : first(ds().searchByID(RoleGroupInfo.NVC_ROLE_GROUP_INFO, guid));
    }

    // ------------------------------------------------------------------
    // subject identifier
    // ------------------------------------------------------------------

    @Override
    public SubjectIdentifier createSubjectID(String principalID, CredentialInfo credentialInfo) {
        enforce(SecurityModel.PERM_ADD_SUBJECT);
        String pid = requirePrincipal(principalID);
        try {
            return inTransaction(() -> {
                if (resolvePrincipal(pid) != null) {
                    throw new SecurityException("Principal ID already exists: " + pid);
                }
                SubjectIdentifier subject = new SubjectIdentifier();
                subject.setGUID(UUID7.randomUUID().toString());
                subject.setSubjectType(BaseSubjectID.SubjectType.USER);
                subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
                subject = ds().insert(subject);

                addPrincipalID(subject, pid);
                if (credentialInfo != null) {
                    createCredential(subject, credentialInfo);
                }
                return subject;
            });
        } catch (SecurityException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SecurityException("Subject creation failed: " + e.getMessage(), e);
        }
    }

    @Override
    public SubjectIdentifier createSubjectID(String principalID, String password, CryptoConst.HashType hashType)
            throws SecurityException {
        SUS.checkIfNulls("password and hash type can't be null", password, hashType);
        CredentialHasher<CIPassword> hasher = SecUtil.lookupCredentialHasher(hashType.getName());
        return createSubjectID(principalID, hasher.hash(password));
    }

    @Override
    public SubjectIdentifier lookupSubjectID(String principalID) {
        return lookupSubjectByGUID(resolveSubjectGUID(principalID));
    }

    @Override
    public void updateSubjectID(SubjectIdentifier update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforceSelfOr(update.getGUID(), SecurityModel.PERM_UPDATE_SUBJECT);
            ds().update(update);
            realm.evictAuthorization(update.getGUID());
        }
    }

    /**
     * Atomically removes the subject and everything keyed to it: principals, every credential
     * row in the registered collections and {@link SubjectAPIKey}, and all grants.
     */
    @Override
    public boolean deleteSubjectID(SubjectIdentifier subject) {
        if (subject == null || SUS.isEmpty(subject.getGUID())) {
            return false;
        }
        String subjectGUID = subject.getGUID();
        enforce(SecurityModel.PERM_DELETE_SUBJECT);
        boolean ret = inTransaction(() -> {
            for (PrincipalIdentifier p : lookupAllPrincipalIdentifiers(subjectGUID)) {
                ds().delete(p, false);
            }
            for (Class<?> credColl : credentialClasses()) {
                List<NVEntity> credentials = ds().search(credColl.getName(), null, eq(MetaToken.SUBJECT_GUID, subjectGUID));
                for (NVEntity ci : credentials) {
                    ds().delete(ci, false);
                }
            }
            for (PermissionGrant g : getPermissionGrants(subjectGUID)) {
                ds().delete(g, false);
            }
            for (RoleGrant g : getRoleGrants(subjectGUID)) {
                ds().delete(g, false);
            }
            for (RoleGroupGrant g : getRoleGroupGrants(subjectGUID)) {
                ds().delete(g, false);
            }
            return ds().delete(subject, false);
        });
        realm.evictAuthorization(subjectGUID);
        return ret;
    }

    // ------------------------------------------------------------------
    // credentials
    // ------------------------------------------------------------------

    @Override
    public CredentialInfo createCredential(String principalID, CredentialInfo credential) {
        String subjectGUID = resolveSubjectGUID(principalID);
        if (subjectGUID == null) {
            throw new SecurityException("Unknown principal: " + principalID);
        }
        return insertCredential(subjectGUID, credential);
    }

    @Override
    public CredentialInfo createCredential(SubjectIdentifier subjectIdentifier, CredentialInfo credential) {
        SUS.checkIfNulls("subject can't be null", subjectIdentifier);
        String subjectGUID = subjectIdentifier.getGUID();
        if (SUS.isEmpty(subjectGUID)) {
            throw new SecurityException("Unknown subject");
        }
        return insertCredential(subjectGUID, credential);
    }

    private CredentialInfo insertCredential(String subjectGUID, CredentialInfo credential) {
        enforceSelfOr(subjectGUID, SecurityModel.PERM_UPDATE_SUBJECT);
        if (!(credential instanceof NVEntity)) {
            throw new IllegalArgumentException("Credential must be an NVEntity to be persisted");
        }
        NVEntity nve = (NVEntity) credential;
        nve.setSubjectGUID(subjectGUID);
        if (credential.getCredentialStatus() == null) {
            credential.setCredentialStatus(SecConst.SecStatus.ACTIVE);
        }
        if (credential instanceof SubjectAPIKey && SUS.isEmpty(((SubjectAPIKey) credential).getSubjectID())) {
            // key ID: what a JWT's sub claim names; never left empty so every key can sign tokens
            ((SubjectAPIKey) credential).setSubjectID(UUID7.randomUUID().toString());
        }
        ds().insert(nve);
        return credential;
    }

    @Override
    public CredentialInfo lookupCredential(String principalID, CredentialInfo.Type type) {
        String subjectGUID = resolveSubjectGUID(principalID);
        if (subjectGUID == null) {
            return null;
        }
        CredentialInfo[] ret = lookupCredentialsBySubjectGUID(subjectGUID, type);
        return ret.length > 0 ? ret[0] : null;
    }

    /**
     * Two paths. A persisted credential (has a GUID) is updated in place after verifying that both
     * the given entity and the stored row belong to {@code subjectIdentifier}. A new
     * {@link CIPassword} (no GUID) replaces every existing password credential of the subject in
     * one transaction. Anything else is rejected.
     *
     * @throws SecurityException        if the credential belongs to another subject or is unknown
     * @throws IllegalArgumentException if the credential is not a persistable entity, or a new
     *                                  non-password credential
     */
    @Override
    public void updateCredential(SubjectIdentifier subjectIdentifier, CredentialInfo update) {
        SUS.checkIfNulls("subjectIdentifier and credential info can't be null", subjectIdentifier, update);
        String subjectGUID = subjectIdentifier.getGUID();
        if (SUS.isEmpty(subjectGUID)) {
            throw new IllegalArgumentException("Subject has no GUID");
        }
        enforceSelfOr(subjectGUID, SecurityModel.PERM_UPDATE_SUBJECT);
        if (!(update instanceof NVEntity)) {
            throw new IllegalArgumentException("Credential must be an NVEntity to be persisted");
        }
        NVEntity nve = (NVEntity) update;

        if (!SUS.isEmpty(nve.getGUID())) {
            if (nve.getSubjectGUID() == null) {
                nve.setSubjectGUID(subjectGUID);
            } else if (!subjectGUID.equals(nve.getSubjectGUID())) {
                throw new SecurityException("Credential does not belong to the subject");
            }
            inTransaction(() -> {
                NVEntity stored = first(ds().searchByID(nve.getClass().getName(), nve.getGUID()));
                if (stored == null) {
                    throw new SecurityException("Unknown credential");
                }
                if (!subjectGUID.equals(stored.getSubjectGUID())) {
                    throw new SecurityException("Credential does not belong to the subject");
                }
                ds().update(nve);
                return null;
            });
            return;
        }

        if (!(update instanceof CIPassword)) {
            throw new IllegalArgumentException("Only CIPassword replacement is supported for a new credential; got "
                    + update.getClass().getName());
        }
        CIPassword newPassword = (CIPassword) update;
        newPassword.setSubjectGUID(subjectGUID);
        if (newPassword.getCredentialStatus() == null) {
            newPassword.setCredentialStatus(SecConst.SecStatus.ACTIVE);
        }
        inTransaction(() -> {
            for (CredentialInfo old : lookupCredentialsBySubjectGUID(subjectGUID, CredentialInfo.Type.PASSWORD)) {
                if (old instanceof NVEntity) {
                    ds().delete((NVEntity) old, false);
                }
            }
            ds().insert(newPassword);
            return null;
        });
    }

    @Override
    public void deleteCredential(CredentialInfo credential) {
        if (credential instanceof NVEntity) {
            NVEntity nve = (NVEntity) credential;
            enforceSelfOr(nve.getSubjectGUID(), SecurityModel.PERM_UPDATE_SUBJECT);
            ds().delete(nve, false);
        }
    }

    @Override
    public CredentialInfo[] lookupAllPrincipalCredentials(String principalID) {
        return lookupCredentialsBySubjectGUID(resolveSubjectGUID(principalID), null);
    }

    @Override
    public CredentialInfo[] lookupCredentialsBySubjectGUID(String subjectGUID, CredentialInfo.Type type) {
        List<CredentialInfo> ret = new ArrayList<>();
        if (SUS.isEmpty(subjectGUID)) {
            return ret.toArray(new CredentialInfo[0]);
        }
        for (Class<?> credColl : credentialClasses()) {
            List<NVEntity> rows = ds().search(credColl.getName(), null, eq(MetaToken.SUBJECT_GUID, subjectGUID));
            for (NVEntity nve : rows) {
                if (nve instanceof CredentialInfo) {
                    CredentialInfo ci = (CredentialInfo) nve;
                    if (type == null || ci.getCredentialType() == type) {
                        ret.add(ci);
                    }
                }
            }
        }
        return ret.toArray(new CredentialInfo[0]);
    }

    // ------------------------------------------------------------------
    // principal identifier
    // ------------------------------------------------------------------

    @Override
    public PrincipalIdentifier addPrincipalID(SubjectIdentifier subject, String principalID) {
        SUS.checkIfNulls("subject can't be null", subject);
        String pid = requirePrincipal(principalID);
        enforceSelfOr(subject.getGUID(), SecurityModel.PERM_UPDATE_SUBJECT);
        PrincipalIdentifier principal = new PrincipalIdentifier(pid);
        principal.setSubjectGUID(subject.getGUID());
        principal.setStatus(SecConst.SecStatus.ACTIVE);
        try {
            return ds().insert(principal);
        } catch (RuntimeException e) {
            throw new SecurityException("Principal ID already exists: " + pid, e);
        }
    }

    @Override
    public PrincipalIdentifier lookupPrincipalID(String principalID) {
        return resolvePrincipal(principalID);
    }

    /**
     * Removes one principal, never the subject's last one. Runs in a transaction that first
     * updates the owning subject row, taking a row lock so concurrent removals on the same subject
     * serialize; then counts, deletes, and re-counts.
     *
     * @return {@code true} if removed; {@code false} if {@code null}, unknown, or the last principal
     */
    @Override
    public boolean deletePrincipalID(PrincipalIdentifier principal) {
        if (principal == null || SUS.isEmpty(principal.getGUID()) || SUS.isEmpty(principal.getSubjectGUID())) {
            return false;
        }
        String subjectGUID = principal.getSubjectGUID();
        enforceSelfOr(subjectGUID, SecurityModel.PERM_UPDATE_SUBJECT);
        return inTransaction(() -> {
            SubjectIdentifier subject = lookupSubjectByGUID(subjectGUID);
            if (subject == null) {
                return false;
            }
            ds().update(subject); // row lock for the rest of the transaction
            if (countPrincipals(subjectGUID) <= 1) {
                return false;
            }
            boolean deleted = ds().delete(principal, false);
            if (deleted && countPrincipals(subjectGUID) == 0) {
                throw new SecurityException("Subject would be left without a principal");
            }
            return deleted;
        });
    }

    @Override
    public PrincipalIdentifier[] lookupAllPrincipalIdentifiers(String subjectGUID) {
        if (SUS.isEmpty(subjectGUID)) {
            return new PrincipalIdentifier[0];
        }
        List<PrincipalIdentifier> list = ds().search(PrincipalIdentifier.NVC_PRINCIPAL_IDENTIFIER, null,
                eq(MetaToken.SUBJECT_GUID, subjectGUID));
        return list.toArray(new PrincipalIdentifier[0]);
    }

    // ------------------------------------------------------------------
    // permissions
    // ------------------------------------------------------------------

    @Override
    public PermissionInfo createPermission(PermissionInfo permission) {
        SUS.checkIfNulls("permission can't be null", permission);
        enforce(SecurityModel.PERM_ADD_PERMISSION);
        return ds().insert(permission);
    }

    @Override
    public PermissionInfo lookupPermission(String appID, String permissionName) {
        for (PermissionInfo p : this.<PermissionInfo>byName(PermissionInfo.NVC_PERMISSION_INFO, permissionName)) {
            if (appIDMatches(p, appID)) {
                return p;
            }
        }
        return null;
    }

    @Override
    public PermissionInfo[] lookupAllPermissionsByAppID(String appID) {
        List<PermissionInfo> ret = new ArrayList<>();
        for (PermissionInfo p : getPermissions()) {
            if (appIDMatches(p, appID)) {
                ret.add(p);
            }
        }
        return ret.toArray(new PermissionInfo[0]);
    }

    @Override
    public void updatePermission(PermissionInfo update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforce(SecurityModel.PERM_UPDATE_PERMISSION);
            ds().update(update);
            realm.evictAllAuthorization();
        }
    }

    @Override
    public boolean deletePermission(PermissionInfo permission) {
        if (permission == null) {
            return false;
        }
        enforce(SecurityModel.PERM_DELETE_PERMISSION);
        boolean ret = ds().delete(permission, false);
        realm.evictAllAuthorization();
        return ret;
    }

    @Override
    public PermissionInfo[] getPermissions() {
        List<PermissionInfo> list = ds().search(PermissionInfo.NVC_PERMISSION_INFO, null);
        return list.toArray(new PermissionInfo[0]);
    }

    // ------------------------------------------------------------------
    // roles
    // ------------------------------------------------------------------

    @Override
    public RoleInfo createRole(RoleInfo role) {
        SUS.checkIfNulls("role can't be null", role);
        enforce(SecurityModel.PERM_ADD_ROLE);
        return ds().insert(role);
    }

    @Override
    public RoleInfo lookupRole(String appID, String roleName) {
        for (RoleInfo r : this.<RoleInfo>byName(RoleInfo.NVC_ROLE_INFO, roleName)) {
            if (appIDMatches(r, appID)) {
                return r;
            }
        }
        return null;
    }

    @Override
    public RoleInfo[] lookupAllRolesByAppID(String appID) {
        List<RoleInfo> ret = new ArrayList<>();
        for (RoleInfo r : getRoles()) {
            if (appIDMatches(r, appID)) {
                ret.add(r);
            }
        }
        return ret.toArray(new RoleInfo[0]);
    }

    @Override
    public void updateRole(RoleInfo update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforce(SecurityModel.PERM_UPDATE_ROLE);
            ds().update(update);
            realm.evictAllAuthorization();
        }
    }

    @Override
    public boolean deleteRole(RoleInfo role) {
        if (role == null) {
            return false;
        }
        enforce(SecurityModel.PERM_DELETE_ROLE);
        boolean ret = ds().delete(role, false);
        realm.evictAllAuthorization();
        return ret;
    }

    @Override
    public RoleInfo[] getRoles() {
        List<RoleInfo> list = ds().search(RoleInfo.NVC_ROLE_INFO, null);
        return list.toArray(new RoleInfo[0]);
    }

    // ------------------------------------------------------------------
    // role groups
    // ------------------------------------------------------------------

    @Override
    public RoleGroupInfo createRoleGroup(RoleGroupInfo roleGroup) {
        SUS.checkIfNulls("role group can't be null", roleGroup);
        enforce(SecurityModel.PERM_ADD_ROLE);
        return ds().insert(roleGroup);
    }

    @Override
    public RoleGroupInfo lookupRoleGroup(String appID, String roleGroupName) {
        for (RoleGroupInfo g : this.<RoleGroupInfo>byName(RoleGroupInfo.NVC_ROLE_GROUP_INFO, roleGroupName)) {
            if (appIDMatches(g, appID)) {
                return g;
            }
        }
        return null;
    }

    @Override
    public RoleGroupInfo[] lookupAllRoleGroupsByAppID(String appID) {
        List<RoleGroupInfo> ret = new ArrayList<>();
        for (RoleGroupInfo g : getRoleGroups()) {
            if (appIDMatches(g, appID)) {
                ret.add(g);
            }
        }
        return ret.toArray(new RoleGroupInfo[0]);
    }

    @Override
    public void updateRoleGroup(RoleGroupInfo update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforce(SecurityModel.PERM_UPDATE_ROLE);
            ds().update(update);
            realm.evictAllAuthorization();
        }
    }

    @Override
    public boolean deleteRoleGroup(RoleGroupInfo roleGroup) {
        if (roleGroup == null) {
            return false;
        }
        enforce(SecurityModel.PERM_DELETE_ROLE);
        boolean ret = ds().delete(roleGroup, false);
        realm.evictAllAuthorization();
        return ret;
    }

    @Override
    public RoleGroupInfo[] getRoleGroups() {
        List<RoleGroupInfo> list = ds().search(RoleGroupInfo.NVC_ROLE_GROUP_INFO, null);
        return list.toArray(new RoleGroupInfo[0]);
    }

    // ------------------------------------------------------------------
    // grants
    // ------------------------------------------------------------------

    @Override
    public PermissionGrant addPermissionGrant(SubjectIdentifier subject, PermissionInfo permissionInfo) {
        SUS.checkIfNulls("subject and permission can't be null", subject, permissionInfo);
        enforce(SecurityModel.PERM_ASSIGN_PERMISSION);
        PermissionGrant grant = new PermissionGrant(permissionInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        PermissionGrant ret = ds().insert(grant);
        realm.evictAuthorization(subject.getGUID());
        return ret;
    }

    @Override
    public boolean deletePermissionGrant(PermissionGrant permissionGrant) {
        if (permissionGrant == null) {
            return false;
        }
        enforce(SecurityModel.PERM_REMOVE_PERMISSION);
        boolean ret = ds().delete(permissionGrant, false);
        realm.evictAuthorization(permissionGrant.getSubjectGUID());
        return ret;
    }

    @Override
    public PermissionGrant[] getPermissionGrants(String subjectGUID) {
        if (SUS.isEmpty(subjectGUID)) {
            return new PermissionGrant[0];
        }
        List<PermissionGrant> list = ds().search(PermissionGrant.NVC_PERMISSION_GRANT, null,
                eq(MetaToken.SUBJECT_GUID, subjectGUID));
        return list.toArray(new PermissionGrant[0]);
    }

    @Override
    public RoleGrant addRoleGrant(SubjectIdentifier subject, RoleInfo roleInfo) {
        SUS.checkIfNulls("subject and role can't be null", subject, roleInfo);
        enforce(SecurityModel.PERM_ASSIGN_ROLE);
        RoleGrant grant = new RoleGrant(roleInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        RoleGrant ret = ds().insert(grant);
        realm.evictAuthorization(subject.getGUID());
        return ret;
    }

    @Override
    public boolean deleteRoleGrant(RoleGrant roleGrant) {
        if (roleGrant == null) {
            return false;
        }
        enforce(SecurityModel.PERM_REMOVE_ROLE);
        boolean ret = ds().delete(roleGrant, false);
        realm.evictAuthorization(roleGrant.getSubjectGUID());
        return ret;
    }

    @Override
    public RoleGrant[] getRoleGrants(String subjectGUID) {
        if (SUS.isEmpty(subjectGUID)) {
            return new RoleGrant[0];
        }
        List<RoleGrant> list = ds().search(RoleGrant.NVC_ROLE_GRANT, null,
                eq(MetaToken.SUBJECT_GUID, subjectGUID));
        return list.toArray(new RoleGrant[0]);
    }

    @Override
    public RoleGroupGrant addRoleGroupGrant(SubjectIdentifier subject, RoleGroupInfo roleGroupInfo) {
        SUS.checkIfNulls("subject and role group can't be null", subject, roleGroupInfo);
        enforce(SecurityModel.PERM_ASSIGN_ROLE);
        RoleGroupGrant grant = new RoleGroupGrant(roleGroupInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        RoleGroupGrant ret = ds().insert(grant);
        realm.evictAuthorization(subject.getGUID());
        return ret;
    }

    @Override
    public boolean deleteRoleGroupGrant(RoleGroupGrant roleGroupGrant) {
        if (roleGroupGrant == null) {
            return false;
        }
        enforce(SecurityModel.PERM_REMOVE_ROLE);
        boolean ret = ds().delete(roleGroupGrant, false);
        realm.evictAuthorization(roleGroupGrant.getSubjectGUID());
        return ret;
    }

    @Override
    public RoleGroupGrant[] getRoleGroupGrants(String subjectGUID) {
        if (SUS.isEmpty(subjectGUID)) {
            return new RoleGroupGrant[0];
        }
        List<RoleGroupGrant> list = ds().search(RoleGroupGrant.NVC_ROLE_GROUP_GRANT, null,
                eq(MetaToken.SUBJECT_GUID, subjectGUID));
        return list.toArray(new RoleGroupGrant[0]);
    }

    // ------------------------------------------------------------------
    // data store
    // ------------------------------------------------------------------

    /** Replaces the backing store; cached authorization info is dropped since it came from the old one. */
    @Override
    public DomainSecurityManager setDataStore(APIDataStore<?, ?> dataStore) {
        SUS.checkIfNulls("dataStore can't be null", dataStore);
        this.dataStore = dataStore;
        realm.evictAllAuthorization();
        return this;
    }

    @Override
    public APIDataStore<?, ?> getDataStore() {
        return dataStore;
    }

    @Override
    public DomainSecurityManager addCredentialType(Class<? extends CredentialInfo> clazz) {
        if (clazz != null) {
            credentialCollections.add(clazz);
        }
        return this;
    }
}
