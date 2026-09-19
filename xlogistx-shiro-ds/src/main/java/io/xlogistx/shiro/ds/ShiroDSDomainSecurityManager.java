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
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.security.JWTProvider;
import org.zoxweb.server.security.PasswordResetTokenUtil;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.server.util.UUID7;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.db.QueryMatchIn;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.filters.FilterType;
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
 * <p><b>Instance grants and sharing.</b> A {@link PermissionGrant} is either catalog-backed
 * ({@code permission_guid}, optionally scoped to one resource through an embedded
 * {@link ResourceMap}) or inlined ({@code permission_token} of the form {@code nventity:<verbs>},
 * always scoped): never both, never neither. A scoped grant flattens to
 * {@code <token>:<resource guid>}, which is what {@code ShiroSecurityController} checks per entity.
 * "A shares X with B" is {@link #addPermissionGrant(SubjectIdentifier, ResourceMap, String)}: one
 * inlined grant row, grantee in {@code subject_guid}, grantor in {@code broker_guid}. The resource
 * must exist; under enforcement an inlined grant requires the caller to own it, and a catalog grant
 * scoped to a resource requires ownership, {@code nventity:share:<guid>}, or the global assign
 * permission. Revoking a scoped grant deletes the grant and its map row; revocation is open to
 * the grantor, the resource owner, and holders of the global remove permission.</p>
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
    /** Principal ID of the one account allowed to hold the wildcard permission; see {@link #setSuperAdminPrincipalID}. */
    public static final String DEFAULT_SUPER_ADMIN_PRINCIPAL_ID = "super-admin@xlogistx.io";

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
            ShiroDSDomainSecurityManager ret = new ShiroDSDomainSecurityManager(dataStore, null, realm);
            ret.registerAsResource();
            return ret;
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
        registerAsResource();
        return this;
    }

    /**
     * Publishes this manager under {@link ResourceManager.Resource#DOMAIN_SECURITY_MANAGER} when the
     * slot is empty, so HTTP services that only know the core interface can find it.
     */
    private void registerAsResource() {
        if (ResourceManager.lookupResource(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER) == null) {
            ResourceManager.SINGLETON.register(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER, this);
        }
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
     * @throws AccessSecurityException on any authentication failure (generic message)
     */
    public Subject loginSubject(String principalID, String password, String domainID, String appID)
            throws AccessSecurityException {
        if (SUS.isEmpty(principalID) || password == null) {
            throw new AccessSecurityException(INVALID_CREDENTIALS);
        }
        return bindSubject(new DomainUsernamePasswordToken(principalID, password, false, null, domainID, appID), INVALID_CREDENTIALS);
    }

    /**
     * Full Shiro login with a raw API key (see {@link #loginApiKey}): session, thread binding,
     * returned {@link Subject}. Domain / app default to the key's own scope when {@code null}.
     */
    public Subject loginSubjectApiKey(String key, String domainID, String appID) throws AccessSecurityException {
        if (SUS.isEmpty(key)) {
            throw new AccessSecurityException(INVALID_KEY);
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
    public Subject loginSubjectJWT(String compactJWT, String host) throws AccessSecurityException {
        return bindSubject(jwtToken(compactJWT, host), INVALID_TOKEN);
    }

    /** Build a subject, log it in with {@code token}, bind it to the calling thread. */
    private Subject bindSubject(AuthenticationToken token, String failureMessage) {
        Subject subject = new Subject.Builder(securityManager()).buildSubject();
        try {
            subject.login(token);
        } catch (AuthenticationException e) {
            logAuthFailure(token, e);
            throw new AccessSecurityException(failureMessage);
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
    public SubjectIdentifier login(String principalID, String credential) throws AccessSecurityException {
        if (SUS.isEmpty(principalID) || credential == null) {
            throw new AccessSecurityException(INVALID_CREDENTIALS);
        }
        return authenticate(new DomainUsernamePasswordToken(principalID, credential, false, null, null, null), INVALID_CREDENTIALS);
    }

    @Override
    public SubjectIdentifier loginApiKey(String key) throws AccessSecurityException {
        if (SUS.isEmpty(key)) {
            throw new AccessSecurityException(INVALID_KEY);
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
     * @throws AccessSecurityException on any failure (generic message; the cause is logged at INFO)
     */
    public SubjectIdentifier loginJWT(String compactJWT) throws AccessSecurityException {
        return authenticate(jwtToken(compactJWT, null), INVALID_TOKEN);
    }

    /** Parse (without verifying) a compact JWT into a Shiro token; any defect becomes {@link AccessSecurityException}. */
    private static JWTAuthenticationToken jwtToken(String compactJWT, String host) {
        if (SUS.isEmpty(compactJWT)) {
            throw new AccessSecurityException(INVALID_TOKEN);
        }
        String trimmed = compactJWT.trim();
        try {
            JWT jwt = SecUtil.parseJWT(trimmed);
            return new JWTAuthenticationToken(new JWTToken(jwt, trimmed), host);
        } catch (Exception e) {
            if (log.isEnabled()) log.getLogger().log(Level.INFO, "unparseable JWT: " + e);
            throw new AccessSecurityException(INVALID_TOKEN);
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

    // ------------------------------------------------------------------
    // password reset
    // ------------------------------------------------------------------

    private static final String INVALID_RESET = "Invalid or expired reset token";
    /** EMAIL-channel token lifetime; default {@code SecStatus.PENDING_RESET_PASSWORD.getValue()} (2 days). */
    private volatile long emailResetTTLMillis = SecConst.SecStatus.PENDING_RESET_PASSWORD.getValue();
    /** ADMIN-channel token lifetime; default 4 hours (the hand-off is synchronous). */
    private volatile long adminResetTTLMillis = 4L * Const.TimeInMillis.HOUR.MILLIS;

    /** Token lifetime per channel; must be positive. */
    public ShiroDSDomainSecurityManager setResetTokenTTL(PasswordResetToken.Channel channel, long ttlMillis) {
        SUS.checkIfNulls("channel can't be null", channel);
        if (ttlMillis <= 0) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        if (channel == PasswordResetToken.Channel.ADMIN) {
            adminResetTTLMillis = ttlMillis;
        } else {
            emailResetTTLMillis = ttlMillis;
        }
        return this;
    }

    public long getResetTokenTTL(PasswordResetToken.Channel channel) {
        return channel == PasswordResetToken.Channel.ADMIN ? adminResetTTLMillis : emailResetTTLMillis;
    }

    /** Unenforced status write: the reset paths are authorized by the token, not by a bound subject. */
    private void setSubjectStatusInternal(SubjectIdentifier subject, SecConst.SecStatus status) {
        subject.setSubjectStatus(status);
        subject.setLastTimeUpdated(System.currentTimeMillis());
        ds().update(subject);
        realm.evictAuthorization(subject.getGUID());
    }

    private List<PasswordResetToken> resetTokensOf(String subjectGUID) {
        return ds().search(PasswordResetToken.NVC_PASSWORD_RESET_TOKEN, null, eq(MetaToken.SUBJECT_GUID, subjectGUID));
    }

    /** Marks every outstanding token of the subject INACTIVE (superseded); returns how many. */
    private int supersedeOutstanding(String subjectGUID, long now) {
        int ret = 0;
        for (PasswordResetToken t : resetTokensOf(subjectGUID)) {
            if (t.isOutstanding(now)) {
                t.setStatus(SecConst.SecStatus.INACTIVE);
                ds().update(t);
                ret++;
            }
        }
        return ret;
    }

    /** True while the subject has an ACTIVE, unexpired reset token (the realm consults this for PENDING subjects). */
    boolean hasOutstandingResetToken(String subjectGUID) {
        long now = System.currentTimeMillis();
        for (PasswordResetToken t : resetTokensOf(subjectGUID)) {
            if (t.isOutstanding(now)) {
                return true;
            }
        }
        return false;
    }

    /** A PENDING_RESET_PASSWORD subject whose token expired is restored to ACTIVE (bounded lockout). */
    void restoreActiveAfterExpiredReset(SubjectIdentifier subject) {
        if (subject != null && subject.getSubjectStatus() == SecConst.SecStatus.PENDING_RESET_PASSWORD) {
            setSubjectStatusInternal(subject, SecConst.SecStatus.ACTIVE);
        }
    }

    /** The subject behind a principal when both are usable for a reset; generic failures otherwise. */
    private SubjectIdentifier resetableSubject(PrincipalIdentifier principal) {
        if (principal == null || !DSAuthorizingRealm.isActiveOrUnset(principal.getStatus())) {
            throw new AccessSecurityException("Unknown principal");
        }
        SubjectIdentifier subject = SUS.isEmpty(principal.getSubjectGUID()) ? null : lookupSubjectByGUID(principal.getSubjectGUID());
        if (subject == null) {
            throw new AccessSecurityException("Unknown principal");
        }
        SecConst.SecStatus status = subject.getSubjectStatus();
        if (status != SecConst.SecStatus.ACTIVE && status != SecConst.SecStatus.PENDING_RESET_PASSWORD) {
            throw new AccessSecurityException("Subject is not active");
        }
        return subject;
    }

    /** Active principals of the subject that are email addresses: the recovery channel. */
    private String[] emailPrincipalsOf(String subjectGUID) {
        List<String> ret = new ArrayList<>();
        for (PrincipalIdentifier p : lookupAllPrincipalIdentifiers(subjectGUID)) {
            if (DSAuthorizingRealm.isActiveOrUnset(p.getStatus()) && FilterType.EMAIL.isValid(p.getPrincipalID())) {
                ret.add(p.getPrincipalID());
            }
        }
        return ret.toArray(new String[0]);
    }

    private PasswordResetRequest issueResetToken(SubjectIdentifier subject, String principalID, PasswordResetToken.Channel channel,
                                                 String brokerGUID, String[] delivery) {
        long now = System.currentTimeMillis();
        long ttl = getResetTokenTTL(channel);
        String clear = PasswordResetTokenUtil.newToken();
        PasswordResetToken row = new PasswordResetToken();
        row.setSubjectGUID(subject.getGUID());
        row.setPrincipalID(principalID);
        row.setTokenHash(PasswordResetTokenUtil.hash(clear));
        row.setExpiryTS(now + ttl);
        row.setConsumedTS(0);
        row.setStatus(SecConst.SecStatus.ACTIVE);
        row.setChannel(channel);
        row.setBrokerGUID(brokerGUID);
        inTransaction(() -> {
            supersedeOutstanding(subject.getGUID(), now);
            ds().insert(row);
            setSubjectStatusInternal(subject, SecConst.SecStatus.PENDING_RESET_PASSWORD);
            return null;
        });
        return new PasswordResetRequest(clear, subject.getGUID(), principalID, delivery, row.getExpiryTS(), channel);
    }

    /**
     * {@inheritDoc}
     * <p>Anonymous by design: no enforcement. Every email principal of the subject is a delivery address.
     */
    @Override
    public PasswordResetRequest requestPasswordReset(String principalID) throws AccessSecurityException {
        PrincipalIdentifier principal = resolvePrincipal(principalID);
        if (principal == null) {
            throw new AccessSecurityException("Unknown principal");
        }
        SubjectIdentifier subject = resetableSubject(principal);
        String[] emails = emailPrincipalsOf(subject.getGUID());
        if (emails.length == 0) {
            throw new NoRecoveryChannelException("Subject has no email principal");
        }
        return issueResetToken(subject, principal.getPrincipalID(), PasswordResetToken.Channel.EMAIL, null, emails);
    }

    /**
     * {@inheritDoc}
     * <p>Requires {@code subject:update} (the wildcard implies it); the bound subject is recorded as the broker.
     */
    @Override
    public PasswordResetRequest adminResetPassword(String principalID) throws AccessSecurityException {
        enforce(SecurityModel.PERM_UPDATE_SUBJECT);
        String pid = requirePrincipal(principalID);
        PrincipalIdentifier principal = resolvePrincipal(pid);
        if (principal == null) {
            throw new AccessSecurityException("Unknown principal: " + pid);
        }
        SubjectIdentifier subject = resetableSubject(principal);
        return issueResetToken(subject, principal.getPrincipalID(), PasswordResetToken.Channel.ADMIN, currentSubjectGUID(),
                emailPrincipalsOf(subject.getGUID()));
    }

    /**
     * {@inheritDoc}
     * <p>Anonymous by design: the token is the authorization. Every failure other than the password
     * policy collapses to one generic message; the reason is logged at INFO when logging is on.
     */
    @Override
    public void completePasswordReset(String principalID, String token, String newPassword) throws AccessSecurityException {
        PrincipalIdentifier principal = resolvePrincipal(principalID);
        if (principal == null || SUS.isEmpty(token)) {
            throw resetFailure(principalID, "unknown principal or empty token");
        }
        SubjectIdentifier subject = SUS.isEmpty(principal.getSubjectGUID()) ? null : lookupSubjectByGUID(principal.getSubjectGUID());
        if (subject == null) {
            throw resetFailure(principalID, "unknown subject");
        }
        FilterType.PASSWORD.validate(newPassword);
        long now = System.currentTimeMillis();
        inTransaction(() -> {
            ds().update(subject); // row lock: two completions serialize on the subject
            PasswordResetToken match = null;
            for (PasswordResetToken t : resetTokensOf(subject.getGUID())) {
                if (t.isOutstanding(now) && PasswordResetTokenUtil.matches(t.getTokenHash(), token)) {
                    match = t;
                }
            }
            if (match == null) {
                throw resetFailure(principalID, "no outstanding token matches");
            }
            SecConst.SecStatus status = subject.getSubjectStatus();
            if (status != SecConst.SecStatus.ACTIVE && status != SecConst.SecStatus.PENDING_RESET_PASSWORD) {
                throw resetFailure(principalID, "subject status " + status);
            }
            replacePassword(subject.getGUID(), HashUtil.toBCryptPassword(newPassword));
            match.setStatus(SecConst.SecStatus.DEACTIVATED);
            match.setConsumedTS(now);
            ds().update(match);
            supersedeOutstanding(subject.getGUID(), now);
            setSubjectStatusInternal(subject, SecConst.SecStatus.ACTIVE);
            return null;
        });
    }

    private static AccessSecurityException resetFailure(String principalID, String why) {
        if (log.isEnabled()) log.getLogger().log(Level.INFO, "password reset failed for " + principalID + ": " + why);
        return new AccessSecurityException(INVALID_RESET);
    }

    /** {@inheritDoc} Self-or-{@code subject:update} under enforcement. */
    @Override
    public boolean cancelPasswordReset(String principalID) {
        PrincipalIdentifier principal = resolvePrincipal(principalID);
        SubjectIdentifier subject = principal == null || SUS.isEmpty(principal.getSubjectGUID()) ? null
                : lookupSubjectByGUID(principal.getSubjectGUID());
        if (subject == null) {
            return false;
        }
        enforceSelfOr(subject.getGUID(), SecurityModel.PERM_UPDATE_SUBJECT);
        return inTransaction(() -> {
            boolean changed = supersedeOutstanding(subject.getGUID(), System.currentTimeMillis()) > 0;
            if (subject.getSubjectStatus() == SecConst.SecStatus.PENDING_RESET_PASSWORD) {
                setSubjectStatusInternal(subject, SecConst.SecStatus.ACTIVE);
                changed = true;
            }
            return changed;
        });
    }

    @Override
    public int purgeExpiredResetTokens() {
        long now = System.currentTimeMillis();
        int ret = 0;
        List<PasswordResetToken> all = ds().search(PasswordResetToken.NVC_PASSWORD_RESET_TOKEN, null);
        for (PasswordResetToken t : all) {
            if (!t.isOutstanding(now) && ds().delete(t, false)) {
                ret++;
            }
        }
        return ret;
    }

    /** Authenticator-only path: no subject, no session, no thread binding. */
    private SubjectIdentifier authenticate(AuthenticationToken token, String failureMessage) {
        AuthenticationInfo info;
        try {
            info = securityManager().authenticate(token);
        } catch (AuthenticationException e) {
            logAuthFailure(token, e);
            throw new AccessSecurityException(failureMessage);
        }
        String subjectGUID = info != null ? DSAuthorizingRealm.subjectGUIDOf(info.getPrincipals()) : null;
        SubjectIdentifier subject = subjectGUID != null ? lookupSubjectByGUID(subjectGUID) : null;
        if (subject == null) {
            throw new AccessSecurityException(failureMessage);
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
    <T> T inTransaction(Supplier<T> body) {
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

    /** Same as {@link #normalizePrincipal} but surfaces the filter's reason as a {@link AccessSecurityException}. */
    static String requirePrincipal(String principalID) {
        try {
            return SecConst.SubjectIDFilter.SINGLETON.validate(principalID);
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new AccessSecurityException("Invalid principal ID: " + e.getMessage(), e);
        }
    }

    private static boolean appIDMatches(AuthzInfo info, String appID) {
        String owned = info.getAppIdDAO() != null ? info.getAppIdDAO().getAppID() : null;
        return SUS.equals(owned, appID, true);
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
    // super-admin and the reserved wildcard
    // ------------------------------------------------------------------

    /**
     * @return the normalized principal ID of the super-admin account (realm property, INI-settable
     * as {@code dsRealm.superAdminPrincipalID}); default {@link #DEFAULT_SUPER_ADMIN_PRINCIPAL_ID}
     */
    public String getSuperAdminPrincipalID() {
        return realm.getSuperAdminPrincipalID();
    }

    /**
     * Names the one account allowed to hold the wildcard permission {@code *}. The value is
     * normalized through {@link SecConst.SubjectIDFilter}; every cached authorization is evicted so
     * a former super-admin loses the wildcard on its next authorization load.
     *
     * @param principalID the super-admin principal ID
     * @return this manager, for call chaining
     */
    public ShiroDSDomainSecurityManager setSuperAdminPrincipalID(String principalID) {
        realm.setSuperAdminPrincipalID(principalID);
        return this;
    }

    /** True if the subject owns the super-admin principal ID. */
    public boolean isSuperAdminSubject(String subjectGUID) {
        if (SUS.isEmpty(subjectGUID)) {
            return false;
        }
        String superAdmin = getSuperAdminPrincipalID();
        for (PrincipalIdentifier p : lookupAllPrincipalIdentifiers(subjectGUID)) {
            if (superAdmin.equals(p.getPrincipalID())) {
                return true;
            }
        }
        return false;
    }

    /** The super-admin subject, or null when it has not been bootstrapped yet. */
    public SubjectIdentifier lookupSuperAdminSubject() {
        return lookupSubjectID(getSuperAdminPrincipalID());
    }

    /** A catalog row whose token is the wildcard: only the reserved {@code super_admin_all} row may be one. */
    private static boolean isReservedPermission(PermissionInfo permission) {
        return permission != null && SecurityModel.isWildcardToken(permission.getPermissionToken());
    }

    private static boolean isReservedName(AuthzInfo info, GetName reserved) {
        return info != null && reserved.getName().equals(info.getName()) && info.getAppIdDAO() == null;
    }

    /** True if the role is the reserved {@code super_admin} role or embeds a reserved permission (refs re-read by GUID). */
    private boolean isReservedRole(RoleInfo role) {
        if (role == null) {
            return false;
        }
        if (isReservedName(role, SecurityModel.Role.SUPER_ADMIN)) {
            return true;
        }
        PermissionInfo[] permissions = role.getPermissions();
        if (permissions != null) {
            for (PermissionInfo p : permissions) {
                if (p == null) {
                    continue;
                }
                PermissionInfo stored = SUS.isEmpty(p.getGUID()) ? null : lookupPermissionByGUID(p.getGUID());
                if (isReservedPermission(stored != null ? stored : p)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True if the group embeds the reserved role (roles re-read by GUID). */
    private boolean groupHasReservedRole(RoleGroupInfo group) {
        RoleInfo[] roles = group != null ? group.getRoles() : null;
        if (roles != null) {
            for (RoleInfo r : roles) {
                if (r == null) {
                    continue;
                }
                RoleInfo stored = SUS.isEmpty(r.getGUID()) ? null : lookupRoleByGUID(r.getGUID());
                if (isReservedRole(stored != null ? stored : r)) {
                    return true;
                }
            }
        }
        return false;
    }

    private PermissionInfo reservedPermissionRow() {
        return lookupPermission(null, SecurityModel.Permission.SUPER_ADMIN_ALL.getName());
    }

    private RoleInfo reservedRoleRow() {
        return lookupRole(null, SecurityModel.Role.SUPER_ADMIN.getName());
    }

    /**
     * Seeds or repairs the built-in catalog ({@link SecurityCatalogSeeder}): every
     * {@link SecurityModel.Permission}, {@link SecurityModel.Role} and {@link SecurityModel.RoleGroup}
     * as global rows. Idempotent; grants nothing.
     */
    public SecurityCatalogSeeder.Report seedCatalog() {
        // a catalog write even when nothing turns out to need repair
        enforce(SecurityModel.PERM_ADD_PERMISSION);
        enforce(SecurityModel.PERM_ADD_ROLE);
        return SecurityCatalogSeeder.seed(this);
    }

    /** A reserved permission, role or group may be granted to the super-admin subject only. */
    private void enforceReservedGrantee(String subjectGUID, String what) {
        if (!isSuperAdminSubject(subjectGUID)) {
            throw new AccessSecurityException("Only the super-admin account may hold " + what, Reason.UNAUTHORIZED);
        }
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
            throw new AccessSecurityException("Authentication required for " + permission, Reason.UNAUTHORIZED);
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

    /** GUID of the bound, authenticated subject; null when nobody is bound. */
    private static String currentSubjectGUID() {
        Subject subject = boundSubject();
        return subject != null && subject.isAuthenticated()
                ? DSAuthorizingRealm.subjectGUIDOf(subject.getPrincipals()) : null;
    }

    /** True if a bound, authenticated subject holds {@code permission}; never throws. */
    private static boolean holds(String permission) {
        Subject subject = boundSubject();
        return subject != null && subject.isAuthenticated() && ShiroUtil.isPermitted(subject, permission);
    }

    // ------------------------------------------------------------------
    // app scope
    // ------------------------------------------------------------------

    /**
     * The label of an app scope: the lower-cased {@code <domain>-<app>} canonical ID, or null for
     * global. Used in cache keys, logs and the CLI; grants are never prefixed with it.
     */
    public static String appScope(AppIDDefault app) {
        return app == null ? null : SUS.toLowerCase(app.getDomainAppID());
    }

    /** Both domain and app must be set; the canonical ID getter validates the two parts. */
    private static void requireApp(AppIDDefault app) {
        if (SUS.isEmpty(app.getDomainID()) || SUS.isEmpty(app.getAppID())) {
            throw new IllegalArgumentException("app scope needs both domain and app id: " + app);
        }
        app.getDomainAppID();
    }

    /** True if the grant is scoped to {@code app} (domain and app compared case-insensitively). */
    private static boolean sameApp(AuthzInfo grant, AppIDDefault app) {
        return app != null && grant.getAppIdDAO() != null && app.equals(grant.getAppIdDAO());
    }

    /** The login scope of the bound, authenticated subject: null for a global login. */
    private static AppIDDefault loginScope() {
        Subject subject = boundSubject();
        return subject != null && subject.isAuthenticated() ? DSAuthorizingRealm.loginScopeOf(subject.getPrincipals()) : null;
    }

    /**
     * Whether the caller's login scope may act on a grant scoped to {@code app}: a global login may
     * act on anything, an app login only on grants of that same app (never on global grants). The
     * super-admin subject may act from any login.
     */
    private boolean scopeAllows(AppIDDefault app) {
        String caller = currentSubjectGUID();
        if (caller != null && isSuperAdminSubject(caller)) {
            return true;
        }
        AppIDDefault session = loginScope();
        return session == null || (app != null && session.equals(app));
    }

    /**
     * {@link #enforce} for a grant that may be scoped: the caller must hold the permission in its
     * current login, and that login's scope must be allowed to act on {@code app} ({@link #scopeAllows}).
     */
    private void enforceScoped(AppIDDefault app, String permission) {
        if (!enforcePermissions) {
            return;
        }
        enforce(permission);
        if (!scopeAllows(app)) {
            throw new AccessSecurityException("A login scoped to " + appScope(loginScope()) + " may not grant "
                    + (app == null ? "globally" : "in app " + appScope(app)), Reason.UNAUTHORIZED);
        }
    }

    /**
     * Who may revoke a role or role-group grant; no-op unless enforcement is on: the grantor
     * ({@code broker_guid}), or a holder of {@code permission:remove:role} whose login scope may act
     * on the grant's scope.
     */
    private void enforceRevokeRole(AuthzInfo stored) {
        if (!enforcePermissions) {
            return;
        }
        String caller = currentSubjectGUID();
        if (caller == null) {
            throw new AccessSecurityException("Authentication required for " + SecurityModel.PERM_REMOVE_ROLE, Reason.UNAUTHORIZED);
        }
        if (caller.equals(stored.getBrokerGUID())) {
            return;
        }
        if (holds(SecurityModel.PERM_REMOVE_ROLE) && scopeAllows(stored.getAppIdDAO())) {
            return;
        }
        throw new AccessSecurityException("Not permitted to revoke this grant: " + SecurityModel.PERM_REMOVE_ROLE, Reason.UNAUTHORIZED);
    }

    private static boolean isOwner(NVEntity resource, String subjectGUID) {
        return subjectGUID != null && resource != null && subjectGUID.equals(resource.getSubjectGUID());
    }

    /**
     * Owner-or-share rule for granting on a resource; no-op unless enforcement is on. The owner may
     * always grant. An inlined grant (a share) is owner-only. A catalog grant scoped to the resource
     * is also allowed to a holder of {@code nventity:share:<guid>} or of the global assign permission.
     */
    private void enforceGrantOnResource(NVEntity resource, boolean inlined) {
        if (!enforcePermissions) {
            return;
        }
        String caller = currentSubjectGUID();
        if (caller == null) {
            throw new AccessSecurityException("Authentication required to grant on " + resource.getGUID(), Reason.UNAUTHORIZED);
        }
        if (isOwner(resource, caller)) {
            return;
        }
        if (inlined) {
            throw new AccessSecurityException("Only the owner may share " + resource.getGUID(), Reason.UNAUTHORIZED);
        }
        if (holds(SecurityModel.toSecTok(SecurityModel.NVENTITY, SecurityModel.SHARE, resource.getGUID()))) {
            return;
        }
        enforce(SecurityModel.PERM_ASSIGN_PERMISSION);
    }

    /**
     * Who may revoke a grant; no-op unless enforcement is on: a holder of the global remove
     * permission, the grantor ({@code broker_guid}), or the owner of the scoped resource.
     */
    private void enforceRevoke(PermissionGrant stored) {
        if (!enforcePermissions) {
            return;
        }
        String caller = currentSubjectGUID();
        if (caller == null) {
            throw new AccessSecurityException("Authentication required for " + SecurityModel.PERM_REMOVE_PERMISSION, Reason.UNAUTHORIZED);
        }
        if (caller.equals(stored.getBrokerGUID())
                || (holds(SecurityModel.PERM_REMOVE_PERMISSION) && scopeAllows(stored.getAppIdDAO()))) {
            return;
        }
        if (stored.getResourceMap() != null) {
            NVEntity resource;
            try {
                resource = loadResource(stored.getResourceMap());
            } catch (IllegalArgumentException e) {
                resource = null;
            }
            if (isOwner(resource, caller)) {
                return;
            }
        }
        throw new AccessSecurityException("Not permitted to revoke grant " + stored.getGUID(), Reason.UNAUTHORIZED);
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
        return createSubjectID(principalID, credentialInfo, BaseSubjectID.SubjectType.USER);
    }

    /**
     * Creates a subject of the given type (the public overloads create {@code USER}; the bootstrap
     * creates the super-admin as {@code SYSTEM}).
     */
    SubjectIdentifier createSubjectID(String principalID, CredentialInfo credentialInfo, BaseSubjectID.SubjectType type) {
        SUS.checkIfNulls("subject type can't be null", type);
        enforce(SecurityModel.PERM_ADD_SUBJECT);
        String pid = requirePrincipal(principalID);
        try {
            return inTransaction(() -> {
                if (resolvePrincipal(pid) != null) {
                    throw new AccessSecurityException("Principal ID already exists: " + pid);
                }
                SubjectIdentifier subject = new SubjectIdentifier();
                subject.setGUID(UUID7.randomUUID().toString());
                subject.setSubjectType(type);
                subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
                subject = ds().insert(subject);

                addPrincipalID(subject, pid);
                if (credentialInfo != null) {
                    createCredential(subject, credentialInfo);
                }
                return subject;
            });
        } catch (AccessSecurityException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new AccessSecurityException("Subject creation failed: " + e.getMessage(), e);
        }
    }

    @Override
    public SubjectIdentifier createSubjectID(String principalID, String password, CryptoConst.HashType hashType)
            throws AccessSecurityException {
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
                deleteGrantRows(g);
            }
            for (RoleGrant g : getRoleGrants(subjectGUID)) {
                ds().delete(g, false);
            }
            for (RoleGroupGrant g : getRoleGroupGrants(subjectGUID)) {
                ds().delete(g, false);
            }
            for (PasswordResetToken t : resetTokensOf(subjectGUID)) {
                ds().delete(t, false);
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
            throw new AccessSecurityException("Unknown principal: " + principalID);
        }
        return insertCredential(subjectGUID, credential);
    }

    @Override
    public CredentialInfo createCredential(SubjectIdentifier subjectIdentifier, CredentialInfo credential) {
        SUS.checkIfNulls("subject can't be null", subjectIdentifier);
        String subjectGUID = subjectIdentifier.getGUID();
        if (SUS.isEmpty(subjectGUID)) {
            throw new AccessSecurityException("Unknown subject");
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
     * @throws AccessSecurityException        if the credential belongs to another subject or is unknown
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
                throw new AccessSecurityException("Credential does not belong to the subject");
            }
            inTransaction(() -> {
                NVEntity stored = first(ds().searchByID(nve.getClass().getName(), nve.getGUID()));
                if (stored == null) {
                    throw new AccessSecurityException("Unknown credential");
                }
                if (!subjectGUID.equals(stored.getSubjectGUID())) {
                    throw new AccessSecurityException("Credential does not belong to the subject");
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
        replacePassword(subjectGUID, newPassword);
    }

    /**
     * Deletes every PASSWORD row of the subject and inserts the new one, in one transaction.
     * Unenforced: the callers decide who may do this ({@link #updateCredential} enforces self-or-admin,
     * {@link #completePasswordReset} is authorized by the reset token).
     */
    private void replacePassword(String subjectGUID, CIPassword newPassword) {
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
            throw new AccessSecurityException("Principal ID already exists: " + pid, e);
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
                throw new AccessSecurityException("Subject would be left without a principal");
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

    /**
     * A wildcard token is accepted only for the reserved {@code super_admin_all} global row, and
     * only while none exists.
     */
    @Override
    public PermissionInfo createPermission(PermissionInfo permission) {
        SUS.checkIfNulls("permission can't be null", permission);
        enforce(SecurityModel.PERM_ADD_PERMISSION);
        if (isReservedPermission(permission)) {
            if (!isReservedName(permission, SecurityModel.Permission.SUPER_ADMIN_ALL)) {
                throw new IllegalArgumentException("wildcard permission tokens are reserved for "
                        + SecurityModel.Permission.SUPER_ADMIN_ALL.getName());
            }
            if (reservedPermissionRow() != null) {
                throw new IllegalArgumentException("reserved permission already exists: "
                        + SecurityModel.Permission.SUPER_ADMIN_ALL.getName());
            }
        }
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

    /** The reserved row is immutable; no other row may acquire a wildcard token. */
    @Override
    public void updatePermission(PermissionInfo update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforce(SecurityModel.PERM_UPDATE_PERMISSION);
            PermissionInfo stored = lookupPermissionByGUID(update.getGUID());
            if (isReservedPermission(stored)) {
                throw new IllegalArgumentException("the reserved permission is immutable: " + stored.getName());
            }
            if (isReservedPermission(update)) {
                throw new IllegalArgumentException("wildcard permission tokens are reserved for "
                        + SecurityModel.Permission.SUPER_ADMIN_ALL.getName());
            }
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
        PermissionInfo stored = SUS.isEmpty(permission.getGUID()) ? null : lookupPermissionByGUID(permission.getGUID());
        if (isReservedPermission(stored != null ? stored : permission)) {
            throw new IllegalArgumentException("the reserved permission cannot be deleted");
        }
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

    /**
     * A role embedding the reserved permission is accepted only as the reserved global
     * {@code super_admin} role, and only while none exists.
     */
    @Override
    public RoleInfo createRole(RoleInfo role) {
        SUS.checkIfNulls("role can't be null", role);
        enforce(SecurityModel.PERM_ADD_ROLE);
        if (isReservedRole(role)) {
            if (!isReservedName(role, SecurityModel.Role.SUPER_ADMIN)) {
                throw new IllegalArgumentException("the reserved permission may only be carried by the role "
                        + SecurityModel.Role.SUPER_ADMIN.getName());
            }
            if (reservedRoleRow() != null) {
                throw new IllegalArgumentException("reserved role already exists: " + SecurityModel.Role.SUPER_ADMIN.getName());
            }
        }
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

    /** The reserved role is immutable through this call; no other role may acquire a reserved permission. */
    @Override
    public void updateRole(RoleInfo update) {
        if (update != null && !SUS.isEmpty(update.getGUID())) {
            enforce(SecurityModel.PERM_UPDATE_ROLE);
            RoleInfo stored = lookupRoleByGUID(update.getGUID());
            if (isReservedRole(stored)) {
                throw new IllegalArgumentException("the reserved role is immutable: " + stored.getName());
            }
            if (isReservedRole(update)) {
                throw new IllegalArgumentException("the reserved permission may only be carried by the role "
                        + SecurityModel.Role.SUPER_ADMIN.getName());
            }
            updateRoleInternal(update);
        }
    }

    /** Update without the immutability check: the seeder's path for repairing built-in roles. */
    void updateRoleInternal(RoleInfo update) {
        enforce(SecurityModel.PERM_UPDATE_ROLE);
        ds().update(update);
        realm.evictAllAuthorization();
    }

    @Override
    public boolean deleteRole(RoleInfo role) {
        if (role == null) {
            return false;
        }
        enforce(SecurityModel.PERM_DELETE_ROLE);
        RoleInfo stored = SUS.isEmpty(role.getGUID()) ? null : lookupRoleByGUID(role.getGUID());
        if (isReservedRole(stored != null ? stored : role)) {
            throw new IllegalArgumentException("the reserved role cannot be deleted");
        }
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

    /** A role group may never embed the reserved role. */
    @Override
    public RoleGroupInfo createRoleGroup(RoleGroupInfo roleGroup) {
        SUS.checkIfNulls("role group can't be null", roleGroup);
        enforce(SecurityModel.PERM_ADD_ROLE);
        if (groupHasReservedRole(roleGroup)) {
            throw new IllegalArgumentException("a role group may not embed " + SecurityModel.Role.SUPER_ADMIN.getName());
        }
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
            if (groupHasReservedRole(update)) {
                throw new IllegalArgumentException("a role group may not embed " + SecurityModel.Role.SUPER_ADMIN.getName());
            }
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

    /**
     * Global catalog grant: requires the global assign permission under enforcement. The reserved
     * wildcard permission may only be granted to the super-admin subject.
     */
    @Override
    public PermissionGrant addPermissionGrant(SubjectIdentifier subject, PermissionInfo permissionInfo) {
        SUS.checkIfNulls("subject and permission can't be null", subject, permissionInfo);
        PermissionGrant grant = new PermissionGrant(permissionInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        grant.validateShape();
        enforce(SecurityModel.PERM_ASSIGN_PERMISSION);
        PermissionInfo stored = SUS.isEmpty(permissionInfo.getGUID()) ? null : lookupPermissionByGUID(permissionInfo.getGUID());
        if (isReservedPermission(stored != null ? stored : permissionInfo)) {
            enforceReservedGrantee(subject.getGUID(), "the wildcard permission");
        }
        return insertGrant(grant);
    }

    /**
     * Catalog grant scoped to an app: it applies only to a login made with that domain and app
     * (plain token, see {@link GrantFlattener}). Under enforcement the caller must hold the assign
     * permission in a login whose scope is global or that same app; the caller is recorded as
     * {@code broker_guid}. The reserved wildcard permission is never scoped.
     */
    public PermissionGrant addPermissionGrant(SubjectIdentifier subject, PermissionInfo permissionInfo, AppIDDefault app) {
        SUS.checkIfNulls("subject, permission and app can't be null", subject, permissionInfo, app);
        requireApp(app);
        PermissionGrant grant = new PermissionGrant(permissionInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        grant.setAppIdDAO(app);
        grant.validateShape();
        enforceScoped(app, SecurityModel.PERM_ASSIGN_PERMISSION);
        PermissionInfo stored = SUS.isEmpty(permissionInfo.getGUID()) ? null : lookupPermissionByGUID(permissionInfo.getGUID());
        if (isReservedPermission(stored != null ? stored : permissionInfo)) {
            throw new AccessSecurityException("The wildcard permission is never app-scoped", Reason.UNAUTHORIZED);
        }
        return insertGrant(grant);
    }

    /**
     * Catalog grant scoped to one resource: the permission token must be {@code <namespace>:<verbs>}
     * (the resource GUID becomes its third part when flattened), the resource must exist, and under
     * enforcement the caller must own it, hold {@code nventity:share:<guid>}, or hold the global
     * assign permission.
     */
    @Override
    public PermissionGrant addPermissionGrant(SubjectIdentifier subject, PermissionInfo permissionInfo, ResourceMap resource) {
        SUS.checkIfNulls("subject, permission and resource can't be null", subject, permissionInfo, resource);
        PermissionGrant grant = new PermissionGrant(permissionInfo.getGUID(), resource);
        grant.setSubjectGUID(subject.getGUID());
        grant.validateShape();
        checkCatalogTokenForScope(permissionInfo.getGUID());
        NVEntity res = loadResource(resource);
        enforceGrantOnResource(res, false);
        return insertGrant(grant);
    }

    /**
     * Inlined grant, the form of a share: {@code nventity:<verbs>} with read, update, share and
     * delete only, always scoped; the resource must exist and, under enforcement, belong to the caller.
     */
    @Override
    public PermissionGrant addPermissionGrant(SubjectIdentifier subject, ResourceMap resource, String permissionToken) {
        SUS.checkIfNulls("subject and resource can't be null", subject, resource);
        if (SUS.isEmpty(permissionToken)) {
            throw new IllegalArgumentException("permission token required for an inlined grant");
        }
        PermissionGrant grant = new PermissionGrant(resource, permissionToken);
        grant.setSubjectGUID(subject.getGUID());
        grant.validateShape();
        NVEntity res = loadResource(resource);
        enforceGrantOnResource(res, true);
        return insertGrant(grant);
    }

    /**
     * Revokes a grant together with its embedded resource map. The grant is reloaded by GUID so a
     * caller-supplied shell still cascades to the stored map; a grant that no longer exists yields false.
     */
    @Override
    public boolean deletePermissionGrant(PermissionGrant permissionGrant) {
        if (permissionGrant == null || SUS.isEmpty(permissionGrant.getGUID())) {
            return false;
        }
        PermissionGrant stored = first(ds().searchByID(PermissionGrant.NVC_PERMISSION_GRANT, permissionGrant.getGUID()));
        if (stored == null) {
            return false;
        }
        enforceRevoke(stored);
        return deleteGrantAndMap(stored);
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

    /**
     * Grants scoped to one resource, whatever the grantee: resource-map rows naming the GUID first,
     * then the grants embedding one of them (the query formatter has no joins).
     */
    @Override
    public PermissionGrant[] getPermissionGrantsByResource(String resourceGUID) {
        List<String> mapGUIDs = mapGUIDsForResource(resourceGUID);
        if (mapGUIDs.isEmpty()) {
            return new PermissionGrant[0];
        }
        List<PermissionGrant> list = ds().search(PermissionGrant.NVC_PERMISSION_GRANT, null,
                new QueryMatchIn<>(PermissionGrant.Param.RESOURCE_MAP.getNVConfig().getName(), mapGUIDs));
        return list.toArray(new PermissionGrant[0]);
    }

    /** Revokes every grant scoped to the resource, each subject to {@link #enforceRevoke}; atomic. */
    @Override
    public int deletePermissionGrantsByResource(String resourceGUID) {
        PermissionGrant[] grants = getPermissionGrantsByResource(resourceGUID);
        if (grants.length == 0) {
            return 0;
        }
        return inTransaction(() -> {
            int count = 0;
            for (PermissionGrant g : grants) {
                enforceRevoke(g);
                if (deleteGrantAndMap(g)) {
                    count++;
                }
            }
            return count;
        });
    }

    /** Records the grantor, writes map row + grant row atomically, evicts the grantee. */
    private PermissionGrant insertGrant(PermissionGrant grant) {
        grant.setBrokerGUID(currentSubjectGUID());
        PermissionGrant ret = inTransaction(() -> ds().insert(grant));
        realm.evictAuthorization(grant.getSubjectGUID());
        return ret;
    }

    /** Deletes the grant and its map row atomically, then evicts the grantee. */
    private boolean deleteGrantAndMap(PermissionGrant stored) {
        boolean ret = inTransaction(() -> deleteGrantRows(stored));
        realm.evictAuthorization(stored.getSubjectGUID());
        return ret;
    }

    /** Grant row first (it references the map), then the map row if the grant embeds one. */
    private boolean deleteGrantRows(PermissionGrant grant) {
        boolean ret = ds().delete(grant, false);
        ResourceMap map = grant.getResourceMap();
        if (map != null && !SUS.isEmpty(map.getGUID())) {
            ds().delete(map, false);
        }
        return ret;
    }

    /**
     * Loads the entity a resource map names, by class name and GUID.
     *
     * @throws IllegalArgumentException if the map is incomplete, the class is unknown, or no row matches
     */
    private NVEntity loadResource(ResourceMap resource) {
        SUS.checkIfNulls("resource map null", resource);
        String type = resource.getResourceType();
        String guid = resource.getResourceGUID();
        if (SUS.isEmpty(type) || SUS.isEmpty(guid)) {
            throw new IllegalArgumentException("resource_map requires both resource_type and resource_guid");
        }
        NVEntity ret;
        try {
            ret = first(ds().searchByID(type, guid));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unknown resource type " + type, e);
        }
        if (ret == null) {
            throw new IllegalArgumentException("Resource not found " + type + ":" + guid);
        }
        return ret;
    }

    /**
     * A catalog permission used with a resource must exist and its token must be
     * {@code <namespace>:<verbs>} so the resource GUID can become its third part.
     */
    private void checkCatalogTokenForScope(String permissionGUID) {
        PermissionInfo permission = lookupPermissionByGUID(permissionGUID);
        if (permission == null) {
            throw new IllegalArgumentException("Unknown permission " + permissionGUID);
        }
        if (!SecurityModel.isInstanceScopable(permission.getPermissionToken())) {
            throw new IllegalArgumentException("permission token cannot be scoped to a resource: " + permission.getPermissionToken());
        }
    }

    /** GUIDs of the resource-map rows naming the given resource GUID. */
    private List<String> mapGUIDsForResource(String resourceGUID) {
        List<String> ret = new ArrayList<>();
        if (SUS.isEmpty(resourceGUID)) {
            return ret;
        }
        List<ResourceMap> maps = ds().search(ResourceMap.NVC_RESOURCE_MAP, null,
                eq(MetaToken.RESOURCE_GUID, resourceGUID));
        for (ResourceMap m : maps) {
            if (!SUS.isEmpty(m.getGUID())) {
                ret.add(m.getGUID());
            }
        }
        return ret;
    }

    /** The reserved {@code super_admin} role may only be granted to the super-admin subject. */
    @Override
    public RoleGrant addRoleGrant(SubjectIdentifier subject, RoleInfo roleInfo) {
        return addRoleGrant(subject, roleInfo, (AppIDDefault) null);
    }

    /**
     * Role grant scoped to an app, the unit of "assigning a subject to an app": the role's name
     * and permissions apply only to a login made with that domain and app. Under enforcement the
     * caller must hold {@code permission:assign:role} in a login whose scope is global or that same
     * app (an app login never grants globally or into another app); the caller is recorded as
     * {@code broker_guid} and may later revoke what it granted. {@code super_admin} is never scoped.
     * Passing a null app is the global grant.
     */
    public RoleGrant addRoleGrant(SubjectIdentifier subject, RoleInfo roleInfo, AppIDDefault app) {
        SUS.checkIfNulls("subject and role can't be null", subject, roleInfo);
        if (app != null) {
            requireApp(app);
        }
        enforceScoped(app, SecurityModel.PERM_ASSIGN_ROLE);
        RoleInfo stored = SUS.isEmpty(roleInfo.getGUID()) ? null : lookupRoleByGUID(roleInfo.getGUID());
        if (isReservedRole(stored != null ? stored : roleInfo)) {
            if (app != null) {
                throw new AccessSecurityException("The " + SecurityModel.Role.SUPER_ADMIN.getName() + " role is never app-scoped", Reason.UNAUTHORIZED);
            }
            enforceReservedGrantee(subject.getGUID(), "the " + SecurityModel.Role.SUPER_ADMIN.getName() + " role");
        }
        RoleGrant grant = new RoleGrant(roleInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        grant.setAppIdDAO(app);
        grant.setBrokerGUID(currentSubjectGUID());
        RoleGrant ret = ds().insert(grant);
        realm.evictAuthorization(subject.getGUID());
        return ret;
    }

    /**
     * Revokes a role grant. The row is reloaded by GUID; under enforcement the caller must be its
     * {@code broker_guid}, or hold {@code permission:remove:role} in a login whose scope may act on
     * the grant's scope (see {@link #scopeAllows}).
     */
    @Override
    public boolean deleteRoleGrant(RoleGrant roleGrant) {
        if (roleGrant == null || SUS.isEmpty(roleGrant.getGUID())) {
            return false;
        }
        RoleGrant stored = first(ds().searchByID(RoleGrant.NVC_ROLE_GRANT, roleGrant.getGUID()));
        if (stored == null) {
            return false;
        }
        enforceRevokeRole(stored);
        boolean ret = ds().delete(stored, false);
        realm.evictAuthorization(stored.getSubjectGUID());
        return ret;
    }

    /** The subject's role grants scoped to {@code app} (never the global ones). */
    public RoleGrant[] getRoleGrants(String subjectGUID, AppIDDefault app) {
        List<RoleGrant> ret = new ArrayList<>();
        for (RoleGrant g : getRoleGrants(subjectGUID)) {
            if (sameApp(g, app)) {
                ret.add(g);
            }
        }
        return ret.toArray(new RoleGrant[0]);
    }

    /**
     * Removes the subject from an app: deletes every role, role-group and permission grant of the
     * subject scoped to {@code app}, each under the revoke rules of its kind, in one transaction.
     *
     * @return the number of grants deleted
     */
    public int revokeAppGrants(String subjectGUID, AppIDDefault app) {
        SUS.checkIfNulls("subject GUID and app can't be null", subjectGUID, app);
        requireApp(app);
        List<RoleGrant> roles = new ArrayList<>();
        for (RoleGrant g : getRoleGrants(subjectGUID)) {
            if (sameApp(g, app)) roles.add(g);
        }
        List<RoleGroupGrant> groups = new ArrayList<>();
        for (RoleGroupGrant g : getRoleGroupGrants(subjectGUID)) {
            if (sameApp(g, app)) groups.add(g);
        }
        List<PermissionGrant> permissions = new ArrayList<>();
        for (PermissionGrant g : getPermissionGrants(subjectGUID)) {
            if (sameApp(g, app)) permissions.add(g);
        }
        for (RoleGrant g : roles) enforceRevokeRole(g);
        for (RoleGroupGrant g : groups) enforceRevokeRole(g);
        for (PermissionGrant g : permissions) enforceRevoke(g);
        int count = inTransaction(() -> {
            int n = 0;
            for (RoleGrant g : roles) if (ds().delete(g, false)) n++;
            for (RoleGroupGrant g : groups) if (ds().delete(g, false)) n++;
            for (PermissionGrant g : permissions) if (deleteGrantRows(g)) n++;
            return n;
        });
        realm.evictAuthorization(subjectGUID);
        return count;
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

    /** A group that (illegitimately) embeds the reserved role may only be granted to the super-admin subject. */
    @Override
    public RoleGroupGrant addRoleGroupGrant(SubjectIdentifier subject, RoleGroupInfo roleGroupInfo) {
        return addRoleGroupGrant(subject, roleGroupInfo, (AppIDDefault) null);
    }

    /** Role-group grant scoped to an app; same rules as {@link #addRoleGrant(SubjectIdentifier, RoleInfo, AppIDDefault)}. */
    public RoleGroupGrant addRoleGroupGrant(SubjectIdentifier subject, RoleGroupInfo roleGroupInfo, AppIDDefault app) {
        SUS.checkIfNulls("subject and role group can't be null", subject, roleGroupInfo);
        if (app != null) {
            requireApp(app);
        }
        enforceScoped(app, SecurityModel.PERM_ASSIGN_ROLE);
        RoleGroupInfo stored = SUS.isEmpty(roleGroupInfo.getGUID()) ? null : lookupRoleGroupByGUID(roleGroupInfo.getGUID());
        if (groupHasReservedRole(stored != null ? stored : roleGroupInfo)) {
            if (app != null) {
                throw new AccessSecurityException("A role group containing " + SecurityModel.Role.SUPER_ADMIN.getName() + " is never app-scoped", Reason.UNAUTHORIZED);
            }
            enforceReservedGrantee(subject.getGUID(), "a role group containing " + SecurityModel.Role.SUPER_ADMIN.getName());
        }
        RoleGroupGrant grant = new RoleGroupGrant(roleGroupInfo.getGUID());
        grant.setSubjectGUID(subject.getGUID());
        grant.setAppIdDAO(app);
        grant.setBrokerGUID(currentSubjectGUID());
        RoleGroupGrant ret = ds().insert(grant);
        realm.evictAuthorization(subject.getGUID());
        return ret;
    }

    /** Revokes a role-group grant; reload and revoke rules as in {@link #deleteRoleGrant}. */
    @Override
    public boolean deleteRoleGroupGrant(RoleGroupGrant roleGroupGrant) {
        if (roleGroupGrant == null || SUS.isEmpty(roleGroupGrant.getGUID())) {
            return false;
        }
        RoleGroupGrant stored = first(ds().searchByID(RoleGroupGrant.NVC_ROLE_GROUP_GRANT, roleGroupGrant.getGUID()));
        if (stored == null) {
            return false;
        }
        enforceRevokeRole(stored);
        boolean ret = ds().delete(stored, false);
        realm.evictAuthorization(stored.getSubjectGUID());
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
