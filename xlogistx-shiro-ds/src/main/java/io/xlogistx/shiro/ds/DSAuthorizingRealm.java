package io.xlogistx.shiro.ds;

import io.xlogistx.shiro.DomainPrincipalCollection;
import org.zoxweb.shared.app.AppIDDefault;
import io.xlogistx.shiro.authc.*;
import org.apache.shiro.authc.*;
import org.apache.shiro.authc.pam.UnsupportedTokenException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.AuthorizationInfoLookup;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.SUS;

import java.util.UUID;

/**
 * Shiro realm backed by a {@link ShiroDSDomainSecurityManager}. Authentication resolves the
 * principal, its subject and the stored credential through the manager and applies the status
 * rules; the matcher ({@link CredentialsInfoMatcher}) does the actual comparison. Authorization
 * flattens the subject's grants with {@link GrantFlattener}. Both caches are keyed by subject GUID
 * so the manager can evict precisely after a mutation.
 * <p>
 * Status rules ({@code null} counts as ACTIVE because older rows were never stamped):
 * subject must be ACTIVE; principal and credential must be ACTIVE or unset; an API key must also
 * be unexpired. Each failure raises the specific Shiro exception so a servlet layer can act on it,
 * while {@link ShiroDSDomainSecurityManager#login} collapses all of them to one generic message.
 */
public class DSAuthorizingRealm
        extends AuthorizingRealm
        implements AuthorizationInfoLookup<AuthorizationInfo, PrincipalCollection> {

    public static final LogWrapper log = new LogWrapper(DSAuthorizingRealm.class).setEnabled(false);

    private volatile ShiroDSDomainSecurityManager dsm;
    private volatile String dataStoreResource = ResourceManager.Resource.DATA_STORE.getName();
    private volatile boolean eagerAuthorization = false;
    private volatile String superAdminPrincipalID = ShiroDSDomainSecurityManager.DEFAULT_SUPER_ADMIN_PRINCIPAL_ID;

    /**
     * INI / bean constructor. Defaults: name {@link ShiroDSDomainSecurityManager#REALM_NAME},
     * {@link CredentialsInfoMatcher}, authentication caching off, authorization caching on. The
     * manager is resolved on first use: either {@link #setDomainSecurityManager} was called, or
     * an {@link APIDataStore} is registered in {@link ResourceManager} under
     * {@link #setDataStoreResource} (default {@code ResourceManager.Resource.DATA_STORE}) and a
     * manager is attached to this realm from it.
     */
    public DSAuthorizingRealm() {
        setName(ShiroDSDomainSecurityManager.REALM_NAME);
        setCredentialsMatcher(new CredentialsInfoMatcher());
        setAuthenticationCachingEnabled(false); // credentials are re-read on every login
        setAuthorizationCachingEnabled(true);
    }

    public DSAuthorizingRealm(ShiroDSDomainSecurityManager dsm) {
        this();
        SUS.checkIfNulls("domain security manager can't be null", dsm);
        this.dsm = dsm;
    }

    /** The bound manager, resolving it from {@link ResourceManager} on first call if needed. */
    public ShiroDSDomainSecurityManager getDomainSecurityManager() {
        return dsm();
    }

    /** Bind (or rebind) the manager this realm reads through. */
    public void setDomainSecurityManager(ShiroDSDomainSecurityManager dsm) {
        this.dsm = dsm;
    }

    public boolean isBound() {
        return dsm != null;
    }

    public boolean isEagerAuthorization() {
        return eagerAuthorization;
    }

    /**
     * When on, a successful authentication also loads the subject's roles and permissions from
     * the store and puts them in the authorization cache, so the first {@code isPermitted} /
     * {@code hasRole} after login is served from cache. Off by default (Shiro's lazy behaviour).
     * INI: {@code dsRealm.eagerAuthorization = true}. Needs a cache manager to be useful.
     */
    public void setEagerAuthorization(boolean eagerAuthorization) {
        this.eagerAuthorization = eagerAuthorization;
    }

    /** Normalized principal ID of the one account allowed to hold the wildcard permission. */
    public String getSuperAdminPrincipalID() {
        return superAdminPrincipalID;
    }

    /**
     * Names the super-admin account. INI: {@code dsRealm.superAdminPrincipalID = admin@example.com}.
     * The value is normalized through {@link SecConst.SubjectIDFilter}; every cached authorization is
     * evicted so a former super-admin loses the wildcard on its next authorization load.
     *
     * @throws IllegalArgumentException if the value is not a valid principal ID
     */
    public void setSuperAdminPrincipalID(String principalID) {
        try {
            this.superAdminPrincipalID = SecConst.SubjectIDFilter.SINGLETON.validate(principalID);
        } catch (NullPointerException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid super-admin principal ID: " + e.getMessage(), e);
        }
        evictAllAuthorization();
    }

    public String getDataStoreResource() {
        return dataStoreResource;
    }

    /** {@link ResourceManager} key of the {@link APIDataStore} to attach to when no manager was set. */
    public void setDataStoreResource(String dataStoreResource) {
        this.dataStoreResource = dataStoreResource;
    }

    private ShiroDSDomainSecurityManager dsm() {
        ShiroDSDomainSecurityManager ret = dsm;
        if (ret == null) {
            synchronized (this) {
                ret = dsm;
                if (ret == null) {
                    Object ds = SUS.isEmpty(dataStoreResource) ? null : ResourceManager.lookupResource(dataStoreResource);
                    if (!(ds instanceof APIDataStore)) {
                        throw new IllegalStateException("Realm '" + getName() + "' has no domain security manager: call "
                                + "setDomainSecurityManager, or register an APIDataStore in ResourceManager under '"
                                + dataStoreResource + "' (found " + (ds == null ? "nothing" : ds.getClass().getName()) + ")");
                    }
                    ret = ShiroDSDomainSecurityManager.attach(this, (APIDataStore<?, ?>) ds); // sets dsm
                    if (log.isEnabled()) log.getLogger().info("realm '" + getName() + "' attached to data store resource '" + dataStoreResource + "'");
                }
            }
        }
        return ret;
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof DomainUsernamePasswordToken
                || token instanceof APIKeyAuthenticationToken
                || token instanceof JWTAuthenticationToken;
    }

    // ------------------------------------------------------------------
    // authentication
    // ------------------------------------------------------------------

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token)
            throws AuthenticationException {
        if (token instanceof DomainUsernamePasswordToken) {
            return passwordInfo((DomainUsernamePasswordToken) token);
        }
        if (token instanceof APIKeyAuthenticationToken) {
            return apiKeyInfo((APIKeyAuthenticationToken) token);
        }
        if (token instanceof JWTAuthenticationToken) {
            return jwtInfo((JWTAuthenticationToken) token);
        }
        throw new UnsupportedTokenException("Unsupported token " + (token != null ? token.getClass().getName() : null));
    }

    private AuthenticationInfo passwordInfo(DomainUsernamePasswordToken token) {
        String username = token.getUsername();
        if (SUS.isEmpty(username)) {
            throw new AccountException("Null usernames are not allowed by this realm");
        }
        ShiroDSDomainSecurityManager dsm = dsm();
        PrincipalIdentifier principal = dsm.lookupPrincipalID(username);
        if (principal == null) {
            throw new UnknownAccountException("Unknown principal");
        }
        if (!isActiveOrUnset(principal.getStatus())) {
            throw new LockedAccountException("Principal is not active");
        }
        SubjectIdentifier subject = activeSubject(principal.getSubjectGUID());

        CIPassword password = null;
        for (CredentialInfo ci : dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), CredentialInfo.Type.PASSWORD)) {
            if (ci instanceof CIPassword) {
                password = (CIPassword) ci;
                break;
            }
        }
        if (password == null) {
            throw new UnknownAccountException("No password credential");
        }
        if (!isActiveOrUnset(password.getCredentialStatus())) {
            throw new ExpiredCredentialsException("Password credential is not active");
        }

        token.setSubjectGUID(subject.getGUID());
        return new DomainAuthenticationInfo(username, subject.getGUID(), password, getName(),
                token.getDomainID(), token.getAppID(), null);
    }

    private AuthenticationInfo apiKeyInfo(APIKeyAuthenticationToken token) {
        String key = token.getAPIKey();
        if (SUS.isEmpty(key)) {
            throw new AccountException("Empty API key");
        }
        ShiroDSDomainSecurityManager dsm = dsm();
        SubjectAPIKey sak = dsm.lookupSubjectAPIKey(key);
        if (sak == null) {
            throw new UnknownAccountException("Unknown API key");
        }
        checkUsable(sak);
        SubjectIdentifier subject = activeSubject(sak.getSubjectGUID());

        String domainID = token.getDomainID();
        String appID = token.getAppID();
        if (sak.getAppID() != null) {
            if (domainID == null) domainID = sak.getAppID().getDomainID();
            if (appID == null) appID = sak.getAppID().getAppID();
        }
        token.setSubjectGUID(subject.getGUID());
        return new DomainAuthenticationInfo(subject.getGUID(), subject.getGUID(), sak, getName(),
                domainID, appID, sak.getGUID());
    }

    /**
     * JWT bearer token: the {@code sub} claim is the key ID ({@link SubjectAPIKey#getSubjectID()})
     * of the {@link SubjectAPIKey} whose secret signed the token. The key's domain / app scope,
     * when set, is what the principal collection carries, so the matcher can hold the claims to it.
     */
    private AuthenticationInfo jwtInfo(JWTAuthenticationToken token) {
        String keyID = token.getJWTSubjectID();
        if (SUS.isEmpty(keyID)) {
            throw new AccountException("JWT has no subject claim");
        }
        ShiroDSDomainSecurityManager dsm = dsm();
        SubjectAPIKey sak = dsm.lookupSubjectAPIKeyByID(keyID);
        if (sak == null) {
            throw new UnknownAccountException("Unknown API key ID");
        }
        checkUsable(sak);
        SubjectIdentifier subject = activeSubject(sak.getSubjectGUID());

        String domainID = null;
        String appID = null;
        if (sak.getAppID() != null) {
            domainID = sak.getAppID().getDomainID();
            appID = sak.getAppID().getAppID();
        }
        if (domainID == null) domainID = token.getDomainID();
        if (appID == null) appID = token.getAppID();
        token.setSubjectID(subject.getGUID());
        return new DomainAuthenticationInfo(subject.getGUID(), subject.getGUID(), sak, getName(),
                domainID, appID, keyID);
    }

    /** Lifecycle / credential status and expiry of an API key, as specific Shiro exceptions. */
    private static void checkUsable(SubjectAPIKey sak) {
        if (!CredentialsInfoMatcher.isUsable(sak)) {
            long expiry = CredentialsInfoMatcher.expiryOf(sak);
            if (expiry != 0 && System.currentTimeMillis() > expiry) {
                throw new ExpiredCredentialsException("API key expired");
            }
            throw new DisabledAccountException("API key is not active");
        }
    }

    /**
     * Subject by GUID; unknown or non-ACTIVE subjects are rejected. A subject locked in
     * {@code PENDING_RESET_PASSWORD} whose reset token has expired is restored to ACTIVE here, so an
     * unclaimed reset request locks an account only for the token's lifetime.
     */
    private SubjectIdentifier activeSubject(String subjectGUID) {
        SubjectIdentifier subject = SUS.isEmpty(subjectGUID) ? null : dsm().lookupSubjectByGUID(subjectGUID);
        if (subject == null) {
            throw new UnknownAccountException("Unknown subject");
        }
        if (subject.getSubjectStatus() == SecConst.SecStatus.PENDING_RESET_PASSWORD
                && !dsm().hasOutstandingResetToken(subjectGUID)) {
            dsm().restoreActiveAfterExpiredReset(subject);
        }
        if (subject.getSubjectStatus() != SecConst.SecStatus.ACTIVE) {
            throw new DisabledAccountException("Subject is not active");
        }
        return subject;
    }

    static boolean isActiveOrUnset(SecConst.SecStatus status) {
        return status == null || status == SecConst.SecStatus.ACTIVE;
    }

    /**
     * After the matcher accepted the credentials, optionally warm the authorization cache. A
     * failure to load grants is logged and swallowed: the login stands and grants load lazily.
     */
    @Override
    protected void assertCredentialsMatch(AuthenticationToken token, AuthenticationInfo info)
            throws AuthenticationException {
        super.assertCredentialsMatch(token, info);
        if (eagerAuthorization && info != null) {
            try {
                getAuthorizationInfo(info.getPrincipals());
            } catch (RuntimeException e) {
                log.getLogger().warning("eager authorization load failed for " + subjectGUIDOf(info.getPrincipals()) + ": " + e);
            }
        }
    }

    // ------------------------------------------------------------------
    // authorization
    // ------------------------------------------------------------------

    /**
     * Roles and permissions of {@code principals}, from cache or freshly flattened from the store
     * (and cached). Public entry point behind {@code ShiroUtil.lookupAuthorizationInfo(DSAuthorizingRealm.class, pc)}.
     */
    @Override
    public AuthorizationInfo lookupAuthorizationInfo(PrincipalCollection principals) {
        return getAuthorizationInfo(principals);
    }

    /**
     * Authorization for the login scope carried by the principals: a login with domain and app
     * loads only the grants scoped to that app, a login without loads only the global grants
     * (the super-admin's global grants apply in either), see {@link GrantFlattener}.
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
        String subjectGUID = subjectGUIDOf(principals);
        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo();
        if (subjectGUID == null) {
            return info;
        }
        AppIDDefault scope = loginScopeOf(principals);
        GrantFlattener.Result flat = GrantFlattener.flatten(dsm(), subjectGUID, scope);
        info.setRoles(flat.roles);
        info.setStringPermissions(flat.permissions);
        if (log.isEnabled()) log.getLogger().info("authz " + subjectGUID + " scope=" + ShiroDSDomainSecurityManager.appScope(scope)
                + " roles=" + flat.roles + " perms=" + flat.permissions);
        return info;
    }

    /** Cache key: the subject GUID for a global login, {@code <guid>|<domain-app>} for an app login. */
    @Override
    protected Object getAuthorizationCacheKey(PrincipalCollection principals) {
        String subjectGUID = subjectGUIDOf(principals);
        if (subjectGUID == null) {
            return super.getAuthorizationCacheKey(principals);
        }
        AppIDDefault scope = loginScopeOf(principals);
        return scope == null ? subjectGUID : subjectGUID + SCOPE_SEP + ShiroDSDomainSecurityManager.appScope(scope);
    }

    private static final String SCOPE_SEP = "|";

    /** Drop the cached authorization info of one subject in every login scope (no-op when caching is off). */
    public void evictAuthorization(String subjectGUID) {
        Cache<Object, AuthorizationInfo> cache = getAuthorizationCache();
        if (cache == null || subjectGUID == null) {
            return;
        }
        cache.remove(subjectGUID);
        String prefix = subjectGUID + SCOPE_SEP;
        java.util.Set<Object> keys = cache.keys();
        if (keys != null) {
            for (Object key : new java.util.ArrayList<>(keys)) {
                if (key instanceof String && ((String) key).startsWith(prefix)) {
                    cache.remove(key);
                }
            }
        }
    }

    /**
     * The login scope of a principal collection produced by this realm: the
     * {@link DomainPrincipalCollection} domain and app when both are set, else null (global).
     * A scope whose domain or app fails validation counts as global, with a warning.
     */
    public static AppIDDefault loginScopeOf(PrincipalCollection principals) {
        if (!(principals instanceof DomainPrincipalCollection)) {
            return null;
        }
        DomainPrincipalCollection dpc = (DomainPrincipalCollection) principals;
        if (SUS.isEmpty(dpc.getDomainID()) || SUS.isEmpty(dpc.getAppID())) {
            return null;
        }
        try {
            return new AppIDDefault(dpc.getDomainID(), dpc.getAppID());
        } catch (RuntimeException e) {
            log.getLogger().warning("invalid login scope " + dpc.getDomainID() + "/" + dpc.getAppID() + " treated as global: " + e);
            return null;
        }
    }

    /** Drop every cached authorization info, after a catalog change that may affect many subjects. */
    public void evictAllAuthorization() {
        Cache<Object, AuthorizationInfo> cache = getAuthorizationCache();
        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * Subject GUID carried by a principal collection produced by this realm: the
     * {@link DomainPrincipalCollection} subject GID, else any {@link UUID} principal.
     */
    public static String subjectGUIDOf(PrincipalCollection principals) {
        if (principals == null) {
            return null;
        }
        if (principals instanceof DomainPrincipalCollection) {
            String gid = ((DomainPrincipalCollection) principals).getSubjectGID();
            if (!SUS.isEmpty(gid)) {
                return gid;
            }
        }
        for (Object p : principals.asList()) {
            if (p instanceof UUID) {
                return p.toString();
            }
        }
        return null;
    }
}
