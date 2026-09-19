package io.xlogistx.shiro.ds.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.shiro.DomainPrincipalCollection;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.APIKeyAuthenticationToken;
import io.xlogistx.shiro.authc.CredentialsInfoMatcher;
import io.xlogistx.shiro.authc.JWTAuthenticationToken;

import io.xlogistx.shiro.ds.DSAuthorizingRealm;
import io.xlogistx.shiro.ds.GrantFlattener;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.security.JWTProvider;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.server.util.cache.JWTTokenCache;
import org.zoxweb.server.task.TaskUtil;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.ResourceManager;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Date;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link ShiroDSDomainSecurityManager} on a real {@link H2PDataStore}.
 * <p>
 * Mirrors {@code H2PDomainSecurityManagerDBTest} (persistence round trips, rollback) and adds
 * the behaviour this implementation introduces: status gating, credential ownership, loud failure
 * on unsupported types, transaction joining, the last-principal lock, API-key login and expiry,
 * and Shiro authorization ({@code isPermitted} / {@code hasRole}) with cache eviction on revoke.
 * <p>
 * Runs on in-memory H2 unless {@code -Dds.url} names another target: an H2 file URL (optionally
 * with {@code ;CIPHER=AES} plus {@code -Dds.file_password}) or a PostgreSQL base endpoint
 * ({@code jdbc:postgresql://host:5432}, target db from {@code -Dds.db}, default
 * {@code testpostgres}, created if missing). {@code -Dds.user} / {@code -Dds.password} apply to
 * both. Names are UUID-suffixed so reruns against a persistent database never collide; nothing is
 * deleted except by the tests that verify deletion.
 */
public class ShiroDSDomainSecurityManagerDBTest {

    private static final String PASSWORD = "Secret123!";
    private static final String NEW_PASSWORD = "N3wSecret456$";
    private static final String DEFAULT_URL = "jdbc:h2:mem:shirods;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    private static ShiroDSDomainSecurityManager dsm;
    private static H2PDataStore ds;

    @BeforeAll
    public static void setup() throws Exception {
        String url = System.getProperty("ds.url", DEFAULT_URL);
        String user = System.getProperty("ds.user");
        String password = System.getProperty("ds.password");
        String filePassword = System.getProperty("ds.file_password");

        NVGenericMap parsed = H2PUtil.parseJdbcURL(url);
        String subprotocol = parsed.getValue(H2PUtil.JDBC_SUBPROTOCOL);
        APIConfigInfo cfg;
        if ("postgresql".equals(subprotocol)) {
            Class.forName("org.postgresql.Driver");
            String host = parsed.getValue(H2PUtil.JDBC_HOST);
            Object port = parsed.getValue(H2PUtil.JDBC_PORT);
            String base = "jdbc:postgresql://" + host + (port != null ? ":" + port : "");
            String targetDb = firstNonEmpty(parsed.getValue(H2PUtil.JDBC_DATABASE), System.getProperty("ds.db"), "testpostgres");
            ensureDatabase(base + "/postgres", user, password, targetDb);
            String targetUrl = base + "/" + targetDb;
            cfg = H2PDSCreator.toAPIConfigInfo(targetUrl, user, password);
            System.out.println("Live PostgreSQL target: " + targetUrl);
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password, filePassword);
            System.out.println("H2 target: " + url);
        }
        ds = new H2PDSCreator().createAPI(null, cfg);

        OPSecUtil.singleton();
        dsm = new ShiroDSDomainSecurityManager(ds);
    }

    @AfterEach
    public void cleanupThread() {
        dsm.logout();
        if (ds.isTransactionActive()) {
            ds.abortTransaction();
        }
    }

    private static String firstNonEmpty(String... values) {
        for (String v : values) {
            if (v != null && !v.isEmpty()) return v;
        }
        return null;
    }

    private static void ensureDatabase(String maintenanceUrl, String user, String password, String db) throws SQLException {
        try (Connection c = DriverManager.getConnection(maintenanceUrl, user, password)) {
            boolean exists;
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?")) {
                ps.setString(1, db);
                try (ResultSet rs = ps.executeQuery()) {
                    exists = rs.next();
                }
            }
            if (!exists) {
                try (Statement s = c.createStatement()) {
                    s.execute("CREATE DATABASE \"" + db + "\"");
                }
            }
        }
    }

    private static String uniquePrincipal() {
        return "dsm-" + UUID.randomUUID() + "@example.com";
    }

    private static SubjectIdentifier newSubject(String principal) {
        return dsm.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));
    }

    private static SubjectAPIKey newAPIKey(SubjectIdentifier subject, String key) {
        SubjectAPIKey sak = new SubjectAPIKey();
        sak.setName("key-" + UUID.randomUUID());
        sak.setSystemID("shiro-ds-test");
        sak.setAPIKey(key);
        sak.setStatus(Const.Status.ACTIVE);
        dsm.createCredential(subject, sak);
        return sak;
    }

    /** API key with a random 32-byte secret and an explicit key ID, optionally scoped to domain/app. */
    private static SubjectAPIKey newSigningKey(SubjectIdentifier subject, String domainID, String appID) {
        SubjectAPIKey sak = new SubjectAPIKey();
        sak.setName("jwt-key-" + UUID.randomUUID());
        sak.setSystemID("shiro-ds-test");
        sak.setPrincipalID("kid-" + UUID.randomUUID());
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        sak.setAPIKeyAsBytes(secret);
        sak.setStatus(Const.Status.ACTIVE);
        if (domainID != null) {
            sak.setAppID(new AppIDDefault(domainID, appID));
        }
        dsm.createCredential(subject, sak);
        return sak;
    }

    /** Token with the given claims, signed with {@code sak}'s secret (bypasses mintJWT to forge variants). */
    private static String signedJWT(SubjectAPIKey sak, String keyID, String domainID, String appID) {
        return JWT.createJWT(CryptoConst.JWTAlgo.HS256, keyID, domainID, appID)
                .hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON);
    }

    // ------------------------------------------------------------------
    // ported persistence scenarios
    // ------------------------------------------------------------------

    @Test
    public void dataStoreIsConfigured() {
        assertSame(ds, dsm.getDataStore());
        assertNotNull(dsm.getShiroSecurityManager());
        assertSame(dsm.getRealm(), dsm.getShiroSecurityManager().getRealms().iterator().next());
    }

    @Test
    public void createSubject_assignsGUID_andIsLookupable() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        assertNotNull(subject.getGUID());
        assertEquals(SecConst.SecStatus.ACTIVE, subject.getSubjectStatus());

        SubjectIdentifier looked = dsm.lookupSubjectID(principal);
        assertNotNull(looked);
        assertEquals(subject.getGUID(), looked.getGUID());

        // principal and credential were stamped ACTIVE on create
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupPrincipalID(principal).getStatus());
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD).getCredentialStatus());
    }

    @Test
    public void createSubject_duplicatePrincipal_isRejected() {
        String principal = uniquePrincipal();
        newSubject(principal);
        AccessSecurityException e = assertThrows(AccessSecurityException.class, () -> newSubject(principal));
        assertTrue(e.getMessage().contains("already exists"), e.getMessage());
    }

    @Test
    public void login_succeedsWithCorrectPassword_failsOtherwise() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);

        assertEquals(subject.getGUID(), dsm.login(principal, PASSWORD).getGUID());
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, "wrong-password"));
        assertThrows(AccessSecurityException.class, () -> dsm.login(uniquePrincipal(), PASSWORD));
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, null));
    }

    @Test
    public void login_principalIsCaseInsensitive() {
        String principal = "Mixed-" + UUID.randomUUID() + "@Example.COM";
        SubjectIdentifier subject = newSubject(principal);
        assertEquals(subject.getGUID(), dsm.login(principal.toLowerCase(), PASSWORD).getGUID());
        assertEquals(subject.getGUID(), dsm.login(principal.toUpperCase(), PASSWORD).getGUID());
        assertNotNull(dsm.lookupPrincipalID(principal.toUpperCase()));
    }

    @Test
    public void principal_isNormalizedBySubjectIDFilter() {
        // surrounding whitespace and case are normalized on write and on every lookup
        String raw = "  Padded-" + UUID.randomUUID() + "@Example.com \t";
        String normalized = SecConst.SubjectIDFilter.SINGLETON.validate(raw);
        SubjectIdentifier subject = newSubject(raw);
        assertEquals(normalized, dsm.lookupPrincipalID(raw).getPrincipalID());
        assertEquals(subject.getGUID(), dsm.login(raw, PASSWORD).getGUID());
        assertEquals(subject.getGUID(), dsm.login(normalized, PASSWORD).getGUID());

        // a non-email handle must meet the minimum length; an invisible character is rejected
        String reason = assertThrows(AccessSecurityException.class, () -> newSubject("short1")).getMessage();
        assertTrue(reason.startsWith("Invalid principal ID"), reason);
        assertThrows(AccessSecurityException.class, () -> newSubject("ma​rio-" + UUID.randomUUID()));
        assertThrows(AccessSecurityException.class, () -> dsm.addPrincipalID(subject, "   "));

        // a rejected identifier can match nothing: lookups return null and login fails, never throw
        assertNull(dsm.lookupPrincipalID("short1"));
        assertNull(dsm.lookupSubjectID("short1"));
        assertEquals(0, dsm.lookupAllPrincipalCredentials("short1").length);
        assertThrows(AccessSecurityException.class, () -> dsm.login("short1", PASSWORD));

        // a plain handle of valid length works end to end
        String handle = "handle" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        SubjectIdentifier byHandle = newSubject(handle.toUpperCase());
        assertEquals(byHandle.getGUID(), dsm.login(handle, PASSWORD).getGUID());
    }

    @Test
    public void lookupCredential_returnsStoredPassword() {
        String principal = uniquePrincipal();
        newSubject(principal);
        CredentialInfo ci = dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD);
        assertInstanceOf(CIPassword.class, ci);
        assertEquals(1, dsm.lookupAllPrincipalCredentials(principal).length);
    }

    @Test
    public void updatePassword_inPlace_changesLoginCredential() throws NoSuchAlgorithmException {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        CIPassword current = (CIPassword) dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD);

        CredentialHasher<CIPassword> hasher = SecUtil.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        CIPassword updated = hasher.update(current, PASSWORD, NEW_PASSWORD);
        dsm.updateCredential(subject, updated);

        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));
        assertEquals(subject.getGUID(), dsm.login(principal, NEW_PASSWORD).getGUID());
        assertEquals(1, dsm.lookupAllPrincipalCredentials(principal).length);
        assertEquals(current.getGUID(), ((CIPassword) dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD)).getGUID());
    }

    @Test
    public void updatePassword_replacement_removesEveryOldPasswordRow() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        // a second, stray password row
        dsm.createCredential(subject, HashUtil.toBCryptPassword("Stray-Pass1!"));
        assertEquals(2, dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), CredentialInfo.Type.PASSWORD).length);

        dsm.updateCredential(subject, HashUtil.toBCryptPassword(NEW_PASSWORD)); // no GUID -> replacement path
        assertEquals(1, dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), CredentialInfo.Type.PASSWORD).length);
        assertEquals(subject.getGUID(), dsm.login(principal, NEW_PASSWORD).getGUID());
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));
    }

    @Test
    public void permissionGrant_roundTripsThroughDataStore() {
        String principal = uniquePrincipal();
        String permName = "perm.read." + UUID.randomUUID();
        SubjectIdentifier subject = newSubject(principal);
        PermissionInfo perm = dsm.createPermission(new PermissionInfo(permName, "system:read"));
        assertNotNull(perm.getGUID());
        assertEquals(perm.getGUID(), dsm.lookupPermission(null, permName).getGUID());

        PermissionGrant grant = dsm.addPermissionGrant(subject, perm);
        PermissionGrant[] grants = dsm.getPermissionGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
    }

    @Test
    public void roleGrant_roundTripsThroughDataStore() {
        String principal = uniquePrincipal();
        String roleName = "role.admin." + UUID.randomUUID();
        SubjectIdentifier subject = newSubject(principal);
        RoleInfo role = new RoleInfo();
        role.setName(roleName);
        role = dsm.createRole(role);
        assertEquals(role.getGUID(), dsm.lookupRole(null, roleName).getGUID());

        RoleGrant grant = dsm.addRoleGrant(subject, role);
        RoleGrant[] grants = dsm.getRoleGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
    }

    @Test
    public void roleGroupGrant_roundTripsThroughDataStore() {
        String suffix = UUID.randomUUID().toString();
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        RoleInfo roleA = new RoleInfo();
        roleA.setName("role.a." + suffix);
        roleA = dsm.createRole(roleA);
        RoleInfo roleB = new RoleInfo();
        roleB.setName("role.b." + suffix);
        roleB = dsm.createRole(roleB);

        RoleGroupInfo group = new RoleGroupInfo(roleA, roleB);
        group.setName("rolegroup." + suffix);
        group = dsm.createRoleGroup(group);

        RoleGroupInfo looked = dsm.lookupRoleGroup(null, group.getName());
        assertEquals(2, looked.getRoles().length);

        RoleGroupGrant grant = dsm.addRoleGroupGrant(subject, group);
        RoleGroupGrant[] grants = dsm.getRoleGroupGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
    }

    @Test
    public void createSubject_partialFailure_rollsBackAtomically() {
        String principal = uniquePrincipal();
        CredentialInfo badCredential = new CredentialInfo() {
            public Type getCredentialType() { return Type.PASSWORD; }
            public SecConst.SecStatus getCredentialStatus() { return SecConst.SecStatus.ACTIVE; }
            public void setCredentialStatus(SecConst.SecStatus status) { }
            public NVGenericMap getProperties() { return new NVGenericMap(); }
        };
        assertThrows(AccessSecurityException.class, () -> dsm.createSubjectID(principal, badCredential));
        assertNull(dsm.lookupSubjectID(principal));
        assertNull(dsm.lookupPrincipalID(principal));
        assertEquals(0, dsm.lookupAllPrincipalCredentials(principal).length);
    }

    // ------------------------------------------------------------------
    // new behaviour
    // ------------------------------------------------------------------

    @Test
    public void login_rejectsSubjectThatIsNotActive() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);

        subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateSubjectID(subject);
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD), "deactivated subject must not log in");

        subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
        dsm.updateSubjectID(subject);
        assertEquals(subject.getGUID(), dsm.login(principal, PASSWORD).getGUID());
    }

    @Test
    public void login_rejectsCredentialThatIsNotActive() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        CIPassword pw = (CIPassword) dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD);

        pw.setCredentialStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateCredential(subject, pw);
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));

        pw.setCredentialStatus(SecConst.SecStatus.ACTIVE);
        dsm.updateCredential(subject, pw);
        assertEquals(subject.getGUID(), dsm.login(principal, PASSWORD).getGUID());
    }

    @Test
    public void login_rejectsPrincipalThatIsNotActive() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PrincipalIdentifier pid = dsm.lookupPrincipalID(principal);
        pid.setStatus(SecConst.SecStatus.INACTIVE);
        ds.update(pid);
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));
        pid.setStatus(SecConst.SecStatus.ACTIVE);
        ds.update(pid);
        assertEquals(subject.getGUID(), dsm.login(principal, PASSWORD).getGUID());
    }

    @Test
    public void verifyPassword_allowsPendingReset_whileLoginDeniesIt() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);

        assertTrue(dsm.verifyPassword(principal, PASSWORD));
        assertTrue(dsm.verifyPassword("  " + principal.toUpperCase() + " ", PASSWORD), "normalized like login");
        assertFalse(dsm.verifyPassword(principal, "wrong-" + PASSWORD));
        assertFalse(dsm.verifyPassword(principal, null));
        assertFalse(dsm.verifyPassword(null, PASSWORD));
        assertFalse(dsm.verifyPassword(uniquePrincipal(), PASSWORD), "unknown principal");

        // a pending reset backed by an outstanding token locks login; a bare status flip without a
        // token is lifted by the realm on the next login (bounded lockout), so issue a real one
        dsm.requestPasswordReset(principal);
        subject = dsm.lookupSubjectByGUID(subject.getGUID());
        assertEquals(SecConst.SecStatus.PENDING_RESET_PASSWORD, subject.getSubjectStatus());
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD), "login denies PENDING_RESET_PASSWORD");
        assertTrue(dsm.verifyPassword(principal, PASSWORD), "verifyPassword accepts PENDING_RESET_PASSWORD");
        assertFalse(dsm.verifyPassword(principal, "wrong-" + PASSWORD));
        assertTrue(dsm.cancelPasswordReset(principal));

        subject = dsm.lookupSubjectByGUID(subject.getGUID());
        subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateSubjectID(subject);
        assertFalse(dsm.verifyPassword(principal, PASSWORD), "deactivated subject cannot verify");

        subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
        dsm.updateSubjectID(subject);
        CIPassword pw = (CIPassword) dsm.lookupCredential(principal, CredentialInfo.Type.PASSWORD);
        pw.setCredentialStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateCredential(subject, pw);
        assertFalse(dsm.verifyPassword(principal, PASSWORD), "inactive credential cannot verify");
    }

    @Test
    public void updateCredential_rejectsCredentialOfAnotherSubject() {
        String principalA = uniquePrincipal();
        SubjectIdentifier a = newSubject(principalA);
        SubjectIdentifier b = newSubject(uniquePrincipal());
        CIPassword pwA = (CIPassword) dsm.lookupCredential(principalA, CredentialInfo.Type.PASSWORD);

        // entity says A, caller says B
        assertThrows(AccessSecurityException.class, () -> dsm.updateCredential(b, pwA));

        // entity claims B but the stored row belongs to A
        pwA.setSubjectGUID(b.getGUID());
        assertThrows(AccessSecurityException.class, () -> dsm.updateCredential(b, pwA));

        assertEquals(a.getGUID(), dsm.login(principalA, PASSWORD).getGUID(), "A's password must be untouched");
    }

    @Test
    public void updateCredential_rejectsUnsupportedTypes() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        CredentialInfo notAnEntity = new CredentialInfo() {
            public Type getCredentialType() { return Type.TOKEN; }
            public SecConst.SecStatus getCredentialStatus() { return null; }
            public void setCredentialStatus(SecConst.SecStatus status) { }
            public NVGenericMap getProperties() { return null; }
        };
        assertThrows(IllegalArgumentException.class, () -> dsm.updateCredential(subject, notAnEntity));

        SubjectAPIKey freshKey = new SubjectAPIKey(); // entity, but no GUID and not a password
        freshKey.setAPIKey("k");
        assertThrows(IllegalArgumentException.class, () -> dsm.updateCredential(subject, freshKey));
    }

    @Test
    public void createSubject_joinsCallerTransaction_andRollsBackWithIt() {
        String principal = uniquePrincipal();
        ds.beginTransaction();
        try {
            assertTrue(ds.isTransactionActive());
            SubjectIdentifier subject = dsm.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));
            assertNotNull(subject.getGUID());
            assertTrue(ds.isTransactionActive(), "manager must not end the caller's transaction");
        } finally {
            ds.abortTransaction();
        }
        assertNull(dsm.lookupSubjectID(principal), "caller rollback must discard the subject");
        assertNull(dsm.lookupPrincipalID(principal));
    }

    @Test
    public void deletePrincipal_neverRemovesTheLastOne() {
        String p1 = uniquePrincipal();
        SubjectIdentifier subject = newSubject(p1);
        PrincipalIdentifier first = dsm.lookupPrincipalID(p1);
        assertFalse(dsm.deletePrincipalID(first), "only principal must be kept");

        PrincipalIdentifier second = dsm.addPrincipalID(subject, uniquePrincipal());
        assertTrue(dsm.deletePrincipalID(second));
        assertEquals(1, dsm.lookupAllPrincipalIdentifiers(subject.getGUID()).length);
        assertFalse(dsm.deletePrincipalID(first));
    }

    @Test
    public void deletePrincipal_concurrent_leavesAtLeastOne() throws Exception {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        PrincipalIdentifier p2 = dsm.addPrincipalID(subject, uniquePrincipal());
        PrincipalIdentifier p1 = dsm.lookupAllPrincipalIdentifiers(subject.getGUID())[0];
        if (p1.getGUID().equals(p2.getGUID())) {
            p1 = dsm.lookupAllPrincipalIdentifiers(subject.getGUID())[1];
        }

        int rounds = 8;
        AtomicInteger removed = new AtomicInteger();
        for (int i = 0; i < rounds; i++) {
            // reset to exactly two principals each round
            PrincipalIdentifier[] now = dsm.lookupAllPrincipalIdentifiers(subject.getGUID());
            if (now.length < 2) {
                dsm.addPrincipalID(subject, uniquePrincipal());
            }
            PrincipalIdentifier[] pair = dsm.lookupAllPrincipalIdentifiers(subject.getGUID());
            CountDownLatch go = new CountDownLatch(1);
            Thread[] threads = new Thread[2];
            for (int t = 0; t < 2; t++) {
                PrincipalIdentifier target = pair[t];
                threads[t] = new Thread(() -> {
                    try {
                        go.await();
                        if (dsm.deletePrincipalID(target)) removed.incrementAndGet();
                    } catch (Exception ignore) {
                        // a lock timeout or the explicit guard both mean "not removed"
                    }
                });
                threads[t].start();
            }
            go.countDown();
            for (Thread t : threads) t.join();
            assertTrue(dsm.lookupAllPrincipalIdentifiers(subject.getGUID()).length >= 1,
                    "subject must never be left without a principal");
        }
        assertTrue(removed.get() >= 1, "at least one concurrent delete should have succeeded");
    }

    @Test
    public void deleteSubject_cascadesEverything_includingApiKeys() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String key = "key-" + UUID.randomUUID();
        newAPIKey(subject, key);
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "x:y"));
        dsm.addPermissionGrant(subject, perm);

        assertTrue(dsm.deleteSubjectID(subject));
        assertNull(dsm.lookupSubjectID(principal));
        assertNull(dsm.lookupPrincipalID(principal));
        assertEquals(0, dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), null).length);
        assertNull(dsm.lookupSubjectAPIKey(key));
        assertEquals(0, dsm.getPermissionGrants(subject.getGUID()).length);
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(key));
    }

    @Test
    public void apiKeyLogin_roundTrip_statusAndExpiry() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        String key = "key-" + UUID.randomUUID();
        SubjectAPIKey sak = newAPIKey(subject, key);

        assertEquals(subject.getGUID(), dsm.loginApiKey(key).getGUID());
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey("key-" + UUID.randomUUID()));
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(null));

        sak.setStatus(Const.Status.SUSPENDED);
        dsm.updateCredential(subject, sak);
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(key), "suspended key must not log in");

        sak.setStatus(Const.Status.ACTIVE);
        sak.setExpiryDate(System.currentTimeMillis() - 1000);
        dsm.updateCredential(subject, sak);
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(key), "expired key must not log in");

        sak.setExpiryDate(System.currentTimeMillis() + 60_000);
        dsm.updateCredential(subject, sak);
        assertEquals(subject.getGUID(), dsm.loginApiKey(key).getGUID());
    }

    @Test
    public void jwtLogin_signatureScopeExpiryStatusAndFreshness() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        SubjectAPIKey sak = newSigningKey(subject, "xlogistx.io", "shirods");
        assertEquals(sak.getGUID(), dsm.lookupSubjectAPIKeyByID(sak.getSubjectID()).getGUID(), "key ID must be searchable");

        String token = ShiroDSDomainSecurityManager.mintJWT(sak, CryptoConst.JWTAlgo.HS256, 60_000);
        assertEquals(subject.getGUID(), dsm.loginJWT(token).getGUID());
        assertEquals(subject.getGUID(), dsm.loginJWT(ShiroDSDomainSecurityManager.mintJWT(sak, CryptoConst.JWTAlgo.HS512, 0)).getGUID(), "HS512, no exp");

        // wrong secret, same claims
        SubjectAPIKey forged = SubjectAPIKey.copy(sak);
        byte[] other = new byte[32];
        new SecureRandom().nextBytes(other);
        forged.setAPIKeyAsBytes(other);
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(signedJWT(forged, sak.getSubjectID(), "xlogistx.io", "shirods")), "forged signature");

        // right secret, claims outside the key's scope
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "other.io", "shirods")), "wrong domain");
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "xlogistx.io", "other-app")), "wrong app");
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), null, null)), "scope claims missing");
        assertEquals(subject.getGUID(), dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "XLOGISTX.IO", "SHIRODS")).getGUID(), "scope is case-insensitive");

        // sub names another key ID
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(signedJWT(sak, "kid-" + UUID.randomUUID(), "xlogistx.io", "shirods")), "unknown key ID");

        // expired / not yet valid
        JWT expired = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        expired.getPayload().setExpirationTime(new Date(System.currentTimeMillis() - 10 * 60_000));
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(expired.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "expired");
        JWT future = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        future.getPayload().setNotBefore(new Date(System.currentTimeMillis() + 10 * 60_000));
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(future.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "not yet valid");

        // garbage
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT("not.a.jwt"));
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(""));
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(null));
        String[] parts = token.split("\\.");
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(parts[0] + "." + parts[1] + ".AAAA"), "bad signature segment");

        // key status
        sak.setStatus(Const.Status.SUSPENDED);
        dsm.updateCredential(subject, sak);
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(token), "suspended key");
        sak.setStatus(Const.Status.ACTIVE);
        dsm.updateCredential(subject, sak);
        assertEquals(subject.getGUID(), dsm.loginJWT(token).getGUID());

        // subject status
        subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateSubjectID(subject);
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(token), "deactivated subject");
        subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
        dsm.updateSubjectID(subject);

        // freshness: iat must be inside the window once the key requires timestamps
        sak.setTimeStampRequired(true);
        dsm.updateCredential(subject, sak);
        JWT stale = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        stale.getPayload().setIssuedAt(new Date(System.currentTimeMillis() - 10 * 60_000));
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(stale.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "stale iat");
        String fresh = ShiroDSDomainSecurityManager.mintJWT(sak, null, 0);
        assertEquals(subject.getGUID(), dsm.loginJWT(fresh).getGUID());

        // replay cache: the same fresh token is refused the second time
        dsm.getCredentialsMatcher().setJWTReplayCache(new JWTTokenCache(60_000, TaskUtil.defaultTaskScheduler()));
        try {
            String once = ShiroDSDomainSecurityManager.mintJWT(sak, null, 0);
            assertEquals(subject.getGUID(), dsm.loginJWT(once).getGUID());
            assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(once), "replayed token");
            assertEquals(subject.getGUID(), dsm.loginJWT(ShiroDSDomainSecurityManager.mintJWT(sak, null, 0)).getGUID(), "a new token still works");
        } finally {
            dsm.getCredentialsMatcher().setJWTReplayCache(null);
        }
    }

    @Test
    public void jwtAndApiKey_loginSubject_bindsAndAuthorizes() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        String permToken = "jwt:" + UUID.randomUUID().toString().replace("-", "") + ":read";
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), permToken));
        dsm.addPermissionGrant(subject, perm);

        SubjectAPIKey unscoped = newSigningKey(subject, null, null);
        Subject viaJWT = dsm.loginSubjectJWT(ShiroDSDomainSecurityManager.mintJWT(unscoped, null, 30_000), "10.0.0.1");
        assertTrue(viaJWT.isAuthenticated());
        assertEquals(subject.getGUID(), DSAuthorizingRealm.subjectGUIDOf(viaJWT.getPrincipals()));
        assertEquals(unscoped.getSubjectID(), ((DomainPrincipalCollection) viaJWT.getPrincipals()).getJWSubjectID(), "key ID travels with the principals");
        assertTrue(viaJWT.isPermitted(permToken));
        assertFalse(viaJWT.isPermitted("jwt:other:read"));
        dsm.logout();
        assertNull(org.apache.shiro.util.ThreadContext.getSubject());

        String rawKey = "key-" + UUID.randomUUID();
        newAPIKey(subject, rawKey);
        Subject viaKey = dsm.loginSubjectApiKey(rawKey, null, null);
        assertTrue(viaKey.isAuthenticated());
        assertEquals(subject.getGUID(), DSAuthorizingRealm.subjectGUIDOf(viaKey.getPrincipals()));
        assertTrue(viaKey.isPermitted(permToken));
        dsm.logout();
        assertThrows(AccessSecurityException.class, () -> dsm.loginSubjectApiKey("key-" + UUID.randomUUID(), null, null));
        assertThrows(AccessSecurityException.class, () -> dsm.loginSubjectJWT("nope", null));
        assertNull(org.apache.shiro.util.ThreadContext.getSubject(), "failed logins must not leave a binding");
    }

    @Test
    public void apiKey_withoutKeyID_getsOneOnInsert() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        SubjectAPIKey sak = newAPIKey(subject, "key-" + UUID.randomUUID());
        assertNotNull(sak.getSubjectID(), "insert stamps a key ID");
        assertEquals(sak.getGUID(), dsm.lookupSubjectAPIKeyByID(sak.getSubjectID()).getGUID());
        assertThrows(IllegalArgumentException.class, () -> ShiroDSDomainSecurityManager.mintJWT(new SubjectAPIKey(), null, 0), "no key ID");
    }

    /**
     * The xlogistx-shiro {@code ShiroUtil} entry points go through {@code SecurityUtils}' global
     * security manager. With {@code installAsGlobal()} every one of them must work against this
     * realm: {@code login(domain, realm, user, password)}, {@code login(AuthenticationToken)} for
     * all three token kinds, {@code loginSubject(..., autoLogin)} and {@code loginBySessionID}.
     */
    @Test
    public void shiroUtil_loginEntryPoints_workAgainstThisManager() throws Exception {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String rawKey = "key-" + UUID.randomUUID();
        newAPIKey(subject, rawKey);
        SubjectAPIKey signing = newSigningKey(subject, null, null);

        dsm.installAsGlobal();
        ThreadContext.remove();
        try {
            // login(domain, realm, username, password)
            assertTrue(ShiroUtil.login(null, null, principal, PASSWORD));
            assertTrue(SecurityUtils.getSubject().isAuthenticated());
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            SecurityUtils.getSubject().logout();
            ThreadContext.remove();
            assertFalse(ShiroUtil.login(null, null, principal, "wrong-" + PASSWORD));
            assertFalse(SecurityUtils.getSubject().isAuthenticated());
            ThreadContext.remove();

            // login(AuthenticationToken): raw key
            ShiroUtil.login(new APIKeyAuthenticationToken(rawKey));
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            SecurityUtils.getSubject().logout();
            ThreadContext.remove();

            // login(AuthenticationToken): JWT
            String compact = ShiroDSDomainSecurityManager.mintJWT(signing, null, 30_000);
            ShiroUtil.login(new JWTAuthenticationToken(new JWTToken(SecUtil.parseJWT(compact), compact)));
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            assertEquals(signing.getSubjectID(), ShiroUtil.subjectJWTID(), "key ID visible through ShiroUtil");
            SecurityUtils.getSubject().logout();
            ThreadContext.remove();

            // login(AuthenticationToken): password token
            ShiroUtil.login(new io.xlogistx.shiro.authc.DomainUsernamePasswordToken(principal, PASSWORD, false, null, null, null));
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            SecurityUtils.getSubject().logout();
            ThreadContext.remove();

            assertThrows(AccessSecurityException.class, () -> ShiroUtil.login(new APIKeyAuthenticationToken("key-" + UUID.randomUUID())));
            ThreadContext.remove();

            // loginSubject(subjectID, credentials, domainID, appID, autoLogin=false) + loginBySessionID
            Subject s = ShiroUtil.loginSubject(principal, PASSWORD, "xlogistx.io", "shirods", false);
            assertTrue(s.isAuthenticated());
            assertEquals("xlogistx.io", ShiroUtil.subjectDomainID());
            assertEquals("shirods", ShiroUtil.subjectAppID());
            String sessionID = String.valueOf(s.getSession().getId());
            ThreadContext.remove(); // another request thread: nothing bound, session still alive
            assertTrue(ShiroUtil.loginBySessionID(sessionID));
            assertTrue(SecurityUtils.getSubject().isAuthenticated());
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            SecurityUtils.getSubject().logout();
            ThreadContext.remove();
            assertFalse(ShiroUtil.loginBySessionID(sessionID), "logged-out session must not resume");
            assertFalse(ShiroUtil.loginBySessionID("no-such-session"));
            ThreadContext.remove();

            assertThrows(AccessSecurityException.class, () -> ShiroUtil.loginSubject(principal, "wrong-" + PASSWORD, null, null, false));
            ThreadContext.remove();

            // autoLogin=true is the trusted-caller path: password not checked, status rules still apply
            Subject auto = ShiroUtil.loginSubject(principal, null, null, null, true);
            assertTrue(auto.isAuthenticated());
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            auto.logout();
            ThreadContext.remove();
            subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
            dsm.updateSubjectID(subject);
            assertThrows(AccessSecurityException.class, () -> ShiroUtil.loginSubject(principal, null, null, null, true), "auto-login must not bypass status gating");
        } finally {
            ThreadContext.remove();
        }
    }

    /**
     * shiro.ini wiring: the realm is declared in the INI with its no-arg constructor, the security
     * manager is the INI's, the store arrives through {@code ResourceManager} and the manager is
     * obtained with {@code fromGlobal()}. Everything (login, ShiroUtil, grants, eviction) must work
     * through that realm.
     */
    @Test
    public void iniConfiguredRealm_resolvesStoreFromResourceManager_andWorks() {
        String iniText = String.join("\n",
                "[main]",
                "cacheManager = org.apache.shiro.cache.MemoryConstrainedCacheManager",
                "securityManager.cacheManager = $cacheManager",
                "matcher = io.xlogistx.shiro.authc.CredentialsInfoMatcher",
                "matcher.clockSkewMillis = 120000",
                "dsRealm = io.xlogistx.shiro.ds.DSAuthorizingRealm",
                "dsRealm.name = ini-shiro-ds",
                "dsRealm.credentialsMatcher = $matcher",
                "securityManager.realm = $dsRealm");
        Ini ini = new Ini();
        ini.load(iniText);
        org.apache.shiro.mgt.SecurityManager iniSM = new BasicIniEnvironment(ini).getSecurityManager();

        ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, ds);
        SecurityUtils.setSecurityManager(iniSM);
        ThreadContext.remove();
        try {
            ShiroDSDomainSecurityManager iniDsm = ShiroDSDomainSecurityManager.fromGlobal();
            assertNotSame(dsm, iniDsm);
            assertFalse(iniDsm.isSelfManaged());
            assertNull(iniDsm.getShiroSecurityManager(), "attached mode owns no security manager");
            assertSame(iniSM, iniDsm.getSecurityManager());
            assertEquals("ini-shiro-ds", iniDsm.getRealm().getName());
            assertTrue(iniDsm.getRealm().isBound());
            assertEquals(120_000, iniDsm.getCredentialsMatcher().getClockSkewMillis(), "INI-declared matcher kept");
            assertSame(iniDsm, ShiroDSDomainSecurityManager.fromGlobal(), "fromGlobal is idempotent");
            assertSame(iniDsm, ShiroDSDomainSecurityManager.fromGlobal(ds));
            assertSame(iniDsm, ShiroDSDomainSecurityManager.attach(iniDsm.getRealm(), ds), "attach on a bound realm with the same store returns it");
            assertThrows(IllegalStateException.class, iniDsm::installAsGlobal);

            String principal = uniquePrincipal();
            SubjectIdentifier subject = newSubject(principal); // same database, either manager sees it
            assertEquals(subject.getGUID(), iniDsm.login(principal, PASSWORD).getGUID(), "authenticator-only login through the INI security manager");
            assertThrows(AccessSecurityException.class, () -> iniDsm.login(principal, "wrong-" + PASSWORD));

            String token = "ini:" + UUID.randomUUID().toString().replace("-", "") + ":read";
            PermissionInfo perm = iniDsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), token));
            assertTrue(ShiroUtil.login(null, null, principal, PASSWORD));
            Subject current = SecurityUtils.getSubject();
            assertTrue(current.isAuthenticated());
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            assertFalse(current.isPermitted(token));
            PermissionGrant grant = iniDsm.addPermissionGrant(subject, perm);
            assertTrue(current.isPermitted(token), "grant visible without re-login");
            assertTrue(iniDsm.deletePermissionGrant(grant));
            assertFalse(current.isPermitted(token), "revocation evicts the INI realm's cache");
            current.logout();
            ThreadContext.remove();

            Subject bound = iniDsm.loginSubject(principal, PASSWORD, null, null);
            assertTrue(bound.isAuthenticated());
            assertSame(bound, ThreadContext.getSubject());
            iniDsm.logout();
            assertNull(ThreadContext.getSubject());
        } finally {
            ThreadContext.remove();
            ResourceManager.SINGLETON.unregister(ResourceManager.Resource.DATA_STORE);
            SecurityUtils.setSecurityManager(dsm.getShiroSecurityManager());
        }
    }

    /** The shipped sample INI loads and its realm attaches explicitly (no ResourceManager). */
    @Test
    public void sampleShiroDsIni_loadsAndAttaches() {
        Ini ini = Ini.fromResourcePath("classpath:shiro-ds.ini");
        org.apache.shiro.mgt.SecurityManager iniSM = new BasicIniEnvironment(ini).getSecurityManager();
        SecurityUtils.setSecurityManager(iniSM);
        ThreadContext.remove();
        try {
            assertThrows(IllegalStateException.class, ShiroDSDomainSecurityManager::fromGlobal, "store not registered, nothing attached");
            ShiroDSDomainSecurityManager sample = ShiroDSDomainSecurityManager.fromGlobal(ds);
            assertEquals("shiro-ds", sample.getRealm().getName());
            assertEquals("DataStore", sample.getRealm().getDataStoreResource());
            assertEquals(60_000, sample.getCredentialsMatcher().getClockSkewMillis());
            assertSame(sample, ShiroDSDomainSecurityManager.fromGlobal());
            String principal = uniquePrincipal();
            SubjectIdentifier subject = newSubject(principal);
            assertEquals(subject.getGUID(), sample.login(principal, PASSWORD).getGUID());
            assertTrue(ShiroUtil.login(null, null, principal, PASSWORD));
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            SecurityUtils.getSubject().logout();
        } finally {
            ThreadContext.remove();
            SecurityUtils.setSecurityManager(dsm.getShiroSecurityManager());
        }
    }

    @Test
    public void iniRealm_withoutStore_failsLoudly() {
        DSAuthorizingRealm unbound = new DSAuthorizingRealm();
        unbound.setDataStoreResource("no-such-resource");
        assertFalse(unbound.isBound());
        IllegalStateException e = assertThrows(IllegalStateException.class, unbound::getDomainSecurityManager);
        assertTrue(e.getMessage().contains("no-such-resource"), e.getMessage());
        assertEquals(ShiroDSDomainSecurityManager.REALM_NAME, unbound.getName(), "no-arg defaults");
        assertTrue(unbound.getCredentialsMatcher() instanceof CredentialsInfoMatcher);

        ShiroDSDomainSecurityManager attached = ShiroDSDomainSecurityManager.attach(unbound, ds);
        assertTrue(unbound.isBound());
        assertSame(attached, unbound.getDomainSecurityManager());
        assertThrows(IllegalStateException.class, () -> ShiroDSDomainSecurityManager.attach(unbound, new org.zoxweb.server.util.MockAPIDataStore()), "bound to a different store");
    }

    @Test
    public void eagerAuthorization_loadsGrantsDuringLogin() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String token = "eager:" + UUID.randomUUID().toString().replace("-", "") + ":read";
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), token));
        dsm.addPermissionGrant(subject, perm);
        assertFalse(dsm.isEagerAuthorization(), "lazy by default, like Shiro");

        // one lazy check so the realm has materialized its authorization cache
        Subject first = dsm.loginSubject(principal, PASSWORD, null, null);
        assertTrue(first.isPermitted(token));
        dsm.logout();
        Cache<Object, AuthorizationInfo> cache = dsm.getRealm().getAuthorizationCache();
        assertNotNull(cache);

        dsm.getRealm().evictAuthorization(subject.getGUID());
        dsm.login(principal, PASSWORD);
        assertNull(cache.get(subject.getGUID()), "lazy: login alone caches nothing");

        dsm.setEagerAuthorization(true);
        try {
            dsm.login(principal, PASSWORD);
            AuthorizationInfo cached = cache.get(subject.getGUID());
            assertNotNull(cached, "eager: grants loaded and cached by the login itself");
            assertTrue(cached.getStringPermissions().contains(token));

            dsm.getRealm().evictAuthorization(subject.getGUID());
            assertThrows(AccessSecurityException.class, () -> dsm.login(principal, "wrong-" + PASSWORD));
            assertNull(cache.get(subject.getGUID()), "a failed login must not load grants");

            Subject s = dsm.loginSubject(principal, PASSWORD, null, null);
            assertNotNull(cache.get(subject.getGUID()), "bound login warms the cache too");
            assertTrue(dsm.getRealm().lookupAuthorizationInfo(s.getPrincipals()).getStringPermissions().contains(token));
            dsm.installAsGlobal();
            AuthorizationInfo viaUtil = ShiroUtil.lookupAuthorizationInfo(DSAuthorizingRealm.class, s.getPrincipals());
            assertNotNull(viaUtil, "AuthorizationInfoLookup wired for ShiroUtil");
            assertTrue(viaUtil.getStringPermissions().contains(token));
            dsm.logout();
        } finally {
            dsm.setEagerAuthorization(false);
            ThreadContext.remove();
        }
    }

    // ------------------------------------------------------------------
    // Shiro authorization
    // ------------------------------------------------------------------

    @Test
    public void authz_permissionGrant_isPermitted_andRevokeClearsCache() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String token = "doc:" + UUID.randomUUID().toString().replace("-", "") + ":read";
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), token));

        Subject shiro = dsm.loginSubject(principal, PASSWORD, null, null);
        assertTrue(shiro.isAuthenticated());
        assertFalse(shiro.isPermitted(token), "nothing granted yet");

        PermissionGrant grant = dsm.addPermissionGrant(subject, perm);
        assertTrue(shiro.isPermitted(token), "grant must be visible without re-login");

        assertTrue(dsm.deletePermissionGrant(grant));
        assertFalse(shiro.isPermitted(token), "revocation must evict the cached authorization");
        dsm.logout();
        assertNull(org.apache.shiro.util.ThreadContext.getSubject());
    }

    @Test
    public void authz_roleGrant_hasRole_andRolePermissions() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String token = "report:" + UUID.randomUUID().toString().replace("-", "") + ":*";
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), token));
        RoleInfo role = new RoleInfo(perm);
        role.setName("role.reporter." + UUID.randomUUID());
        role = dsm.createRole(role);

        Subject shiro = dsm.loginSubject(principal, PASSWORD, null, null);
        assertFalse(shiro.hasRole(role.getName()));

        dsm.addRoleGrant(subject, role);
        assertTrue(shiro.hasRole(role.getName()));
        assertTrue(shiro.isPermitted(token.replace("*", "view")), "role permission must be implied");
    }

    @Test
    public void authz_roleGroupGrant_expandsToRolesAndTheirPermissions() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        String tokenA = "grp:" + UUID.randomUUID().toString().replace("-", "") + ":a";
        String tokenB = "grp:" + UUID.randomUUID().toString().replace("-", "") + ":b";
        RoleInfo roleA = new RoleInfo(dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), tokenA)));
        roleA.setName("role.ga." + UUID.randomUUID());
        roleA = dsm.createRole(roleA);
        RoleInfo roleB = new RoleInfo(dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), tokenB)));
        roleB.setName("role.gb." + UUID.randomUUID());
        roleB = dsm.createRole(roleB);
        RoleGroupInfo group = new RoleGroupInfo(roleA, roleB);
        group.setName("rolegroup." + UUID.randomUUID());
        group = dsm.createRoleGroup(group);

        Subject shiro = dsm.loginSubject(principal, PASSWORD, null, null);
        RoleGroupGrant grant = dsm.addRoleGroupGrant(subject, group);
        assertTrue(shiro.hasRole(roleA.getName()));
        assertTrue(shiro.hasRole(roleB.getName()));
        assertTrue(shiro.isPermitted(tokenA));
        assertTrue(shiro.isPermitted(tokenB));

        dsm.deleteRoleGroupGrant(grant);
        assertFalse(shiro.hasRole(roleA.getName()));
        assertFalse(shiro.isPermitted(tokenB));
    }

    @Test
    public void enforcement_deniesWithoutSubject_allowsWithPermission() {
        String adminPrincipal = uniquePrincipal();
        SubjectIdentifier admin = newSubject(adminPrincipal);
        PermissionInfo canAddPerm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(),
                org.zoxweb.shared.security.model.SecurityModel.PERM_ADD_PERMISSION));
        dsm.addPermissionGrant(admin, canAddPerm);

        dsm.setEnforcePermissions(true);
        try {
            assertThrows(AccessSecurityException.class,
                    () -> dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "n:o")),
                    "no bound subject -> denied");

            dsm.loginSubject(adminPrincipal, PASSWORD, null, null);
            assertNotNull(dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "y:es")).getGUID());
            assertThrows(AccessSecurityException.class, () -> dsm.createRole(new RoleInfo()), "role:create not granted");

            // self-service stays open: the bound subject may change its own credential
            CIPassword mine = (CIPassword) dsm.lookupCredential(adminPrincipal, CredentialInfo.Type.PASSWORD);
            dsm.updateCredential(admin, mine);
        } finally {
            dsm.setEnforcePermissions(false);
        }
    }

    // ------------------------------------------------------------------
    // app-scoped grants: a grant with app_id is the subject's assignment to that app; the login
    // (domain + app) decides which grants apply (user decisions 2026-09-17/18)
    // ------------------------------------------------------------------

    private static final String DOMAIN = "xlogistx.io";

    private static AppIDDefault app(String name) {
        return new AppIDDefault(DOMAIN, name);
    }

    private static Subject loginApp(String principal, String appName) {
        dsm.logout();
        return dsm.loginSubject(principal, PASSWORD, DOMAIN, appName);
    }

    @Test
    public void appGrant_loginScopeSelectsGrants_andRevokeAppRemovesThem() {
        dsm.seedCatalog();
        RoleInfo domainAdmin = dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName());
        RoleInfo userRole = dsm.lookupRole(null, SecurityModel.Role.USER.getName());
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        AppIDDefault a = app("appa");
        RoleGrant grant = dsm.addRoleGrant(subject, domainAdmin, a);
        dsm.addRoleGrant(subject, userRole); // global
        assertNotNull(grant.getGUID());
        assertNull(grant.getBrokerGUID(), "nobody bound -> no broker");
        RoleGrant[] scoped = dsm.getRoleGrants(subject.getGUID(), a);
        assertEquals(1, scoped.length);
        assertEquals(a, scoped[0].getAppIdDAO(), "app_id round-trips through the store");
        assertEquals(0, dsm.getRoleGrants(subject.getGUID(), app("other")).length);
        assertEquals("xlogistx.io-appa", ShiroDSDomainSecurityManager.appScope(a));

        // global login: global grants only
        dsm.logout();
        Subject global = dsm.loginSubject(principal, PASSWORD, null, null);
        assertTrue(global.hasRole("user"));
        assertFalse(global.hasRole("domain_admin"), "the app grant needs an app login");
        assertFalse(global.isPermitted("subject:create"));

        // app login: that app's grants only, as plain tokens
        Subject inA = loginApp(principal, "appa");
        assertTrue(inA.hasRole("domain_admin"));
        assertTrue(inA.isPermitted("subject:create"));
        assertTrue(inA.isPermitted("nventity:read:" + UUID.randomUUID()));
        assertFalse(inA.hasRole("user"), "global grants stay out of an app login");

        // another app: nothing
        Subject inOther = loginApp(principal, "other");
        assertFalse(inOther.hasRole("domain_admin"));
        assertFalse(inOther.isPermitted("subject:create"));
        assertFalse(inOther.hasRole("user"));

        Subject again = loginApp(principal, "appa");
        assertTrue(again.isPermitted("subject:create"));
        assertEquals(1, dsm.revokeAppGrants(subject.getGUID(), a));
        assertFalse(again.isPermitted("subject:create"), "the app-scoped cache entry is evicted on revoke");
        assertFalse(again.hasRole("domain_admin"));
        assertEquals(0, dsm.revokeAppGrants(subject.getGUID(), a), "nothing left");
        dsm.logout();
        assertTrue(dsm.loginSubject(principal, PASSWORD, null, null).hasRole("user"), "global grant untouched");
        dsm.logout();
    }

    @Test
    public void appGrant_scopedRoleGroup_appliesInThatAppLoginOnly() {
        dsm.seedCatalog();
        RoleGroupInfo appUsers = dsm.lookupRoleGroup(null, SecurityModel.RoleGroup.APP_USERS.getName());
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        AppIDDefault b = app("appb");
        RoleGroupGrant g = dsm.addRoleGroupGrant(subject, appUsers, b);
        assertEquals(b, g.getAppIdDAO());
        Subject inB = loginApp(principal, "appb");
        assertTrue(inB.hasRole("app_user"));
        assertTrue(inB.hasRole("user"), "every role of the group, plain");
        dsm.logout();
        Subject global = dsm.loginSubject(principal, PASSWORD, null, null);
        assertFalse(global.hasRole("app_user"));
        assertFalse(global.hasRole("user"));
        dsm.logout();
        assertTrue(dsm.deleteRoleGroupGrant(g));
        assertFalse(dsm.deleteRoleGroupGrant(g), "already gone");
        assertFalse(loginApp(principal, "appb").hasRole("app_user"));
        dsm.logout();
    }

    @Test
    public void appGrant_enforcement_appAdminActsInsideItsAppLoginOnly_andBrokerRevokes() {
        dsm.seedCatalog();
        RoleInfo appAdmin = dsm.lookupRole(null, SecurityModel.Role.APP_ADMIN.getName());
        RoleInfo appUser = dsm.lookupRole(null, SecurityModel.Role.APP_USER.getName());
        AppIDDefault a = app("appc");
        AppIDDefault b = app("appd");
        String adminPrincipal = uniquePrincipal();
        String bystanderPrincipal = uniquePrincipal();
        SubjectIdentifier admin = newSubject(adminPrincipal);
        SubjectIdentifier user = newSubject(uniquePrincipal());
        newSubject(bystanderPrincipal);
        dsm.addRoleGrant(admin, appAdmin, a); // app admin of a: permission:assign:role / :remove:role inside a

        dsm.setEnforcePermissions(true);
        try {
            assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(user, appUser, a), "nobody bound");

            dsm.loginSubject(adminPrincipal, PASSWORD, null, null);
            assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(user, appUser, a), "a global login holds none of the app grants");
            dsm.logout();

            loginApp(adminPrincipal, "appc");
            RoleGrant granted = dsm.addRoleGrant(user, appUser, a);
            assertEquals(admin.getGUID(), granted.getBrokerGUID(), "the grantor is recorded as broker");
            assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(user, appUser, b), "another app");
            assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(user, appUser), "an app login never grants globally");
            dsm.logout();

            loginApp(bystanderPrincipal, "appc");
            assertThrows(AccessSecurityException.class, () -> dsm.deleteRoleGrant(granted), "not broker, no permission");
            assertThrows(AccessSecurityException.class, () -> dsm.revokeAppGrants(user.getGUID(), a));
            assertEquals(1, dsm.getRoleGrants(user.getGUID(), a).length, "nothing was deleted");
            dsm.logout();

            loginApp(adminPrincipal, "appc");
            assertEquals(1, dsm.revokeAppGrants(user.getGUID(), a), "the broker revokes what it granted");
            assertEquals(0, dsm.getRoleGrants(user.getGUID(), a).length);
            dsm.logout();
        } finally {
            dsm.setEnforcePermissions(false);
            dsm.logout();
        }
    }

    // ------------------------------------------------------------------
    // instance grants and sharing (design page section 12, item 23)
    // ------------------------------------------------------------------

    /** A "file": any persisted entity with a subject_guid owner; PropertyDAO is the lightest. */
    private static PropertyDAO newResource(SubjectIdentifier owner) {
        PropertyDAO p = new PropertyDAO();
        p.setName("file-" + UUID.randomUUID());
        p.setSubjectGUID(owner.getGUID());
        return ds.insert(p);
    }

    private static ResourceMap mapOf(NVEntity resource) {
        return new ResourceMap(resource);
    }

    private static String nve(String verb, String guid) {
        return SecurityModel.NVENTITY + ":" + verb + ":" + guid;
    }

    private static Subject login(String principal) {
        dsm.logout();
        return dsm.loginSubject(principal, PASSWORD, null, null);
    }

    private static boolean mapRowExists(String mapGUID) {
        return !ds.searchByID(ResourceMap.NVC_RESOURCE_MAP, mapGUID).isEmpty();
    }

    @Test
    public void share_inlinedGrant_permitsGranteeOnThatResourceOnly() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a), y = newResource(a);
        assertEquals(a.getGUID(), x.getSubjectGUID(), "resource must carry its owner");

        Subject shiroB = login(pb);
        assertFalse(shiroB.isPermitted(nve("read", x.getGUID())));

        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "nventity:read,share");
        assertEquals("nventity:read,share", grant.getPermissionToken());
        assertNull(grant.getPermissionGUID());
        assertEquals(b.getGUID(), grant.getSubjectGUID());
        assertNotNull(grant.getResourceMap().getGUID(), "map row gets its own GUID");
        assertEquals(x.getGUID(), grant.getResourceMap().getResourceGUID());
        assertEquals(PropertyDAO.class.getName(), grant.getResourceMap().getResourceType());
        assertTrue(mapRowExists(grant.getResourceMap().getGUID()));

        assertTrue(shiroB.isPermitted(nve("read", x.getGUID())), "read on X granted");
        assertTrue(shiroB.isPermitted(nve("share", x.getGUID())), "share on X granted");
        assertFalse(shiroB.isPermitted(nve("update", x.getGUID())), "update on X not granted");
        assertFalse(shiroB.isPermitted(nve("delete", x.getGUID())));
        assertFalse(shiroB.isPermitted(nve("read", y.getGUID())), "Y not shared");
        assertFalse(shiroB.isPermitted("nventity:read"), "no global read");

        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains("nventity:read,share:" + x.getGUID()));

        PermissionGrant[] stored = dsm.getPermissionGrants(b.getGUID());
        assertEquals(1, stored.length);
        assertNotNull(stored[0].getResourceMap(), "map must load eagerly with the grant");
        assertEquals(x.getGUID(), stored[0].getResourceMap().getResourceGUID());
    }

    @Test
    public void share_tokenNormalized() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);

        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), " NVEntity:Read , Share ");
        assertEquals("nventity:read,share", grant.getPermissionToken());

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("read", x.getGUID())));
        assertTrue(shiroB.isPermitted("NVENTITY:READ:" + x.getGUID().toUpperCase()), "ShiroUtil lower-cases checks; direct checks are case-insensitive by Shiro");
    }

    @Test
    public void share_invalidTokensRejected() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);

        String[] bad = {"nventity:create", "nventity:*", "nventity:read:" + x.getGUID(), "doc:read", "nventity:",
                "nventity:read,,share", "nventity:read,bogus", "nventity", "", null};
        for (String token : bad) {
            assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, mapOf(x), token),
                    "token must be rejected: " + token);
        }
        assertEquals(0, dsm.getPermissionGrants(b.getGUID()).length, "nothing persisted");
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length, "no map rows either");
    }

    @Test
    public void share_missingResourceRejected() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);

        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(UUID.randomUUID().toString(), PropertyDAO.class.getName()), "nventity:read"),
                "unknown GUID");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(x.getGUID(), "com.example.NoSuchClass"), "nventity:read"),
                "unknown class");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(x.getGUID(), null), "nventity:read"),
                "blank type");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(null, PropertyDAO.class.getName()), "nventity:read"),
                "blank guid");
        assertEquals(0, dsm.getPermissionGrants(b.getGUID()).length);
    }

    @Test
    public void share_catalogScopedGrant_flattensWithGuid() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:update"));

        PermissionGrant grant = dsm.addPermissionGrant(b, update, mapOf(x));
        assertEquals(update.getGUID(), grant.getPermissionGUID());
        assertNull(grant.getPermissionToken());
        assertEquals(x.getGUID(), grant.getResourceMap().getResourceGUID());

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("update", x.getGUID())));
        assertFalse(shiroB.isPermitted(nve("update", y.getGUID())));
        assertFalse(shiroB.isPermitted(nve("read", x.getGUID())));
        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains("nventity:update:" + x.getGUID()));

        PermissionInfo scoped = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:read:abc"));
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, scoped, mapOf(x)),
                "a token that already has an instance part cannot be scoped");
        PermissionInfo onePart = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity"));
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, onePart, mapOf(x)));
        PermissionInfo unsaved = new PermissionInfo("perm." + UUID.randomUUID(), "nventity:read");
        unsaved.setGUID(UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, unsaved, mapOf(x)), "unknown catalog row");
    }

    @Test
    public void share_globalCatalogGrant_unchanged() {
        String pb = uniquePrincipal();
        SubjectIdentifier b = newSubject(pb);
        PermissionInfo read = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:read"));

        dsm.logout();
        PermissionGrant grant = dsm.addPermissionGrant(b, read);
        assertNull(grant.getResourceMap());
        assertNull(grant.getPermissionToken());
        assertNull(grant.getBrokerGUID(), "nobody bound -> no grantor recorded");

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("read", UUID.randomUUID().toString())), "global read implies every instance");
        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains("nventity:read"));

        // exclusivity: a grant carrying both forms is refused before anything is written
        PermissionGrant both = new PermissionGrant(read.getGUID(), mapOf(newResource(b)));
        both.setPermissionToken("nventity:read");
        assertThrows(IllegalArgumentException.class, both::validateShape);
    }

    @Test
    public void share_revoke_evictsAndDeletesMapRow() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "nventity:read");
        String mapGUID = grant.getResourceMap().getGUID();

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("read", x.getGUID())));

        assertTrue(dsm.deletePermissionGrant(grant));
        assertFalse(shiroB.isPermitted(nve("read", x.getGUID())), "revocation must evict the cached authorization");
        assertFalse(mapRowExists(mapGUID), "map row deleted with the grant");
        assertTrue(ds.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, grant.getGUID()).isEmpty());
        assertFalse(dsm.deletePermissionGrant(grant), "second revoke finds nothing");

        // a shell with only the GUID still cascades to the stored map
        PermissionGrant again = dsm.addPermissionGrant(b, mapOf(x), "nventity:read");
        PermissionGrant shell = new PermissionGrant();
        shell.setGUID(again.getGUID());
        assertTrue(dsm.deletePermissionGrant(shell));
        assertFalse(mapRowExists(again.getResourceMap().getGUID()));
    }

    @Test
    public void share_enforcement_nonOwnerCannotInlineShare() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc);
        PropertyDAO x = newResource(a);

        dsm.setEnforcePermissions(true);
        try {
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "nventity:read"), "nobody bound");
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "nventity:read"), "B does not own X");
            assertEquals(0, dsm.getPermissionGrants(c.getGUID()).length);

            login(pa);
            PermissionGrant grant = dsm.addPermissionGrant(c, mapOf(x), "nventity:read");
            assertEquals(a.getGUID(), grant.getBrokerGUID(), "grantor recorded");
            assertEquals(c.getGUID(), grant.getSubjectGUID());
        } finally {
            dsm.setEnforcePermissions(false);
        }
        Subject shiroC = login(pc);
        assertTrue(shiroC.isPermitted(nve("read", x.getGUID())));
    }

    @Test
    public void share_enforcement_shareHolderCanCatalogGrant_notInline() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:update"));
        // A shares X with B including the share verb (enforcement off: nobody needs to be bound)
        dsm.addPermissionGrant(b, mapOf(x), "nventity:read,share");

        dsm.setEnforcePermissions(true);
        try {
            login(pb);
            PermissionGrant grant = dsm.addPermissionGrant(c, update, mapOf(x));
            assertEquals(b.getGUID(), grant.getBrokerGUID());
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "nventity:read"),
                    "a share holder may not create an inlined grant");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, update, mapOf(y)),
                    "no share on Y and no global assign");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, update), "global grant needs the assign permission");
        } finally {
            dsm.setEnforcePermissions(false);
        }
        Subject shiroC = login(pc);
        assertTrue(shiroC.isPermitted(nve("update", x.getGUID())));
        assertFalse(shiroC.isPermitted(nve("update", y.getGUID())));
    }

    @Test
    public void share_enforcement_grantorOrOwnerCanRevoke() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), padmin = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), admin = newSubject(padmin);
        PropertyDAO x = newResource(a);
        PermissionInfo canRemove = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), SecurityModel.PERM_REMOVE_PERMISSION));
        dsm.addPermissionGrant(admin, canRemove);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:update"));

        login(pa);
        PermissionGrant aToB = dsm.addPermissionGrant(b, mapOf(x), "nventity:read,share");
        assertEquals(a.getGUID(), aToB.getBrokerGUID());

        dsm.setEnforcePermissions(true);
        try {
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "nobody bound");
            login(pc);
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "C is nobody here");
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "the grantee cannot revoke its own grant");
            login(pa);
            assertTrue(dsm.deletePermissionGrant(aToB), "the grantor revokes");

            // B (share holder) grants C; the owner A, not the grantor, revokes
            PermissionGrant aToB2 = dsm.addPermissionGrant(b, mapOf(x), "nventity:share");
            login(pb);
            PermissionGrant bToC = dsm.addPermissionGrant(c, update, mapOf(x));
            login(pa);
            assertTrue(dsm.deletePermissionGrant(bToC), "the resource owner revokes");

            // global remove permission revokes anything
            login(pb);
            PermissionGrant bToC2 = dsm.addPermissionGrant(c, update, mapOf(x));
            login(padmin);
            assertTrue(dsm.deletePermissionGrant(bToC2));
            assertTrue(dsm.deletePermissionGrant(aToB2));
        } finally {
            dsm.setEnforcePermissions(false);
        }
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length);
    }

    @Test
    public void share_deleteSubjectID_cleansGranteeMapRows() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "nventity:read");
        String mapGUID = grant.getResourceMap().getGUID();
        assertEquals(1, dsm.getPermissionGrantsByResource(x.getGUID()).length);

        assertTrue(dsm.deleteSubjectID(b));
        assertTrue(ds.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, grant.getGUID()).isEmpty());
        assertFalse(mapRowExists(mapGUID), "map row must not be orphaned");
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length);
    }

    @Test
    public void share_getAndDeletePermissionGrantsByResource() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), pd = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), d = newSubject(pd);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionGrant gb = dsm.addPermissionGrant(b, mapOf(x), "nventity:read");
        PermissionGrant gc = dsm.addPermissionGrant(c, mapOf(x), "nventity:read,update");
        PermissionGrant gd = dsm.addPermissionGrant(d, mapOf(y), "nventity:read");

        PermissionGrant[] onX = dsm.getPermissionGrantsByResource(x.getGUID());
        assertEquals(2, onX.length);
        for (PermissionGrant g : onX) {
            assertEquals(x.getGUID(), g.getResourceMap().getResourceGUID());
        }
        assertEquals(0, dsm.getPermissionGrantsByResource(UUID.randomUUID().toString()).length);
        assertEquals(0, dsm.getPermissionGrantsByResource(null).length);

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("read", x.getGUID())));

        assertEquals(2, dsm.deletePermissionGrantsByResource(x.getGUID()));
        assertFalse(shiroB.isPermitted(nve("read", x.getGUID())), "grantee evicted");
        assertFalse(mapRowExists(gb.getResourceMap().getGUID()));
        assertFalse(mapRowExists(gc.getResourceMap().getGUID()));
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length);
        assertEquals(1, dsm.getPermissionGrantsByResource(y.getGUID()).length, "Y untouched");
        assertTrue(mapRowExists(gd.getResourceMap().getGUID()));
        assertEquals(0, dsm.deletePermissionGrantsByResource(x.getGUID()), "idempotent");
    }

    @Test
    public void share_flattenerSkipsGrantWithMissingCatalogRow() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "nventity:update"));
        dsm.addPermissionGrant(b, update, mapOf(x));

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve("update", x.getGUID())));

        assertTrue(dsm.deletePermission(update));
        Subject again = login(pb);
        assertTrue(again.isAuthenticated(), "a dangling grant must not break login");
        assertFalse(again.isPermitted(nve("update", x.getGUID())));
        assertEquals(1, dsm.getPermissionGrants(b.getGUID()).length, "the grant row itself stays");
    }

    // ------------------------------------------------------------------
    // password reset (email principal = recovery channel; admin path for the rest)
    // ------------------------------------------------------------------

    private static String uniqueUsername() {
        return "user-" + UUID.randomUUID().toString().replace("-", "");
    }

    private static PasswordResetToken[] tokensOf(String subjectGUID) {
        java.util.List<PasswordResetToken> list = ds.search(PasswordResetToken.NVC_PASSWORD_RESET_TOKEN, null,
                new org.zoxweb.shared.db.QueryMatch<>(org.zoxweb.shared.util.MetaToken.SUBJECT_GUID.getName(), subjectGUID,
                        Const.RelationalOperator.EQUAL));
        return list.toArray(new PasswordResetToken[0]);
    }

    private static void expireOutstanding(String subjectGUID) {
        for (PasswordResetToken t : tokensOf(subjectGUID)) {
            if (t.getStatus() == SecConst.SecStatus.ACTIVE) {
                t.setExpiryTS(System.currentTimeMillis() - 1);
                ds.update(t);
            }
        }
    }

    @Test
    public void reset_request_emailPrincipal_issuesTokenAndLocksLogin() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        long before = System.currentTimeMillis();

        PasswordResetRequest req = dsm.requestPasswordReset("  " + principal.toUpperCase() + " ");
        assertNotNull(req.getToken());
        assertEquals(43, req.getToken().length());
        assertEquals(principal, req.getPrincipalID(), "normalized");
        assertEquals(subject.getGUID(), req.getSubjectGUID());
        assertArrayEquals(new String[]{principal}, req.getDeliveryPrincipalIDs());
        assertEquals(PasswordResetToken.Channel.EMAIL, req.getChannel());
        long ttl = dsm.getResetTokenTTL(PasswordResetToken.Channel.EMAIL);
        assertTrue(req.getExpiryTS() >= before + ttl && req.getExpiryTS() <= System.currentTimeMillis() + ttl);
        assertFalse(req.toString().contains(req.getToken()), "toString masks the token");

        assertEquals(SecConst.SecStatus.PENDING_RESET_PASSWORD, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD), "login denied while pending");
        assertTrue(dsm.verifyPassword(principal, PASSWORD), "verifyPassword still works while pending");
        PasswordResetToken[] rows = tokensOf(subject.getGUID());
        assertEquals(1, rows.length);
        assertNotEquals(req.getToken(), rows[0].getTokenHash(), "only the hash is stored");
        assertEquals(SecConst.SecStatus.ACTIVE, rows[0].getStatus());
        assertNull(rows[0].getBrokerGUID());
    }

    @Test
    public void reset_complete_withValidToken_replacesPasswordOnce() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PasswordResetRequest req = dsm.requestPasswordReset(principal);

        dsm.completePasswordReset(principal, req.getToken(), NEW_PASSWORD);
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
        assertNotNull(dsm.login(principal, NEW_PASSWORD));
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD), "old password gone");
        assertEquals(1, dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), CredentialInfo.Type.PASSWORD).length);
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, req.getToken(), NEW_PASSWORD), "single use");
        PasswordResetToken[] rows = tokensOf(subject.getGUID());
        assertEquals(1, rows.length);
        assertEquals(SecConst.SecStatus.DEACTIVATED, rows[0].getStatus());
        assertTrue(rows[0].getConsumedTS() > 0);
    }

    @Test
    public void reset_complete_rejectsWrongToken_andExpiredToken() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PasswordResetRequest req = dsm.requestPasswordReset(principal);

        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, req.getToken() + "x", NEW_PASSWORD));
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, "", NEW_PASSWORD));
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, null, NEW_PASSWORD));
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(uniquePrincipal(), req.getToken(), NEW_PASSWORD), "token of another subject");
        assertEquals(SecConst.SecStatus.PENDING_RESET_PASSWORD, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());

        expireOutstanding(subject.getGUID());
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, req.getToken(), NEW_PASSWORD), "expired");
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, NEW_PASSWORD));
    }

    @Test
    public void reset_complete_rejectsWeakPassword_andKeepsToken() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PasswordResetRequest req = dsm.requestPasswordReset(principal);
        assertThrows(IllegalArgumentException.class, () -> dsm.completePasswordReset(principal, req.getToken(), "weak"));
        assertEquals(SecConst.SecStatus.PENDING_RESET_PASSWORD, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
        dsm.completePasswordReset(principal, req.getToken(), NEW_PASSWORD);
        assertNotNull(dsm.login(principal, NEW_PASSWORD));
    }

    @Test
    public void reset_secondRequest_supersedesFirst() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PasswordResetRequest first = dsm.requestPasswordReset(principal);
        PasswordResetRequest second = dsm.requestPasswordReset(principal);
        assertNotEquals(first.getToken(), second.getToken());
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, first.getToken(), NEW_PASSWORD));
        dsm.completePasswordReset(principal, second.getToken(), NEW_PASSWORD);
        int active = 0, inactive = 0, consumed = 0;
        for (PasswordResetToken t : tokensOf(subject.getGUID())) {
            if (t.getStatus() == SecConst.SecStatus.ACTIVE) active++;
            if (t.getStatus() == SecConst.SecStatus.INACTIVE) inactive++;
            if (t.getStatus() == SecConst.SecStatus.DEACTIVATED) consumed++;
        }
        assertEquals(0, active);
        assertEquals(1, inactive);
        assertEquals(1, consumed);
    }

    @Test
    public void reset_request_usernameOnlySubject_throwsNoRecoveryChannel_adminPathWorks() {
        String username = uniqueUsername();
        SubjectIdentifier subject = newSubject(username);
        assertThrows(NoRecoveryChannelException.class, () -> dsm.requestPasswordReset(username));
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus(), "nothing issued");
        assertEquals(0, tokensOf(subject.getGUID()).length);

        long before = System.currentTimeMillis();
        PasswordResetRequest req = dsm.adminResetPassword(username);
        assertEquals(PasswordResetToken.Channel.ADMIN, req.getChannel());
        assertEquals(0, req.getDeliveryPrincipalIDs().length);
        long ttl = dsm.getResetTokenTTL(PasswordResetToken.Channel.ADMIN);
        assertEquals(4L * Const.TimeInMillis.HOUR.MILLIS, ttl);
        assertTrue(req.getExpiryTS() >= before + ttl && req.getExpiryTS() <= System.currentTimeMillis() + ttl);
        assertThrows(AccessSecurityException.class, () -> dsm.login(username, PASSWORD));
        dsm.completePasswordReset(username, req.getToken(), NEW_PASSWORD);
        assertNotNull(dsm.login(username, NEW_PASSWORD));
    }

    @Test
    public void reset_request_byUsername_whenSubjectAlsoHasEmails_deliversToAllEmails() {
        String username = uniqueUsername();
        String email1 = uniquePrincipal();
        String email2 = uniquePrincipal();
        SubjectIdentifier subject = newSubject(username);
        dsm.addPrincipalID(subject, email1);
        dsm.addPrincipalID(subject, email2);

        PasswordResetRequest req = dsm.requestPasswordReset(username);
        assertEquals(username, req.getPrincipalID());
        java.util.Set<String> delivery = new java.util.HashSet<>(java.util.Arrays.asList(req.getDeliveryPrincipalIDs()));
        assertEquals(new java.util.HashSet<>(java.util.Arrays.asList(email1, email2)), delivery);
        // any principal of the subject completes the reset
        dsm.completePasswordReset(email2, req.getToken(), NEW_PASSWORD);
        assertNotNull(dsm.login(username, NEW_PASSWORD));
        assertNotNull(dsm.login(email1, NEW_PASSWORD));
    }

    @Test
    public void reset_request_deactivatedOrUnknown_refused() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateSubjectID(subject);
        assertThrows(AccessSecurityException.class, () -> dsm.requestPasswordReset(principal));
        assertFalse(assertThrows(AccessSecurityException.class, () -> dsm.requestPasswordReset(principal)) instanceof NoRecoveryChannelException);
        assertEquals(SecConst.SecStatus.DEACTIVATED, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
        assertThrows(AccessSecurityException.class, () -> dsm.adminResetPassword(principal));

        AccessSecurityException unknown = assertThrows(AccessSecurityException.class, () -> dsm.requestPasswordReset(uniquePrincipal()));
        assertFalse(unknown instanceof NoRecoveryChannelException, "unknown principals are not distinguishable from channel-less ones by type? they are: generic");
        assertThrows(AccessSecurityException.class, () -> dsm.adminResetPassword(uniquePrincipal()));
    }

    @Test
    public void reset_expiredPending_isLiftedOnLogin() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        dsm.requestPasswordReset(principal);
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));
        expireOutstanding(subject.getGUID());
        assertNotNull(dsm.login(principal, PASSWORD), "old password works again once the token expired");
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
    }

    @Test
    public void reset_cancel_restoresActiveAndKillsToken() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        PasswordResetRequest req = dsm.requestPasswordReset(principal);
        assertTrue(dsm.cancelPasswordReset(principal));
        assertEquals(SecConst.SecStatus.ACTIVE, dsm.lookupSubjectByGUID(subject.getGUID()).getSubjectStatus());
        assertThrows(AccessSecurityException.class, () -> dsm.completePasswordReset(principal, req.getToken(), NEW_PASSWORD));
        assertNotNull(dsm.login(principal, PASSWORD));
        assertFalse(dsm.cancelPasswordReset(principal), "nothing left to cancel");
        assertFalse(dsm.cancelPasswordReset(uniquePrincipal()));
    }

    @Test
    public void reset_enforcement_adminNeedsSubjectUpdate_selfServiceNeedsNobody() {
        String target = uniquePrincipal();
        newSubject(target);
        String adminPrincipal = uniquePrincipal();
        SubjectIdentifier admin = newSubject(adminPrincipal);
        String plainPrincipal = uniquePrincipal();
        newSubject(plainPrincipal);
        PermissionInfo canUpdate = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), SecurityModel.PERM_UPDATE_SUBJECT));

        dsm.setEnforcePermissions(true);
        try {
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.adminResetPassword(target), "nobody bound");
            dsm.loginSubject(plainPrincipal, PASSWORD, null, null);
            assertThrows(AccessSecurityException.class, () -> dsm.adminResetPassword(target), "no subject:update");
            dsm.logout();

            // grant outside enforcement, then the admin path works
            dsm.setEnforcePermissions(false);
            dsm.addPermissionGrant(admin, canUpdate);
            dsm.setEnforcePermissions(true);
            dsm.loginSubject(adminPrincipal, PASSWORD, null, null);
            PasswordResetRequest adminReq = dsm.adminResetPassword(target);
            assertEquals(admin.getGUID(), tokensOf(adminReq.getSubjectGUID())[0].getBrokerGUID(), "broker recorded");
            dsm.logout();

            // self-service paths need nobody bound
            PasswordResetRequest req = dsm.requestPasswordReset(target);
            dsm.completePasswordReset(target, req.getToken(), NEW_PASSWORD);
        } finally {
            dsm.setEnforcePermissions(false);
        }
        assertNotNull(dsm.login(target, NEW_PASSWORD));
    }

    @Test
    public void reset_deleteSubject_cascadesTokens_andPurgeKeepsOutstanding() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        dsm.requestPasswordReset(principal);
        assertEquals(1, tokensOf(subject.getGUID()).length);
        assertTrue(dsm.deleteSubjectID(subject));
        assertEquals(0, tokensOf(subject.getGUID()).length, "tokens deleted with the subject");

        String p2 = uniquePrincipal();
        SubjectIdentifier s2 = newSubject(p2);
        PasswordResetRequest first = dsm.requestPasswordReset(p2);   // superseded below
        PasswordResetRequest second = dsm.requestPasswordReset(p2);  // outstanding
        String p3 = uniquePrincipal();
        SubjectIdentifier s3 = newSubject(p3);
        PasswordResetRequest third = dsm.requestPasswordReset(p3);
        expireOutstanding(s3.getGUID());                              // expired
        int purged = dsm.purgeExpiredResetTokens();
        assertTrue(purged >= 2, "superseded + expired removed, got " + purged);
        PasswordResetToken[] left = tokensOf(s2.getGUID());
        assertEquals(1, left.length);
        assertEquals(SecConst.SecStatus.ACTIVE, left[0].getStatus());
        assertEquals(0, tokensOf(s3.getGUID()).length);
        assertNotNull(first);
        assertNotNull(third);
        dsm.completePasswordReset(p2, second.getToken(), NEW_PASSWORD);
    }

    @Test
    public void reset_managerIsRegisteredAsResource_onInstallAsGlobal() {
        Object previous = ResourceManager.lookupResource(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER);
        org.apache.shiro.mgt.SecurityManager previousSM = null;
        try {
            previousSM = SecurityUtils.getSecurityManager();
        } catch (RuntimeException ignore) {
            // none installed
        }
        try {
            ResourceManager.SINGLETON.unregister(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER);
            assertNull(ResourceManager.lookupResource(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER));
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(ds);
            local.installAsGlobal();
            DomainSecurityManager found = ResourceManager.lookupResource(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER);
            assertSame(local, found, "installAsGlobal fills the empty slot");
            // an occupied slot is left alone
            new ShiroDSDomainSecurityManager(ds).installAsGlobal();
            assertSame(local, ResourceManager.lookupResource(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER));
            assertFalse(found.verifyPassword(uniquePrincipal(), PASSWORD), "reachable through the core interface");
        } finally {
            ResourceManager.SINGLETON.unregister(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER);
            if (previous != null) {
                ResourceManager.SINGLETON.register(ResourceManager.Resource.DOMAIN_SECURITY_MANAGER, previous);
            }
            if (previousSM != null) {
                SecurityUtils.setSecurityManager(previousSM);
            }
        }
    }
}
