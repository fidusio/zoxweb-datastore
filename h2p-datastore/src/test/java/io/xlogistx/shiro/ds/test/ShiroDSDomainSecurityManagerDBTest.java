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
import org.zoxweb.server.task.TaskUtil;
import org.zoxweb.server.util.cache.JWTTokenCache;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.*;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.Date;
import java.util.List;
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
    /** {@link #ds} in the system context: fixtures and raw checks, see {@link TestVault#systemView}. */
    private static org.zoxweb.shared.api.APIDataStore<?, ?> sys;

    @BeforeAll
    public static void setup() throws Exception {
        // prerequisite of every run: the vault, which supplies the master key and, when it holds them, the db.* settings
        TestVault.load();
        String url = firstNonEmpty(System.getProperty("ds.url"), TestVault.db("db.url"), DEFAULT_URL);
        String user = firstNonEmpty(System.getProperty("ds.user"), TestVault.db("db.user"));
        String password = firstNonEmpty(System.getProperty("ds.password"), TestVault.db("db.password"));
        String filePassword = firstNonEmpty(System.getProperty("ds.file_password"), TestVault.db("db.enc-password"));

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
        ds = new H2PDSCreator().createAPI(null, TestVault.secure(cfg));
        sys = TestVault.systemView(ds);

        OPSecUtil.singleton();
        dsm = new ShiroDSDomainSecurityManager(ds);
        dsm.setSuperAdminPrincipalID(TestVault.superAdminID()); // from the vault, never hard coded
        dsm.seedCatalog(); // the common app and its catalog: every catalog row belongs to an app (2026-10-01)
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

    /** A third-party API key of {@code subject}: the default purpose of a {@link SubjectAPIKey}, never a login. */
    private static SubjectAPIKey newAPIKey(SubjectIdentifier subject, String key) {
        SubjectAPIKey sak = new SubjectAPIKey();
        sak.setName("key-" + UUID.randomUUID());
        sak.setSystemID("shiro-ds-test");
        sak.setAPIKey(key);
        sak.setStatus(Const.Status.ACTIVE);
        dsm.createCredential(subject, sak);
        return sak;
    }

    /** Signing key (SYMMETRIC_KEY: the only kind that verifies a JWT login) with a random 32-byte secret and an explicit key ID, optionally scoped to domain/app. */
    private static SubjectAPIKey newSigningKey(SubjectIdentifier subject, String domainID, String appID) {
        SubjectAPIKey sak = new SubjectAPIKey();
        sak.setCredentialType(org.zoxweb.shared.security.CredentialInfo.Type.SYMMETRIC_KEY);
        sak.setName("jwt-key-" + UUID.randomUUID());
        sak.setSystemID("shiro-ds-test");
        sak.setPrincipalID("kid-" + UUID.randomUUID());
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        sak.setAPIKeyAsBytes(secret);
        sak.setStatus(Const.Status.ACTIVE);
        if (domainID != null) {
            sak.setAppID(appRecord(domainID, appID)); // a scoped key references the app record
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
        sys.update(pid);
        assertThrows(AccessSecurityException.class, () -> dsm.login(principal, PASSWORD));
        pid.setStatus(SecConst.SecStatus.ACTIVE);
        sys.update(pid);
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
        SubjectAPIKey sak = newAPIKey(subject, key);
        assertNotNull(dsm.lookupSubjectAPIKeyByID(sak.getSubjectID()));
        PermissionInfo perm = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "x:y"));
        dsm.addPermissionGrant(subject, perm);

        assertTrue(dsm.deleteSubjectID(subject));
        assertNull(dsm.lookupSubjectID(principal));
        assertNull(dsm.lookupPrincipalID(principal));
        assertEquals(0, dsm.lookupCredentialsBySubjectGUID(subject.getGUID(), null).length);
        assertNull(dsm.lookupSubjectAPIKeyByID(sak.getSubjectID()), "the key row went with the subject");
        assertEquals(0, dsm.getPermissionGrants(subject.getGUID()).length);
    }

    /**
     * An API key is the subject's credential for a third-party API (user rule 2026-10-03): sealed in
     * the store, served in clear to its logged-in owner through the datastore, to nobody else, and
     * never a login to this system — not as a raw key, and not as the secret behind a JWT.
     */
    @Test
    public void thirdPartyApiKey_sealedInTheStore_servedInClearToItsLoggedInOwner_neverALogin() throws Exception {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier owner = newSubject(pa);
        newSubject(pb);
        byte[] secretBytes = new byte[32];
        new SecureRandom().nextBytes(secretBytes);

        // the owner, logged in, stores the key of its third-party account
        login(pa);
        SubjectAPIKey sak = new SubjectAPIKey();
        sak.setName("claude.ai");
        sak.setSystemID("api.anthropic.com");
        sak.setAPIKeyAsBytes(secretBytes);
        sak.setStatus(Const.Status.ACTIVE);
        dsm.createCredential(owner, sak);
        final String secret = sak.getAPIKey(), guid = sak.getGUID(), keyID = sak.getSubjectID();
        assertEquals(org.zoxweb.shared.security.CredentialInfo.Type.API_KEY, sak.getCredentialType(),
                "a key is a third-party key unless it is made a signing key");
        assertFalse(sak.isSigningKey());

        // at rest: a sealed record, never the secret
        byte[] raw = rawApiKeyColumn(guid);
        assertNotNull(raw);
        assertFalse(new String(raw, java.nio.charset.StandardCharsets.ISO_8859_1).contains(secret), "the secret is not stored in the clear");

        // the logged-in owner looks the key up from the datastore, which serves it in clear
        List<SubjectAPIKey> mine = ds.search(SubjectAPIKey.NVC_SUBJECT_API_KEY, null,
                new QueryMatch<>(Const.RelationalOperator.EQUAL, owner.getGUID(), MetaToken.SUBJECT_GUID.getName()));
        assertEquals(1, mine.size());
        assertEquals("claude.ai", mine.get(0).getName());
        assertEquals(secret, mine.get(0).getAPIKey(), "the owner gets the key in clear");
        assertArrayEquals(secretBytes, mine.get(0).getAPIKeyAsBytes());
        assertEquals(org.zoxweb.shared.security.CredentialInfo.Type.API_KEY, mine.get(0).getCredentialType());

        // another logged-in subject, and nobody at all: no row
        login(pb);
        assertTrue(ds.searchByID(SubjectAPIKey.NVC_SUBJECT_API_KEY, guid).isEmpty(), "a stranger gets nothing");
        dsm.logout();
        assertTrue(ds.searchByID(SubjectAPIKey.NVC_SUBJECT_API_KEY, guid).isEmpty(), "nobody logged in gets nothing");

        // never a login: the raw key is refused, known or unknown
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(secret));
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey("key-" + UUID.randomUUID()));
        assertThrows(AccessSecurityException.class, () -> dsm.loginApiKey(null));
        // it does not sign a login token ...
        assertThrows(IllegalArgumentException.class, () -> ShiroDSDomainSecurityManager.mintJWT(sak, null, 30_000));
        // ... and a token forged by someone who knows the secret and the key id is refused
        SubjectAPIKey forged = new SubjectAPIKey();
        forged.setCredentialType(org.zoxweb.shared.security.CredentialInfo.Type.SYMMETRIC_KEY);
        forged.setPrincipalID(keyID);
        forged.setAPIKeyAsBytes(secretBytes);
        String token = ShiroDSDomainSecurityManager.mintJWT(forged, null, 30_000);
        assertThrows(AccessSecurityException.class, () -> dsm.loginJWT(token), "a third-party key never verifies a login");
        assertThrows(AccessSecurityException.class, () -> dsm.loginSubjectJWT(token, null));
        assertNull(org.apache.shiro.util.ThreadContext.getSubject(), "nothing bound");

        // the key cannot be turned into a signing key behind the owner's back either: the purpose is stored
        assertFalse(dsm.lookupSubjectAPIKeyByID(keyID).isSigningKey());
    }

    /** Raw {@code api_key} column of a key row, bypassing the datastore's read path. */
    private static byte[] rawApiKeyColumn(String guid) throws SQLException {
        try (Connection con = ds.newConnection();
             PreparedStatement ps = con.prepareStatement("SELECT \"api_key\" FROM \"subject_api_key\" WHERE \"guid\" = ?")) {
            ps.setObject(1, UUID.fromString(guid));
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getBytes(1) : null;
            }
        }
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
    public void jwt_loginSubject_bindsAndAuthorizes() {
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

        assertThrows(AccessSecurityException.class, () -> dsm.loginSubjectJWT("nope", null));
        assertNull(org.apache.shiro.util.ThreadContext.getSubject(), "failed logins must not leave a binding");
    }

    @Test
    public void apiKey_withoutKeyID_getsOneOnInsert() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        SubjectAPIKey sak = newAPIKey(subject, "key-" + UUID.randomUUID());
        assertNotNull(sak.getSubjectID(), "insert stamps a key ID");
        assertEquals(sak.getGUID(), dsm.lookupSubjectAPIKeyByID(sak.getSubjectID()).getGUID());
        SubjectAPIKey noID = new SubjectAPIKey();
        noID.setCredentialType(org.zoxweb.shared.security.CredentialInfo.Type.SYMMETRIC_KEY);
        assertThrows(IllegalArgumentException.class, () -> ShiroDSDomainSecurityManager.mintJWT(noID, null, 0), "no key ID");
        assertThrows(IllegalArgumentException.class, () -> ShiroDSDomainSecurityManager.mintJWT(sak, null, 0), "a third-party key signs nothing");
        assertThrows(IllegalArgumentException.class, () -> sak.setCredentialType(org.zoxweb.shared.security.CredentialInfo.Type.PASSWORD));
    }

    /**
     * The xlogistx-shiro {@code ShiroUtil} entry points go through {@code SecurityUtils}' global
     * security manager. With {@code installAsGlobal()} every one of them must work against this
     * realm: {@code login(domain, realm, user, password)}, {@code login(AuthenticationToken)} for
     * the password and JWT tokens (a raw API key token is refused: an API key is not a login),
     * {@code loginSubject(..., autoLogin)} and {@code loginBySessionID}.
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

            // login(AuthenticationToken): a raw key is not a login, even the key of an existing subject
            assertThrows(AccessSecurityException.class, () -> ShiroUtil.login(new APIKeyAuthenticationToken(rawKey)));
            assertFalse(SecurityUtils.getSubject().isAuthenticated());
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
        assertEquals(ShiroUtil.REALM_NAME, unbound.getName(), "no-arg defaults");
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

        // the cache key of a login with no domain/app: the subject in the common app (2026-10-01)
        String cacheKey = subject.getGUID() + "|" + ShiroDSDomainSecurityManager.COMMON_SCOPE;
        dsm.getRealm().evictAuthorization(subject.getGUID());
        dsm.login(principal, PASSWORD);
        assertNull(cache.get(cacheKey), "lazy: login alone caches nothing");

        dsm.setEagerAuthorization(true);
        try {
            dsm.login(principal, PASSWORD);
            AuthorizationInfo cached = cache.get(cacheKey);
            assertNotNull(cached, "eager: grants loaded and cached by the login itself");
            assertTrue(cached.getStringPermissions().contains(token));

            dsm.getRealm().evictAuthorization(subject.getGUID());
            assertThrows(AccessSecurityException.class, () -> dsm.login(principal, "wrong-" + PASSWORD));
            assertNull(cache.get(cacheKey), "a failed login must not load grants");

            Subject s = dsm.loginSubject(principal, PASSWORD, null, null);
            assertNotNull(cache.get(cacheKey), "bound login warms the cache too");
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
        return appRecord(DOMAIN, name);
    }

    /** The app record, created with its starter catalog and registrar when the test store does not have it yet. */
    private static AppIDDefault appRecord(String domainID, String appID) {
        AppIDDefault stored = dsm.lookupApp(domainID, appID);
        return stored != null ? stored : dsm.createApp(domainID, appID);
    }

    /** A role of the app's own catalog (every app has its own copy of the starter roles). */
    private static RoleInfo appRole(AppIDDefault app, SecurityModel.Role role) {
        RoleInfo ret = dsm.lookupRole(ShiroUtil.appScope(app), role.getName());
        assertNotNull(ret, role.getName() + " in " + ShiroUtil.appScope(app));
        return ret;
    }

    private static Subject loginApp(String principal, String appName) {
        dsm.logout();
        return dsm.loginSubject(principal, PASSWORD, DOMAIN, appName);
    }

    @Test
    public void appGrant_loginScopeSelectsGrants_andRevokeAppRemovesThem() {
        RoleInfo userRole = dsm.lookupRole(null, SecurityModel.Role.USER.getName()); // the common app's
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        AppIDDefault a = app("appa");
        RoleInfo appAdmin = appRole(a, SecurityModel.Role.APP_ADMIN); // the app's own role: the common one is not grantable in it
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGrant(subject, dsm.lookupRole(null, SecurityModel.Role.APP_ADMIN.getName()), a),
                "a role of the common app can not be granted in another app");
        RoleGrant grant = dsm.addRoleGrant(subject, appAdmin, a);
        dsm.addRoleGrant(subject, userRole); // no app = the common app
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGrant(subject, appAdmin), "an app's role is not grantable in the common app");
        assertNotNull(grant.getGUID());
        assertNull(grant.getBrokerGUID(), "nobody bound -> no broker");
        RoleGrant[] scoped = dsm.getRoleGrants(subject.getGUID(), a);
        assertEquals(1, scoped.length);
        assertEquals(a, scoped[0].getAppID(), "app_id round-trips through the store");
        assertEquals(0, dsm.getRoleGrants(subject.getGUID(), app("other")).length);
        assertEquals("xlogistx.io-appa", ShiroUtil.appScope(a));

        // a login with no domain/app is a login into the common app: the common grants only
        dsm.logout();
        Subject global = dsm.loginSubject(principal, PASSWORD, null, null);
        assertTrue(global.hasRole("user"));
        assertFalse(global.hasRole("app_admin"), "the app grant needs an app login");
        assertFalse(global.isPermitted("subject:create"));
        dsm.logout();
        Subject inCommon = dsm.loginSubject(principal, PASSWORD, ShiroDSDomainSecurityManager.COMMON_DOMAIN_ID, ShiroDSDomainSecurityManager.COMMON_APP_ID);
        assertTrue(inCommon.hasRole("user"), "an explicit login into the common app sees the same grants");
        assertFalse(inCommon.hasRole("app_admin"));

        // app login: that app's grants only, as plain tokens
        Subject inA = loginApp(principal, "appa");
        assertTrue(inA.hasRole("app_admin"));
        assertTrue(inA.isPermitted("subject:create"));
        assertFalse(inA.hasRole("user"), "the common grants stay out of an app login");

        // another app: nothing
        Subject inOther = loginApp(principal, "other");
        assertFalse(inOther.hasRole("app_admin"));
        assertFalse(inOther.isPermitted("subject:create"));
        assertFalse(inOther.hasRole("user"));

        Subject again = loginApp(principal, "appa");
        assertTrue(again.isPermitted("subject:create"));
        assertEquals(1, dsm.revokeAppGrants(subject.getGUID(), a));
        assertFalse(again.isPermitted("subject:create"), "the app-scoped cache entry is evicted on revoke");
        assertFalse(again.hasRole("app_admin"));
        assertEquals(0, dsm.revokeAppGrants(subject.getGUID(), a), "nothing left");
        dsm.logout();
        assertTrue(dsm.loginSubject(principal, PASSWORD, null, null).hasRole("user"), "global grant untouched");
        dsm.logout();
    }

    @Test
    public void appGrant_scopedRoleGroup_appliesInThatAppLoginOnly() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);
        AppIDDefault b = app("appb");
        RoleGroupInfo appUsers = dsm.lookupRoleGroup(ShiroUtil.appScope(b), SecurityModel.RoleGroup.APP_USERS.getName());
        assertNotNull(appUsers, "the app's own role group");
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGroupGrant(subject,
                dsm.lookupRoleGroup(null, SecurityModel.RoleGroup.APP_USERS.getName()), b), "the common group is not grantable in another app");
        RoleGroupGrant g = dsm.addRoleGroupGrant(subject, appUsers, b);
        assertEquals(b, g.getAppID());
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
        AppIDDefault a = app("appc");
        AppIDDefault b = app("appd");
        RoleInfo appAdmin = appRole(a, SecurityModel.Role.APP_ADMIN);
        RoleInfo appUser = appRole(a, SecurityModel.Role.APP_USER);
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
    // app model (user decisions 2026-10-01/02): every app owns its catalog, has a registrar,
    // no domain/app = the common app; plan xlogistx-shiro-ds/app-model.md
    // ------------------------------------------------------------------

    /**
     * A registrar sign-up as an application runs it: the registrar is logged in with a JWT signed by
     * its key, without being bound; the work runs inside a {@link io.xlogistx.shiro.SubjectSwap};
     * the registrar is logged out.
     */
    private static <V> V asRegistrar(SubjectAPIKey key, java.util.concurrent.Callable<V> work) throws Exception {
        Subject registrar = dsm.loginUnboundSubjectJWT(ShiroDSDomainSecurityManager.mintJWT(key, null, 5 * 60_000L), null);
        try (io.xlogistx.shiro.SubjectSwap swap = new io.xlogistx.shiro.SubjectSwap(registrar)) {
            return work.call();
        } finally {
            registrar.logout();
        }
    }

    private static String currentSubjectGUID() {
        return DSAuthorizingRealm.subjectGUIDOf(SecurityUtils.getSubject().getPrincipals());
    }

    @Test
    public void app_createGivesStarterCatalogAndRegistrar_deleteRemovesThem() {
        String name = "reg" + Long.toHexString(System.nanoTime());
        String managerPrincipal = uniquePrincipal();
        SubjectIdentifier manager = newSubject(managerPrincipal);
        ShiroDSDomainSecurityManager.AppCreation created = dsm.createApp(DOMAIN, name, manager);
        AppIDDefault app = created.app;
        String label = ShiroUtil.appScope(app);
        assertEquals(DOMAIN + "-" + name, label);
        assertEquals(label, app.getName());
        assertEquals(app.getGUID(), dsm.lookupApp(DOMAIN, name).getGUID());

        // the starter set: the non platform-only roles with their permissions, and their groups — the app's own rows
        for (SecurityModel.Role r : SecurityModel.Role.values()) {
            RoleInfo row = dsm.lookupRole(label, r.getName());
            assertEquals(!r.isPlatformOnly(), row != null, r.getName() + " in " + label);
            if (row != null) {
                assertEquals(app.getGUID(), row.getAppID().getGUID(), "owned by the app");
                assertEquals(r.getPermissions().length, row.getPermissions().length, r.getName());
                for (PermissionInfo p : row.getPermissions()) {
                    assertEquals(app.getGUID(), dsm.lookupPermissionByGUID(p.getGUID()).getAppID().getGUID(), "its permissions are the app's copies");
                }
            }
        }
        for (SecurityModel.RoleGroup g : SecurityModel.RoleGroup.values()) {
            assertEquals(!g.isPlatformOnly(), dsm.lookupRoleGroup(label, g.getName()) != null, g.getName());
        }
        assertNotNull(dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName()), "the common app keeps the platform-only roles");
        assertNotNull(dsm.lookupRole(null, SecurityModel.Role.APP_REGISTRAR.getName()), "and has a registrar role of its own");

        // the registrar: a SYSTEM subject, a key scoped to the app (secret readable here, once), the app_registrar role
        assertNotNull(created.registrar);
        assertEquals(BaseSubjectID.SubjectType.SYSTEM, created.registrar.getSubjectType());
        assertEquals("registrar." + label, ShiroDSDomainSecurityManager.registrarPrincipal(app));
        assertEquals(created.registrar.getGUID(), dsm.lookupRegistrar(app).getGUID());
        assertEquals(created.registrar.getGUID(), dsm.lookupSubjectID("registrar." + label).getGUID());
        assertNotNull(created.registrarKey);
        assertNotNull(created.registrarKey.getAPIKeyAsBytes());
        assertEquals(app, created.registrarKey.getAppID());
        assertNull(dsm.ensureRegistrar(app).registrarKey, "created once: no secret the second time");
        assertEquals(created.registrar.getGUID(), dsm.ensureRegistrar(app).registrar.getGUID());
        Subject reg = dsm.loginSubjectJWT(ShiroDSDomainSecurityManager.mintJWT(created.registrarKey, null, 60_000), null);
        assertTrue(reg.hasRole(SecurityModel.Role.APP_REGISTRAR.getName()));
        assertTrue(reg.isPermitted(SecurityModel.PERM_ADD_SUBJECT));
        assertFalse(reg.isPermitted(SecurityModel.PERM_ASSIGN_ROLE));
        assertFalse(reg.isPermitted(SecurityModel.PERM_UPDATE_SUBJECT));
        assertFalse(reg.isPermitted(SecurityModel.PERM_CREATE_APP_ID));
        dsm.logout();

        // the first manager holds the app's app_admin, which manages the app's catalog
        assertNotNull(created.firstManagerGrant);
        assertEquals(app.getGUID(), created.firstManagerGrant.getAppID().getGUID());
        Subject inApp = loginApp(managerPrincipal, name);
        assertTrue(inApp.hasRole(SecurityModel.Role.APP_ADMIN.getName()));
        assertTrue(inApp.isPermitted(SecurityModel.PERM_ADD_ROLE));
        assertTrue(inApp.isPermitted(SecurityModel.PERM_ADD_PERMISSION));
        assertFalse(inApp.isPermitted(SecurityModel.PERM_CREATE_APP_ID));
        dsm.logout();

        // delete: the record, its catalog, its registrar and the grants in it go; the common app never does
        assertThrows(IllegalArgumentException.class, () -> dsm.deleteApp(dsm.commonApp()));
        assertTrue(dsm.deleteApp(app));
        assertNull(dsm.lookupApp(DOMAIN, name));
        assertNull(dsm.lookupRegistrar(app));
        assertNull(dsm.lookupRole(label, SecurityModel.Role.APP_ADMIN.getName()));
        assertEquals(0, dsm.lookupAllPermissionsByAppID(label).length);
        assertEquals(0, dsm.getRoleGrants(manager.getGUID(), app).length);
        assertNotNull(dsm.lookupSubjectByGUID(manager.getGUID()), "a subject that held a grant in the app stays");
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGrant(manager, dsm.lookupRole(null, SecurityModel.Role.USER.getName()), app), "unknown app");

        // the canonical id splits at the last separator: a hyphenated domain survives
        AppIDDefault hyphenated = AppIDDefault.create("my-site.com-shop");
        assertEquals("my-site.com", hyphenated.getDomainID());
        assertEquals("shop", hyphenated.getAppID());
    }

    @Test
    public void registrar_signUpThroughTheSubjectSwap_createsOrJoins_andNothingElse() throws Exception {
        String name = "signup" + Long.toHexString(System.nanoTime());
        ShiroDSDomainSecurityManager.AppCreation created = dsm.createApp(DOMAIN, name, null);
        AppIDDefault app = created.app;
        SubjectAPIKey key = created.registrarKey;
        String newcomer = uniquePrincipal();
        String existingPrincipal = uniquePrincipal();
        SubjectIdentifier existing = newSubject(existingPrincipal);

        dsm.setEnforcePermissions(true);
        try {
            // a sign-up is a subject creation: nobody logged in may not
            assertThrows(AccessSecurityException.class, () -> dsm.registerSubject(newcomer, PASSWORD));

            // the swap: the registrar is bound for the work only, the thread's previous subject comes back
            dsm.loginSubject(existingPrincipal, PASSWORD, null, null);
            SubjectIdentifier made = asRegistrar(key, () -> {
                assertTrue(SecurityUtils.getSubject().hasRole(SecurityModel.Role.APP_REGISTRAR.getName()), "the registrar is the bound subject");
                assertEquals(created.registrar.getGUID(), currentSubjectGUID());
                return dsm.registerSubject(newcomer, PASSWORD);
            });
            assertEquals(existing.getGUID(), currentSubjectGUID(), "the previous subject is bound again");
            dsm.logout();
            assertNotNull(made);
            assertEquals(made.getGUID(), dsm.lookupSubjectID(newcomer).getGUID());
            assertEquals(BaseSubjectID.SubjectType.USER, made.getSubjectType());
            Subject in = loginApp(newcomer, name);
            assertTrue(in.hasRole(SecurityModel.Role.APP_USER.getName()), "the app's app_user, always that one");
            assertFalse(in.hasRole(SecurityModel.Role.APP_ADMIN.getName()));
            dsm.logout();
            RoleGrant[] grants = dsm.getRoleGrants(made.getGUID(), app);
            assertEquals(1, grants.length);
            assertEquals(created.registrar.getGUID(), grants[0].getBrokerGUID(), "the registrar is recorded as the grantor");

            // again: no second subject, no second grant
            assertEquals(made.getGUID(), asRegistrar(key, () -> dsm.registerSubject(newcomer, PASSWORD)).getGUID());
            assertEquals(1, dsm.getRoleGrants(made.getGUID(), app).length);

            // an existing account joins the app with its password, and only with it
            assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.registerSubject(existingPrincipal, "wrong-" + PASSWORD)));
            assertEquals(0, dsm.getRoleGrants(existing.getGUID(), app).length, "nothing granted on a failed sign-up");
            assertEquals(existing.getGUID(), asRegistrar(key, () -> dsm.registerSubject(existingPrincipal, PASSWORD)).getGUID());
            assertTrue(loginApp(existingPrincipal, name).hasRole(SecurityModel.Role.APP_USER.getName()));
            dsm.logout();

            // the registrar may do nothing else
            assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.createApp(DOMAIN, name + "x")));
            assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.addRoleGrant(made, appRole(app, SecurityModel.Role.APP_ADMIN), app)));
            assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.deleteSubjectID(made)));
            assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.rotateRegistrarKey(app)));

            // a failure inside the swap restores the thread as well
            dsm.loginSubject(existingPrincipal, PASSWORD, null, null);
            assertThrows(IllegalStateException.class, () -> asRegistrar(key, () -> {
                throw new IllegalStateException("boom");
            }));
            assertEquals(existing.getGUID(), currentSubjectGUID());
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.registerSubject(uniquePrincipal(), PASSWORD), "nothing left bound after the swap");
        } finally {
            dsm.setEnforcePermissions(false);
            dsm.logout();
        }

        // rotation: the old key is dead, the new one signs up
        SubjectAPIKey rotated = dsm.rotateRegistrarKey(app);
        assertNotNull(rotated.getAPIKeyAsBytes());
        assertFalse(java.util.Arrays.equals(key.getAPIKeyAsBytes(), rotated.getAPIKeyAsBytes()));
        assertThrows(AccessSecurityException.class, () -> asRegistrar(key, () -> dsm.registerSubject(uniquePrincipal(), PASSWORD)), "the old key");
        String late = uniquePrincipal();
        assertNotNull(asRegistrar(rotated, () -> dsm.registerSubject(late, PASSWORD)));
        assertTrue(loginApp(late, name).hasRole(SecurityModel.Role.APP_USER.getName()));
        dsm.logout();

        // with nobody logged in and enforcement off, a sign-up lands in the common app
        String common = uniquePrincipal();
        SubjectIdentifier inCommon = dsm.registerSubject(common, PASSWORD);
        assertEquals(1, dsm.getRoleGrants(inCommon.getGUID(), dsm.commonApp()).length);
        assertTrue(dsm.loginSubject(common, PASSWORD, null, null).hasRole(SecurityModel.Role.APP_USER.getName()));
        dsm.logout();
    }

    @Test
    public void appCatalog_isolated_managerWritesItsOwnAppOnly_sharesFollowTheData() {
        String nameA = "isoa" + Long.toHexString(System.nanoTime()), nameB = "isob" + Long.toHexString(System.nanoTime());
        String adminPrincipal = uniquePrincipal(), userPrincipal = uniquePrincipal();
        SubjectIdentifier admin = newSubject(adminPrincipal);
        SubjectIdentifier user = newSubject(userPrincipal);
        AppIDDefault a = dsm.createApp(DOMAIN, nameA, admin).app;
        AppIDDefault b = dsm.createApp(DOMAIN, nameB, null).app;
        String labelA = ShiroUtil.appScope(a), labelB = ShiroUtil.appScope(b);

        // a role of A may carry A's permissions only; a group of A, A's roles only
        PermissionInfo readOfB = dsm.lookupPermission(labelB, SecurityModel.Permission.SUBJECT_READ.getName());
        RoleInfo mixed = new RoleInfo("mixed-" + UUID.randomUUID(), "a's role with b's permission", readOfB);
        mixed.setAppID(a);
        assertThrows(IllegalArgumentException.class, () -> dsm.createRole(mixed));
        RoleGroupInfo mixedGroup = new RoleGroupInfo(appRole(b, SecurityModel.Role.USER));
        mixedGroup.setName("mixed-group-" + UUID.randomUUID());
        mixedGroup.setAppID(a);
        assertThrows(IllegalArgumentException.class, () -> dsm.createRoleGroup(mixedGroup));

        dsm.setEnforcePermissions(true);
        try {
            loginApp(adminPrincipal, nameA);
            // the manager of A creates A's permission and role, stamped as broker
            PermissionInfo ownPerm = new PermissionInfo("report.read." + UUID.randomUUID().toString().replace("-", ""), "report:read");
            ownPerm.setAppID(a);
            ownPerm = dsm.createPermission(ownPerm);
            assertEquals(a.getGUID(), ownPerm.getAppID().getGUID());
            assertEquals(admin.getGUID(), ownPerm.getBrokerGUID());
            RoleInfo ownRole = new RoleInfo("reporter-" + UUID.randomUUID(), "A's reporter", ownPerm);
            ownRole.setAppID(a);
            ownRole = dsm.createRole(ownRole);
            assertEquals(a.getGUID(), ownRole.getAppID().getGUID());
            assertEquals(ownRole.getGUID(), dsm.lookupRole(labelA, ownRole.getName()).getGUID());
            assertNull(dsm.lookupRole(labelB, ownRole.getName()), "nothing of A exists in B");
            assertNull(dsm.lookupRole(null, ownRole.getName()), "nor in the common app");

            // not in B, not in the common app
            PermissionInfo inB = new PermissionInfo("report.read.b." + UUID.randomUUID().toString().replace("-", ""), "report:read");
            inB.setAppID(b);
            assertThrows(AccessSecurityException.class, () -> dsm.createPermission(inB));
            PermissionInfo inCommon = new PermissionInfo("report.read.c." + UUID.randomUUID().toString().replace("-", ""), "report:read");
            assertThrows(AccessSecurityException.class, () -> dsm.createPermission(inCommon), "no app = the common app, not the manager's");
            RoleInfo roleOfB = appRole(b, SecurityModel.Role.APP_USER);
            assertThrows(AccessSecurityException.class, () -> dsm.updateRole(roleOfB));
            assertThrows(AccessSecurityException.class, () -> dsm.deleteRole(roleOfB));

            // A's role is grantable in A, and only there
            RoleGrant grant = dsm.addRoleGrant(user, ownRole, a);
            assertEquals(a.getGUID(), grant.getAppID().getGUID());
            RoleInfo finalRole = ownRole;
            assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(user, finalRole, b), "A's manager may not grant in B");
            dsm.logout();
            dsm.loginSubject(userPrincipal, PASSWORD, DOMAIN, nameA);
            assertTrue(SecurityUtils.getSubject().isPermitted("report:read"));
            assertTrue(SecurityUtils.getSubject().hasRole(ownRole.getName()));
            dsm.logout();
            dsm.loginSubject(userPrincipal, PASSWORD, DOMAIN, nameB);
            assertFalse(SecurityUtils.getSubject().isPermitted("report:read"));
            dsm.logout();
        } finally {
            dsm.setEnforcePermissions(false);
            dsm.logout();
        }
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGrant(user, dsm.lookupRole(labelA, SecurityModel.Role.APP_USER.getName()), b),
                "even unenforced, a role is grantable in its own app only");

        // a share follows the data: granted on the resource, it applies in every login scope
        PropertyDAO file = newResource(admin);
        dsm.addPermissionGrant(user, mapOf(file), "resource:read");
        String token = SecurityModel.toResourceToken(file.getGUID(), user.getGUID(), "read");
        assertTrue(dsm.loginSubject(userPrincipal, PASSWORD, DOMAIN, nameA).isPermitted(token));
        dsm.logout();
        assertTrue(dsm.loginSubject(userPrincipal, PASSWORD, DOMAIN, nameB).isPermitted(token));
        dsm.logout();
        assertTrue(dsm.loginSubject(userPrincipal, PASSWORD, null, null).isPermitted(token));
        dsm.logout();

        assertTrue(dsm.deleteApp(a));
        assertTrue(dsm.deleteApp(b));
    }

    // ------------------------------------------------------------------
    // instance grants and sharing (design page section 12, item 23)
    // ------------------------------------------------------------------

    /** A "file": any persisted entity with a subject_guid owner; PropertyDAO is the lightest. */
    private static PropertyDAO newResource(SubjectIdentifier owner) {
        PropertyDAO p = new PropertyDAO();
        p.setName("file-" + UUID.randomUUID());
        p.setSubjectGUID(owner.getGUID());
        return sys.insert(p);
    }

    private static ResourceMap mapOf(NVEntity resource) {
        return new ResourceMap(resource);
    }

    /** The standardized token the checker evaluates for {@code subject}: resource:<guid>:<subject guid>:<verb>. */
    private static String nve(Subject subject, String verb, String guid) {
        return SecurityModel.toResourceToken(guid, DSAuthorizingRealm.subjectGUIDOf(subject.getPrincipals()), verb);
    }

    private static Subject login(String principal) {
        dsm.logout();
        return dsm.loginSubject(principal, PASSWORD, null, null);
    }

    private static boolean mapRowExists(String mapGUID) {
        return !sys.searchByID(ResourceMap.NVC_RESOURCE_MAP, mapGUID).isEmpty();
    }

    @Test
    public void share_inlinedGrant_permitsGranteeOnThatResourceOnly() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a), y = newResource(a);
        assertEquals(a.getGUID(), x.getSubjectGUID(), "resource must carry its owner");

        Subject shiroB = login(pb);
        assertFalse(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())));

        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "resource:read,share");
        assertEquals("resource:read,share", grant.getPermissionToken());
        assertNull(grant.getPermissionGUID());
        assertEquals(b.getGUID(), grant.getSubjectGUID());
        assertNotNull(grant.getResourceMap().getGUID(), "map row gets its own GUID");
        assertEquals(x.getGUID(), grant.getResourceMap().getResourceGUID());
        assertEquals(PropertyDAO.class.getName(), grant.getResourceMap().getResourceType());
        assertTrue(mapRowExists(grant.getResourceMap().getGUID()));

        assertTrue(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())), "read on X granted");
        assertTrue(shiroB.isPermitted(nve(shiroB, "share", x.getGUID())), "share on X granted");
        assertFalse(shiroB.isPermitted(nve(shiroB, "update", x.getGUID())), "update on X not granted");
        assertFalse(shiroB.isPermitted(nve(shiroB, "delete", x.getGUID())));
        assertFalse(shiroB.isPermitted(nve(shiroB, "read", y.getGUID())), "Y not shared");
        assertFalse(shiroB.isPermitted("resource:read"), "no global read");

        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains(SecurityModel.toResourceToken(x.getGUID(), b.getGUID(), "read,share")));

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

        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), " Resource:Read , Share ");
        assertEquals("resource:read,share", grant.getPermissionToken());

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())));
        assertTrue(shiroB.isPermitted("RESOURCE:" + x.getGUID().toUpperCase() + ":" + b.getGUID().toUpperCase() + ":READ"),
                "ShiroUtil lower-cases checks; direct checks are case-insensitive by Shiro");
        // the synthesized self permission: B owns nothing here, but holds resource:B:B:read,update,delete,share
        assertTrue(shiroB.isPermitted(SecurityModel.toResourceToken(b.getGUID(), b.getGUID(), "delete")), "self permission");
        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions
                .contains(SecurityModel.toResourceToken(b.getGUID(), b.getGUID(), SecurityModel.RESOURCE_SELF_VERBS)));
    }

    @Test
    public void share_invalidTokensRejected() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);

        String[] bad = {"resource:create", "resource:*", "resource:read:" + x.getGUID(), "doc:read", "resource:",
                "resource:read,,share", "resource:read,bogus", "resource", "", null};
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
                () -> dsm.addPermissionGrant(b, new ResourceMap(UUID.randomUUID().toString(), PropertyDAO.class.getName()), "resource:read"),
                "unknown GUID");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(x.getGUID(), "com.example.NoSuchClass"), "resource:read"),
                "unknown class");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(x.getGUID(), null), "resource:read"),
                "blank type");
        assertThrows(IllegalArgumentException.class,
                () -> dsm.addPermissionGrant(b, new ResourceMap(null, PropertyDAO.class.getName()), "resource:read"),
                "blank guid");
        assertEquals(0, dsm.getPermissionGrants(b.getGUID()).length);
    }

    @Test
    public void share_catalogScopedGrant_flattensWithGuid() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));

        PermissionGrant grant = dsm.addPermissionGrant(b, update, mapOf(x));
        assertEquals(update.getGUID(), grant.getPermissionGUID());
        assertNull(grant.getPermissionToken());
        assertEquals(x.getGUID(), grant.getResourceMap().getResourceGUID());

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "update", x.getGUID())));
        assertFalse(shiroB.isPermitted(nve(shiroB, "update", y.getGUID())));
        assertFalse(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())));
        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains(SecurityModel.toResourceToken(x.getGUID(), b.getGUID(), "update")));

        PermissionInfo scoped = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:read:abc"));
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, scoped, mapOf(x)),
                "a token that already has an instance part cannot be scoped");
        PermissionInfo onePart = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource"));
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, onePart, mapOf(x)));
        PermissionInfo unsaved = new PermissionInfo("perm." + UUID.randomUUID(), "resource:read");
        unsaved.setGUID(UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, unsaved, mapOf(x)), "unknown catalog row");
    }

    @Test
    public void share_globalCatalogGrant_unchanged() {
        String pb = uniquePrincipal();
        SubjectIdentifier b = newSubject(pb);
        PermissionInfo read = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), SecurityModel.toResourceToken("*", "*", "read"))); // global: resource:*:*:read

        dsm.logout();
        PermissionGrant grant = dsm.addPermissionGrant(b, read);
        assertNull(grant.getResourceMap());
        assertNull(grant.getPermissionToken());
        assertNull(grant.getBrokerGUID(), "nobody bound -> no grantor recorded");

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "read", UUID.randomUUID().toString())), "global read implies every instance");
        assertTrue(GrantFlattener.flatten(dsm, b.getGUID()).permissions.contains("resource:*:*:read"));

        // exclusivity: a grant carrying both forms is refused before anything is written
        PermissionGrant both = new PermissionGrant(read.getGUID(), mapOf(newResource(b)));
        both.setPermissionToken("resource:read");
        assertThrows(IllegalArgumentException.class, both::validateShape);
    }

    @Test
    public void share_revoke_evictsAndDeletesMapRow() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
        String mapGUID = grant.getResourceMap().getGUID();

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())));

        assertTrue(dsm.deletePermissionGrant(grant));
        assertFalse(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())), "revocation must evict the cached authorization");
        assertFalse(mapRowExists(mapGUID), "map row deleted with the grant");
        assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, grant.getGUID()).isEmpty());
        assertFalse(dsm.deletePermissionGrant(grant), "second revoke finds nothing");

        // a shell with only the GUID still cascades to the stored map
        PermissionGrant again = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
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
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "resource:read"), "nobody bound");
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "resource:read"), "B does not own X");
            assertEquals(0, dsm.getPermissionGrants(c.getGUID()).length);

            login(pa);
            PermissionGrant grant = dsm.addPermissionGrant(c, mapOf(x), "resource:read");
            assertEquals(a.getGUID(), grant.getBrokerGUID(), "grantor recorded");
            assertEquals(c.getGUID(), grant.getSubjectGUID());
        } finally {
            dsm.setEnforcePermissions(false);
        }
        Subject shiroC = login(pc);
        assertTrue(shiroC.isPermitted(nve(shiroC, "read", x.getGUID())));
    }

    @Test
    public void share_enforcement_shareHolderCanGrant_inlineAndCatalog() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));
        // A shares X with B including the share verb (enforcement off: nobody needs to be bound)
        dsm.addPermissionGrant(b, mapOf(x), "resource:read,share");

        dsm.setEnforcePermissions(true);
        try {
            login(pb);
            PermissionGrant grant = dsm.addPermissionGrant(c, update, mapOf(x));
            assertEquals(b.getGUID(), grant.getBrokerGUID());
            // user decision 2026-09-29: share is a permission — a holder of resource:X:B:share may re-share, inlined too
            PermissionGrant reshare = dsm.addPermissionGrant(c, mapOf(x), "resource:read");
            assertEquals(b.getGUID(), reshare.getBrokerGUID(), "a share holder may create an inlined grant");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, update, mapOf(y)),
                    "no share on Y and no global assign");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(c, update), "global grant needs the assign permission");
            // C received read (inlined) + update (catalog) on X but no share verb: C cannot share X
            login(pc);
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(b, mapOf(x), "resource:read"),
                    "a grantee without the share verb may not share");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(b, update, mapOf(x)),
                    "nor catalog-grant on it");
        } finally {
            dsm.setEnforcePermissions(false);
        }
        Subject shiroC = login(pc);
        assertTrue(shiroC.isPermitted(nve(shiroC, "update", x.getGUID())));
        assertFalse(shiroC.isPermitted(nve(shiroC, "update", y.getGUID())));
    }

    @Test
    public void share_enforcement_grantorOrOwnerCanRevoke() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), padmin = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), admin = newSubject(padmin);
        PropertyDAO x = newResource(a);
        PermissionInfo canRemove = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), SecurityModel.PERM_REMOVE_PERMISSION));
        dsm.addPermissionGrant(admin, canRemove);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));

        login(pa);
        PermissionGrant aToB = dsm.addPermissionGrant(b, mapOf(x), "resource:read"); // no share verb
        assertEquals(a.getGUID(), aToB.getBrokerGUID());

        dsm.setEnforcePermissions(true);
        try {
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "nobody bound");
            login(pc);
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "C is nobody here");
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.deletePermissionGrant(aToB), "a grantee without share cannot revoke its own grant");
            login(pa);
            assertTrue(dsm.deletePermissionGrant(aToB), "the grantor revokes");

            // B (share holder) grants C; the owner A, not the grantor, revokes (owner = self permission)
            PermissionGrant aToB2 = dsm.addPermissionGrant(b, mapOf(x), "resource:share");
            login(pb);
            PermissionGrant bToC = dsm.addPermissionGrant(c, update, mapOf(x));
            login(pa);
            assertTrue(dsm.deletePermissionGrant(bToC), "the resource owner revokes");

            // a share holder may revoke a grant on the resource it may share (decision 2026-09-29)
            login(pa);
            PermissionGrant aToC = dsm.addPermissionGrant(c, mapOf(x), "resource:read");
            login(pb);
            assertTrue(dsm.deletePermissionGrant(aToC), "share holder revokes");

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

    /**
     * The standardized resource check end to end (user decision 2026-09-29): ownership is the
     * synthesized self permission, a share is a grant token, a stranger holds neither — through
     * {@code ShiroUtil.checkResourcePermission} and the {@code SecurityController} the datastores call.
     */
    @Test
    public void resource_checkResourcePermission_ownerGranteeStranger() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc);
        PropertyDAO x = newResource(a);
        dsm.addPermissionGrant(b, mapOf(x), "resource:read,share");
        io.xlogistx.shiro.mgt.ShiroSecurityController controller = new io.xlogistx.shiro.mgt.ShiroSecurityController();

        // nobody bound: denied, never an exception from the boolean forms
        dsm.logout();
        assertFalse(controller.isNVEntityAccessible(x.getGUID(), x.getSubjectGUID(), org.zoxweb.shared.util.CRUD.READ));
        assertNull(controller.currentSubjectGUID());

        // owner: every self verb, through the self permission (no grant row exists for A)
        login(pa);
        for (String verb : new String[]{"create", "read", "update", "delete", "share"}) {
            assertEquals(a.getGUID(), ShiroUtil.checkResourcePermission(x, verb), verb);
        }
        assertEquals(a.getGUID(), controller.checkNVEntityAccess(x, org.zoxweb.shared.util.CRUD.READ, org.zoxweb.shared.util.CRUD.UPDATE));
        assertTrue(controller.isNVEntityAccessible(x.getGUID(), x.getSubjectGUID(), org.zoxweb.shared.util.CRUD.DELETE));
        assertEquals(a.getGUID(), controller.currentSubjectGUID());
        assertTrue(ShiroUtil.isResourcePermitted(a.getGUID(), a.getGUID(), "create"), "create is a self verb: a subject creates its own rows");
        assertFalse(ShiroUtil.isResourcePermitted(b.getGUID(), b.getGUID(), "create"), "but not rows owned by someone else");

        // grantee: read and share, nothing else; the owner GUID comes back as the key-chain root
        login(pb);
        assertFalse(ShiroUtil.isResourcePermitted(x.getGUID(), x.getSubjectGUID(), "create"), "a share never carries create");
        assertEquals(a.getGUID(), ShiroUtil.checkResourcePermission(x, "read"));
        assertEquals(a.getGUID(), controller.checkNVEntityAccess(Const.LogicalOperator.OR, x,
                org.zoxweb.shared.util.CRUD.UPDATE, org.zoxweb.shared.util.CRUD.READ), "OR: read suffices");
        assertThrows(AccessSecurityException.class, () -> controller.checkNVEntityAccess(x,
                org.zoxweb.shared.util.CRUD.UPDATE, org.zoxweb.shared.util.CRUD.READ), "AND: update missing");
        assertThrows(AccessSecurityException.class, () -> ShiroUtil.checkResourcePermission(x, "delete"));
        assertTrue(controller.isNVEntityAccessible(x.getGUID(), x.getSubjectGUID(), org.zoxweb.shared.util.CRUD.READ));
        assertFalse(controller.isNVEntityAccessible(x.getGUID(), x.getSubjectGUID(), org.zoxweb.shared.util.CRUD.UPDATE));

        // stranger: nothing
        login(pc);
        assertThrows(AccessSecurityException.class, () -> ShiroUtil.checkResourcePermission(x, "read"));
        assertFalse(controller.isNVEntityAccessible(x.getGUID(), x.getSubjectGUID(), org.zoxweb.shared.util.CRUD.READ));
        // but its own resource is fine
        PropertyDAO own = newResource(c);
        assertEquals(c.getGUID(), controller.checkNVEntityAccess(own, org.zoxweb.shared.util.CRUD.UPDATE));
    }

    /**
     * Datastore access control end to end (user rule 2026-09-30, built 2026-10-02): with the Shiro
     * controller on the store, every read and write through it is a resource permission check for
     * the bound subject — plaintext rows included — while the manager (lookups, subject creation,
     * login and its grant loading, sharing) keeps working through its system view of the store.
     */
    @Test
    public void datastoreAccessControl_endToEnd_withTheShiroController() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        try {
            assertTrue(ds.isAccessControlActive());

            // nobody logged in: the store gives nothing and takes nothing ...
            dsm.logout();
            assertTrue(ds.searchByID(SubjectIdentifier.class.getName(), a.getGUID()).isEmpty());
            PropertyDAO anonymous = new PropertyDAO();
            anonymous.setName("acl-anonymous-" + UUID.randomUUID());
            assertThrows(AccessSecurityException.class, () -> ds.insert(anonymous));
            // ... yet the manager works with nobody logged in: lookups, a new subject, a login
            assertEquals(a.getGUID(), dsm.lookupSubjectID(pa).getGUID());
            SubjectIdentifier c = newSubject(pc);
            assertNotNull(dsm.login(pc, PASSWORD));

            // a logged-in subject: its own rows, its own subject record, nobody else's
            login(pa);
            PropertyDAO x = new PropertyDAO();
            x.setName("acl-file-" + UUID.randomUUID());
            x = ds.insert(x);
            final PropertyDAO stored = x;
            final String xGUID = x.getGUID();
            assertEquals(a.getGUID(), x.getSubjectGUID(), "a new row belongs to the bound subject");
            assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID).size());
            assertEquals(1, ds.searchByID(SubjectIdentifier.class.getName(), a.getGUID()).size(), "a subject owns its own record");
            assertTrue(ds.searchByID(SubjectIdentifier.class.getName(), b.getGUID()).isEmpty(), "and reads no other subject's");
            PropertyDAO planted = new PropertyDAO();
            planted.setName("acl-planted-" + UUID.randomUUID());
            planted.setSubjectGUID(b.getGUID());
            assertThrows(AccessSecurityException.class, () -> ds.insert(planted), "no rows in somebody else's name");

            // a stranger: the plaintext row is not there, and a forged owner does not open it
            login(pb);
            assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID).isEmpty());
            PropertyDAO forged = new PropertyDAO();
            forged.setGUID(xGUID);
            forged.setName("hacked");
            forged.setSubjectGUID(b.getGUID());
            assertThrows(AccessSecurityException.class, () -> ds.update(forged));
            assertThrows(AccessSecurityException.class, () -> ds.delete(forged, false));

            // the owner shares it for reading through the manager; the grant is what the store honours
            login(pa);
            PermissionGrant share = dsm.addPermissionGrant(b, mapOf(stored), "resource:read");
            login(pb);
            List<PropertyDAO> shared = ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID);
            assertEquals(1, shared.size(), "a read share returns the row");
            assertEquals(a.getGUID(), shared.get(0).getSubjectGUID());
            shared.get(0).setName("renamed by a reader");
            assertThrows(AccessSecurityException.class, () -> ds.update(shared.get(0)), "read is not update");
            assertThrows(AccessSecurityException.class, () -> ds.delete(shared.get(0), false), "read is not delete");

            // the share is revoked: the row is gone again for b
            login(pa);
            assertTrue(dsm.deletePermissionGrant(share));
            login(pb);
            assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID).isEmpty());

            // the system context reads it with nobody logged in, and ends with the call
            dsm.logout();
            assertEquals(1, ShiroUtil.runAsSystem(() -> ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID)).size());
            assertFalse(ShiroUtil.isSystemContext());
            assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, xGUID).isEmpty());

            // the owner deletes its row; the manager deletes a subject with its rows
            login(pa);
            assertTrue(ds.delete(stored, false));
            dsm.logout();
            assertTrue(dsm.deleteSubjectID(c));
        } finally {
            dsm.logout();
        }
    }

    /**
     * Key chain root (user decisions 2026-09-29 and 2026-10-02): a new subject gets its
     * EncapsulatedKey wrapped under the master key in the same transaction, and deleting the subject
     * removes it; a KeyMaker without master key, or no KeyMaker at all, fails the creation — there is
     * no keyless subject.
     */
    @Test
    public void subjectKey_createdWithSubject_removedWithSubject() {
        org.zoxweb.server.security.KeyMakerProvider km = org.zoxweb.server.security.KeyMakerProvider.SINGLETON;
        try {
            String pa = uniquePrincipal();
            SubjectIdentifier a = newSubject(pa);
            List<org.zoxweb.shared.crypto.EncapsulatedKey> keys = sys.search(org.zoxweb.shared.crypto.EncapsulatedKey.NVCE_ENCAPSULATED_KEY, null,
                    new QueryMatch<>(Const.RelationalOperator.EQUAL, a.getGUID(), MetaToken.SUBJECT_GUID.getName()));
            assertEquals(1, keys.size(), "one subject key");
            org.zoxweb.shared.crypto.EncapsulatedKey sk = keys.get(0);
            assertEquals(a.getGUID(), sk.getReferenceGUID(), "bound to the subject");
            assertEquals(a.getGUID(), sk.getSubjectGUID());
            assertEquals(org.zoxweb.shared.crypto.KeyLockType.SUBJECT_ID, sk.getKeyLockType());
            assertEquals(32, sk.getKeySize(), "wrapped under the 32-byte master key");
            // the chain opens: master -> subject key material
            byte[] material = km.getKey(sys, null, a.getGUID());
            assertEquals(32, material.length);

            assertTrue(dsm.deleteSubjectID(a));
            assertTrue(sys.search(org.zoxweb.shared.crypto.EncapsulatedKey.NVCE_ENCAPSULATED_KEY, null,
                    new QueryMatch<>(Const.RelationalOperator.EQUAL, a.getGUID(), MetaToken.SUBJECT_GUID.getName())).isEmpty(),
                    "subject key removed with the subject");

        } finally {
            dsm.logout();
        }
        // master key not loaded: the database is closed — no subject, not even a lookup
        String pb = uniquePrincipal();
        km.setMasterSecretKey((javax.crypto.SecretKey) null);
        try {
            assertThrows(AccessSecurityException.class, () -> newSubject(pb));
            assertThrows(AccessSecurityException.class, () -> dsm.lookupSubjectID(pb), "no database without the master key");
        } finally {
            TestVault.load(); // the master key back
        }
        assertNull(dsm.lookupSubjectID(pb), "nothing was created");
        // no key maker on the store: the same
        String pc = uniquePrincipal();
        ds.getAPIConfigInfo().setKeyMaker(null);
        try {
            assertThrows(AccessSecurityException.class, () -> newSubject(pc));
            assertThrows(AccessSecurityException.class, () -> dsm.lookupSubjectID(pc), "no database without the key maker");
        } finally {
            ds.getAPIConfigInfo().setKeyMaker(km);
        }
        assertNull(dsm.lookupSubjectID(pc), "nothing was created");
    }

    /**
     * The controller opens a sealed value in its storage form, the packed {@code CipherCodecs} record
     * (never canonical text): the owner and a read grantee get the clear text through the owner's key
     * chain, a stranger is denied, bytes that are not a record are refused.
     */
    @Test
    public void controller_decryptValue_opensPackedRecord() {
        org.zoxweb.server.security.KeyMakerProvider km = org.zoxweb.server.security.KeyMakerProvider.SINGLETON;
        try {
            String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
            SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
            newSubject(pc);
            PropertyDAO x = newResource(a);
            km.createNVEntityKey(sys, x, km.getKey(sys, null, a.getGUID()));
            dsm.addPermissionGrant(b, mapOf(x), "resource:read");
            io.xlogistx.shiro.mgt.ShiroSecurityController controller = new io.xlogistx.shiro.mgt.ShiroSecurityController();

            login(pa);
            Object sealed = controller.encryptValue(ds, x, null,
                    new org.zoxweb.shared.util.NVPair("secret", "s3cr3t-value", org.zoxweb.shared.filters.FilterType.ENCRYPT), null);
            byte[] packed = org.zoxweb.server.security.CipherCodecs.EDEncoder.encode(
                    assertInstanceOf(org.zoxweb.shared.crypto.EncryptedData.class, sealed));
            assertEquals("s3cr3t-value", controller.decryptValue(ds, x, packed, null), "owner");
            assertNull(controller.decryptValue(ds, x, null, null));
            assertThrows(IllegalArgumentException.class, () -> controller.decryptValue(ds, x,
                    "s3cr3t-value".getBytes(java.nio.charset.StandardCharsets.UTF_8), null), "clear text is not a record");

            login(pb);
            assertEquals("s3cr3t-value", controller.decryptValue(ds, x, packed, null), "read grantee, owner's chain");

            login(pc);
            assertThrows(AccessSecurityException.class, () -> controller.decryptValue(ds, x, packed, null), "stranger");
        } finally {
            dsm.logout();
        }
    }

    @Test
    public void share_deleteSubjectID_cleansGranteeMapRows() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionGrant grant = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
        String mapGUID = grant.getResourceMap().getGUID();
        assertEquals(1, dsm.getPermissionGrantsByResource(x.getGUID()).length);

        assertTrue(dsm.deleteSubjectID(b));
        assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, grant.getGUID()).isEmpty());
        assertFalse(mapRowExists(mapGUID), "map row must not be orphaned");
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length);
    }

    @Test
    public void share_getAndDeletePermissionGrantsByResource() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), pd = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), d = newSubject(pd);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionGrant gb = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
        PermissionGrant gc = dsm.addPermissionGrant(c, mapOf(x), "resource:read,update");
        PermissionGrant gd = dsm.addPermissionGrant(d, mapOf(y), "resource:read");

        PermissionGrant[] onX = dsm.getPermissionGrantsByResource(x.getGUID());
        assertEquals(2, onX.length);
        for (PermissionGrant g : onX) {
            assertEquals(x.getGUID(), g.getResourceMap().getResourceGUID());
        }
        assertEquals(0, dsm.getPermissionGrantsByResource(UUID.randomUUID().toString()).length);
        assertEquals(0, dsm.getPermissionGrantsByResource(null).length);

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())));

        assertEquals(2, dsm.deletePermissionGrantsByResource(x.getGUID()));
        assertFalse(shiroB.isPermitted(nve(shiroB, "read", x.getGUID())), "grantee evicted");
        assertFalse(mapRowExists(gb.getResourceMap().getGUID()));
        assertFalse(mapRowExists(gc.getResourceMap().getGUID()));
        assertEquals(0, dsm.getPermissionGrantsByResource(x.getGUID()).length);
        assertEquals(1, dsm.getPermissionGrantsByResource(y.getGUID()).length, "Y untouched");
        assertTrue(mapRowExists(gd.getResourceMap().getGUID()));
        assertEquals(0, dsm.deletePermissionGrantsByResource(x.getGUID()), "idempotent");
    }

    // ------------------------------------------------------------------
    // share rules (user decision 2026-10-06): inlined shares only, catalog-scoped grants unchanged
    // ------------------------------------------------------------------

    private static long keyRowCount() {
        return sys.search(org.zoxweb.shared.crypto.EncapsulatedKey.NVCE_ENCAPSULATED_KEY, null).size();
    }

    private static int inlinedSharesOf(SubjectIdentifier grantee, NVEntity resource) {
        int n = 0;
        for (PermissionGrant g : dsm.getPermissionGrantsByResource(resource.getGUID())) {
            if (g.getPermissionToken() != null && grantee.getGUID().equals(g.getSubjectGUID())) {
                n++;
            }
        }
        return n;
    }

    /** Enforcement off, nobody bound: removes what a share-rule test created on the shared database. */
    private static void cleanupShareTest(PropertyDAO[] resources, SubjectIdentifier... subjects) {
        dsm.setEnforcePermissions(false);
        dsm.logout();
        for (PropertyDAO r : resources) {
            dsm.deletePermissionGrantsByResource(r.getGUID());
            sys.delete(r, false);
        }
        for (SubjectIdentifier s : subjects) {
            dsm.deleteSubjectID(s);
        }
    }

    @Test
    public void shareRules_oneSharePerGranteePerResource() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));
        long keys = keyRowCount();
        try {
            dsm.setEnforcePermissions(true);
            login(pa);
            PermissionGrant first = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> dsm.addPermissionGrant(b, mapOf(x), "resource:read,update"), "second share to the same grantee on the same resource");
            assertTrue(e.getMessage().contains(first.getGUID()), "the refusal names the existing share");
            assertEquals(1, inlinedSharesOf(b, x));
            // another resource is another share
            dsm.addPermissionGrant(b, mapOf(y), "resource:read");
            assertEquals(1, inlinedSharesOf(b, y));
            // a catalog-scoped grant is not a share: it is not counted and not refused
            PermissionGrant catalog = dsm.addPermissionGrant(b, update, mapOf(x));
            assertNull(catalog.getPermissionToken());
            assertEquals(1, inlinedSharesOf(b, x));
            assertEquals(2, dsm.getPermissionGrantsByResource(x.getGUID()).length, "one share + one catalog-scoped grant");
            assertEquals(keys, keyRowCount(), "no key row touched");
        } finally {
            cleanupShareTest(new PropertyDAO[]{x, y}, a, b);
            dsm.deletePermission(update);
        }
    }

    /** Logs {@code principal} in and asks Shiro; a login logs the previous subject out, so re-login the actor afterwards. */
    private static boolean permitted(String principal, String verb, NVEntity resource) {
        Subject s = login(principal);
        return s.isPermitted(nve(s, verb, resource.getGUID()));
    }

    @Test
    public void shareRules_ownerChangesShareInPlace_ownerOnly() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc);
        PropertyDAO x = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));
        long keys = keyRowCount();
        try {
            dsm.setEnforcePermissions(true);
            login(pa);
            PermissionGrant share = dsm.addPermissionGrant(b, mapOf(x), "resource:read");
            PermissionGrant catalog = dsm.addPermissionGrant(c, update, mapOf(x));

            assertTrue(permitted(pb, "read", x));
            assertFalse(permitted(pb, "update", x));
            // the grantee cannot change its own share, nor can a stranger, nor nobody
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.updatePermissionGrant(share, "resource:read,update"), "grantee is not the owner");
            login(pc);
            assertThrows(AccessSecurityException.class, () -> dsm.updatePermissionGrant(share, "resource:read,update"), "C is not the owner");
            dsm.logout();
            assertThrows(AccessSecurityException.class, () -> dsm.updatePermissionGrant(share, "resource:read,update"), "nobody bound");
            assertFalse(permitted(pb, "update", x), "nothing changed");

            login(pa);
            PermissionGrant changed = dsm.updatePermissionGrant(share, " Resource:Update, read ");
            assertEquals(share.getGUID(), changed.getGUID(), "changed in place");
            assertEquals("resource:update,read", changed.getPermissionToken(), "normalized");
            assertEquals(b.getGUID(), changed.getSubjectGUID());
            assertEquals(a.getGUID(), changed.getBrokerGUID(), "grantor kept");
            assertEquals(x.getGUID(), changed.getResourceMap().getResourceGUID());
            assertEquals(1, inlinedSharesOf(b, x), "still one share");
            assertTrue(permitted(pb, "update", x), "the grantee's cached permissions follow the change");
            assertTrue(permitted(pb, "read", x));

            login(pa);
            PermissionGrant back = dsm.updatePermissionGrant(changed, "resource:read");
            assertEquals(share.getGUID(), back.getGUID());
            assertFalse(permitted(pb, "update", x), "update taken back");
            assertTrue(permitted(pb, "read", x));

            // invalid tokens and non-shares are refused
            login(pa);
            assertThrows(IllegalArgumentException.class, () -> dsm.updatePermissionGrant(share, "resource:create"));
            assertThrows(IllegalArgumentException.class, () -> dsm.updatePermissionGrant(share, "doc:read"));
            assertThrows(IllegalArgumentException.class, () -> dsm.updatePermissionGrant(share, ""));
            assertThrows(IllegalArgumentException.class, () -> dsm.updatePermissionGrant(catalog, "resource:read"), "a catalog-scoped grant is not a share");
            PermissionGrant unknown = new PermissionGrant();
            unknown.setGUID(UUID.randomUUID().toString());
            assertThrows(IllegalArgumentException.class, () -> dsm.updatePermissionGrant(unknown, "resource:read"));
            assertEquals(keys, keyRowCount(), "no key row touched");
        } finally {
            cleanupShareTest(new PropertyDAO[]{x}, a, b, c);
            dsm.deletePermission(update);
        }
    }

    @Test
    public void shareRules_nonOwnerSharerGivesReadOrReadShareOnly_andNeverChangesAnExistingShare() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), pd = uniquePrincipal(), pe = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), d = newSubject(pd), e = newSubject(pe);
        PropertyDAO x = newResource(a);
        long keys = keyRowCount();
        try {
            dsm.setEnforcePermissions(true);
            login(pa);
            dsm.addPermissionGrant(b, mapOf(x), "resource:read,update,share");

            login(pb);
            PermissionGrant bToC = dsm.addPermissionGrant(c, mapOf(x), "resource:read");
            assertEquals(b.getGUID(), bToC.getBrokerGUID());
            PermissionGrant bToD = dsm.addPermissionGrant(d, mapOf(x), "resource:read,share");
            assertEquals("resource:read,share", bToD.getPermissionToken());
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(e, mapOf(x), "resource:read,update"), "B is not the owner: no update");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(e, mapOf(x), "resource:delete"), "B is not the owner: no delete");
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(e, mapOf(x), "resource:read,share,update"), "not even with share");
            assertEquals(0, dsm.getPermissionGrants(e.getGUID()).length);

            // C holds read only: C may not share at all
            login(pc);
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(e, mapOf(x), "resource:read"), "no share verb");

            // D holds read,share: D may share with E, but B's existing share is the owner's to change
            login(pd);
            PermissionGrant dToE = dsm.addPermissionGrant(e, mapOf(x), "resource:read");
            assertEquals(d.getGUID(), dToE.getBrokerGUID());
            assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, mapOf(x), "resource:read,share"), "B already has a share");
            assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(c, mapOf(x), "resource:read,share"), "C already has a share");
            assertEquals(1, inlinedSharesOf(b, x));
            assertEquals(1, inlinedSharesOf(c, x));

            // the owner itself is held to one share per grantee too: it changes B's share in place instead
            login(pa);
            assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, mapOf(x), "resource:read"));
            assertEquals(keys, keyRowCount(), "no key row touched");
        } finally {
            cleanupShareTest(new PropertyDAO[]{x}, a, b, c, d, e);
        }
    }

    @Test
    public void shareRules_revokeCascadesToWhatTheGranteeIssued() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), pd = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), d = newSubject(pd);
        PropertyDAO x = newResource(a), y = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));
        long keys = keyRowCount();
        try {
            dsm.setEnforcePermissions(true);
            login(pa);
            PermissionGrant aToB = dsm.addPermissionGrant(b, mapOf(x), "resource:read,update,share");
            PermissionGrant aToBonY = dsm.addPermissionGrant(b, mapOf(y), "resource:read");
            login(pb);
            PermissionGrant bToC = dsm.addPermissionGrant(c, mapOf(x), "resource:read,share");
            PermissionGrant bToCcatalog = dsm.addPermissionGrant(c, update, mapOf(x)); // catalog-scoped: left alone by the cascade
            login(pc);
            PermissionGrant cToD = dsm.addPermissionGrant(d, mapOf(x), "resource:read");
            assertTrue(permitted(pd, "read", x));
            assertTrue(permitted(pc, "read", x));
            assertTrue(permitted(pb, "read", x));
            assertEquals(4, dsm.getPermissionGrantsByResource(x.getGUID()).length);

            // the grantor of C's share revokes it: D's goes with it, B's stays
            login(pb);
            assertTrue(dsm.deletePermissionGrant(bToC));
            assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, cToD.getGUID()).isEmpty(), "C->D revoked with B->C");
            assertFalse(mapRowExists(cToD.getResourceMap().getGUID()), "and its map row");
            assertFalse(mapRowExists(bToC.getResourceMap().getGUID()));
            assertFalse(permitted(pc, "read", x), "C evicted");
            assertFalse(permitted(pd, "read", x), "D evicted");
            assertTrue(permitted(pb, "read", x), "B untouched");
            assertFalse(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, bToCcatalog.getGUID()).isEmpty(), "catalog-scoped grant left alone");
            assertTrue(permitted(pc, "update", x), "C keeps its catalog-scoped update");

            // rebuild the chain, then the owner revokes B: B, C and D go in one call
            login(pb);
            bToC = dsm.addPermissionGrant(c, mapOf(x), "resource:read,share");
            login(pc);
            cToD = dsm.addPermissionGrant(d, mapOf(x), "resource:read");
            assertTrue(permitted(pd, "read", x));
            login(pa);
            assertTrue(dsm.deletePermissionGrant(aToB));
            for (PermissionGrant gone : new PermissionGrant[]{aToB, bToC, cToD}) {
                assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, gone.getGUID()).isEmpty(), "revoked: " + gone.getPermissionToken());
                assertFalse(mapRowExists(gone.getResourceMap().getGUID()));
            }
            assertFalse(permitted(pb, "read", x));
            assertFalse(permitted(pc, "read", x));
            assertFalse(permitted(pd, "read", x));
            assertEquals(1, dsm.getPermissionGrantsByResource(x.getGUID()).length, "only the catalog-scoped grant remains");
            assertFalse(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, aToBonY.getGUID()).isEmpty(), "B's share on another resource untouched");
            assertTrue(permitted(pb, "read", y));
            assertEquals(keys, keyRowCount(), "no key row touched");
        } finally {
            cleanupShareTest(new PropertyDAO[]{x, y}, a, b, c, d);
            dsm.deletePermission(update);
        }
    }

    @Test
    public void shareRules_takingShareAwayCascades_granteeKeepsReducedShare() {
        String pa = uniquePrincipal(), pb = uniquePrincipal(), pc = uniquePrincipal(), pd = uniquePrincipal(), pe = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb), c = newSubject(pc), d = newSubject(pd), e = newSubject(pe);
        PropertyDAO x = newResource(a);
        long keys = keyRowCount();
        try {
            dsm.setEnforcePermissions(true);
            login(pa);
            PermissionGrant aToB = dsm.addPermissionGrant(b, mapOf(x), "resource:read,update,share");
            login(pb);
            PermissionGrant bToC = dsm.addPermissionGrant(c, mapOf(x), "resource:read,share");
            login(pc);
            PermissionGrant cToD = dsm.addPermissionGrant(d, mapOf(x), "resource:read");
            assertTrue(permitted(pd, "read", x));
            assertTrue(permitted(pb, "update", x));

            // A keeps B's update but takes share away: C and D go, B keeps read,update
            login(pa);
            PermissionGrant reduced = dsm.updatePermissionGrant(aToB, "resource:read,update");
            assertEquals(aToB.getGUID(), reduced.getGUID());
            assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, bToC.getGUID()).isEmpty(), "B->C revoked");
            assertTrue(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, cToD.getGUID()).isEmpty(), "C->D revoked");
            assertFalse(mapRowExists(bToC.getResourceMap().getGUID()));
            assertFalse(mapRowExists(cToD.getResourceMap().getGUID()));
            assertFalse(permitted(pc, "read", x));
            assertFalse(permitted(pd, "read", x));
            assertTrue(permitted(pb, "read", x), "B keeps its reduced share");
            assertTrue(permitted(pb, "update", x));
            assertFalse(permitted(pb, "share", x));
            assertEquals(1, dsm.getPermissionGrantsByResource(x.getGUID()).length);
            login(pb);
            assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(e, mapOf(x), "resource:read"), "B can no longer share");

            // a change that keeps share cascades nothing
            login(pa);
            dsm.updatePermissionGrant(aToB, "resource:read,share");
            login(pb);
            PermissionGrant bToE = dsm.addPermissionGrant(e, mapOf(x), "resource:read");
            login(pa);
            dsm.updatePermissionGrant(aToB, "resource:read,update,share");
            assertFalse(sys.searchByID(PermissionGrant.NVC_PERMISSION_GRANT, bToE.getGUID()).isEmpty(), "share kept: nothing cascaded");
            assertEquals(2, dsm.getPermissionGrantsByResource(x.getGUID()).length);
            assertTrue(permitted(pe, "read", x));
            assertEquals(keys, keyRowCount(), "no key row touched");
        } finally {
            cleanupShareTest(new PropertyDAO[]{x}, a, b, c, d, e);
        }
    }

    @Test
    public void share_flattenerSkipsGrantWithMissingCatalogRow() {
        String pa = uniquePrincipal(), pb = uniquePrincipal();
        SubjectIdentifier a = newSubject(pa), b = newSubject(pb);
        PropertyDAO x = newResource(a);
        PermissionInfo update = dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "resource:update"));
        dsm.addPermissionGrant(b, update, mapOf(x));

        Subject shiroB = login(pb);
        assertTrue(shiroB.isPermitted(nve(shiroB, "update", x.getGUID())));

        assertTrue(dsm.deletePermission(update));
        Subject again = login(pb);
        assertTrue(again.isAuthenticated(), "a dangling grant must not break login");
        assertFalse(again.isPermitted(nve(again, "update", x.getGUID())));
        assertEquals(1, dsm.getPermissionGrants(b.getGUID()).length, "the grant row itself stays");
    }

    // ------------------------------------------------------------------
    // password reset (email principal = recovery channel; admin path for the rest)
    // ------------------------------------------------------------------

    private static String uniqueUsername() {
        return "user-" + UUID.randomUUID().toString().replace("-", "");
    }

    private static PasswordResetToken[] tokensOf(String subjectGUID) {
        java.util.List<PasswordResetToken> list = sys.search(PasswordResetToken.NVC_PASSWORD_RESET_TOKEN, null,
                new org.zoxweb.shared.db.QueryMatch<>(org.zoxweb.shared.util.MetaToken.SUBJECT_GUID.getName(), subjectGUID,
                        Const.RelationalOperator.EQUAL));
        return list.toArray(new PasswordResetToken[0]);
    }

    private static void expireOutstanding(String subjectGUID) {
        for (PasswordResetToken t : tokensOf(subjectGUID)) {
            if (t.getStatus() == SecConst.SecStatus.ACTIVE) {
                t.setExpiryTS(System.currentTimeMillis() - 1);
                sys.update(t);
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
