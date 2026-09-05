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
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.util.Const;
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
        SecurityException e = assertThrows(SecurityException.class, () -> newSubject(principal));
        assertTrue(e.getMessage().contains("already exists"), e.getMessage());
    }

    @Test
    public void login_succeedsWithCorrectPassword_failsOtherwise() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = newSubject(principal);

        assertEquals(subject.getGUID(), dsm.login(principal, PASSWORD).getGUID());
        assertThrows(SecurityException.class, () -> dsm.login(principal, "wrong-password"));
        assertThrows(SecurityException.class, () -> dsm.login(uniquePrincipal(), PASSWORD));
        assertThrows(SecurityException.class, () -> dsm.login(principal, null));
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
        String reason = assertThrows(SecurityException.class, () -> newSubject("short1")).getMessage();
        assertTrue(reason.startsWith("Invalid principal ID"), reason);
        assertThrows(SecurityException.class, () -> newSubject("ma​rio-" + UUID.randomUUID()));
        assertThrows(SecurityException.class, () -> dsm.addPrincipalID(subject, "   "));

        // a rejected identifier can match nothing: lookups return null and login fails, never throw
        assertNull(dsm.lookupPrincipalID("short1"));
        assertNull(dsm.lookupSubjectID("short1"));
        assertEquals(0, dsm.lookupAllPrincipalCredentials("short1").length);
        assertThrows(SecurityException.class, () -> dsm.login("short1", PASSWORD));

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

        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD));
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
        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD));
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
        assertThrows(SecurityException.class, () -> dsm.createSubjectID(principal, badCredential));
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
        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD), "deactivated subject must not log in");

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
        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD));

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
        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD));
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

        subject.setSubjectStatus(SecConst.SecStatus.PENDING_RESET_PASSWORD);
        dsm.updateSubjectID(subject);
        assertThrows(SecurityException.class, () -> dsm.login(principal, PASSWORD), "login denies PENDING_RESET_PASSWORD");
        assertTrue(dsm.verifyPassword(principal, PASSWORD), "verifyPassword accepts PENDING_RESET_PASSWORD");
        assertFalse(dsm.verifyPassword(principal, "wrong-" + PASSWORD));

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
        assertThrows(SecurityException.class, () -> dsm.updateCredential(b, pwA));

        // entity claims B but the stored row belongs to A
        pwA.setSubjectGUID(b.getGUID());
        assertThrows(SecurityException.class, () -> dsm.updateCredential(b, pwA));

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
        assertThrows(SecurityException.class, () -> dsm.loginApiKey(key));
    }

    @Test
    public void apiKeyLogin_roundTrip_statusAndExpiry() {
        SubjectIdentifier subject = newSubject(uniquePrincipal());
        String key = "key-" + UUID.randomUUID();
        SubjectAPIKey sak = newAPIKey(subject, key);

        assertEquals(subject.getGUID(), dsm.loginApiKey(key).getGUID());
        assertThrows(SecurityException.class, () -> dsm.loginApiKey("key-" + UUID.randomUUID()));
        assertThrows(SecurityException.class, () -> dsm.loginApiKey(null));

        sak.setStatus(Const.Status.SUSPENDED);
        dsm.updateCredential(subject, sak);
        assertThrows(SecurityException.class, () -> dsm.loginApiKey(key), "suspended key must not log in");

        sak.setStatus(Const.Status.ACTIVE);
        sak.setExpiryDate(System.currentTimeMillis() - 1000);
        dsm.updateCredential(subject, sak);
        assertThrows(SecurityException.class, () -> dsm.loginApiKey(key), "expired key must not log in");

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
        assertThrows(SecurityException.class, () -> dsm.loginJWT(signedJWT(forged, sak.getSubjectID(), "xlogistx.io", "shirods")), "forged signature");

        // right secret, claims outside the key's scope
        assertThrows(SecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "other.io", "shirods")), "wrong domain");
        assertThrows(SecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "xlogistx.io", "other-app")), "wrong app");
        assertThrows(SecurityException.class, () -> dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), null, null)), "scope claims missing");
        assertEquals(subject.getGUID(), dsm.loginJWT(signedJWT(sak, sak.getSubjectID(), "XLOGISTX.IO", "SHIRODS")).getGUID(), "scope is case-insensitive");

        // sub names another key ID
        assertThrows(SecurityException.class, () -> dsm.loginJWT(signedJWT(sak, "kid-" + UUID.randomUUID(), "xlogistx.io", "shirods")), "unknown key ID");

        // expired / not yet valid
        JWT expired = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        expired.getPayload().setExpirationTime(new Date(System.currentTimeMillis() - 10 * 60_000));
        assertThrows(SecurityException.class, () -> dsm.loginJWT(expired.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "expired");
        JWT future = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        future.getPayload().setNotBefore(new Date(System.currentTimeMillis() + 10 * 60_000));
        assertThrows(SecurityException.class, () -> dsm.loginJWT(future.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "not yet valid");

        // garbage
        assertThrows(SecurityException.class, () -> dsm.loginJWT("not.a.jwt"));
        assertThrows(SecurityException.class, () -> dsm.loginJWT(""));
        assertThrows(SecurityException.class, () -> dsm.loginJWT(null));
        String[] parts = token.split("\\.");
        assertThrows(SecurityException.class, () -> dsm.loginJWT(parts[0] + "." + parts[1] + ".AAAA"), "bad signature segment");

        // key status
        sak.setStatus(Const.Status.SUSPENDED);
        dsm.updateCredential(subject, sak);
        assertThrows(SecurityException.class, () -> dsm.loginJWT(token), "suspended key");
        sak.setStatus(Const.Status.ACTIVE);
        dsm.updateCredential(subject, sak);
        assertEquals(subject.getGUID(), dsm.loginJWT(token).getGUID());

        // subject status
        subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
        dsm.updateSubjectID(subject);
        assertThrows(SecurityException.class, () -> dsm.loginJWT(token), "deactivated subject");
        subject.setSubjectStatus(SecConst.SecStatus.ACTIVE);
        dsm.updateSubjectID(subject);

        // freshness: iat must be inside the window once the key requires timestamps
        sak.setTimeStampRequired(true);
        dsm.updateCredential(subject, sak);
        JWT stale = JWT.createJWT(CryptoConst.JWTAlgo.HS256, sak.getSubjectID(), "xlogistx.io", "shirods");
        stale.getPayload().setIssuedAt(new Date(System.currentTimeMillis() - 10 * 60_000));
        assertThrows(SecurityException.class, () -> dsm.loginJWT(stale.hash(sak.getAPIKeyAsBytes(), JWTProvider.SINGLETON)), "stale iat");
        String fresh = ShiroDSDomainSecurityManager.mintJWT(sak, null, 0);
        assertEquals(subject.getGUID(), dsm.loginJWT(fresh).getGUID());

        // replay cache: the same fresh token is refused the second time
        dsm.getCredentialsMatcher().setJWTReplayCache(new JWTTokenCache(60_000, TaskUtil.defaultTaskScheduler()));
        try {
            String once = ShiroDSDomainSecurityManager.mintJWT(sak, null, 0);
            assertEquals(subject.getGUID(), dsm.loginJWT(once).getGUID());
            assertThrows(SecurityException.class, () -> dsm.loginJWT(once), "replayed token");
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
        assertThrows(SecurityException.class, () -> dsm.loginSubjectApiKey("key-" + UUID.randomUUID(), null, null));
        assertThrows(SecurityException.class, () -> dsm.loginSubjectJWT("nope", null));
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

            assertThrows(AccessException.class, () -> ShiroUtil.login(new APIKeyAuthenticationToken("key-" + UUID.randomUUID())));
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

            assertThrows(AccessException.class, () -> ShiroUtil.loginSubject(principal, "wrong-" + PASSWORD, null, null, false));
            ThreadContext.remove();

            // autoLogin=true is the trusted-caller path: password not checked, status rules still apply
            Subject auto = ShiroUtil.loginSubject(principal, null, null, null, true);
            assertTrue(auto.isAuthenticated());
            assertEquals(subject.getGUID(), ShiroUtil.subjectUserID());
            auto.logout();
            ThreadContext.remove();
            subject.setSubjectStatus(SecConst.SecStatus.DEACTIVATED);
            dsm.updateSubjectID(subject);
            assertThrows(AccessException.class, () -> ShiroUtil.loginSubject(principal, null, null, null, true), "auto-login must not bypass status gating");
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
            assertThrows(SecurityException.class, () -> iniDsm.login(principal, "wrong-" + PASSWORD));

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
            assertThrows(SecurityException.class, () -> dsm.login(principal, "wrong-" + PASSWORD));
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
            assertThrows(AccessException.class,
                    () -> dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "n:o")),
                    "no bound subject -> denied");

            dsm.loginSubject(adminPrincipal, PASSWORD, null, null);
            assertNotNull(dsm.createPermission(new PermissionInfo("perm." + UUID.randomUUID(), "y:es")).getGUID());
            assertThrows(AccessException.class, () -> dsm.createRole(new RoleInfo()), "role:create not granted");

            // self-service stays open: the bound subject may change its own credential
            CIPassword mine = (CIPassword) dsm.lookupCredential(adminPrincipal, CredentialInfo.Type.PASSWORD);
            dsm.updateCredential(admin, mine);
        } finally {
            dsm.setEnforcePermissions(false);
        }
    }
}
