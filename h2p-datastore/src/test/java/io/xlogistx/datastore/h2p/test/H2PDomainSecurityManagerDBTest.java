package io.xlogistx.datastore.h2p.test;


import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PExceptionHandler;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.security.DomainSecurityManagerDefault;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.util.NVGenericMap;

import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link DomainSecurityManager} backed by the real
 * <p>
 * These mirror {@code DomainSecurityManagerDefaultTest} (which runs against a mock store) but
 * exercise the actual Mongo persistence path. Each test uses a fresh, unique principal / entity
 * name (UUID-suffixed) so the suite is safe to re-run against a persistent database. The tests
 * do not delete anything — all created data is left in the store for inspection.
 */
public class H2PDomainSecurityManagerDBTest {
    // replicaSet=rs0 targets the replica set (required for transactions).


    private static final String PASSWORD = "Secret123!";
    private static final String NEW_PASSWORD = "N3wSecret456$";

//    private static APIDataStore<Connection, Connection> h2pDataStore;
    //private static CommonDataStoreTest<Connection, H2PDataStore> cdst;
    private static DomainSecurityManager domainSecurityManager;
    private static H2PDataStore ds;

    /** Target Postgres database name; auto-created if missing. Override with -Dds.db. */
    private static final String DB_NAME = System.getProperty("ds.db");

    @BeforeAll
    @SuppressWarnings("unused")
    public static void setup() throws Exception {
        // One JDBC URL drives both engines (set via -Dds.url). Examples:
        //   H2 file (encrypted):  -Dds.url=jdbc:h2:file:./data/dsm;CIPHER=AES;MODE=PostgreSQL  (+ -Dds.file_password=...)
        //   H2 in-memory:         -Dds.url=jdbc:h2:mem:dsm;DB_CLOSE_DELAY=-1;MODE=PostgreSQL
        //   PostgreSQL:           -Dds.url=jdbc:postgresql://host:5432  (db optional; auto-created)
        String url = System.getProperty("ds.url");
        Assumptions.assumeTrue(url != null && !url.isEmpty(),
                "set -Dds.url=jdbc:h2:... or jdbc:postgresql://host:port (+ -Dds.user / -Dds.password, and -Dds.file_password for an encrypted H2 DB) to run this test");

        String user = System.getProperty("ds.user");
        String password = System.getProperty("ds.password");
        String filePassword = System.getProperty("ds.file_password"); // H2 encrypted (CIPHER) DB only

        // Structured parse -> branch on the engine.
        NVGenericMap parsed = H2PUtil.parseJdbcURL(url);
        String subprotocol = parsed.getValue(H2PUtil.JDBC_SUBPROTOCOL);

        H2PDSCreator creator = new H2PDSCreator();
        APIConfigInfo cfg;

        if ("postgresql".equals(subprotocol)) {
            Class.forName("org.postgresql.Driver");
            // Rebuild the base endpoint (host[:port]) from the parsed URL, then create the target DB if missing.
            String host = parsed.getValue(H2PUtil.JDBC_HOST);
            Object port = parsed.getValue(H2PUtil.JDBC_PORT);
            String base = "jdbc:postgresql://" + host + (port != null ? ":" + port : "");
            String targetDb = firstNonEmpty(parsed.getValue(H2PUtil.JDBC_DATABASE), DB_NAME, "testpostgres");
            ensureDatabase(base + "/postgres", user, password, targetDb);
            String targetUrl = base + "/" + targetDb;
            cfg = creator.toAPIConfigInfo(targetUrl, user, password); // auto-selects the postgres driver
            System.out.println("Live PostgreSQL target: " + targetUrl);
        } else {
            // H2 (mem/file/tcp). CIPHER, if any, is in the URL; the file password is passed separately.
            cfg = creator.toAPIConfigInfo(url, user, password, filePassword);
            System.out.println("H2 target: " + url);
        }

        ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);

        OPSecUtil.singleton();
        domainSecurityManager = new DomainSecurityManagerDefault().setDataStore(ds).addCredentialType(CIPassword.class);
    }

    /** First non-null, non-empty value (env-var fallback chains). */
    private static String firstNonEmpty(String... values) {
        if (values != null) {
            for (String v : values) {
                if (v != null && !v.isEmpty()) {
                    return v;
                }
            }
        }
        return null;
    }

    /** Create the test database if it does not already exist (CREATE DATABASE cannot run in a txn). */
    private static void ensureDatabase(String maintenanceUrl, String user, String password, String db)
            throws SQLException {
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
                    System.out.println("created database " + db);
                }
            }
        }
    }
    /** A unique principal per invocation so reruns never collide on the unique index. */
    private static String uniquePrincipal() {
        return "dsm-" + UUID.randomUUID() + "@example.com";
    }

    @Test
    public void dataStoreIsConfigured() {
        assertSame(ds, domainSecurityManager.getDataStore());
    }

    @Test
    public void createSubject_assignsGUID_andIsLookupable() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));

        assertNotNull(subject.getGUID());

        SubjectIdentifier looked = domainSecurityManager.lookupSubjectID(principal);
        assertNotNull(looked);
        assertEquals(subject.getGUID(), looked.getGUID());
    }

    @Test
    public void login_succeedsWithCorrectPassword_failsOtherwise() {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));

        assertEquals(subject.getGUID(), domainSecurityManager.login(principal, PASSWORD).getGUID());

        assertThrows(SecurityException.class, () -> domainSecurityManager.login(principal, "wrong-password"));
        assertThrows(SecurityException.class, () -> domainSecurityManager.login(uniquePrincipal(), PASSWORD));
    }

    @Test
    public void lookupCredential_returnsStoredPassword() {
        String principal = uniquePrincipal();
        domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));

        CredentialInfo ci = domainSecurityManager.lookupCredential(principal, CredentialInfo.Type.PASSWORD);
        assertInstanceOf(CIPassword.class, ci);
        assertEquals(1, domainSecurityManager.lookupAllPrincipalCredentials(principal).length);
    }

    /**
     * Password update through the security-manager API: the stored CIPassword is re-hashed
     * with the new password (same credential row — GUID preserved), persisted via
     * updateCredential, and login flips from the old password to the new one.
     */
    @Test
    public void updatePassword_changesLoginCredential() throws NoSuchAlgorithmException {
        String principal = uniquePrincipal();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));
        assertEquals(subject.getGUID(), domainSecurityManager.login(principal, PASSWORD).getGUID());

        CIPassword current = (CIPassword) domainSecurityManager.lookupCredential(principal, CredentialInfo.Type.PASSWORD);
        assertNotNull(current);

        // Re-hash with the new password; update() validates the old password and keeps identity.
        CredentialHasher<CIPassword> hasher = SecUtil.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        CIPassword updated = hasher.update(current, PASSWORD, NEW_PASSWORD);
        domainSecurityManager.updateCredential(subject, updated);

        // Old password rejected, new password accepted.
        assertThrows(SecurityException.class, () -> domainSecurityManager.login(principal, PASSWORD),
                "old password must no longer authenticate");
        assertEquals(subject.getGUID(), domainSecurityManager.login(principal, NEW_PASSWORD).getGUID(),
                "new password must authenticate");

        // Updated in place: still exactly one credential, same GUID as before.
        assertEquals(1, domainSecurityManager.lookupAllPrincipalCredentials(principal).length);
        CIPassword reloaded = (CIPassword) domainSecurityManager.lookupCredential(principal, CredentialInfo.Type.PASSWORD);
        assertNotNull(reloaded);
        assertEquals(current.getGUID(), reloaded.getGUID(), "credential must be updated in place, not replaced");
    }

    @Test
    public void permissionGrant_roundTripsThroughDataStore() {
        String principal = uniquePrincipal();
        String permName = "perm.read." + UUID.randomUUID();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));
        PermissionInfo perm = domainSecurityManager.createPermission(new PermissionInfo(permName, "system:read"));

        assertNotNull(perm.getGUID());
        assertEquals(perm.getGUID(), domainSecurityManager.lookupPermission(null, permName).getGUID());

        PermissionGrant grant = domainSecurityManager.addPermissionGrant(subject, perm);
        assertEquals(perm.getGUID(), grant.getPermissionGUID());

        PermissionGrant[] grants = domainSecurityManager.getPermissionGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
    }


    @Test void createFullRole()
    {
        PermissionInfo perm = domainSecurityManager.createPermission(new PermissionInfo(UUID.randomUUID() + ".perm", "read:batata"));
        RoleInfo role  = new RoleInfo(perm);
        role.setName(UUID.randomUUID() + ".role");
        domainSecurityManager.createRole(role);
    }

    @Test
    public void roleGrant_roundTripsThroughDataStore() {
        String principal = uniquePrincipal();
        String roleName = "role.admin." + UUID.randomUUID();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));

        RoleInfo role = new RoleInfo();
        role.setName(roleName);
        role = domainSecurityManager.createRole(role);

        assertNotNull(role.getGUID());
        assertEquals(role.getGUID(), domainSecurityManager.lookupRole(null, roleName).getGUID());

        RoleGrant grant = domainSecurityManager.addRoleGrant(subject, role);
        assertEquals(role.getGUID(), grant.getRoleGUID());

        RoleGrant[] grants = domainSecurityManager.getRoleGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
    }

    /**
     * createRoleGroup + addRoleGroupGrant round-trip. The group's roles field is a
     * GET_NAME_MAP of non-embedded RoleInfo references, so reading the group back also
     * exercises the reference sub-document resolution path (lookupByReferenceIDsMaybe).
     */
    @Test
    public void roleGroupGrant_roundTripsThroughDataStore() {
        String principal = uniquePrincipal();
        String suffix = UUID.randomUUID().toString();
        SubjectIdentifier subject = domainSecurityManager.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));

        // Two persisted roles bundled into one role group.
        RoleInfo roleA = new RoleInfo();
        roleA.setName("role.group-member.a." + suffix);
        roleA = domainSecurityManager.createRole(roleA);
        RoleInfo roleB = new RoleInfo();
        roleB.setName("role.group-member.b." + suffix);
        roleB = domainSecurityManager.createRole(roleB);

        String groupName = "rolegroup.admins." + suffix;
        RoleGroupInfo group = new RoleGroupInfo(roleA, roleB);
        group.setName(groupName);
        group = domainSecurityManager.createRoleGroup(group);
        assertNotNull(group.getGUID());

        // Lookup by name — both role references must resolve on read-back.
        RoleGroupInfo looked = domainSecurityManager.lookupRoleGroup(null, groupName);
        assertNotNull(looked);
        assertEquals(group.getGUID(), looked.getGUID());
        RoleInfo[] roles = looked.getRoles();
        assertEquals(2, roles.length, "both role references must resolve on read");
        for (RoleInfo expected : new RoleInfo[]{roleA, roleB}) {
            boolean matched = false;
            for (RoleInfo r : roles) {
                if (expected.getGUID().equals(r.getGUID())) {
                    matched = true;
                    break;
                }
            }
            assertTrue(matched, "role " + expected.getName() + " must be resolved in the group");
        }

        // Grant the role group to the subject; the grant is searchable by subject GUID.
        RoleGroupGrant grant = domainSecurityManager.addRoleGroupGrant(subject, group);
        assertEquals(group.getGUID(), grant.getRoleGroupGUID());

        RoleGroupGrant[] grants = domainSecurityManager.getRoleGroupGrants(subject.getGUID());
        assertEquals(1, grants.length);
        assertEquals(grant.getGUID(), grants[0].getGUID());
        assertEquals(group.getGUID(), grants[0].getRoleGroupGUID());
    }

    /**
     * Atomicity: createSubjectID inserts the subject and its principal, then calls createCredential.
     * A CredentialInfo that is NOT an NVEntity makes createCredential throw AFTER those two inserts,
     * so the whole unit of work must roll back — no subject, principal, or credential may survive.
     */
    @Test
    public void createSubject_partialFailure_rollsBackAtomically() {
        String principal = uniquePrincipal();

        // Non-NVEntity credential — rejected by createCredential mid-transaction.
        CredentialInfo badCredential = new CredentialInfo() {
            @Override
            public Type getCredentialType() {
                return Type.PASSWORD;
            }

            @Override
            public SecConst.SecStatus getCredentialStatus() {
                return SecConst.SecStatus.ACTIVE;
            }

            @Override
            public void setCredentialStatus(SecConst.SecStatus status) {

            }

            @Override
            public NVGenericMap getProperties() {
                return new NVGenericMap();
            }
        };

        assertThrows(SecurityException.class,
                () -> domainSecurityManager.createSubjectID(principal, badCredential),
                "createSubjectID must fail when the credential cannot be persisted");

        // Nothing from the failed, rolled-back transaction persisted.
        assertNull(domainSecurityManager.lookupSubjectID(principal),
                "subject must be rolled back");
        assertNull(domainSecurityManager.lookupPrincipalID(principal),
                "principal must be rolled back");
        assertEquals(0, domainSecurityManager.lookupAllPrincipalCredentials(principal).length,
                "no credential must persist");
    }
}
