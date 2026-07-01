package io.xlogistx.datastore.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.xlogistx.datastore.XlogistxMongoDSCreator;
import io.xlogistx.datastore.XlogistxMongoDataStore;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.datastore.test.CommonDataStoreTest;
import org.zoxweb.server.security.DomainSecurityManagerDefault;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.security.CredentialInfo;
import org.zoxweb.shared.security.DomainSecurityManager;
import org.zoxweb.shared.security.PermissionGrant;
import org.zoxweb.shared.security.PermissionInfo;
import org.zoxweb.shared.security.RoleGrant;
import org.zoxweb.shared.security.RoleInfo;
import org.zoxweb.shared.security.SubjectIdentifier;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for {@link DomainSecurityManager} backed by the real
 * {@link XlogistxMongoDataStore}. Requires a MongoDB instance reachable at {@link #DB_URL}.
 * <p>
 * These mirror {@code DomainSecurityManagerDefaultTest} (which runs against a mock store) but
 * exercise the actual Mongo persistence path. Each test uses a fresh, unique principal / entity
 * name (UUID-suffixed) so the suite is safe to re-run against a persistent database. The tests
 * do not delete anything — all created data is left in the store for inspection.
 */
public class DomainSecurityManagerDBTest {
    public static final String DB_URL = "mongodb://localhost:27017/xlog_datastore_test";

    private static final String PASSWORD = "Secret123!";

    private static XlogistxMongoDataStore mongoDataStore;
    private static CommonDataStoreTest<MongoClient, MongoDatabase> cdst;
    private static DomainSecurityManager domainSecurityManager;

    @BeforeAll
    public static void setup() {
        XlogistxMongoDSCreator creator = new XlogistxMongoDSCreator();
        APIConfigInfo configInfo = creator.toAPIConfigInfo(DB_URL);
        System.out.println("Config\n" + GSONUtil.toJSONDefault(configInfo, true));

        mongoDataStore = new XlogistxMongoDataStore();
        mongoDataStore.setAPIConfigInfo(configInfo);
        OPSecUtil.singleton();
        cdst = new CommonDataStoreTest<>(mongoDataStore);
        domainSecurityManager = new DomainSecurityManagerDefault().setDataStore(mongoDataStore);
    }

    /** A unique principal per invocation so reruns never collide on the unique index. */
    private static String uniquePrincipal() {
        return "dsm-" + UUID.randomUUID() + "@example.com";
    }

    @Test
    public void dataStoreIsConfigured() {
        assertSame(mongoDataStore, domainSecurityManager.getDataStore());
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
}
