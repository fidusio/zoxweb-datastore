package io.xlogistx.shiro.ds.test;

import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.shiro.ds.GrantFlattener;
import io.xlogistx.shiro.ds.SecuritySetup;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import io.xlogistx.shiro.ds.tools.SecurityAdminTool;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.subject.Subject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.BaseSubjectID;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.ResourceManager;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog seeding, super-admin bootstrap and the {@code *} exclusivity guards, on the same store
 * setup as {@link ShiroDSDomainSecurityManagerDBTest} (H2 in-memory, or PostgreSQL via
 * {@code -Dds.url}). The super-admin principal is overridden per test class run with a unique
 * value so reruns against a persistent database never collide, and never touch the real
 * super-admin, the one the vault names. Those stand-in accounts, the app records the tests create, and a
 * {@code xlogistx.com-common} record created by a stand-in are removed after the class
 * ({@link #removeStandIns}) — a shared database must not be left with a look-alike super-admin or
 * with the common app owned by one.
 */
public class SecurityCatalogDBTest {

    private static final String PASSWORD = "Sup3r-Secret!";
    private static final String DEFAULT_URL = "jdbc:h2:mem:shirocat;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    private static ShiroDSDomainSecurityManager dsm;
    private static H2PDataStore ds;
    /** {@link #ds} in the system context: fixtures and raw checks, see {@link TestVault#systemView}. */
    private static org.zoxweb.shared.api.APIDataStore<?, ?> sys;
    private static String superAdmin;
    /** Principals this run used as super-admin on the shared store; removed in {@link #removeStandIns}. */
    private static final List<String> standInSuperAdmins = new ArrayList<>();
    /** App records this run created on the shared store; removed in {@link #removeStandIns}. */
    private static final List<AppIDDefault> createdApps = new ArrayList<>();

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
            cfg = H2PDSCreator.toAPIConfigInfo(base + "/" + targetDb, user, password);
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password, filePassword);
        }
        ds = new H2PDSCreator().createAPI(null, TestVault.secure(cfg));
        sys = TestVault.systemView(ds);
        OPSecUtil.singleton();
        dsm = new ShiroDSDomainSecurityManager(ds);
        superAdmin = "super-admin-" + UUID.randomUUID() + "@xlogistx.io";
        dsm.setSuperAdminPrincipalID(superAdmin);
        standInSuperAdmins.add(superAdmin);
    }

    @AfterAll
    public static void removeStandIns() {
        dsm.setEnforcePermissions(false);
        dsm.logout();
        for (AppIDDefault app : createdApps) {
            if (dsm.lookupApp(app.getDomainID(), app.getAppID()) != null) {
                dsm.deleteApp(app); // the record, its catalog, its registrar
            }
        }
        AppIDDefault common = dsm.commonApp();
        for (String principal : standInSuperAdmins) {
            SubjectIdentifier standIn = dsm.lookupSubjectID(principal);
            if (standIn == null) {
                continue;
            }
            if (common != null && standIn.getGUID().equals(common.getSubjectGUID())) {
                // the common app stays (its catalog references it); it just stops naming a stand-in as its owner
                common.setSubjectGUID(null);
                sys.update(common);
            }
            dsm.deleteSubjectID(standIn);
        }
    }

    @AfterEach
    public void cleanupThread() {
        dsm.setEnforcePermissions(false);
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
        return "cat-" + UUID.randomUUID() + "@example.com";
    }

    private static SubjectIdentifier newSubject(String principal) {
        return dsm.createSubjectID(principal, HashUtil.toBCryptPassword(PASSWORD));
    }

    private static Subject login(String principal, String password) {
        dsm.logout();
        return dsm.loginSubject(principal, password, null, null);
    }

    private static SecuritySetup.Result bootstrap() {
        return SecuritySetup.bootstrapSuperAdmin(dsm, PASSWORD);
    }

    private static PermissionInfo reservedPermission() {
        dsm.seedCatalog();
        return dsm.lookupPermission(null, SecurityModel.Permission.SUPER_ADMIN_ALL.getName());
    }

    private static RoleInfo reservedRole() {
        dsm.seedCatalog();
        return dsm.lookupRole(null, SecurityModel.Role.SUPER_ADMIN.getName());
    }

    // ------------------------------------------------------------------
    // seeder
    // ------------------------------------------------------------------

    @Test
    public void seedCatalog_isIdempotent() {
        dsm.seedCatalog();
        SecuritySetup.Report second = dsm.seedCatalog();
        assertTrue(second.isNoOp(), "second run must change nothing: " + second);
        assertEquals(SecurityModel.Permission.values().length, second.permissionsExisting);
        assertEquals(SecurityModel.Role.values().length, second.rolesExisting);
        assertEquals(SecurityModel.RoleGroup.values().length, second.roleGroupsExisting);

        for (SecurityModel.Permission p : SecurityModel.Permission.values()) {
            PermissionInfo row = dsm.lookupPermission(null, p.getName());
            assertNotNull(row, p.getName());
            assertEquals(p.getValue(), row.getPermissionToken(), p.getName());
            assertEquals(ShiroDSDomainSecurityManager.COMMON_SCOPE, ShiroUtil.scopeLabel(row.getAppID()),
                    "every catalog row belongs to the common app");
            assertEquals(dsm.commonApp().getGUID(), row.getAppID().getGUID(), "and references its one record");
            assertEquals(1, countByName(PermissionInfo.NVC_PERMISSION_INFO, p.getName()), "exactly one row named " + p.getName());
        }
        for (SecurityModel.Role r : SecurityModel.Role.values()) {
            assertNotNull(dsm.lookupRole(null, r.getName()), r.getName());
            assertEquals(1, countByName(RoleInfo.NVC_ROLE_INFO, r.getName()));
        }
        for (SecurityModel.RoleGroup g : SecurityModel.RoleGroup.values()) {
            assertNotNull(dsm.lookupRoleGroup(null, g.getName()), g.getName());
            assertEquals(1, countByName(RoleGroupInfo.NVC_ROLE_GROUP_INFO, g.getName()));
        }
    }

    /** Rows of that name in the common app's catalog (other apps carry their own rows of the same names). */
    private static int countByName(org.zoxweb.shared.util.NVConfigEntity nvce, String name) {
        int count = 0;
        for (Object row : sys.search(nvce, null, new org.zoxweb.shared.db.QueryMatch<>(
                org.zoxweb.shared.util.MetaToken.NAME.getName(), name, org.zoxweb.shared.util.Const.RelationalOperator.EQUAL))) {
            if (ShiroDSDomainSecurityManager.COMMON_SCOPE.equals(ShiroUtil.scopeLabel(((AuthzInfo) row).getAppID()))) {
                count++;
            }
        }
        return count;
    }

    @Test
    public void seedCatalog_rolesCarryModelPermissions_andRepairsDrift() {
        dsm.seedCatalog();
        for (SecurityModel.Role model : SecurityModel.Role.values()) {
            RoleInfo row = dsm.lookupRole(null, model.getName());
            Set<String> expected = new HashSet<>();
            for (SecurityModel.Permission p : model.getPermissions()) expected.add(p.getValue());
            Set<String> actual = new HashSet<>();
            if (row.getPermissions() != null) {
                for (PermissionInfo p : row.getPermissions()) actual.add(p.getPermissionToken());
            }
            assertEquals(expected, actual, model.getName());
        }

        // drift: strip a permission from domain_admin directly in the store, re-seed repairs it
        RoleInfo domainAdmin = dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName());
        int before = domainAdmin.getPermissions().length;
        domainAdmin.removePermission(domainAdmin.getPermissions()[0]);
        sys.update(domainAdmin);
        assertEquals(before - 1, dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName()).getPermissions().length);

        SecuritySetup.Report report = dsm.seedCatalog();
        assertEquals(1, report.rolesUpdated, report.toString());
        assertEquals(before, dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName()).getPermissions().length);
    }

    @Test
    public void seedCatalog_roleGroupsCarryRoles() {
        dsm.seedCatalog();
        for (SecurityModel.RoleGroup model : SecurityModel.RoleGroup.values()) {
            RoleGroupInfo row = dsm.lookupRoleGroup(null, model.getName());
            Set<String> expected = new HashSet<>();
            for (SecurityModel.Role r : model.getRoles()) expected.add(r.getName());
            Set<String> actual = new HashSet<>();
            for (RoleInfo r : row.getRoles()) actual.add(r.getName());
            assertEquals(expected, actual, model.getName());
            assertFalse(actual.contains(SecurityModel.Role.SUPER_ADMIN.getName()));
        }
    }

    // ------------------------------------------------------------------
    // bootstrap
    // ------------------------------------------------------------------

    @Test
    public void bootstrapSuperAdmin_createsSystemAccount_andIsPermittedAnything() {
        SecuritySetup.Result first = bootstrap();
        assertNotNull(first.subject);
        assertEquals(superAdmin, first.principalID);
        assertTrue(first.wildcardVerified, first.toString());
        assertEquals(BaseSubjectID.SubjectType.SYSTEM, first.subject.getSubjectType());
        assertTrue(dsm.isSuperAdminSubject(first.subject.getGUID()));

        Subject shiro = login(superAdmin, PASSWORD);
        assertTrue(shiro.hasRole(SecurityModel.Role.SUPER_ADMIN.getName()));
        assertTrue(shiro.isPermitted("anything:" + UUID.randomUUID()));
        assertTrue(shiro.isPermitted(SecurityModel.PERM_ADD_PERMISSION));
        assertTrue(shiro.isPermitted("resource:" + UUID.randomUUID() + ":" + UUID.randomUUID() + ":read"));
        assertTrue(GrantFlattener.flatten(dsm, first.subject.getGUID()).permissions.contains("*"));

        SecuritySetup.Result second = SecuritySetup.bootstrapSuperAdmin(dsm, null);
        assertFalse(second.subjectCreated);
        assertFalse(second.roleGranted, "role grant must not be duplicated");
        assertTrue(second.wildcardVerified);
        assertEquals(first.subject.getGUID(), second.subject.getGUID());
        assertEquals(1, dsm.getRoleGrants(first.subject.getGUID()).length);
    }

    @Test
    public void bootstrap_createsCommonApp_once() {
        SecuritySetup.Result first = bootstrap();
        assertNotNull(first.app, first.toString());
        assertEquals("xlogistx.com", first.app.getDomainID());
        assertEquals("common", first.app.getAppID());
        assertEquals("xlogistx.com-common", first.app.getDomainAppID());
        assertEquals("xlogistx.com-common", first.app.getName());
        if (first.appCreated) {
            // a fresh store: the super-admin that ran the setup owns the record
            assertEquals(first.subject.getGUID(), first.app.getSubjectGUID());
        }

        AppIDDefault stored = dsm.lookupApp("XLOGISTX.COM", "Common"); // normalized by the entity's filters
        assertNotNull(stored);
        assertEquals(first.app.getGUID(), stored.getGUID());

        SecuritySetup.Result second = SecuritySetup.bootstrapSuperAdmin(dsm, null);
        assertFalse(second.appCreated, "the common app is created once");
        assertEquals(first.app.getGUID(), second.app.getGUID());
        assertEquals(first.app.getGUID(), SecuritySetup.ensureCommonApp(dsm, null).getGUID());
        assertEquals(1, sys.search(AppIDDefault.NVC_APP_ID_DEFAULT, null,
                new org.zoxweb.shared.db.QueryMatch<>("domain_id", "xlogistx.com", org.zoxweb.shared.util.Const.RelationalOperator.EQUAL),
                org.zoxweb.shared.util.Const.LogicalOperator.AND,
                new org.zoxweb.shared.db.QueryMatch<>("app_id", "common", org.zoxweb.shared.util.Const.RelationalOperator.EQUAL)).size(),
                "exactly one xlogistx.com-common row");

        // the super-admin holds no grant scoped to the app: it belongs to no domain/app
        for (RoleGrant g : dsm.getRoleGrants(first.subject.getGUID())) {
            assertNull(g.getAppID(), "super-admin grants carry no app");
        }
    }

    @Test
    public void createApp_refusesDuplicateAndInvalid_andNeedsAppCreateUnderEnforcement() {
        bootstrap();
        String appID = "app" + Long.toHexString(System.nanoTime());
        AppIDDefault created = dsm.createApp("Example.com", appID);
        createdApps.add(created);
        assertEquals("example.com", created.getDomainID());
        assertEquals("example.com-" + appID, created.getName());
        assertEquals(created.getGUID(), dsm.lookupApp("example.com", appID).getGUID());
        assertThrows(IllegalArgumentException.class, () -> dsm.createApp("example.com", appID), "duplicate");
        assertThrows(IllegalArgumentException.class, () -> dsm.createApp("nodot", appID), "domain needs a dot");
        assertNull(dsm.lookupApp("example.com", appID + "x"));

        String principal = uniquePrincipal();
        newSubject(principal);
        login(principal, PASSWORD);
        dsm.setEnforcePermissions(true);
        assertThrows(AccessSecurityException.class, () -> dsm.createApp("example.com", appID + "b"), "no app:create");

        Subject admin = login(superAdmin, PASSWORD);
        AppIDDefault bySuperAdmin = dsm.createApp("example.com", appID + "b");
        createdApps.add(bySuperAdmin);
        assertEquals(dsm.lookupSuperAdminSubject().getGUID(), bySuperAdmin.getSubjectGUID(), "owner = the bound creator");
        assertTrue(admin.isPermitted(SecurityModel.PERM_CREATE_APP_ID));
    }

    /**
     * Under the datastore access control the super-admin's single {@code *} implies every resource
     * permission: it reads, updates and deletes any row — another subject's or one without owner —
     * through the store, in a login with no domain/app.
     */
    @Test
    public void datastoreAccessControl_superAdminReachesEveryRow_ordinarySubjectOnlyItsOwn() {
        bootstrap();
        String principal = uniquePrincipal();
        SubjectIdentifier owner = newSubject(principal);
        org.zoxweb.shared.data.PropertyDAO owned = new org.zoxweb.shared.data.PropertyDAO();
        owned.setName("acl-owned-" + UUID.randomUUID());
        owned.setSubjectGUID(owner.getGUID());
        owned = sys.insert(owned);
        org.zoxweb.shared.data.PropertyDAO ownerless = new org.zoxweb.shared.data.PropertyDAO();
        ownerless.setName("acl-ownerless-" + UUID.randomUUID());
        ownerless = sys.insert(ownerless);
        final org.zoxweb.shared.data.PropertyDAO ownerlessRow = ownerless;
        final String ownedGUID = owned.getGUID(), ownerlessGUID = ownerless.getGUID();

        try {
            login(principal, PASSWORD);
            assertEquals(1, ds.searchByID(org.zoxweb.shared.data.PropertyDAO.NVC_PROPERTY_DAO, ownedGUID).size());
            assertTrue(ds.searchByID(org.zoxweb.shared.data.PropertyDAO.NVC_PROPERTY_DAO, ownerlessGUID).isEmpty(), "no owner, no grant: not visible");

            login(superAdmin, PASSWORD);
            java.util.List<org.zoxweb.shared.data.PropertyDAO> seen = ds.searchByID(org.zoxweb.shared.data.PropertyDAO.NVC_PROPERTY_DAO, ownedGUID);
            assertEquals(1, seen.size(), "* reads another subject's row");
            seen.get(0).setName("renamed by the super-admin");
            ds.update(seen.get(0));
            assertEquals(owner.getGUID(), ((org.zoxweb.shared.data.PropertyDAO) ds.searchByID(
                    org.zoxweb.shared.data.PropertyDAO.NVC_PROPERTY_DAO, ownedGUID).get(0)).getSubjectGUID(), "the owner stays the owner");
            assertEquals(1, ds.searchByID(org.zoxweb.shared.data.PropertyDAO.NVC_PROPERTY_DAO, ownerlessGUID).size(), "* reads a row without owner");
            assertTrue(ds.delete(ownerlessRow, false));
            assertTrue(ds.delete(seen.get(0), false));
        } finally {
            dsm.logout();
        }
    }

    /**
     * Upgrade path: a store seeded before every catalog row belonged to an app (rows with no
     * {@code app_id}) is repaired by the next seeding — the same rows are attached to the common app,
     * nothing is duplicated — and the bootstrap gives the common app its registrar.
     */
    @Test
    public void seedCatalog_attachesPreAppModelRowsToTheCommonApp_bootstrapAddsItsRegistrar() {
        String url = "jdbc:h2:mem:preapp-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            local.setSuperAdminPrincipalID("pre-app-admin-" + UUID.randomUUID() + "@example.com");
            PermissionInfo old = TestVault.systemView(store).insert(SecurityModel.Permission.SUBJECT_READ.toPermissionInfo());
            RoleInfo oldRole = TestVault.systemView(store).insert(SecurityModel.Role.toRole(SecurityModel.Role.USER.getName(), SecurityModel.Role.USER.getDescription()));
            assertNull(old.getAppID());

            SecuritySetup.Report report = local.seedCatalog();
            AppIDDefault common = local.commonApp();
            assertNotNull(common, "the seeding creates the common app");
            assertNull(common.getSubjectGUID(), "no super-admin yet: no owner yet");
            PermissionInfo repaired = local.lookupPermission(null, SecurityModel.Permission.SUBJECT_READ.getName());
            assertEquals(old.getGUID(), repaired.getGUID(), "the same row, not a second one");
            assertEquals(common.getGUID(), repaired.getAppID().getGUID());
            RoleInfo repairedRole = local.lookupRole(null, SecurityModel.Role.USER.getName());
            assertEquals(oldRole.getGUID(), repairedRole.getGUID());
            assertEquals(common.getGUID(), repairedRole.getAppID().getGUID());
            assertEquals(SecurityModel.Permission.values().length - 1, report.permissionsCreated);
            assertEquals(1, report.permissionsExisting);
            assertEquals(1, report.rolesExisting);

            SecuritySetup.Result boot = SecuritySetup.bootstrapSuperAdmin(local, PASSWORD);
            assertTrue(boot.wildcardVerified, boot.toString());
            assertEquals(boot.subject.getGUID(), local.commonApp().getSubjectGUID(), "the bootstrap hands the common app to the super-admin");
            assertNotNull(boot.registrar, "and gives it its registrar");
            assertNotNull(boot.registrarKey, "whose secret is shown once");
            assertEquals(ShiroDSDomainSecurityManager.registrarPrincipal(common), local.lookupAllPrincipalIdentifiers(boot.registrar.getGUID())[0].getPrincipalID());
            assertNull(SecuritySetup.bootstrapSuperAdmin(local, null).registrarKey, "not a second time");
            assertTrue(local.loginSubjectJWT(ShiroDSDomainSecurityManager.mintJWT(boot.registrarKey, null, 60_000), null)
                    .hasRole(SecurityModel.Role.APP_REGISTRAR.getName()));
            local.logout();
        } finally {
            store.close();
        }
    }

    @Test
    public void bootstrap_requiresPassword_onlyWhenCreating() {
        String other = "other-admin-" + UUID.randomUUID() + "@example.com";
        ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(ds);
        local.setSuperAdminPrincipalID(other);
        standInSuperAdmins.add(other);
        assertThrows(IllegalArgumentException.class, () -> SecuritySetup.bootstrapSuperAdmin(local, null));
        assertNull(local.lookupSubjectID(other));
        SecuritySetup.Result created = SecuritySetup.bootstrapSuperAdmin(local, PASSWORD);
        assertTrue(created.subjectCreated);
        assertDoesNotThrow(() -> SecuritySetup.bootstrapSuperAdmin(local, null));
    }

    @Test
    public void resetSuperAdminPassword_replacesCredential() {
        bootstrap();
        String fresh = "N3w-Sup3r-Secret!";
        SecuritySetup.resetSuperAdminPassword(dsm, fresh);
        assertThrows(AccessSecurityException.class, () -> dsm.login(superAdmin, PASSWORD));
        assertNotNull(dsm.login(superAdmin, fresh));
        SecuritySetup.resetSuperAdminPassword(dsm, PASSWORD);
        assertNotNull(dsm.login(superAdmin, PASSWORD));
    }

    // ------------------------------------------------------------------
    // exclusivity of the wildcard
    // ------------------------------------------------------------------

    @Test
    public void wildcard_cannotReachAnotherSubject_viaAnyGrantPath() {
        bootstrap();
        String pb = uniquePrincipal();
        SubjectIdentifier b = newSubject(pb);
        PermissionInfo all = reservedPermission();
        RoleInfo superRole = reservedRole();
        org.zoxweb.shared.data.PropertyDAO x = new org.zoxweb.shared.data.PropertyDAO();
        x.setName("res-" + UUID.randomUUID());
        x.setSubjectGUID(b.getGUID());
        x = sys.insert(x);
        ResourceMap map = new ResourceMap(x);

        assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(b, all), "global grant of *");
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, all, map), "scoped grant of *");
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, map, "*"), "inlined *");
        assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(b, superRole), "super_admin role");

        // a group smuggled in directly through the store
        RoleGroupInfo smuggled = new RoleGroupInfo(superRole);
        smuggled.setName("smuggled-" + UUID.randomUUID());
        RoleGroupInfo stored = sys.insert(smuggled);
        assertThrows(AccessSecurityException.class, () -> dsm.addRoleGroupGrant(b, stored), "group containing super_admin");
        RoleGroupInfo viaManager = new RoleGroupInfo(superRole);
        viaManager.setName("via-manager-" + UUID.randomUUID());
        assertThrows(IllegalArgumentException.class, () -> dsm.createRoleGroup(viaManager));

        assertEquals(0, dsm.getPermissionGrants(b.getGUID()).length);
        assertEquals(0, dsm.getRoleGrants(b.getGUID()).length);
        assertEquals(0, dsm.getRoleGroupGrants(b.getGUID()).length);
        Subject shiroB = login(pb, PASSWORD);
        assertFalse(shiroB.isPermitted("x:y"));
    }

    @Test
    public void wildcard_catalogRowsRejected() {
        bootstrap();
        assertThrows(IllegalArgumentException.class, () -> dsm.createPermission(new PermissionInfo("evil-" + UUID.randomUUID(), "*")));
        assertThrows(IllegalArgumentException.class, () -> dsm.createPermission(new PermissionInfo("evil-" + UUID.randomUUID(), "*:read")));
        assertThrows(IllegalArgumentException.class,
                () -> dsm.createPermission(new PermissionInfo(SecurityModel.Permission.SUPER_ADMIN_ALL.getName(), "*")), "second reserved row");

        PermissionInfo normal = dsm.createPermission(new PermissionInfo("normal-" + UUID.randomUUID(), "doc:read"));
        normal.setPermissionToken("*");
        assertThrows(IllegalArgumentException.class, () -> dsm.updatePermission(normal));
        assertEquals("doc:read", dsm.lookupPermissionByGUID(normal.getGUID()).getPermissionToken());

        PermissionInfo reserved = reservedPermission();
        assertThrows(IllegalArgumentException.class, () -> dsm.createRole(new RoleInfo("evil-role-" + UUID.randomUUID(), "x", reserved)));
        RoleInfo normalRole = dsm.createRole(new RoleInfo("normal-role-" + UUID.randomUUID(), "x", normal));
        normalRole.addPermission(reserved);
        assertThrows(IllegalArgumentException.class, () -> dsm.updateRole(normalRole));

        RoleInfo superRole = reservedRole();
        superRole.setDescription("changed");
        assertThrows(IllegalArgumentException.class, () -> dsm.updateRole(superRole));
        assertThrows(IllegalArgumentException.class, () -> dsm.deleteRole(superRole));
        assertThrows(IllegalArgumentException.class, () -> dsm.deletePermission(reserved));
        PermissionInfo reservedCopy = reservedPermission();
        reservedCopy.setDescription("changed");
        assertThrows(IllegalArgumentException.class, () -> dsm.updatePermission(reservedCopy));
        assertNotNull(reservedPermission(), "reserved row survives");
        assertNotNull(reservedRole(), "reserved role survives");
    }

    @Test
    public void realm_dropsSmuggledWildcardRow() {
        bootstrap();
        String pb = uniquePrincipal();
        SubjectIdentifier b = newSubject(pb);
        PermissionInfo smuggled = sys.insert(new PermissionInfo("smuggled-" + UUID.randomUUID(), "*"));
        PermissionGrant grant = new PermissionGrant(smuggled.getGUID());
        grant.setSubjectGUID(b.getGUID());
        sys.insert(grant);

        GrantFlattener.Result flat = GrantFlattener.flatten(dsm, b.getGUID());
        for (String p : flat.permissions) {
            assertFalse(SecurityModel.isWildcardToken(p), p);
        }
        Subject shiroB = login(pb, PASSWORD);
        assertFalse(shiroB.isPermitted("x:y"));

        String adminGUID = dsm.lookupSuperAdminSubject().getGUID();
        assertTrue(GrantFlattener.flatten(dsm, adminGUID).permissions.contains("*"));
    }

    // ------------------------------------------------------------------
    // enforcement
    // ------------------------------------------------------------------

    @Test
    public void enforcement_superAdminSeedsAndCreates_ordinaryCannot() {
        bootstrap();
        String pb = uniquePrincipal();
        newSubject(pb);
        dsm.setEnforcePermissions(true);
        try {
            login(pb, PASSWORD);
            assertThrows(AccessSecurityException.class, () -> dsm.createPermission(new PermissionInfo("p-" + UUID.randomUUID(), "a:b")));
            assertThrows(AccessSecurityException.class, () -> dsm.seedCatalog());
            assertThrows(AccessSecurityException.class, () -> dsm.createSubjectID(uniquePrincipal(), HashUtil.toBCryptPassword(PASSWORD)));

            login(superAdmin, PASSWORD);
            assertNotNull(dsm.createPermission(new PermissionInfo("p-" + UUID.randomUUID(), "a:b")).getGUID());
            RoleInfo role = new RoleInfo("r-" + UUID.randomUUID(), "x");
            assertNotNull(dsm.createRole(role).getGUID());
            assertTrue(dsm.seedCatalog().isNoOp());
            assertNotNull(dsm.createSubjectID(uniquePrincipal(), HashUtil.toBCryptPassword(PASSWORD)));
        } finally {
            dsm.setEnforcePermissions(false);
        }
    }

    // ------------------------------------------------------------------
    // property + INI
    // ------------------------------------------------------------------

    @Test
    public void superAdminPrincipalID_hasNoDefault_isNormalizedWhenSet() {
        ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(ds);
        // nothing names a super-admin in code: until the start-up code sets the vault's id there is none
        assertNull(local.getSuperAdminPrincipalID());
        assertThrows(IllegalStateException.class, local::requireSuperAdminPrincipalID);
        assertNull(local.lookupSuperAdminSubject());
        assertFalse(local.isSuperAdminSubject(UUID.randomUUID().toString()));
        assertThrows(IllegalStateException.class, () -> SecuritySetup.bootstrapSuperAdmin(local, PASSWORD),
                "no bootstrap without a super-admin id");

        local.setSuperAdminPrincipalID("  Other-Admin@Example.COM ");
        assertEquals("other-admin@example.com", local.getSuperAdminPrincipalID());
        assertEquals("other-admin@example.com", local.requireSuperAdminPrincipalID());
        assertThrows(IllegalArgumentException.class, () -> local.setSuperAdminPrincipalID("x"));

        // the vault is where the id lives
        assertNotNull(TestVault.superAdminID());
        local.setSuperAdminPrincipalID(TestVault.superAdminID());
        assertEquals(TestVault.superAdminID(), local.getSuperAdminPrincipalID());
    }

    // ------------------------------------------------------------------
    // CLI
    // ------------------------------------------------------------------

    @Test
    public void tool_run_bootstrapOnFreshH2_thenVerify() {
        String url = "jdbc:h2:mem:tool-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String admin = TestVault.superAdminID(); // the tool takes the super-admin id from the vault
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = toolBootstrap(out, err, url);
        assertEquals(SecurityAdminTool.EXIT_OK, code, err + "\n" + out);
        assertTrue(out.toString().contains("super-admin " + admin + " "), out.toString());
        assertTrue(out.toString().contains("initial password: the secret store's " + SecurityAdminTool.SUPER_ADMIN_PASSWORD), out.toString());
        // the initial password is the vault's: not nameable on the command line
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=bootstrap-super-admin", "db.url=" + url, "password=" + PASSWORD));
        assertTrue(err.toString().contains("does not take password="), err.toString());
        err.reset();
        // the super-admin is not nameable on the command line
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=bootstrap-super-admin", "db.url=" + url, "principal.id=someone-else@example.com"));
        assertTrue(err.toString().contains("does not take principal.id"), err.toString());
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=reset-super-admin-password", "db.url=" + url, "password=" + PASSWORD, "principal.id=someone-else@example.com"));
        err.reset();
        assertTrue(out.toString().contains("wildcard=verified"), out.toString());

        // second run: idempotent, no password needed
        code = tool(out, err,
                "command=bootstrap-super-admin", "db.url=" + url);
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());

        // verify with a fresh manager on the same database
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            local.setSuperAdminPrincipalID(admin);
            Subject shiro = local.loginSubject(admin, adminPassword(), null, null);
            assertTrue(shiro.isPermitted("anything:" + UUID.randomUUID()));
            local.logout();
            local.createSubjectID("plain-" + UUID.randomUUID() + "@example.com", HashUtil.toBCryptPassword(PASSWORD));
        } finally {
            store.close();
        }

        code = tool(out, err,
                "command=grant-role", "db.url=" + url,
                "principal.id=nobody-" + UUID.randomUUID() + "@example.com", "role=super_admin");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, code, "unknown subject");

        err.reset();
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err), "command=list-catalog", "db.url=" + url);
        assertEquals(SecurityAdminTool.EXIT_USAGE, code, "no secret store, no run");
        assertTrue(err.toString().contains("secret store required"), err.toString());
        // the database is the vault's db.url: naming one on the command line is refused
        err.reset();
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                TestVault.toolArgsFor(url, "command=list-catalog", "db.url=" + url));
        assertEquals(SecurityAdminTool.EXIT_USAGE, code, "db.url= is not a command-line option");
        assertTrue(err.toString().contains("db.url= is not accepted"), err.toString());
        code = tool(out, err, "command=list-catalog", "db.url=" + url);
        assertEquals(SecurityAdminTool.EXIT_OK, code);
        assertTrue(out.toString().contains("super_admin_all = *"));
    }

    @Test
    public void tool_grantRoleSuperAdmin_toOtherSubject_fails() {
        String url = "jdbc:h2:mem:tool2-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String other = "tool2-user-" + UUID.randomUUID() + "@example.com";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(SecurityAdminTool.EXIT_OK, toolBootstrap(out, err, url), err.toString());
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            new ShiroDSDomainSecurityManager(store).createSubjectID(other, HashUtil.toBCryptPassword(PASSWORD));
        } finally {
            store.close();
        }
        int code = tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + other, "role=super_admin");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, code, err.toString());
        assertTrue(err.toString().contains("super-admin"), err.toString());
        code = tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + other, "role=domain_admin");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
    }

    @Test
    public void tool_createSubject_remoteAdminWithDomainAdminRole_neverSuperAdmin() {
        String url = "jdbc:h2:mem:tool3-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String superAdmin = TestVault.superAdminID();
        String remoteAdmin = "remote-admin-" + UUID.randomUUID();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        // this store's super-admin is the one the vault names
        assertEquals(SecurityAdminTool.EXIT_OK, toolBootstrap(out, err, url), err.toString());

        // create-subject needs principal.id
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=create-subject", "db.url=" + url, "password=" + PASSWORD));
        // and never hands out super_admin
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin, "password=" + PASSWORD, "role=super_admin"));
        // unknown role is rejected before anything is created
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin + "-x", "password=" + PASSWORD, "role=no_such_role"));

        out.reset();
        int code = tool(out, err,
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin, "password=" + PASSWORD, "role=domain_admin");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
        assertTrue(out.toString().contains("(created)") && out.toString().contains("domain_admin=granted"), out.toString());

        // rerun: idempotent, password untouched, role already there
        out.reset();
        code = tool(out, err,
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin, "role=domain_admin");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
        assertTrue(out.toString().contains("(existing, password unchanged)") && out.toString().contains("domain_admin=existing"), out.toString());

        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            local.setSuperAdminPrincipalID(superAdmin);
            Subject shiro = local.loginSubject(remoteAdmin, PASSWORD, null, null);
            assertTrue(shiro.hasRole(SecurityModel.Role.DOMAIN_ADMIN.getName()));
            assertTrue(shiro.isPermitted(SecurityModel.PERM_ADD_SUBJECT));
            assertFalse(shiro.isPermitted("anything:" + UUID.randomUUID()), "remote-admin must not hold the wildcard");
            local.logout();
            assertNull(local.lookupSubjectID(remoteAdmin + "-x"), "rejected create must not leave a subject behind");
            shiro = local.loginSubject(superAdmin, adminPassword(), null, null);
            assertTrue(shiro.isPermitted("anything:" + UUID.randomUUID()));
            local.logout();
        } finally {
            store.close();
        }
    }

    @Test
    public void appGrant_reservedRoleAndPermission_neverScoped_superAdminAppliesEverywhere() {
        bootstrap();
        SubjectIdentifier sa = dsm.lookupSuperAdminSubject();
        assertNotNull(sa);
        AppIDDefault app = dsm.lookupApp("xlogistx.io", "appx") != null ? dsm.lookupApp("xlogistx.io", "appx") : dsm.createApp("xlogistx.io", "appx");
        createdApps.add(app);
        assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(sa, reservedRole(), app),
                "super_admin is global only, even for the super-admin subject");
        assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(sa, reservedPermission(), app),
                "the wildcard is global only");
        assertThrows(IllegalArgumentException.class, () -> dsm.addRoleGrant(sa, reservedRole(), new AppIDDefault()),
                "an app scope needs domain and app");

        // the exception to the login-scope rule: the super-admin's global * applies in any app login
        dsm.logout();
        Subject inApp = dsm.loginSubject(superAdmin, PASSWORD, "xlogistx.io", "appx");
        assertTrue(inApp.isPermitted("anything:" + UUID.randomUUID()));
        assertTrue(inApp.hasRole(SecurityModel.Role.SUPER_ADMIN.getName()));
        dsm.logout();
        assertTrue(dsm.loginSubject(superAdmin, PASSWORD, null, null).isPermitted("anything:" + UUID.randomUUID()));
        dsm.logout();
    }

    @Test
    public void tool_appScopedGrants_createSubject_grantRole_revoke() {
        String url = "jdbc:h2:mem:toolapp-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String principal = "app-admin-" + UUID.randomUUID() + "@example.com";
        String shop = "xlogistx.io-shopapp";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=seed-catalog", "db.url=" + url), err.toString());
        // the apps first: each gets its own starter roles and its registrar (secret printed once)
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=create-app", "db.url=" + url, "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("registrar of " + shop), out.toString());
        assertTrue(out.toString().contains("secret: "), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=create-app", "db.url=" + url, "app.id=" + shop), "already exists");
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=create-app", "db.url=" + url, "domain.id=xlogistx.io", "app.id=other"), err.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=xlogistx.io-nosuchapp"), "unknown app");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=create-subject", "db.url=" + url, "principal.id=" + principal, "password=" + PASSWORD,
                "role=app_admin", "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("role app_admin [app " + shop + "]=granted"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=domain_admin", "app.id=" + shop), "domain_admin exists in the common app only");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), err.toString());
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), "idempotent");
        assertTrue(out.toString().contains("already granted"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=super_admin", "app.id=" + shop), "never scoped");
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_user", "domain.id=xlogistx.io", "app.id=other"), err.toString());
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_user", "app.id=noseparator"), "malformed app.id");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=list-grants", "db.url=" + url, "principal.id=" + principal), err.toString());
        String listed = out.toString();
        assertFalse(listed.contains("domain_admin"), listed);
        assertTrue(listed.contains("role app_admin [app " + shop + "]"), listed);
        assertTrue(listed.contains("role app_user [app xlogistx.io-other]"), listed);
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=list-apps", "db.url=" + url), err.toString());
        assertTrue(out.toString().contains(shop + " guid="), out.toString());
        assertTrue(out.toString().contains(ShiroDSDomainSecurityManager.COMMON_SCOPE + " guid="), out.toString());
        assertTrue(out.toString().contains("[common]"), out.toString());

        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            Subject inShop = local.loginSubject(principal, PASSWORD, "xlogistx.io", "shopapp");
            assertTrue(inShop.isPermitted("subject:create"));
            assertTrue(inShop.hasRole("app_admin"));
            assertTrue(inShop.isPermitted("role:create"), "an app admin manages its app's catalog");
            assertFalse(inShop.hasRole("app_user"), "the other app's grant");
            local.logout();
            Subject inOther = local.loginSubject(principal, PASSWORD, "xlogistx.io", "other");
            assertTrue(inOther.hasRole("app_user"));
            assertFalse(inOther.isPermitted("subject:create"));
            local.logout();
            assertFalse(local.loginSubject(principal, PASSWORD, null, null).isPermitted("subject:create"), "global login: no app grant");
            local.logout();
        } finally {
            store.close();
        }

        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("revoked 1 grant(s) of role app_admin"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), "nothing left to revoke");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, tool(out, err,
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=domain_admin"), "global grant does not exist, only the scoped one");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, tool(out, err,
                "command=revoke-app", "db.url=" + url, "principal.id=" + principal, "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("revoked 0 grant(s) [app " + shop + "]"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_USAGE, tool(out, err,
                "command=revoke-app", "db.url=" + url, "principal.id=" + principal), "app.id required");

        store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            Subject inShop = local.loginSubject(principal, PASSWORD, "xlogistx.io", "shopapp");
            assertFalse(inShop.isPermitted("subject:create"), "removed from the shop app");
            assertFalse(inShop.hasRole("app_admin"));
            local.logout();
            assertTrue(local.loginSubject(principal, PASSWORD, "xlogistx.io", "other").hasRole("app_user"), "the other app's assignment survives");
            local.logout();
        } finally {
            store.close();
        }
    }

    @Test
    public void tool_run_dbSettingsFromSecretStore() throws Exception {
        String vaultUrl = "jdbc:h2:mem:vault-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String cliUrl = "jdbc:h2:mem:vaultcli-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String vaultPassword = "V4ult-Secret!";
        File vaultFile = File.createTempFile("shiro-ds-vault", ".bcfks");
        assertTrue(vaultFile.delete());
        try {
            try (SecretStore vault = SecretStore.create(vaultFile, vaultPassword.toCharArray())) {
                vault.put("db.url", vaultUrl).put("db.user", "sa").put("db.password", "").put("other.secret", "not a db setting");
                vault.save();
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();

            // a vault without the master key: the run stops before it touches the database
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("holds no " + SecurityAdminTool.MASTER_KEY_ALIAS), err.toString());
            assertFalse(catalogSeeded(vaultUrl), "nothing written without the master key");
            err.reset();
            try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                vault.putSecretKey(SecurityAdminTool.MASTER_KEY_ALIAS, TestVault.masterKey());
                vault.save();
            }

            // a vault without the super-admin id: the run stops as well
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("holds no " + SecretStore.SUPER_ADMIN_ID), err.toString());
            assertFalse(catalogSeeded(vaultUrl), "nothing written without the super-admin id");
            err.reset();
            try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                vault.setSuperAdminID(TestVault.superAdminID());
                vault.save();
            }

            // a vault without the super-admin's initial password: the run stops as well
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("holds no " + SecurityAdminTool.SUPER_ADMIN_PASSWORD), err.toString());
            assertFalse(catalogSeeded(vaultUrl), "nothing written without the initial password");
            err.reset();
            String initial = "Init-" + UUID.randomUUID().toString().substring(0, 8) + "-aA1!";
            try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                vault.put(SecurityAdminTool.SUPER_ADMIN_PASSWORD, initial);
                vault.save();
            }

            // vault named but unreadable: missing file, wrong password, no password source
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile + ".missing", "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("secret store not found"), err.toString());
            err.reset();
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile, "store.password=wrong-" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("cannot open secret store"), err.toString());
            err.reset();
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile), "no console in tests, so no password source");
            assertTrue(err.toString().contains("store.password required"), err.toString());
            err.reset();

            // the command line cannot name another database: refused, nothing written anywhere
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "db.url=" + cliUrl, "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("db.url= is not accepted"), err.toString());
            assertFalse(catalogSeeded(cliUrl), "the command-line database is never touched");
            assertFalse(catalogSeeded(vaultUrl), "and neither is the vault's on a refused run");
            err.reset();

            // the database comes from the vault
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(catalogSeeded(vaultUrl), "seeded through the vault's db.url");
            out.reset();
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(out.toString().contains("super_admin_all = *"), out.toString());

            // the super-admin's initial password is the vault's: password= is refused, the account gets the entry
            String vaultAdmin = TestVault.superAdminID();
            err.reset();
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=bootstrap-super-admin", "store=" + vaultFile, "store.password=" + vaultPassword, "password=" + PASSWORD));
            assertTrue(err.toString().contains("does not take password="), err.toString());
            out.reset();
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=bootstrap-super-admin", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(out.toString().contains("initial password: the secret store's " + SecurityAdminTool.SUPER_ADMIN_PASSWORD), out.toString());
            assertTrue(superAdminLogsIn(vaultUrl, vaultAdmin, initial), "created with the vault's initial password");
            assertFalse(superAdminLogsIn(vaultUrl, vaultAdmin, PASSWORD));
            // it is the initial password only: a rerun leaves the account alone, and a reset changes its password
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=bootstrap-super-admin", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(superAdminLogsIn(vaultUrl, vaultAdmin, initial), "a rerun changes nothing");
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=reset-super-admin-password", "store=" + vaultFile, "store.password=" + vaultPassword, "password=" + PASSWORD), err.toString());
            assertTrue(superAdminLogsIn(vaultUrl, vaultAdmin, PASSWORD), "changed later");
            assertFalse(superAdminLogsIn(vaultUrl, vaultAdmin, initial));
            // and the entry does not put the old password back
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=bootstrap-super-admin", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(superAdminLogsIn(vaultUrl, vaultAdmin, PASSWORD), "the initial password is not re-applied");

            // db.url is mandatory too: without it the vault is refused
            try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                assertTrue(vault.remove(SecretStore.StoreParam.DB_URL.getName()));
                vault.save();
            }
            err.reset();
            assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(err.toString().contains("holds no " + SecretStore.StoreParam.DB_URL.getName()), err.toString());
        } finally {
            //noinspection ResultOfMethodCallIgnored
            vaultFile.delete();
        }
    }

    /** Whether {@code principal} logs in with {@code password} on a database of the vault test. */
    private static boolean superAdminLogsIn(String url, String principal, String password) {
        H2PDataStore store = new H2PDSCreator().createAPI(null, TestVault.secure(H2PDSCreator.toAPIConfigInfo(url, null, null, null)));
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            local.setSuperAdminPrincipalID(principal);
            try {
                return local.login(principal, password) != null;
            } catch (AccessSecurityException e) {
                return false;
            }
        } finally {
            store.close();
        }
    }

    /** For the databases of the vault test, whose own vault names the H2 defaults as credentials. */
    private static boolean catalogSeeded(String url) {
        H2PDataStore store = new H2PDSCreator().createAPI(null, TestVault.secure(H2PDSCreator.toAPIConfigInfo(url, null, null, null)));
        try {
            return new ShiroDSDomainSecurityManager(store).getPermissions().length > 0;
        } finally {
            store.close();
        }
    }

    private static H2PDataStore SecurityAdminTool_openStore(String url) {
        // the same credentials a tool run takes from the vault for this URL (none in the throw-away vault)
        return new H2PDSCreator().createAPI(null, TestVault.secure(H2PDSCreator.toAPIConfigInfo(url,
                TestVault.db("db.user"), TestVault.db("db.password"), TestVault.db("db.enc-password"))));
    }

    /** The password a tool bootstrap gives the super-admin: the vault's mandatory initial password. */
    private static String adminPassword() {
        return TestVault.superAdminPassword();
    }

    /** A tool bootstrap on {@code url}: no password argument, the vault supplies the initial one. */
    private static int toolBootstrap(ByteArrayOutputStream out, ByteArrayOutputStream err, String url) {
        return tool(out, err, "command=bootstrap-super-admin", "db.url=" + url);
    }

    /**
     * A tool run the way every run starts: the vault first, then the arguments. The tool takes its
     * database from the vault's {@code db.url} and from nowhere else, so an argument
     * {@code "db.url=<url>"} given here is not passed on: it selects the vault this run uses, one
     * made for that database ({@link TestVault#vaultFor}). Without it the run's own vault is used.
     */
    private static int tool(ByteArrayOutputStream out, ByteArrayOutputStream err, String... args) {
        String url = null;
        List<String> rest = new ArrayList<>();
        for (String arg : args) {
            if (arg.startsWith("db.url=")) {
                url = arg.substring("db.url=".length());
            } else {
                rest.add(arg);
            }
        }
        String[] toolArgs = url != null ? TestVault.toolArgsFor(url, rest.toArray(new String[0])) : TestVault.toolArgs(rest.toArray(new String[0]));
        return SecurityAdminTool.run(new PrintStream(out), new PrintStream(err), toolArgs);
    }
}
