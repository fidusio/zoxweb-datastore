package io.xlogistx.shiro.ds.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.shiro.ds.GrantFlattener;
import io.xlogistx.shiro.ds.SecurityBootstrap;
import io.xlogistx.shiro.ds.SecurityCatalogSeeder;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import io.xlogistx.shiro.ds.tools.SecurityAdminTool;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.subject.Subject;
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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog seeding, super-admin bootstrap and the {@code *} exclusivity guards, on the same store
 * setup as {@link ShiroDSDomainSecurityManagerDBTest} (H2 in-memory, or PostgreSQL via
 * {@code -Dds.url}). The super-admin principal is overridden per test class run with a unique
 * value so reruns against a persistent database never collide.
 */
public class SecurityCatalogDBTest {

    private static final String PASSWORD = "Sup3r-Secret!";
    private static final String DEFAULT_URL = "jdbc:h2:mem:shirocat;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    private static ShiroDSDomainSecurityManager dsm;
    private static H2PDataStore ds;
    private static String superAdmin;

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
            cfg = H2PDSCreator.toAPIConfigInfo(base + "/" + targetDb, user, password);
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password, filePassword);
        }
        ds = new H2PDSCreator().createAPI(null, cfg);
        OPSecUtil.singleton();
        dsm = new ShiroDSDomainSecurityManager(ds);
        superAdmin = "super-admin-" + UUID.randomUUID() + "@xlogistx.io";
        dsm.setSuperAdminPrincipalID(superAdmin);
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

    private static SecurityBootstrap.Result bootstrap() {
        return SecurityBootstrap.bootstrapSuperAdmin(dsm, PASSWORD);
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
        SecurityCatalogSeeder.Report second = dsm.seedCatalog();
        assertTrue(second.isNoOp(), "second run must change nothing: " + second);
        assertEquals(SecurityModel.Permission.values().length, second.permissionsExisting);
        assertEquals(SecurityModel.Role.values().length, second.rolesExisting);
        assertEquals(SecurityModel.RoleGroup.values().length, second.roleGroupsExisting);

        for (SecurityModel.Permission p : SecurityModel.Permission.values()) {
            PermissionInfo row = dsm.lookupPermission(null, p.getName());
            assertNotNull(row, p.getName());
            assertEquals(p.getValue(), row.getPermissionToken(), p.getName());
            assertNull(row.getAppIdDAO(), "global catalog row");
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

    private static int countByName(org.zoxweb.shared.util.NVConfigEntity nvce, String name) {
        return ds.search(nvce, null, new org.zoxweb.shared.db.QueryMatch<>(
                org.zoxweb.shared.util.MetaToken.NAME.getName(), name, org.zoxweb.shared.util.Const.RelationalOperator.EQUAL)).size();
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
        ds.update(domainAdmin);
        assertEquals(before - 1, dsm.lookupRole(null, SecurityModel.Role.DOMAIN_ADMIN.getName()).getPermissions().length);

        SecurityCatalogSeeder.Report report = dsm.seedCatalog();
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
        SecurityBootstrap.Result first = bootstrap();
        assertNotNull(first.subject);
        assertEquals(superAdmin, first.principalID);
        assertTrue(first.wildcardVerified, first.toString());
        assertEquals(BaseSubjectID.SubjectType.SYSTEM, first.subject.getSubjectType());
        assertTrue(dsm.isSuperAdminSubject(first.subject.getGUID()));

        Subject shiro = login(superAdmin, PASSWORD);
        assertTrue(shiro.hasRole(SecurityModel.Role.SUPER_ADMIN.getName()));
        assertTrue(shiro.isPermitted("anything:" + UUID.randomUUID()));
        assertTrue(shiro.isPermitted(SecurityModel.PERM_ADD_PERMISSION));
        assertTrue(shiro.isPermitted("nventity:read:" + UUID.randomUUID()));
        assertTrue(GrantFlattener.flatten(dsm, first.subject.getGUID()).permissions.contains("*"));

        SecurityBootstrap.Result second = SecurityBootstrap.bootstrapSuperAdmin(dsm, null);
        assertFalse(second.subjectCreated);
        assertFalse(second.roleGranted, "role grant must not be duplicated");
        assertTrue(second.wildcardVerified);
        assertEquals(first.subject.getGUID(), second.subject.getGUID());
        assertEquals(1, dsm.getRoleGrants(first.subject.getGUID()).length);
    }

    @Test
    public void bootstrap_requiresPassword_onlyWhenCreating() {
        String other = "other-admin-" + UUID.randomUUID() + "@example.com";
        ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(ds);
        local.setSuperAdminPrincipalID(other);
        assertThrows(IllegalArgumentException.class, () -> SecurityBootstrap.bootstrapSuperAdmin(local, null));
        assertNull(local.lookupSubjectID(other));
        SecurityBootstrap.Result created = SecurityBootstrap.bootstrapSuperAdmin(local, PASSWORD);
        assertTrue(created.subjectCreated);
        assertDoesNotThrow(() -> SecurityBootstrap.bootstrapSuperAdmin(local, null));
    }

    @Test
    public void resetSuperAdminPassword_replacesCredential() {
        bootstrap();
        String fresh = "N3w-Sup3r-Secret!";
        SecurityBootstrap.resetSuperAdminPassword(dsm, fresh);
        assertThrows(AccessSecurityException.class, () -> dsm.login(superAdmin, PASSWORD));
        assertNotNull(dsm.login(superAdmin, fresh));
        SecurityBootstrap.resetSuperAdminPassword(dsm, PASSWORD);
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
        x = ds.insert(x);
        ResourceMap map = new ResourceMap(x);

        assertThrows(AccessSecurityException.class, () -> dsm.addPermissionGrant(b, all), "global grant of *");
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, all, map), "scoped grant of *");
        assertThrows(IllegalArgumentException.class, () -> dsm.addPermissionGrant(b, map, "*"), "inlined *");
        assertThrows(AccessSecurityException.class, () -> dsm.addRoleGrant(b, superRole), "super_admin role");

        // a group smuggled in directly through the store
        RoleGroupInfo smuggled = new RoleGroupInfo(superRole);
        smuggled.setName("smuggled-" + UUID.randomUUID());
        RoleGroupInfo stored = ds.insert(smuggled);
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
        PermissionInfo smuggled = ds.insert(new PermissionInfo("smuggled-" + UUID.randomUUID(), "*"));
        PermissionGrant grant = new PermissionGrant(smuggled.getGUID());
        grant.setSubjectGUID(b.getGUID());
        ds.insert(grant);

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
    public void superAdminPrincipalID_isNormalized_andIniSettable() {
        ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(ds);
        assertEquals(ShiroDSDomainSecurityManager.DEFAULT_SUPER_ADMIN_PRINCIPAL_ID, local.getSuperAdminPrincipalID());
        local.setSuperAdminPrincipalID("  Other-Admin@Example.COM ");
        assertEquals("other-admin@example.com", local.getSuperAdminPrincipalID());
        assertThrows(IllegalArgumentException.class, () -> local.setSuperAdminPrincipalID("x"));

        Object previous = ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE.getName());
        org.apache.shiro.mgt.SecurityManager previousSM = null;
        try {
            previousSM = SecurityUtils.getSecurityManager();
        } catch (RuntimeException ignore) {
            // none installed
        }
        try {
            ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, ds);
            Ini ini = new Ini();
            ini.setSectionProperty("main", "dsRealm", "io.xlogistx.shiro.ds.DSAuthorizingRealm");
            ini.setSectionProperty("main", "dsRealm.superAdminPrincipalID", "Ini-Admin@Example.com");
            ini.setSectionProperty("main", "securityManager.realm", "$dsRealm");
            SecurityUtils.setSecurityManager(new BasicIniEnvironment(ini).getSecurityManager());
            assertEquals("ini-admin@example.com", ShiroDSDomainSecurityManager.fromGlobal(ds).getSuperAdminPrincipalID());
        } finally {
            if (previousSM != null) {
                SecurityUtils.setSecurityManager(previousSM);
            }
            if (previous != null) {
                ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, previous);
            }
        }
    }

    // ------------------------------------------------------------------
    // CLI
    // ------------------------------------------------------------------

    @Test
    public void tool_run_bootstrapOnFreshH2_thenVerify() {
        String url = "jdbc:h2:mem:tool-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String admin = "tool-admin-" + UUID.randomUUID() + "@example.com";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=bootstrap-super-admin", "db.url=" + url, "password=" + PASSWORD, "principal.id=" + admin);
        assertEquals(SecurityAdminTool.EXIT_OK, code, err + "\n" + out);
        assertTrue(out.toString().contains("wildcard=verified"), out.toString());

        // second run: idempotent, no password needed
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=bootstrap-super-admin", "db.url=" + url, "principal.id=" + admin);
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());

        // verify with a fresh manager on the same database
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            local.setSuperAdminPrincipalID(admin);
            Subject shiro = local.loginSubject(admin, PASSWORD, null, null);
            assertTrue(shiro.isPermitted("anything:" + UUID.randomUUID()));
            local.logout();
            local.createSubjectID("plain-" + UUID.randomUUID() + "@example.com", HashUtil.toBCryptPassword(PASSWORD));
        } finally {
            store.close();
        }

        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url,
                "principal.id=" + admin.replace("tool-admin", "nobody"), "role=super_admin");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, code, "unknown subject");

        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err), "command=seed-catalog");
        assertEquals(SecurityAdminTool.EXIT_USAGE, code, "no db.url anywhere");
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err), "command=list-catalog", "db.url=" + url);
        assertEquals(SecurityAdminTool.EXIT_OK, code);
        assertTrue(out.toString().contains("super_admin_all = *"));
    }

    @Test
    public void tool_grantRoleSuperAdmin_toOtherSubject_fails() {
        String url = "jdbc:h2:mem:tool2-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String admin = "tool2-admin-" + UUID.randomUUID() + "@example.com";
        String other = "tool2-user-" + UUID.randomUUID() + "@example.com";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=bootstrap-super-admin", "db.url=" + url, "password=" + PASSWORD, "principal.id=" + admin), err.toString());
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            new ShiroDSDomainSecurityManager(store).createSubjectID(other, HashUtil.toBCryptPassword(PASSWORD));
        } finally {
            store.close();
        }
        int code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + other, "role=super_admin");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, code, err.toString());
        assertTrue(err.toString().contains("super-admin"), err.toString());
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + other, "role=domain_admin");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
    }

    @Test
    public void tool_createSubject_remoteAdminWithDomainAdminRole_neverSuperAdmin() {
        String url = "jdbc:h2:mem:tool3-" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String superAdmin = "super-admin-" + UUID.randomUUID() + "@example.com";
        String remoteAdmin = "remote-admin-" + UUID.randomUUID();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        // this store's super-admin is an explicit principal, not the default one
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=bootstrap-super-admin", "db.url=" + url, "principal.id=" + superAdmin, "password=" + PASSWORD), err.toString());

        // create-subject needs principal.id
        assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=create-subject", "db.url=" + url, "password=" + PASSWORD));
        // and never hands out super_admin
        assertEquals(SecurityAdminTool.EXIT_FAILURE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin, "password=" + PASSWORD, "role=super_admin"));
        // unknown role is rejected before anything is created
        assertEquals(SecurityAdminTool.EXIT_FAILURE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin + "-x", "password=" + PASSWORD, "role=no_such_role"));

        out.reset();
        int code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=create-subject", "db.url=" + url, "principal.id=" + remoteAdmin, "password=" + PASSWORD, "role=domain_admin");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
        assertTrue(out.toString().contains("(created)") && out.toString().contains("domain_admin=granted"), out.toString());

        // rerun: idempotent, password untouched, role already there
        out.reset();
        code = SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
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
            shiro = local.loginSubject(superAdmin, PASSWORD, null, null);
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
        AppIDDefault app = new AppIDDefault("xlogistx.io", "appx");
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

        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=seed-catalog", "db.url=" + url), err.toString());
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=create-subject", "db.url=" + url, "principal.id=" + principal, "password=" + PASSWORD,
                "role=domain_admin", "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("role domain_admin [app " + shop + "]=granted"), out.toString());
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), err.toString());
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), "idempotent");
        assertTrue(out.toString().contains("already granted"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=super_admin", "app.id=" + shop), "never scoped");
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_user", "domain.id=xlogistx.io", "app.id=other"), err.toString());
        assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=grant-role", "db.url=" + url, "principal.id=" + principal, "role=app_user", "app.id=noseparator"), "malformed app.id");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=list-grants", "db.url=" + url, "principal.id=" + principal), err.toString());
        String listed = out.toString();
        assertTrue(listed.contains("role domain_admin [app " + shop + "]"), listed);
        assertTrue(listed.contains("role app_admin [app " + shop + "]"), listed);
        assertTrue(listed.contains("role app_user [app xlogistx.io-other]"), listed);

        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            Subject inShop = local.loginSubject(principal, PASSWORD, "xlogistx.io", "shopapp");
            assertTrue(inShop.isPermitted("subject:create"));
            assertTrue(inShop.hasRole("app_admin"));
            assertTrue(inShop.hasRole("domain_admin"));
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
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("revoked 1 grant(s) of role app_admin"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_FAILURE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=app_admin", "app.id=" + shop), "nothing left to revoke");
        assertEquals(SecurityAdminTool.EXIT_FAILURE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=revoke-role", "db.url=" + url, "principal.id=" + principal, "role=domain_admin"), "global grant does not exist, only the scoped one");
        out.reset();
        assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=revoke-app", "db.url=" + url, "principal.id=" + principal, "app.id=" + shop), err.toString());
        assertTrue(out.toString().contains("revoked 1 grant(s) [app " + shop + "]"), out.toString());
        assertEquals(SecurityAdminTool.EXIT_USAGE, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                "command=revoke-app", "db.url=" + url, "principal.id=" + principal), "app.id required");

        store = SecurityAdminTool_openStore(url);
        try {
            ShiroDSDomainSecurityManager local = new ShiroDSDomainSecurityManager(store);
            Subject inShop = local.loginSubject(principal, PASSWORD, "xlogistx.io", "shopapp");
            assertFalse(inShop.isPermitted("subject:create"), "removed from the shop app");
            assertFalse(inShop.hasRole("domain_admin"));
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

            // command line wins over the vault: the catalog lands in cliUrl, vaultUrl stays empty
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "db.url=" + cliUrl, "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(catalogSeeded(cliUrl), "seeded through db.url=");
            assertFalse(catalogSeeded(vaultUrl), "vault url untouched while db.url= is given");

            // no db.* on the command line: everything comes from the vault
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=seed-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(catalogSeeded(vaultUrl), "seeded through the vault's db.url");
            out.reset();
            assertEquals(SecurityAdminTool.EXIT_OK, SecurityAdminTool.run(new PrintStream(out), new PrintStream(err),
                    "command=list-catalog", "store=" + vaultFile, "store.password=" + vaultPassword), err.toString());
            assertTrue(out.toString().contains("super_admin_all = *"), out.toString());
        } finally {
            //noinspection ResultOfMethodCallIgnored
            vaultFile.delete();
        }
    }

    private static boolean catalogSeeded(String url) {
        H2PDataStore store = SecurityAdminTool_openStore(url);
        try {
            return new ShiroDSDomainSecurityManager(store).getPermissions().length > 0;
        } finally {
            store.close();
        }
    }

    private static H2PDataStore SecurityAdminTool_openStore(String url) {
        return new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(url, null, null, null));
    }
}
