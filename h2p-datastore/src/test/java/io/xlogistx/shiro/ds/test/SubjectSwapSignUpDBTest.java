package io.xlogistx.shiro.ds.test;

import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.shiro.ShiroSession;
import io.xlogistx.shiro.SubjectSwap;
import io.xlogistx.shiro.authc.DomainUsernamePasswordToken;
import io.xlogistx.shiro.ds.DSAuthorizingRealm;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.security.CredentialInfo;
import org.zoxweb.shared.security.RoleGrant;
import org.zoxweb.shared.security.RoleInfo;
import org.zoxweb.shared.security.SubjectAPIKey;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.BaseSubjectID;
import org.zoxweb.shared.util.NamedValue;
import org.zoxweb.shared.util.ResourceManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The registrar sign-up through the clean swap, xlogistx-shiro's {@link SubjectSwap} (it replaced the
 * manager's own {@code runAs} on 2026-10-03, after this test had passed). Tested the way production
 * runs (user, 2026-10-03):
 * <ul>
 * <li>the keystore named by {@code -Dstore} / {@code -Dstore.password} is mandatory — its
 * {@code db.*} entries are the database, its {@code master-key} is the key maker's master key, its
 * {@code super-admin-id} names the super-admin; no
 * throw-away vault, no in-memory database;</li>
 * <li>the database is persistent and was set up by the operator's tool first:
 * {@code bootstrap-super-admin}, then {@code create-app app.id=xlogistx.io-swapsignup}. The test
 * creates neither and deletes nothing: what it signs up stays, and a rerun finds it;</li>
 * <li>the registrar's signing key is not kept from the creation: it is read back from the datastore,
 * where it is sealed, through the master key; the registrar is logged in with the manager's
 * {@code loginUnboundSubjectJWT}, which binds nothing.</li>
 * </ul>
 * The same scenario runs twice: with the realm built in code (the manager's self-managed mode), then
 * with the realm declared in {@code shiro-ds.ini} (attached mode).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SubjectSwapSignUpDBTest {

    private static final String PASSWORD = "Secret123!";
    private static final String DOMAIN = "xlogistx.io";
    private static final String APP = "swapsignup";
    /** A fixed account: created by the first run, found and joined again by every later one. */
    private static final String RESIDENT = "swap-resident@example.com";

    private static H2PDataStore ds;
    private static AppIDDefault app;

    @BeforeAll
    public static void setup() throws Exception {
        String named = System.getProperty(TestVault.STORE_PROPERTY);
        if (named == null || named.trim().isEmpty()) {
            throw new IllegalStateException("this test simulates production: run it with -D" + TestVault.STORE_PROPERTY
                    + "=<keystore> -D" + TestVault.STORE_PASSWORD_PROPERTY + "=<password>");
        }
        TestVault.load(); // the master key into the key maker
        String url = TestVault.db("db.url");
        assertNotNull(url, "the keystore names the database (db.url)");
        APIConfigInfo cfg;
        if (url.startsWith("jdbc:postgresql:")) {
            Class.forName("org.postgresql.Driver");
            cfg = H2PDSCreator.toAPIConfigInfo(url, TestVault.db("db.user"), TestVault.db("db.password"));
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, TestVault.db("db.user"), TestVault.db("db.password"), TestVault.db("db.enc-password"));
        }
        ds = new H2PDSCreator().createAPI(null, TestVault.secure(cfg));
        System.out.println("target: " + url);

        ShiroDSDomainSecurityManager operator = new ShiroDSDomainSecurityManager(ds);
        operator.setSuperAdminPrincipalID(TestVault.superAdminID()); // the keystore names the super-admin
        assertNotNull(operator.lookupSuperAdminSubject(), "set the database up first: SecurityAdminTool command=bootstrap-super-admin"
                + " (super-admin " + TestVault.superAdminID() + ")");
        app = operator.lookupApp(DOMAIN, APP);
        assertNotNull(app, "create the app first: SecurityAdminTool command=create-app app.id=" + DOMAIN + "-" + APP);
    }

    @AfterAll
    public static void close() {
        ThreadContext.remove();
        if (ds != null) {
            ds.close();
        }
    }

    @AfterEach
    public void unbind() {
        ThreadContext.remove();
    }

    // ------------------------------------------------------------------ the two phases

    /** Phase A: the realm and its security manager are built in code by the manager itself. */
    @Test
    @Order(1)
    public void phaseA_realmBuiltInCode() throws Exception {
        ShiroDSDomainSecurityManager m = new ShiroDSDomainSecurityManager(ds);
        m.setSuperAdminPrincipalID(TestVault.superAdminID());
        assertTrue(m.isSelfManaged());
        scenario(m, "code");
    }

    /** Phase B: the realm is declared in shiro-ds.ini, the security manager is the INI's, the manager attaches to it. */
    @Test
    @Order(2)
    public void phaseB_realmFromShiroIni() throws Exception {
        Ini ini = Ini.fromResourcePath("classpath:shiro-ds.ini");
        org.apache.shiro.mgt.SecurityManager iniSM = new BasicIniEnvironment(ini).getSecurityManager();
        Object previousStore = ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE.getName());
        ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, ds);
        SecurityUtils.setSecurityManager(iniSM);
        ThreadContext.remove();
        try {
            ShiroDSDomainSecurityManager m = ShiroDSDomainSecurityManager.fromGlobal(ds);
            m.setSuperAdminPrincipalID(TestVault.superAdminID()); // not in the INI: the keystore names the super-admin
            assertFalse(m.isSelfManaged());
            assertSame(iniSM, m.getSecurityManager(), "the INI security manager is the one in use");
            assertEquals("shiro-ds", m.getRealm().getName(), "the realm declared in the INI");
            scenario(m, "ini");
        } finally {
            ThreadContext.remove();
            SecurityUtils.setSecurityManager(null);
            if (previousStore != null) {
                ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, previousStore);
            }
        }
    }

    // ------------------------------------------------------------------ the scenario

    private void scenario(ShiroDSDomainSecurityManager m, String phase) throws Exception {
        String run = phase + "-" + Long.toHexString(System.nanoTime());
        SubjectIdentifier registrar = m.lookupRegistrar(app);
        assertNotNull(registrar, "the app has its registrar");
        // the signing key, read back from the datastore through the master key
        SubjectAPIKey key = signingKeyOf(m, registrar);
        assertTrue(key.isSigningKey());
        assertNotNull(key.getAPIKeyAsBytes(), "the sealed secret opens with the master key");
        RoleInfo appAdmin = m.lookupRole(ShiroUtil.appScope(app), SecurityModel.Role.APP_ADMIN.getName());
        assertNotNull(appAdmin);

        m.setEnforcePermissions(true); // production: every mutation needs a permitted, logged-in subject
        try {
            ThreadContext.remove();

            // nobody logged in: no sign-up
            String first = "swap-" + run + "-1@example.com";
            assertThrows(AccessSecurityException.class, () -> m.registerSubject(first, PASSWORD));

            // the registrar is logged in WITHOUT being bound to the thread
            Subject reg = registrarLogin(m, key);
            assertTrue(reg.isAuthenticated());
            assertNull(ThreadContext.getSubject(), "logging the registrar in binds nothing");
            assertEquals(registrar.getGUID(), guidOf(reg));
            assertTrue(reg.hasRole(SecurityModel.Role.APP_REGISTRAR.getName()));

            // 1 + 3. sign-up inside the swap, on a thread with nobody bound: created, nothing left bound
            SubjectIdentifier made;
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                assertSame(reg, ThreadContext.getSubject(), "the registrar is the acting subject");
                made = m.registerSubject(first, PASSWORD);
            }
            assertNull(ThreadContext.getSubject(), "nothing left bound after the swap");
            assertNotNull(made);
            assertEquals(BaseSubjectID.SubjectType.USER, made.getSubjectType());
            assertEquals(made.getGUID(), m.lookupSubjectID(first).getGUID(), "persisted");
            Subject in = m.loginSubject(first, PASSWORD, DOMAIN, APP);
            assertTrue(in.hasRole(SecurityModel.Role.APP_USER.getName()), "the app's app_user");
            assertFalse(in.hasRole(SecurityModel.Role.APP_ADMIN.getName()));
            m.logout();
            RoleGrant[] grants = m.getRoleGrants(made.getGUID(), app);
            assertEquals(1, grants.length);
            assertEquals(registrar.getGUID(), grants[0].getBrokerGUID(), "the registrar is recorded as the grantor");

            // persistence: the resident account is created once, ever; every later sign-up joins it
            SubjectIdentifier residentBefore = m.lookupSubjectID(RESIDENT);
            SubjectIdentifier resident;
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                resident = m.registerSubject(RESIDENT, PASSWORD);
            }
            if (residentBefore != null) {
                assertEquals(residentBefore.getGUID(), resident.getGUID(), "an existing account joins, no second subject");
            }
            assertEquals(1, m.getRoleGrants(resident.getGUID(), app).length, "one app_user grant, however often it signs up");
            System.out.println("[" + phase + "] resident " + resident.getGUID() + (residentBefore != null ? " (found from an earlier run or phase)" : " (created now)"));

            // 2. a subject bound before the swap is bound again after it — the same instance
            Subject user = m.loginSubject(RESIDENT, PASSWORD, DOMAIN, APP);
            assertSame(user, ThreadContext.getSubject());
            String second = "swap-" + run + "-2@example.com";
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                assertSame(reg, ThreadContext.getSubject());
                assertNotNull(m.registerSubject(second, PASSWORD));
            }
            assertSame(user, ThreadContext.getSubject(), "the previous subject is back");
            assertTrue(user.isAuthenticated());
            assertEquals(resident.getGUID(), guidOf(user));
            // the resident itself may not sign anybody up
            assertThrows(AccessSecurityException.class, () -> m.registerSubject("swap-" + run + "-no@example.com", PASSWORD));

            // 4. a failure inside the swap restores the thread as well
            assertThrows(IllegalStateException.class, () -> {
                try (SubjectSwap swap = new SubjectSwap(reg)) {
                    throw new IllegalStateException("boom");
                }
            });
            assertSame(user, ThreadContext.getSubject());
            assertThrows(AccessSecurityException.class, () -> {
                try (SubjectSwap swap = new SubjectSwap(reg)) {
                    m.registerSubject(RESIDENT, "wrong-" + PASSWORD); // a known principal must prove its password
                }
            });
            assertSame(user, ThreadContext.getSubject());

            // 5. the registrar may do nothing but sign up
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                assertThrows(AccessSecurityException.class, () -> m.createApp(DOMAIN, "swapx" + Long.toHexString(System.nanoTime())));
                assertThrows(AccessSecurityException.class, () -> m.addRoleGrant(made, appAdmin, app));
                assertThrows(AccessSecurityException.class, () -> m.deleteSubjectID(made));
                assertThrows(AccessSecurityException.class, () -> m.rotateRegistrarKey(app));
            }
            assertSame(user, ThreadContext.getSubject());

            // 10. the datastore's own access check follows the swap
            assertEquals(1, ds.searchByID(SubjectIdentifier.class.getName(), resident.getGUID()).size(), "the user reads its own record");
            assertTrue(ds.searchByID(SubjectIdentifier.class.getName(), registrar.getGUID()).isEmpty(), "and not the registrar's");
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                assertEquals(1, ds.searchByID(SubjectIdentifier.class.getName(), registrar.getGUID()).size(), "inside the swap: the registrar's own record");
                assertTrue(ds.searchByID(SubjectIdentifier.class.getName(), resident.getGUID()).isEmpty(), "and not the user's");
            }
            assertEquals(1, ds.searchByID(SubjectIdentifier.class.getName(), resident.getGUID()).size());

            // 8. nested swaps restore in reverse order
            Subject other = passwordLogin(m, first);
            try (SubjectSwap outer = new SubjectSwap(reg)) {
                assertSame(reg, ThreadContext.getSubject());
                try (SubjectSwap inner = new SubjectSwap(other)) {
                    assertSame(other, ThreadContext.getSubject());
                    assertThrows(AccessSecurityException.class, () -> m.registerSubject("swap-" + run + "-in@example.com", PASSWORD), "an app user signs nobody up");
                }
                assertSame(reg, ThreadContext.getSubject());
            }
            assertSame(user, ThreadContext.getSubject());
            other.logout();
            m.logout();
            assertNull(ThreadContext.getSubject());

            // 6. one registrar login serves many sign-ups; so does a fresh login for each
            for (int i = 0; i < 3; i++) {
                String p = "swap-" + run + "-r" + i + "@example.com";
                try (SubjectSwap swap = new SubjectSwap(reg)) {
                    assertNotNull(m.registerSubject(p, PASSWORD));
                }
                assertNull(ThreadContext.getSubject());
            }
            for (int i = 0; i < 2; i++) {
                String p = "swap-" + run + "-f" + i + "@example.com";
                Subject fresh = registrarLogin(m, key);
                try (SubjectSwap swap = new SubjectSwap(fresh)) {
                    assertNotNull(m.registerSubject(p, PASSWORD));
                } finally {
                    fresh.logout();
                }
                assertNull(ThreadContext.getSubject());
            }

            // 9. pooled worker threads share the registrar subject; every worker ends unbound
            ExecutorService pool = Executors.newFixedThreadPool(3);
            try {
                List<Future<String>> results = new ArrayList<>();
                for (int i = 0; i < 6; i++) {
                    final String p = "swap-" + run + "-t" + i + "@example.com";
                    results.add(pool.submit(() -> {
                        assertNull(ThreadContext.getSubject(), "a worker starts unbound");
                        SubjectIdentifier s;
                        try (SubjectSwap swap = new SubjectSwap(reg)) {
                            s = m.registerSubject(p, PASSWORD);
                        }
                        assertNull(ThreadContext.getSubject(), "a worker ends unbound");
                        return s.getGUID();
                    }));
                }
                Set<String> guids = new HashSet<>();
                for (Future<String> f : results) {
                    guids.add(f.get(120, TimeUnit.SECONDS));
                }
                assertEquals(6, guids.size(), "six sign-ups, six subjects");
            } finally {
                pool.shutdownNow();
            }
            assertNull(ThreadContext.getSubject());

            // 11. a swap carried by a ShiroSession is closed when the session closes
            Subject caller = m.loginSubject(RESIDENT, PASSWORD, DOMAIN, APP);
            ShiroSession<Object> session = new ShiroSession<>(caller);
            SubjectSwap carried = new SubjectSwap(reg);
            session.getProperties().build(new NamedValue<SubjectSwap>(SubjectSwap.SUBJECT_SWAP, carried));
            assertSame(reg, ThreadContext.getSubject());
            assertNotNull(m.registerSubject("swap-" + run + "-s@example.com", PASSWORD));
            session.close();
            assertSame(caller, ThreadContext.getSubject(), "closing the session closed the swap: the caller is back");
            assertFalse(caller.isAuthenticated(), "and the session logged its own subject out");
            assertTrue(reg.isAuthenticated(), "the registrar's login is untouched");
            ThreadContext.remove();

            // 7. a logged-out registrar signs nobody up
            reg.logout();
            try (SubjectSwap swap = new SubjectSwap(reg)) {
                assertThrows(AccessSecurityException.class, () -> m.registerSubject("swap-" + run + "-out@example.com", PASSWORD));
            }
            assertNull(ThreadContext.getSubject());
            assertNull(m.lookupSubjectID("swap-" + run + "-out@example.com"));
        } finally {
            m.setEnforcePermissions(false);
            ThreadContext.remove();
        }
    }

    // ------------------------------------------------------------------ helpers

    /** The registrar's signing key as the datastore serves it to the manager: the secret in clear, opened with the master key. */
    private static SubjectAPIKey signingKeyOf(ShiroDSDomainSecurityManager m, SubjectIdentifier registrar) {
        for (CredentialInfo ci : m.lookupCredentialsBySubjectGUID(registrar.getGUID(), CredentialInfo.Type.SYMMETRIC_KEY)) {
            if (ci instanceof SubjectAPIKey) {
                return (SubjectAPIKey) ci;
            }
        }
        return fail("the registrar has no signing key");
    }

    /** Logs the registrar in with a JWT signed by its key; the subject is returned, not bound to the thread. */
    private static Subject registrarLogin(ShiroDSDomainSecurityManager m, SubjectAPIKey key) {
        return m.loginUnboundSubjectJWT(ShiroDSDomainSecurityManager.mintJWT(key, null, 5 * 60_000L), null);
    }

    /** Logs a user of the app in with its password; the subject is returned, not bound to the thread. */
    private static Subject passwordLogin(ShiroDSDomainSecurityManager m, String principal) {
        Subject subject = new Subject.Builder(m.getSecurityManager()).buildSubject();
        subject.login(new DomainUsernamePasswordToken(principal, PASSWORD, false, null, DOMAIN, APP));
        return subject;
    }

    private static String guidOf(Subject subject) {
        return DSAuthorizingRealm.subjectGUIDOf(subject.getPrincipals());
    }
}
