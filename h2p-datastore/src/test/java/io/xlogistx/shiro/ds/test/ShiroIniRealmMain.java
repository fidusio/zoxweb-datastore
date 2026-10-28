package io.xlogistx.shiro.ds.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PSetup;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.DomainUsernamePasswordToken;
import io.xlogistx.shiro.ds.DSAuthorizingRealm;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.util.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.Console;
import java.io.File;
import java.util.List;

/**
 * Builds a deployment's datastore and Shiro realm from its two files and nothing else:
 * <ol>
 * <li>the {@link SecretStore} vault ({@code store=}, {@code store.password=} or a console prompt):
 * its {@code master-key} goes into the {@link KeyMakerProvider}, its {@code db.*} entries are the
 * database, its {@code super-admin-id} names the super-admin;</li>
 * <li>the datastore is created on that database with the {@link ShiroSecurityController} and the
 * key maker — <b>nothing is set up</b>: no bootstrap, no catalog, no app, no table is created by
 * this class (the database is what it is; use {@code SecurityAdminTool} / {@code H2PSetup} for that);</li>
 * <li>the realm is built from {@code shiro.ini=}: the datastore is registered under
 * {@code ResourceManager.Resource.DATA_STORE}, the INI security manager is loaded with
 * {@link ShiroUtil#loadSecurityManager(String)} and installed as the global one, and
 * {@link ShiroDSDomainSecurityManager#fromGlobal()} attaches the manager to the INI's
 * {@link DSAuthorizingRealm}; the super-admin id from the vault is handed to it.</li>
 * </ol>
 * {@link #build} returns everything built so a caller can log in and test; {@link #main} builds,
 * prints what it built and closes the datastore.
 * <pre>
 * cd h2p-datastore
 * java -cp "$(cat ../.claude/test-runner/cp-shiro.txt)" io.xlogistx.shiro.ds.test.ShiroIniRealmMain
 *      store=&lt;vault&gt; [store.password=&lt;pw&gt;] shiro.ini=&lt;shiro.ini&gt;
 * </pre>
 */
public class ShiroIniRealmMain {

    public static final String STORE = "store";
    public static final String STORE_PASSWORD = "store.password";
    public static final String SHIRO_INI = "shiro.ini";

    /** What {@link #build} made. */
    public static final class Built implements AutoCloseable {
        public final File store;
        public final String dbURL;
        public final String superAdminID;
        /** The deployment's default app from {@code [xlogistx] app.id} of the INI, or null when the INI has none. */
        public final String appID;
        public final H2PDataStore dataStore;
        public final SecurityManager securityManager;
        public final DSAuthorizingRealm realm;
        public final ShiroDSDomainSecurityManager dsm;

        Built(File store, String dbURL, String superAdminID, String appID, H2PDataStore dataStore,
              SecurityManager securityManager, ShiroDSDomainSecurityManager dsm) {
            this.store = store;
            this.dbURL = dbURL;
            this.superAdminID = superAdminID;
            this.appID = appID;
            this.dataStore = dataStore;
            this.securityManager = securityManager;
            this.realm = dsm.getRealm();
            this.dsm = dsm;
        }

        /** Unbinds the thread, drops the global security manager and closes the datastore. */
        @Override
        public void close() {
            ThreadContext.remove();
            SecurityUtils.setSecurityManager(null);
            dataStore.close();
        }

        @Override
        public String toString() {
            return "store=" + store + "\ndb.url=" + dbURL + "\nsuper-admin-id=" + superAdminID
                    + "\napp.id=" + (appID != null ? appID : "(none in the ini)")
                    + "\nrealm=" + realm.getName() + " (" + realm.getClass().getName() + ")"
                    + "\nsecurityManager=" + securityManager.getClass().getName()
                    + "\nmanager=" + dsm.getClass().getName() + " attached=" + !dsm.isSelfManaged();
        }
    }

    /**
     * Opens the vault, creates the datastore on its database and builds the realm from the INI.
     *
     * @param store         the {@link SecretStore} file
     * @param storePassword its password (zeroed by the caller)
     * @param shiroIni      the INI: a file path, or a Shiro resource path such as {@code classpath:shiro-ds.ini}
     */
    public static Built build(File store, char[] storePassword, String shiroIni) throws Exception {
        if (store == null || !store.isFile()) {
            throw new IllegalArgumentException("secret store not found: " + store);
        }
        if (shiroIni == null || shiroIni.trim().isEmpty()) {
            throw new IllegalArgumentException("shiro.ini required");
        }
        shiroIni = shiroIni.trim();
        if (!shiroIni.contains(":") && !new File(shiroIni).isFile()) {
            throw new IllegalArgumentException("shiro.ini not found: " + new File(shiroIni).getAbsolutePath());
        }
        OPSecUtil.singleton();

        // 1. the vault: master key, db.*, super-admin id
        NVGenericMap db;
        String superAdminID;
        try (SecretStore vault = SecretStore.open(store, storePassword)) {
            List<SecretStore.StoreParam> missing = vault.missingMandatory();
            if (!missing.isEmpty()) {
                throw new IllegalStateException(store + " lacks mandatory entries: " + missing);
            }
            SecretKey masterKey = vault.getSecretKey(SecretStore.StoreParam.MASTER_KEY.getName());
            KeyMakerProvider.SINGLETON.setMasterSecretKey(new SecretKeySpec(masterKey.getEncoded(), masterKey.getAlgorithm()));
            db = vault.toNVGenericMap("db.");
            superAdminID = vault.getSuperAdminID();
        }
        String url = db.getValue(SecretStore.StoreParam.DB_URL.getName());
        String user = db.getValue(SecretStore.StoreParam.DB_USER.getName());
        String password = db.getValue(SecretStore.StoreParam.DB_PASSWORD.getName());
        String encPassword = db.getValue(SecretStore.StoreParam.DB_ENC_PASSWORD.getName());

        // 2. the datastore, with the controller and the key maker; nothing is set up
        APIConfigInfo cfg;
        if (url.startsWith("jdbc:postgresql:")) {
            Class.forName("org.postgresql.Driver");
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password);
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password, encPassword);
        }
        cfg.setSecurityController(new ShiroSecurityController());
        cfg.setKeyMaker(KeyMakerProvider.SINGLETON);
        H2PDataStore ds = new H2PDSCreator().createAPI(null, cfg);

        // 3. the realm from the INI
        ShiroDSDomainSecurityManager dsm;
        try {
            ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, ds);
            SecurityManager sm = ShiroUtil.loadSecurityManager(shiroIni);
            SecurityUtils.setSecurityManager(sm);
            ThreadContext.remove();
            dsm = ShiroDSDomainSecurityManager.fromGlobal();
            dsm.setSuperAdminPrincipalID(superAdminID); // never in the INI: the vault names the super-admin
            Ini ini = Ini.fromResourcePath(shiroIni);
            String appID = ini.getSectionProperty(H2PSetup.INI_APP_SECTION, H2PSetup.INI_APP_ID_KEY);
            return new Built(store, url, superAdminID, appID != null && !appID.trim().isEmpty() ? appID.trim() : null,
                    ds, sm, dsm);
        } catch (RuntimeException | Error e) {
            SecurityUtils.setSecurityManager(null);
            ds.close();
            throw e;
        }
    }

    /**
     * Logs in through Shiro only: the thread's {@link Subject} from {@link SecurityUtils#getSubject()}
     * and a {@link DomainUsernamePasswordToken} (the only password token the {@link DSAuthorizingRealm}
     * supports), no manager involved. The global security manager set by {@link #build} does the rest.
     *
     * @param principalID the principal (subject id / email)
     * @param password    its password
     * @return the authenticated subject
     * @throws AuthenticationException when the realm rejects the credentials
     */
    public static Subject login(String principalID, char[] password) throws AuthenticationException {
        Subject subject = SecurityUtils.getSubject();
        DomainUsernamePasswordToken token = new DomainUsernamePasswordToken(principalID, new String(password), false, null, null, null);
        try {
            subject.login(token);
        } finally {
            token.clear();
        }
        return subject;
    }

    public static void main(String[] args) throws Exception {
        H2PDataStore.log.setEnabled(false);
        long ts = System.currentTimeMillis();
        ParamUtil.ParamMap params = ParamUtil.parse("=", args);
        String store = params.stringValue(STORE, true);
        String shiroIni = params.stringValue(SHIRO_INI, true);
        char[] adminPassword = params.charsValue("admin-password", true);
        if (store == null || shiroIni == null) {
            System.err.println("usage: " + ShiroIniRealmMain.class.getName() + " " + STORE + "=<secret store> ["
                    + STORE_PASSWORD + "=<password>] " + SHIRO_INI + "=<shiro.ini>");
            System.exit(1);
        }
        String given = params.stringValue(STORE_PASSWORD, true);
        char[] password;
        Console console = null;
        if (given != null) {
            password = given.toCharArray();
        } else {
            console = System.console();
            if (console == null) {
                System.err.println(STORE_PASSWORD + "= required (no console to ask)");
                System.exit(1);
                return;
            }
            password = console.readPassword("Password for secret store " + new File(store).getName() + ": ");
        }
        try (Built built = build(new File(store), password, shiroIni)) {
            System.out.println(built);
            if(SUS.isEmpty(adminPassword)) {
                if(console == null) {
                    console = System.console();
                }
                if(console != null) {
                    adminPassword = console.readPassword("Admin password", true);
                }
            }
            // the datastore and the realm are up: log in and test here, e.g.
            if(!SUS.isEmpty(adminPassword)) {
                Subject sub = ShiroUtil.login(built.superAdminID, adminPassword, null, null, false );
                //built.dsm.loginSubject(built.superAdminID, new String(adminPassword), null, null); //... built.dsm.logout();
                System.out.println("SubjectPrincipalID: " + ShiroUtil.subjectPrincipalID());
                System.out.println("SubjectGUID: " + ShiroUtil.subjectGUID());
            }
        } finally {
            java.util.Arrays.fill(password, '\0');
        }
        ts =  System.currentTimeMillis() - ts;
        System.out.println("It took overall: " + Const.TimeInMillis.toString(ts));
    }
}
