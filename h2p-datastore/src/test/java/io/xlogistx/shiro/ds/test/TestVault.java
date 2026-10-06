package io.xlogistx.shiro.ds.test;

import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.ds.tools.SecurityAdminTool;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.util.NVGenericMap;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.UUID;

/**
 * The prerequisite of every test run, the same as the tool's (user rule 2026-10-02): a
 * {@link SecretStore} is opened with its password, and the {@code db.*} settings, the
 * {@code master-key} secret key and the super-admin id
 * ({@value SecretStore#SUPER_ADMIN_ID}; never hard coded, user rule 2026-10-03) are taken from it. The master key goes
 * into the {@link KeyMakerProvider}; every store the tests open carries that key maker and the
 * {@link ShiroSecurityController}, so subjects get their subject key and encryption at rest and
 * the store's access check are on, as in a real run.
 * <p>
 * {@code -Dstore=<file> -Dstore.password=<pw>} name a real vault: its {@code db.*} entries are then
 * the target of the run unless {@code -Dds.url} overrides them. Without {@code -Dstore} a
 * throw-away vault is created with a fresh master key, a generated super-admin id, a generated
 * initial super-admin password, a {@code db.url} naming a private in-memory H2 database and no other
 * {@code db.*} entry, and the tests stay on their in-memory H2 default.
 */
final class TestVault {

    static final String STORE_PROPERTY = "store";
    static final String STORE_PASSWORD_PROPERTY = "store.password";

    private static File file;
    private static String password;
    private static NVGenericMap db;
    private static SecretKey masterKey;
    private static String superAdminID;
    private static String superAdminPassword;

    private TestVault() {
    }

    /** Opens the vault once per JVM and loads its master key into the key maker. */
    static synchronized void load() {
        if (file == null) {
            OPSecUtil.singleton();
            try {
                String named = System.getProperty(STORE_PROPERTY);
                File vaultFile;
                String vaultPassword;
                if (named != null && !named.trim().isEmpty()) {
                    vaultFile = new File(named.trim());
                    vaultPassword = System.getProperty(STORE_PASSWORD_PROPERTY);
                    if (vaultPassword == null || vaultPassword.isEmpty()) {
                        throw new IllegalStateException("-D" + STORE_PASSWORD_PROPERTY + " required with -D" + STORE_PROPERTY);
                    }
                } else {
                    vaultFile = File.createTempFile("shiro-ds-test", ".store");
                    if (!vaultFile.delete()) {
                        throw new IllegalStateException("cannot prepare " + vaultFile);
                    }
                    vaultFile.deleteOnExit();
                    vaultPassword = "T3st-" + UUID.randomUUID();
                    try (SecretStore created = SecretStore.create(vaultFile, vaultPassword.toCharArray())) {
                        created.createSecretKey(SecurityAdminTool.MASTER_KEY_ALIAS);
                        created.setSuperAdminID("super-admin-" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
                        created.put(SecurityAdminTool.SUPER_ADMIN_PASSWORD, "Init-" + UUID.randomUUID().toString().substring(0, 8) + "-aA1!");
                        created.put(SecretStore.StoreParam.DB_URL.getName(), "jdbc:h2:mem:throwaway" + UUID.randomUUID().toString().substring(0, 8)
                                + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
                        created.save();
                    }
                }
                try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                    SecretKey stored = vault.getSecretKey(SecurityAdminTool.MASTER_KEY_ALIAS);
                    if (stored == null) {
                        throw new IllegalStateException(vaultFile + " holds no " + SecurityAdminTool.MASTER_KEY_ALIAS + " secret key");
                    }
                    java.util.List<SecretStore.StoreParam> missing = vault.missingMandatory();
                    if (!missing.isEmpty()) {
                        throw new IllegalStateException(vaultFile + " lacks mandatory entries: " + missing);
                    }
                    String admin = vault.getSuperAdminID();
                    db = vault.toNVGenericMap("db.");
                    masterKey = new SecretKeySpec(stored.getEncoded(), stored.getAlgorithm());
                    superAdminID = admin;
                    superAdminPassword = vault.get(SecurityAdminTool.SUPER_ADMIN_PASSWORD);
                }
                file = vaultFile;
                password = vaultPassword;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("cannot load the test vault: " + e.getMessage(), e);
            }
        }
        KeyMakerProvider.SINGLETON.setMasterSecretKey(masterKey);
    }

    /** The vault's master key, e.g. to put into another vault a test builds. */
    static SecretKey masterKey() {
        load();
        return masterKey;
    }

    /** The super-admin id of the vault ({@value SecretStore#SUPER_ADMIN_ID}): the only place it is defined. */
    static String superAdminID() {
        load();
        return superAdminID;
    }

    /** The super-admin's initial password of the vault ({@code super-admin-password}, mandatory): what a tool bootstrap creates the account with. */
    static String superAdminPassword() {
        load();
        return superAdminPassword;
    }

    /** The {@code db.*} text secret of the vault ({@code db.url}, {@code db.user}, ...), or null. */
    static String db(String name) {
        load();
        return db.getValue(name);
    }

    /** {@code cfg} as every run opens a store: Shiro controller + key maker with the master key loaded. */
    static APIConfigInfo secure(APIConfigInfo cfg) {
        load();
        cfg.setSecurityController(new ShiroSecurityController());
        cfg.setKeyMaker(KeyMakerProvider.SINGLETON);
        return cfg;
    }

    /** Tool arguments with the vault in front: {@code store=<file> store.password=<pw>} + {@code args}. */
    static String[] toolArgs(String... args) {
        load();
        return toolArgs(file, args);
    }

    private static String[] toolArgs(File vaultFile, String... args) {
        String[] ret = new String[args.length + 2];
        ret[0] = "store=" + vaultFile;
        ret[1] = "store.password=" + password;
        System.arraycopy(args, 0, ret, 2, args.length);
        return ret;
    }

    /** Vaults made by {@link #vaultFor}, one per database URL. */
    private static final java.util.Map<String, File> VAULTS_BY_URL = new java.util.HashMap<>();

    /**
     * A vault for another database: the tool takes its database from the vault's mandatory
     * {@code db.url} only, so a test that works on a private database gives the tool a vault of its
     * own. It is a copy of the run's vault (same password, master key, super-admin id and initial
     * password, same {@code db.user} / {@code db.password} / {@code db.enc-password}) with
     * {@code db.url} = {@code url}. Created once per URL, deleted when the JVM exits.
     */
    static synchronized File vaultFor(String url) {
        load();
        File ret = VAULTS_BY_URL.get(url);
        if (ret == null) {
            try {
                ret = File.createTempFile("shiro-ds-test-url", ".store");
                if (!ret.delete()) {
                    throw new IllegalStateException("cannot prepare " + ret);
                }
                ret.deleteOnExit();
                try (SecretStore copy = SecretStore.create(ret, password.toCharArray())) {
                    copy.putSecretKey(SecurityAdminTool.MASTER_KEY_ALIAS, masterKey);
                    copy.setSuperAdminID(superAdminID);
                    copy.put(SecurityAdminTool.SUPER_ADMIN_PASSWORD, superAdminPassword);
                    for (SecretStore.StoreParam p : new SecretStore.StoreParam[]{SecretStore.StoreParam.DB_USER,
                            SecretStore.StoreParam.DB_PASSWORD, SecretStore.StoreParam.DB_ENC_PASSWORD}) {
                        String value = db.getValue(p.getName());
                        if (value != null && !value.isEmpty()) {
                            copy.put(p.getName(), value);
                        }
                    }
                    copy.put(SecretStore.StoreParam.DB_URL.getName(), url);
                    copy.save();
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("cannot create a vault for " + url + ": " + e.getMessage(), e);
            }
            VAULTS_BY_URL.put(url, ret);
        }
        return ret;
    }

    /** Tool arguments for a run on the database {@code url}: the vault made for it by {@link #vaultFor}, then {@code args}. */
    static String[] toolArgsFor(String url, String... args) {
        return toolArgs(vaultFor(url), args);
    }

    /**
     * A view of {@code store} whose every call runs in the system context: what a test uses for its
     * fixtures and its raw checks (rows written or read with nobody logged in, or in another
     * subject's name), which the store's own access check refuses on the plain store.
     */
    static APIDataStore<?, ?> systemView(APIDataStore<?, ?> store) {
        return (APIDataStore<?, ?>) Proxy.newProxyInstance(APIDataStore.class.getClassLoader(),
                new Class<?>[]{APIDataStore.class},
                (proxy, method, args) -> {
                    Throwable[] failure = new Throwable[1];
                    Object ret = ShiroUtil.runAsSystem(() -> {
                        try {
                            return method.invoke(store, args);
                        } catch (InvocationTargetException e) {
                            failure[0] = e.getCause();
                        } catch (IllegalAccessException e) {
                            failure[0] = e;
                        }
                        return null;
                    });
                    if (failure[0] != null) {
                        throw failure[0];
                    }
                    return ret;
                });
    }
}
