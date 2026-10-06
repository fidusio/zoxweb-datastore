package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PExceptionHandler;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.crypto.EncapsulatedKey;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.util.NVGenericMap;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

/**
 * Shared fixture of the h2p tests. <b>The prerequisite of every run (user rule 2026-10-02):</b> a
 * {@link SecretStore} vault is opened with its password and its {@code db.*} settings and its
 * {@value #MASTER_KEY_ALIAS} secret key are taken from it ({@link #loadVault}); the master key goes
 * into the {@link KeyMakerProvider}, and every store a test opens carries that key maker and the
 * stub controller ({@link #secure}) — a store without them refuses to connect.
 * {@code -Dstore=<file> -Dstore.password=<pw>} name a real vault: a PostgreSQL {@code db.url} in it
 * is then the live target of the PostgreSQL-capable suites. Without {@code -Dstore} a throw-away
 * vault is created with a fresh master key and the tests stay on in-memory H2, unless
 * {@code -Dh2p.pg.url} (+ {@code h2p.pg.user/password/db}) points at a live PostgreSQL.
 * Also: subject keys, raw column reads.
 */
final class CryptoTestSupport {

    static final SecureRandom RANDOM = new SecureRandom();
    static final String MASTER_KEY_ALIAS = "master-key";
    static final String STORE_PROPERTY = "store";
    static final String STORE_PASSWORD_PROPERTY = "store.password";
    static SecretKey masterKey;
    private static NVGenericMap vaultDB;

    private CryptoTestSupport() {
    }

    /** Opens the vault once per JVM; every call (re)loads its master key into the key maker. */
    static synchronized void loadVault() {
        if (masterKey == null) {
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
                    vaultFile = File.createTempFile("h2p-test", ".store");
                    if (!vaultFile.delete()) {
                        throw new IllegalStateException("cannot prepare " + vaultFile);
                    }
                    vaultFile.deleteOnExit();
                    vaultPassword = "T3st-" + UUID.randomUUID();
                    try (SecretStore created = SecretStore.create(vaultFile, vaultPassword.toCharArray())) {
                        created.createSecretKey(MASTER_KEY_ALIAS);
                        created.save();
                    }
                }
                try (SecretStore vault = SecretStore.open(vaultFile, vaultPassword.toCharArray())) {
                    SecretKey stored = vault.getSecretKey(MASTER_KEY_ALIAS);
                    if (stored == null) {
                        throw new IllegalStateException(vaultFile + " holds no " + MASTER_KEY_ALIAS + " secret key");
                    }
                    vaultDB = vault.toNVGenericMap("db.");
                    masterKey = new SecretKeySpec(stored.getEncoded(), stored.getAlgorithm());
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("cannot load the test vault: " + e.getMessage(), e);
            }
        }
        KeyMakerProvider.SINGLETON.setMasterSecretKey(masterKey);
    }

    static synchronized SecretKey masterKey() {
        loadVault();
        return masterKey;
    }

    /** The {@code db.*} text secret of the vault ({@code db.url}, {@code db.user}, {@code db.password}, {@code db.enc-password}), or null. */
    static String db(String name) {
        loadVault();
        return vaultDB.getValue(name);
    }

    /** The vault's {@code db.url} when it names a PostgreSQL database, else null. */
    static String vaultPostgresURL() {
        String url = db("db.url");
        return url != null && url.startsWith("jdbc:postgresql:") ? url : null;
    }

    /** {@code cfg} as every run opens a store: the stub controller + the key maker with the vault's master key loaded. */
    static APIConfigInfo secure(APIConfigInfo cfg) {
        loadVault();
        cfg.setSecurityController(new TestSecurityController());
        cfg.setKeyMaker(KeyMakerProvider.SINGLETON);
        return cfg;
    }

    /** A store on {@link #config}: flags off build the incomplete configurations a store must refuse. */
    static H2PDataStore newStore(String h2Name, boolean withController, boolean withKeyMaker) {
        loadVault();
        APIConfigInfo cfg = config(h2Name);
        if (withController) cfg.setSecurityController(new TestSecurityController());
        if (withKeyMaker) cfg.setKeyMaker(KeyMakerProvider.SINGLETON);
        H2PDataStore ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        return ds;
    }

    static APIConfigInfo config(String h2Name) {
        String pg = System.getProperty("h2p.pg.url");
        if (pg != null && !pg.isEmpty()) {
            int schemeEnd = pg.indexOf("://");
            int pathStart = schemeEnd >= 0 ? pg.indexOf('/', schemeEnd + 3) : -1;
            String base = pathStart >= 0 ? pg.substring(0, pathStart) : pg;
            String db = System.getProperty("h2p.pg.db", "testpostgres");
            APIConfigInfo cfg = new H2PDSCreator().toAPIConfigInfo(base + "/" + db,
                    System.getProperty("h2p.pg.user"), System.getProperty("h2p.pg.password"));
            cfg.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");
            return cfg;
        }
        if (vaultPostgresURL() != null) {
            APIConfigInfo cfg = new H2PDSCreator().toAPIConfigInfo(vaultPostgresURL(), db("db.user"), db("db.password"));
            cfg.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");
            return cfg;
        }
        return H2PDSCreator.toAPIConfigInfo("jdbc:h2:mem:" + h2Name + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
    }

    /** A subject GUID whose subject key (wrapped under the master key) is stored — created with the subject in production. */
    static String newSubjectWithKey(H2PDataStore ds) {
        String guid = newSubjectGUID();
        SubjectIdentifier subject = new SubjectIdentifier();
        subject.setGUID(guid); // also sets subject_guid
        EncapsulatedKey sk = KeyMakerProvider.SINGLETON.createSubjectIDKey(subject, KeyMakerProvider.SINGLETON.getMasterKey());
        // the subject key is written by the security manager when it creates the subject: system context
        TestSecurityController.system(() -> ds.insert(sk));
        return guid;
    }

    static String newSubjectGUID() {
        return IDGs.UUIDV7.genID();
    }

    /** Raw column value, bypassing the datastore's read path. */
    static Object rawColumn(H2PDataStore ds, String table, String column, String guid) throws Exception {
        try (Connection con = ds.newConnection();
             PreparedStatement ps = con.prepareStatement("SELECT \"" + column + "\" FROM \"" + table + "\" WHERE \"guid\" = ?")) {
            ps.setObject(1, IDGs.UUIDV7.decode(guid));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getObject(1) : null;
            }
        }
    }

    static int countKeyRows(H2PDataStore ds, String referenceGUID) throws Exception {
        try (Connection con = ds.newConnection();
             PreparedStatement ps = con.prepareStatement("SELECT COUNT(*) FROM \"encapsulated_key\" WHERE \"reference_guid\" = ?")) {
            ps.setObject(1, IDGs.UUIDV7.decode(referenceGUID));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }
}
