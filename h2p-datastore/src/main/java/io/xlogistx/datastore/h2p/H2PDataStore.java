/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.xlogistx.datastore.h2p;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.xlogistx.datastore.h2p.H2PDSCreator.H2PParam;
import org.zoxweb.server.api.APIServiceProviderBase;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.util.DateUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.server.util.MetaUtil;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIDocumentStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APIFileInfoMap;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.FileInfo;
import org.zoxweb.shared.data.LongSequence;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.io.SharedIOUtil;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.security.SecurityController;
import org.zoxweb.shared.util.*;
import org.zoxweb.shared.util.ExceptionReason.Reason;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * H2 implementation of {@link APIDataStore}.
 *
 * <p><b>Storage model (fully normalized, relational).</b> One table per {@link NVConfigEntity}
 * type. Each row has a {@code guid uuid PRIMARY KEY} (UUID v7). Attributes map by kind
 * ({@link H2PUtil#classify}): scalars → typed columns; reserved/reference-id → {@code uuid};
 * {@code byte[]} → {@code bytea}; a single entity reference → a {@code uuid} column with a
 * FOREIGN KEY to the referenced type's table; an entity collection → a join table
 * ({@code <table>__<attr>}) with FK constraints and {@code ON DELETE CASCADE}; and schemaless
 * containers (NVGenericMap, NamedValue, primitive lists) → a {@code varchar} column holding JSON
 * via {@link GSONUtil#toJSONDefault}/{@code fromJSONDefault}. Referenced entities are stored as
 * their own rows and resolved on read — no binary serialization, DB-enforced referential integrity.
 *
 * <p><b>Dialect.</b> The emitted SQL is PostgreSQL-portable (types {@code uuid}, {@code bytea},
 * {@code varchar}, {@code bigint}, {@code double precision}, {@code boolean}; {@code CREATE TABLE
 * IF NOT EXISTS}; {@code FOREIGN KEY … ON DELETE CASCADE}; standard {@code INFORMATION_SCHEMA}).
 * The same code runs on H2 (in {@code MODE=PostgreSQL}) and on a real PostgreSQL server by only
 * swapping the JDBC driver + URL.
 *
 * <p><b>Access control.</b> With a {@link SecurityController} on the {@link APIConfigInfo}, every
 * entity that passes through this store is checked for the subject bound to the calling thread,
 * encrypted or not: a row is returned only when the subject may {@code read} it (a denied row is
 * silently absent — fewer search results, a null reference, a missing collection member), and is
 * updated or deleted only when the subject holds that verb on it (denied ⇒
 * {@link AccessSecurityException}). The store never compares owners itself: it asks the controller,
 * with the row's GUID and its <em>stored</em> {@code subject_guid}. Code that must reach the store
 * with nobody logged in runs inside the controller's system context
 * ({@link SecurityController#runAsSystem}). Without a controller nothing is checked.
 */
@SuppressWarnings("serial")
public class H2PDataStore extends APIServiceProviderBase<Connection, Connection>
        implements APIDataStore<Connection, Connection>, APIDocumentStore<Connection, Connection> {

    public static final LogWrapper log = new LogWrapper(H2PDataStore.class);

    static final String SEQ_TABLE = "sys_long_sequence";
    static final String DEM_TABLE = "dynamic_enum_map";
    static final String FILE_VERSION_TABLE = "sys_file_version";
    static final String FILE_HEAD_TABLE = "sys_file_head";
    /** {@code sys_file_version.enc}: {@link #FILE_ENC_PLAIN} or {@link #FILE_ENC_VX} (AESCrypt VX container under the file's entity key). */
    static final String FILE_ENC_COLUMN = "enc";
    static final int FILE_ENC_PLAIN = 0;
    static final int FILE_ENC_VX = 1;
    static final String META_CATALOG_TABLE = "sys_meta_catalog";

    private volatile boolean driverLoaded = false;
    private volatile String name;
    private volatile String description;

    private final Set<Connection> connections = new HashSet<>();
    private final Lock lock = new ReentrantLock();
    private final Lock ddlLock = new ReentrantLock();
    private final H2PMetaManager metaManager = new H2PMetaManager();
    private final Set<String> createdTables = ConcurrentHashMap.newKeySet();
    // Resolved once from the config at creation; drives getDSType() and the schemaless dialect codec.
    private volatile DSType currentDSType = DSType.UNKNOWN;
    private volatile H2PDialect dialect = H2PDialect.H2;
    // Lazily-built HikariCP connection pool (both engines); closed+reset on reconfigure and close().
    private volatile HikariDataSource pool = null;
    // One-shot guard for the file-storage DDL (sys_file_version/sys_file_head); reset on reconfigure.
    private volatile boolean fileTablesEnsured = false;
    // Types already written to sys_meta_catalog this session (avoid an upsert per operation);
    // one-shot DDL guard for the catalog table itself. Both reset on reconfigure.
    private final Set<String> catalogSynced = ConcurrentHashMap.newKeySet();
    private volatile boolean metaCatalogEnsured = false;

    /**
     * A JDBC transaction is bound to the calling thread via this ThreadLocal connection
     * (autoCommit=false). Every data operation routes through {@link #acquire()}: when a
     * transaction is active the op joins it; otherwise it runs on a fresh auto-committed
     * connection — identical to the non-transactional path. Mirrors the ambient-session
     * design of {@code XlogistxMongoDataStore} (there a {@code ThreadLocal<ClientSession>}).
     * Schema DDL is always run out-of-band on its own connection ({@link #execDDL}) because
     * H2 implicitly commits on DDL, which would otherwise end the ambient transaction early.
     */
    private final ThreadLocal<Connection> txConnection = new ThreadLocal<>();

    /** Encryption at rest (fields + files); a SecurityController AND a KeyMaker with its master key are required to connect at all ({@link #requireMasterKey}). */
    private final H2PFieldCrypto crypto = new H2PFieldCrypto(this);

    public H2PDataStore() {
    }

    /** @return true when this store encrypts ENCRYPT* attributes and file content (controller + key maker configured). */
    public boolean isEncryptionActive() {
        return crypto.active();
    }

    /** @return true when this store checks every read and write against its {@link SecurityController} (one is configured). */
    public boolean isAccessControlActive() {
        return crypto.controller() != null;
    }

    public H2PDataStore(APIConfigInfo configInfo) {
        setAPIConfigInfo(configInfo);
    }

    public H2PMetaManager getMetaManager() {
        return metaManager;
    }

    // ---------- Config / lifecycle ----------

    @Override
    public void setAPIConfigInfo(APIConfigInfo configInfo) {
        super.setAPIConfigInfo(configInfo);
        // Resolve the target engine once, at creation, and pick the matching schemaless dialect codec.
        this.currentDSType = H2PDSCreator.resolveDSType(configInfo);
        this.dialect = H2PDialect.forDSType(currentDSType);
        // A new config may point at a different database: retire the old pool so the next op
        // connects with the new URL/credentials, and forget the old database's tables.
        HikariDataSource p = pool;
        if (p != null) {
            pool = null;
            SharedIOUtil.close(p);
        }
        createdTables.clear();
        metaManager.clear();
        fileTablesEnsured = false;
        catalogSynced.clear();
        metaCatalogEnsured = false;
    }

    @Override
    public Connection connect() throws APIException {
        if (!driverLoaded) {
            lock.lock();
            try {
                if (!driverLoaded) {
                    SUS.checkIfNulls("Configuration null", getAPIConfigInfo());
                    String driverClassName = getAPIConfigInfo().getProperties().getValue(H2PParam.DRIVER);
                    try {
                        Class.forName(driverClassName);
                        driverLoaded = true;
                    } catch (ClassNotFoundException e) {
                        throw new APIException("JDBC driver not loaded: " + driverClassName);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return newConnection();
    }

    /**
     * The database is never used without the master key (user rule 2026-10-02): every connection
     * this store hands out or uses goes through here first. The configuration must carry a
     * {@link SecurityController} and a {@code KeyMaker} whose master key is loaded — the two that
     * make encryption at rest and the access check active. Anything less and the store refuses to
     * connect, so no read, write, DDL, dump or restore can reach the database.
     *
     * @throws AccessSecurityException when the controller, the key maker or the master key is missing
     */
    private void requireMasterKey() {
        APIConfigInfo aci = getAPIConfigInfo();
        SUS.checkIfNulls("Configuration null", aci);
        boolean noController = aci.getSecurityController() == null, noKeyMaker = aci.getKeyMaker() == null;
        if (noController || noKeyMaker) {
            throw new AccessSecurityException("Database access refused: the store configuration needs a SecurityController"
                    + " and a KeyMaker with the master key loaded (missing: " + (noController ? "SecurityController" : "")
                    + (noController && noKeyMaker ? ", " : "") + (noKeyMaker ? "KeyMaker" : "") + ")");
        }
        byte[] masterKey;
        try {
            masterKey = aci.getKeyMaker().getMasterKey();
        } catch (RuntimeException e) {
            throw new AccessSecurityException("Database access refused: master key not loaded (" + e.getMessage() + ")");
        }
        if (masterKey == null || masterKey.length == 0) {
            throw new AccessSecurityException("Database access refused: master key not loaded");
        }
        java.util.Arrays.fill(masterKey, (byte) 0);
    }

    @Override
    public Connection newConnection() throws APIException {
        requireMasterKey();
        try {
            // Both engines are pooled via HikariCP. A pooled close() returns the connection, so the
            // acquire/close/transaction machinery is engine-agnostic.
            Connection conn = pool().getConnection();
            synchronized (connections) {
                // Callers that close a connection themselves (instead of via this store) leave a dead
                // reference in the set — purge those before they accumulate.
                if (connections.size() >= 64) {
                    connections.removeIf(c -> {
                        try {
                            return c.isClosed();
                        } catch (SQLException e) {
                            return true;
                        }
                    });
                }
                connections.add(conn);
            }
            return conn;
        } catch (SQLException e) {
            APIException apiEx = new APIException("Connection failed: " + e.getMessage());
            apiEx.initCause(e);
            throw apiEx;
        } catch (RuntimeException e) {
            // Hikari wraps a failed pool bootstrap (bad URL / credentials / file password) in a
            // RuntimeException; surface the underlying SQL failure as an APIException.
            APIException apiEx = new APIException("Connection failed: " + e.getMessage());
            apiEx.initCause(e);
            throw apiEx;
        }
    }

    /**
     * Lazily-built HikariCP pool — used for both engines (H2 included: `DB_CLOSE_DELAY=-1` keeps the
     * DB alive independently of pooling, and pooled connections remove the per-op open/auth cost that
     * H2 file/tcp modes otherwise pay). A pooled {@code connection.close()} returns the connection to
     * the pool, so the acquire/close/transaction machinery is engine-agnostic.
     */
    private HikariDataSource pool() {
        HikariDataSource p = pool;
        if (p == null) {
            synchronized (this) {
                p = pool;
                if (p == null) {
                    APIConfigInfo aci = getAPIConfigInfo();
                    SUS.checkIfNulls("Configuration null", aci);
                    HikariConfig cfg = new HikariConfig();
                    cfg.setJdbcUrl(H2PParam.dataStoreURI(aci));
                    cfg.setUsername(aci.getProperties().getValue(H2PParam.USER));
                    cfg.setPassword(H2PParam.dataStorePassword(aci));
                    String driver = aci.getProperties().getValue(H2PParam.DRIVER);
                    if (driver != null && !driver.isEmpty()) {
                        cfg.setDriverClassName(driver);
                    }
                    cfg.setMaximumPoolSize(intParam(H2PParam.POOL_MAX_SIZE, 10));
                    cfg.setMinimumIdle(intParam(H2PParam.POOL_MIN_IDLE, 2));
                    cfg.setPoolName("h2p-" + (name != null ? name : currentDSType));
                    p = new HikariDataSource(cfg);
                    pool = p;
                }
            }
        }
        return p;
    }

    private int intParam(H2PParam param, int defaultValue) {
        String v = getAPIConfigInfo().getProperties().getValue(param);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** @return the ambient transaction connection if one is active on this thread, else a fresh connection. */
    private Connection acquire() {
        touch();
        Connection tx = txConnection.get();
        return tx != null ? tx : connect();
    }

    /** @return the Connection bound to the current thread's transaction, or null if none is active. */
    public Connection getTransactionConnection() {
        return txConnection.get();
    }

    /**
     * True while an ambient transaction started by {@link #beginTransaction()} is bound to the
     * calling thread. Callers use it to join an existing transaction instead of nesting one,
     * which {@link #beginTransaction()} rejects.
     */
    @Override
    public boolean isTransactionActive() {
        return txConnection.get() != null;
    }

    /**
     * Run a DDL statement. Normally on its own auto-committed connection (H2 commits implicitly on
     * DDL, which would end an ambient transaction early). <b>PostgreSQL with an ambient transaction
     * open on this thread is the exception:</b> the DDL runs on the transaction connection, under a
     * SAVEPOINT. PostgreSQL DDL is transactional, and an out-of-band connection would block on the
     * relation locks the open transaction already holds — e.g. a join table's
     * {@code FOREIGN KEY … REFERENCES role_info} while the transaction has just created or written
     * {@code role_info} — and the transaction, idle, would never release them: a deadlock that the
     * lock manager cannot see (found 2026-09-29 bootstrapping a fresh database). The SAVEPOINT keeps a
     * failed statement (e.g. a duplicate constraint) from aborting the whole transaction.
     */
    private void execDDL(String sql) {
        Connection tx = txConnection.get();
        if (tx != null && currentDSType == DSType.POSTGRES) {
            Statement stmt = null;
            java.sql.Savepoint sp = null;
            try {
                sp = tx.setSavepoint();
                stmt = tx.createStatement();
                if (log.isEnabled()) log.getLogger().info("DDL (in tx): " + sql);
                stmt.execute(sql);
                tx.releaseSavepoint(sp);
            } catch (SQLException e) {
                if (sp != null) {
                    try {
                        tx.rollback(sp);
                    } catch (SQLException ignore) {
                        // surface the original failure
                    }
                }
                throw mapOrWrap(e);
            } finally {
                close(stmt);
            }
            return;
        }
        Connection con = null;
        Statement stmt = null;
        try {
            con = newConnection();
            stmt = con.createStatement();
            if (log.isEnabled()) log.getLogger().info("DDL: " + sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(stmt, con);
        }
    }

    private void close(AutoCloseable... closeables) {
        Connection tx = txConnection.get();
        for (AutoCloseable c : closeables) {
            if (c != null) {
                if (c instanceof Connection) {
                    if (c == tx) {
                        continue; // ambient transaction connection stays open until end/abort
                    }
                    synchronized (connections) {
                        connections.remove(c);
                    }
                }
                SharedIOUtil.close(c);
            }
        }
    }

    @Override
    public void close() throws APIException {
        synchronized (connections) {
            connections.forEach(SharedIOUtil::close);
            connections.clear();
        }
        HikariDataSource p = pool;
        if (p != null) {
            SharedIOUtil.close(p); // shut the pool down
            pool = null;
        }
        if (log.isEnabled()) log.getLogger().info("Closed");
    }

    /**
     * Health check — the JDBC counterpart of {@code XlogistxMongoDataStore.ping}. Validates the
     * store by taking a pooled connection and running {@code SELECT 1}, then reports via
     * {@link DatabaseMetaData} and the HikariCP pool — portable to both engines, no dialect SQL.
     *
     * @param detailed if true also reports the JDBC driver, URL, resolved DSType and pool sizing
     * @return status info ({@code status}, {@code version}, {@code latency_millis}, {@code connections})
     * @throws APIException if the database is not reachable
     */
    @Override
    public NVGenericMap ping(boolean detailed) throws APIException {
        String product;
        String productVersion;
        String driverName = null;
        String driverVersion = null;
        String jdbcURL = null;
        long latencyMillis;
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            long ts = System.currentTimeMillis();
            con = connect();
            stmt = con.createStatement();
            rs = stmt.executeQuery("SELECT 1");
            if (!rs.next()) {
                throw new SQLException("ping query returned no result");
            }
            latencyMillis = System.currentTimeMillis() - ts;
            DatabaseMetaData dmd = con.getMetaData();
            product = dmd.getDatabaseProductName();
            productVersion = dmd.getDatabaseProductVersion();
            if (detailed) {
                driverName = dmd.getDriverName();
                driverVersion = dmd.getDriverVersion();
                jdbcURL = dmd.getURL();
            }
        } catch (Exception e) {
            if (log.isEnabled()) log.getLogger().log(Level.WARNING, "ping failed", e);
            throw mapOrWrap(e);
        } finally {
            close(rs, stmt, con);
        }

        NVGenericMap ret = new NVGenericMap();
        ret.build("time_stamp", DateUtil.DEFAULT_DATE_FORMAT_TZ.format(new Date()));
        ret.build("status", "UP");
        ret.build("version", product + " " + productVersion);
        ret.build(new NVLong("latency_millis", latencyMillis));

        HikariDataSource p = pool;
        HikariPoolMXBean mx = p != null ? p.getHikariPoolMXBean() : null;
        if (mx != null) {
            NVGenericMap connectionsInfo = new NVGenericMap("connections");
            connectionsInfo.build(new NVInt("active", mx.getActiveConnections()))
                    .build(new NVInt("idle", mx.getIdleConnections()))
                    .build(new NVInt("total", mx.getTotalConnections()))
                    .build(new NVInt("waiting", mx.getThreadsAwaitingConnection()));
            ret.build(connectionsInfo);
        }

        if (detailed) {
            NVGenericMap driverInfo = new NVGenericMap("driver");
            driverInfo.build("name", driverName)
                    .build("version", driverVersion);
            ret.build(driverInfo);

            NVGenericMap dbInfo = new NVGenericMap("database");
            dbInfo.build("url", jdbcURL)
                    .build("ds_type", "" + getDSType())
                    .build("dialect", "" + dialect);
            ret.build(dbInfo);

            if (p != null) {
                NVGenericMap poolInfo = new NVGenericMap("pool");
                poolInfo.build("name", p.getPoolName())
                        .build(new NVInt("max_size", p.getMaximumPoolSize()))
                        .build(new NVInt("min_idle", p.getMinimumIdle()));
                ret.build(poolInfo);
            }
        }

        return ret;
    }

    // ---------- Transactions (ambient ThreadLocal connection) ----------

    /**
     * Starts a JDBC transaction bound to the calling thread and returns its Connection.
     * Every subsequent data op on this thread joins the transaction until
     * {@link #endTransaction()} (commit) or {@link #abortTransaction()} (rollback).
     *
     * @throws IllegalStateException if a transaction is already active on this thread (no nesting).
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T beginTransaction() {
        if (txConnection.get() != null) {
            throw new IllegalStateException("A transaction is already active on this thread");
        }
        Connection con = newConnection();
        try {
            con.setAutoCommit(false);
            txConnection.set(con);
            return (T) con;
        } catch (SQLException e) {
            close(con); // don't leak the connection when the transaction can't start
            throw mapOrWrap(e);
        }
    }

    /** Commits the ambient transaction; on commit failure rolls back and rethrows. No-op if none is active. */
    @Override
    public void endTransaction() {
        Connection con = txConnection.get();
        if (con == null) {
            return;
        }
        try {
            con.commit();
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException ignore) {
                // best-effort rollback; surface the original commit failure
            }
            throw mapOrWrap(e);
        } finally {
            cleanupTransaction(con);
        }
    }

    /** Rolls back the ambient transaction. No-op if none is active. */
    @Override
    public void abortTransaction() {
        Connection con = txConnection.get();
        if (con == null) {
            return;
        }
        try {
            con.rollback();
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            cleanupTransaction(con);
        }
    }

    private void cleanupTransaction(Connection con) {
        txConnection.remove();
        try {
            con.setAutoCommit(true);
        } catch (SQLException ignore) {
            // closing anyway
        }
        synchronized (connections) {
            connections.remove(con);
        }
        SharedIOUtil.close(con);
    }

    @Override
    public boolean isProviderActive() {
        return driverLoaded;
    }

    // Config/exception-handler storage, lookupProperty (ASYNC_CREATE/RETRY_DELAY), lastTimeAccessed/
    // inactivityDuration (touch()-driven) and pendingCalls-based isBusy() come from
    // APIServiceProviderBase — same lifecycle plumbing as the Mongo datastores.

    @Override
    public void setDescription(String str) {
        this.description = str;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toCanonicalID() {
        return H2PDSCreator.API_NAME + ":" + (name != null ? name : "");
    }

    @Override
    public String getStoreName() {
        return getAPIConfigInfo() != null ? H2PParam.dataStoreName(getAPIConfigInfo()) : null;
    }

    @Override
    public Set<String> getStoreTables() {
        return metaManager.getTables();
    }

    @Override
    @SuppressWarnings("unchecked")
    public IDGenerator<String, UUID> getIDGenerator() {
        return IDGs.UUIDV7;
    }

    @Override
    public boolean isValidReferenceID(String refID) {
        return IDGs.UUIDV7.isValid(refID);
    }

    // ---------- Schema helpers ----------

    static String tableName(NVConfigEntity nvce) {
        return H2PUtil.sqlName(nvce.getName());
    }

    /** Per-attribute storage plan, cached per entity type. */
    private static final class AttrInfo {
        final NVConfig nvc;
        final String name;
        /** {@code name.toLowerCase()} — the key rows are materialized under; precomputed (hot read path). */
        final String lowerName;
        final H2PUtil.AttrKind kind;
        /** Declared {@code ENCRYPT} / {@code ENCRYPT_MASK} (see {@link H2PFieldCrypto}); {@code masked} implies {@code encrypted}. */
        final boolean encrypted;
        final boolean masked;
        /** Referenced type for ENTITY_REF / ENTITY_COLLECTION, resolved once — see {@link #childNVCE}. */
        volatile NVConfigEntity child;
        volatile boolean childUnresolvable;

        AttrInfo(NVConfig nvc) {
            this.nvc = nvc;
            this.name = nvc.getName();
            this.lowerName = this.name.toLowerCase();
            this.kind = H2PUtil.classify(nvc);
            this.encrypted = H2PFieldCrypto.isEncrypted(nvc.getValueFilter());
            this.masked = H2PFieldCrypto.isMasked(nvc.getValueFilter());
        }

        boolean isColumn() {
            return kind == H2PUtil.AttrKind.SCALAR || kind == H2PUtil.AttrKind.BLOB
                    || kind == H2PUtil.AttrKind.ENTITY_REF || kind == H2PUtil.AttrKind.SCHEMALESS;
        }
    }

    private final Map<String, List<AttrInfo>> attrCache = new ConcurrentHashMap<>();
    // Per-type SQL, built once — a type's column list never changes.
    private final Map<String, String> insertSQLCache = new ConcurrentHashMap<>();
    private final Map<String, String> updateSQLCache = new ConcurrentHashMap<>();

    private List<AttrInfo> attrInfos(NVConfigEntity nvce) {
        return attrCache.computeIfAbsent(nvce.getName().toLowerCase(), k -> {
            // Identifier gate, once per type on first touch: entity/attribute names become SQL
            // identifiers verbatim, so they must be printable ASCII and <= 63 bytes (PostgreSQL's
            // limit). Bad names are REJECTED loudly — fix them at the source, they are never mangled.
            H2PUtil.checkNameForSQL("entity type", nvce.getName());
            List<AttrInfo> list = new ArrayList<>();
            for (NVConfig nvc : nvce.getAttributes()) {
                AttrInfo ai = new AttrInfo(nvc);
                if (ai.kind != H2PUtil.AttrKind.PK && ai.kind != H2PUtil.AttrKind.EXCLUDED) {
                    H2PUtil.checkNameForSQL("attribute", ai.name);
                    if (ai.encrypted && (ai.kind != H2PUtil.AttrKind.SCALAR
                            || nvc.getMetaType() != String.class || H2PUtil.isUUIDField(nvc))) {
                        // An encrypted attribute is stored as the record's canonical text in its own
                        // varchar column: only a String scalar can carry it, and a *guid column is a
                        // native uuid that could never hold ciphertext.
                        throw new IllegalArgumentException("ENCRYPT/ENCRYPT_MASK attribute must be a String scalar (not a *guid): "
                                + nvce.getName() + "." + ai.name + " is " + ai.kind + " of " + nvc.getMetaType()
                                + ". FIX YOUR CODE: declare it as String or drop the filter.");
                    }
                    list.add(ai);
                }
            }
            return list;
        });
    }

    /** True if the type declares at least one ENCRYPT* attribute (decides whether the crypto pass runs). */
    private static boolean hasEncryptedAttrs(List<AttrInfo> infos) {
        for (AttrInfo ai : infos) if (ai.encrypted) return true;
        return false;
    }

    /** Join table name for an entity-collection attribute: {@code <table>__<attr>} (63-byte safe). */
    private static String joinTableName(NVConfigEntity nvce, AttrInfo ai) {
        return H2PUtil.sqlName(nvce.getName() + "__" + ai.name);
    }

    /**
     * The referenced type of an ENTITY_REF / ENTITY_COLLECTION attribute, memoized per attribute.
     * The lookup key is a Java class name while {@link H2PMetaManager} is keyed by meta-type name
     * ({@code nvce.getName()}, e.g. {@code address_dao}), so the registry can never hit — without this
     * memo every row read would pay a {@code Class.forName} + reflective {@code newInstance()} per
     * reference attribute.
     */
    private NVConfigEntity childNVCE(AttrInfo ai) {
        NVConfigEntity c = ai.child;
        if (c == null && !ai.childUnresolvable) {
            c = resolveNVCE(H2PUtil.childEntityClass(ai.nvc).getName());
            if (c != null) ai.child = c;
            else ai.childUnresolvable = true;
        }
        return c;
    }

    /**
     * First-touch gate for a type's physical schema, run once per type per JVM (guarded by
     * {@code createdTables}; reconfigure clears the guard so a new database re-syncs). One
     * {@code INFORMATION_SCHEMA.COLUMNS} probe decides the branch:
     * <ul>
     *   <li><b>Table absent</b> — create it (typed columns, {@code bytea} blobs, {@code uuid} FK
     *       columns for single references, dialect JSON for schemaless).</li>
     *   <li><b>Table pre-existing</b> — {@link #syncExistingTable additive sync + type gate}:
     *       added attributes get {@code ALTER TABLE ADD COLUMN IF NOT EXISTS} (nullable, so old
     *       rows read as attribute defaults — document-store missing-key semantics); a changed
     *       column type is rejected loudly; deleted attributes leave dead columns untouched.</li>
     * </ul>
     * Then — after recursively ensuring referenced types' tables exist — FOREIGN KEY constraints,
     * join tables for entity collections and indexes are applied idempotently (covers collections/
     * refs added to a pre-existing table too). The bare table is registered before FKs are added,
     * so cyclic type references resolve.
     */
    private void ensureTable(NVConfigEntity nvce) {
        String key = nvce.getName().toLowerCase();
        if (createdTables.contains(key)) return;
        ddlLock.lock();
        try {
            if (createdTables.contains(key)) return;
            List<AttrInfo> infos = attrInfos(nvce);

            Map<String, String> existingColumns = readColumnTypes(tableName(nvce));
            if (existingColumns.isEmpty()) {
                StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(H2PUtil.q(tableName(nvce))).append(" (")
                        .append(H2PUtil.q(MetaToken.GUID.getName())).append(" uuid PRIMARY KEY");
                for (AttrInfo ai : infos) {
                    if (!ai.isColumn()) continue; // ENTITY_COLLECTION -> join table, no column
                    sb.append(", ").append(H2PUtil.q(ai.name)).append(' ').append(columnDDLType(ai));
                    if (ai.kind == H2PUtil.AttrKind.SCALAR && ai.nvc.isUnique()) sb.append(" UNIQUE");
                }
                sb.append(')');
                execDDL(sb.toString());
            } else {
                syncExistingTable(nvce, infos, existingColumns);
            }
            metaManager.register(nvce);
            createdTables.add(key); // register bare table before FKs so cyclic refs resolve
            registerInCatalog(nvce);

            // FKs and join tables (child tables ensured first)
            for (AttrInfo ai : infos) {
                if (ai.kind == H2PUtil.AttrKind.ENTITY_REF) {
                    NVConfigEntity child = childNVCE(ai);
                    if (child == null) continue;
                    ensureTable(child);
                    execDDLQuiet("ALTER TABLE " + H2PUtil.q(tableName(nvce)) + " ADD CONSTRAINT "
                            + H2PUtil.q(H2PUtil.sqlName("fk_" + nvce.getName() + "_" + ai.name))
                            + " FOREIGN KEY (" + H2PUtil.q(ai.name) + ") REFERENCES "
                            + H2PUtil.q(tableName(child)) + "(" + H2PUtil.q(MetaToken.GUID.getName()) + ")");
                    // A FOREIGN KEY indexes the referenced side only; the referencing column needs its own
                    // index or every join/cascade over it is a full scan (true on both H2 and PostgreSQL).
                    createIndex(tableName(nvce), ai.name);
                } else if (ai.kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                    NVConfigEntity child = childNVCE(ai);
                    if (child == null) continue;
                    ensureTable(child);
                    String jt = joinTableName(nvce, ai);
                    execDDLQuiet("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(jt) + " ("
                            + H2PUtil.q("parent_guid") + " uuid, "
                            + H2PUtil.q("child_guid") + " uuid, "
                            + H2PUtil.q("ord") + " integer, "
                            + "FOREIGN KEY (" + H2PUtil.q("parent_guid") + ") REFERENCES "
                            + H2PUtil.q(tableName(nvce)) + "(" + H2PUtil.q(MetaToken.GUID.getName()) + ") ON DELETE CASCADE, "
                            + "FOREIGN KEY (" + H2PUtil.q("child_guid") + ") REFERENCES "
                            + H2PUtil.q(tableName(child)) + "(" + H2PUtil.q(MetaToken.GUID.getName()) + "))");
                    // parent_guid: every collection read filters+orders on it. child_guid: cascade / child delete.
                    createIndex(jt, "parent_guid", "ord");
                    createIndex(jt, "child_guid");
                }
            }

            // UUID scalars (subject_guid, reference ids) are lookup keys — UNIQUE already carries an index.
            for (AttrInfo ai : infos) {
                if (ai.kind == H2PUtil.AttrKind.SCALAR && H2PUtil.isUUIDField(ai.nvc) && !ai.nvc.isUnique()) {
                    createIndex(tableName(nvce), ai.name);
                }
            }
        } finally {
            ddlLock.unlock();
        }
    }

    /** The DDL column type for a column-mapped attribute — the single mapping create and sync share. */
    private String columnDDLType(AttrInfo ai) {
        switch (ai.kind) {
            case BLOB:
                return "bytea";
            case ENTITY_REF:
                return "uuid";
            case SCHEMALESS:
                return dialect.schemalessColumnType();
            default:
                return H2PUtil.scalarColumnType(ai.nvc);
        }
    }

    /**
     * {@code column_name (lowercased) -> normalized data type} for the table in the current schema;
     * an empty map means the table does not exist. One query — the probe that decides create vs.
     * sync in {@link #ensureTable}. Runs on its own connection (schema probing is out-of-band).
     */
    private Map<String, String> readColumnTypes(String table) {
        Map<String, String> ret = new HashMap<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = newConnection();
            ps = con.prepareStatement("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
                    + " WHERE UPPER(TABLE_NAME)=UPPER(?) AND TABLE_SCHEMA = CURRENT_SCHEMA");
            ps.setString(1, table);
            rs = ps.executeQuery();
            while (rs.next()) {
                ret.put(rs.getString(1).toLowerCase(), H2PUtil.normalizeSqlType(rs.getString(2)));
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
        return ret;
    }

    /**
     * Additive schema sync + type gate for a pre-existing table (schema evolution), run once per
     * type per JVM from {@link #ensureTable}:
     * <ul>
     *   <li><b>Added attribute</b> → {@code ALTER TABLE ADD COLUMN IF NOT EXISTS}. The column is
     *       nullable, so pre-existing rows read the attribute at its default — exactly a document
     *       store's missing-key semantics. A unique scalar gets a best-effort UNIQUE constraint
     *       (pre-existing duplicate data logs a warning instead of failing). Added collections,
     *       FK constraints and indexes are covered by the idempotent DDL that follows in
     *       {@code ensureTable}.</li>
     *   <li><b>Deleted attribute</b> → its column / join table stays as harmless dead weight; reads
     *       and writes ignore it. Never dropped.</li>
     *   <li><b>Type change</b> → REJECTED with an actionable exception. Altering a column's type is
     *       data-destructive and is never done automatically — migrate via dump (old entity classes)
     *       / restore (new classes), or revert the attribute's type.</li>
     * </ul>
     */
    private void syncExistingTable(NVConfigEntity nvce, List<AttrInfo> infos, Map<String, String> existing) {
        String table = tableName(nvce);
        String guidType = existing.get(MetaToken.GUID.getName());
        if (!"uuid".equals(guidType)) {
            throw new APIException("SCHEMA MISMATCH: table \"" + table + "\" exists but has no 'guid' uuid column"
                    + " (found: " + guidType + ") — it was not created by this datastore."
                    + " Rename the entity type or the conflicting table.");
        }
        for (AttrInfo ai : infos) {
            if (!ai.isColumn()) continue; // collections have no column; their join tables are ensured after
            String ddlType = columnDDLType(ai);
            String expected = H2PUtil.normalizeSqlType(ddlType);
            String found = existing.get(ai.lowerName);
            if (found == null) {
                // Additive evolution: the attribute was added since the table was created.
                execDDL("ALTER TABLE " + H2PUtil.q(table) + " ADD COLUMN IF NOT EXISTS "
                        + H2PUtil.q(ai.name) + ' ' + ddlType);
                if (ai.kind == H2PUtil.AttrKind.SCALAR && ai.nvc.isUnique()) {
                    execDDLQuiet("ALTER TABLE " + H2PUtil.q(table) + " ADD CONSTRAINT "
                            + H2PUtil.q(H2PUtil.sqlName("uq_" + table + "_" + ai.name))
                            + " UNIQUE (" + H2PUtil.q(ai.name) + ")");
                }
            } else if (!found.equals(expected)) {
                throw new APIException("SCHEMA TYPE MISMATCH: column \"" + ai.name + "\" of table \"" + table
                        + "\" is '" + found + "' in the database, but attribute '" + ai.name
                        + "' of entity type '" + nvce.getName() + "' now maps to '" + expected + "'."
                        + " Altering a column's type is data-destructive and is NEVER done automatically."
                        + " FIX: migrate the data (dump with the old entity classes, restore with the new —"
                        + " see H2PDumpRestore) or revert the attribute's type.");
            }
        }
    }

    /**
     * {@code CREATE INDEX IF NOT EXISTS} (supported by H2 and PostgreSQL 9.5+) over the given columns.
     * The name goes through {@link H2PUtil#sqlName} so long table/attribute names stay within
     * PostgreSQL's 63-byte identifier limit without hash-less truncation collisions.
     */
    private void createIndex(String table, String... columns) {
        StringBuilder name = new StringBuilder("idx_").append(table);
        StringBuilder cols = new StringBuilder();
        for (String c : columns) {
            name.append('_').append(c);
            if (cols.length() > 0) cols.append(", ");
            cols.append(H2PUtil.q(c));
        }
        execDDLQuiet("CREATE INDEX IF NOT EXISTS " + H2PUtil.q(H2PUtil.sqlName(name.toString()))
                + " ON " + H2PUtil.q(table) + " (" + cols + ")");
    }


    @Override
    public DSType getDSType() {
        return currentDSType;
    }

    // Duplicate-object SQLStates: PG 42P07 (table), 42710 (constraint/object), 42701 (column);
    // H2 42101 (table), 42111 (index), 90045 (constraint).
    private static final Set<String> DUPLICATE_DDL_SQLSTATES = new HashSet<>(Arrays.asList(
            "42P07", "42710", "42701", "42101", "42111", "90045"));

    /** Run DDL that may already have been applied (ADD CONSTRAINT / join table); log-and-ignore duplicates. */
    private void execDDLQuiet(String sql) {
        try {
            execDDL(sql);
        } catch (RuntimeException e) {
            // Only a duplicate-object error is expected here; anything else (connection loss,
            // syntax, permissions) means the FK/join table/index is genuinely missing — surface it.
            String sqlState = null;
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof SQLException) {
                    sqlState = ((SQLException) t).getSQLState();
                    break;
                }
            }
            if (sqlState != null && DUPLICATE_DDL_SQLSTATES.contains(sqlState)) {
                if (log.isEnabled()) log.getLogger().log(Level.FINE, "execDDLQuiet duplicate ignored: " + sql, e);
            } else {
                log.getLogger().log(Level.WARNING, "execDDLQuiet failed: " + sql, e);
            }
        }
    }

    private boolean tableExists(Connection con, NVConfigEntity nvce) throws SQLException {
        String key = nvce.getName().toLowerCase();
        if (createdTables.contains(key)) return true;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean exists;
        try {
            // Scoped to the connection's current schema — a same-named table in another schema of the
            // database must not count as ours (CURRENT_SCHEMA works on both H2 and PostgreSQL).
            ps = con.prepareStatement(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME)=UPPER(?)"
                            + " AND TABLE_TYPE='BASE TABLE'"
                            + " AND TABLE_SCHEMA = CURRENT_SCHEMA");
            ps.setString(1, tableName(nvce));
            rs = ps.executeQuery();
            exists = rs.next();
        } finally {
            close(rs, ps);
        }
        if (exists) {
            // First sight of a pre-existing table on the read path: run the same one-time schema
            // pass as the write path (additive column sync + type gate + registration + idempotent
            // FK/join/index DDL) and cache the type — without the cache every select would pay an
            // INFORMATION_SCHEMA round trip; without the sync a projection on an added attribute or
            // a read of an added collection would fail on the missing column/join table.
            ensureTable(nvce);
        }
        return exists;
    }

    // ---------- Meta catalog (sys_meta_catalog) ----------

    /**
     * Persistent type catalog: one row per entity table ({@code table_name} → {@code class_type},
     * the entity's Java class name). Written whenever a table is created ({@link #ensureTable}) or
     * first confirmed ({@link #tableExists}) — {@link H2PMetaManager} only knows types touched this
     * session, the catalog lets a fresh JVM enumerate every stored type ({@link #discoverStoreTypes},
     * the whole-store {@code dump} relies on it). Best-effort: a catalog failure never fails the data
     * operation that triggered it. Runs on its own auto-commit connection (DEM-style portable upsert)
     * — registration must survive a rolled-back ambient transaction.
     */
    private void registerInCatalog(NVConfigEntity nvce) {
        String key = nvce.getName().toLowerCase();
        if (!catalogSynced.add(key)) return;
        Connection con = null;
        PreparedStatement upd = null;
        PreparedStatement ins = null;
        try {
            if (!metaCatalogEnsured) {
                execDDL("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(META_CATALOG_TABLE) + " ("
                        + H2PUtil.q("table_name") + " VARCHAR PRIMARY KEY, "
                        + H2PUtil.q("class_type") + " VARCHAR)");
                metaCatalogEnsured = true;
            }
            String classType = nvce.getMetaTypeBase().getName();
            // PostgreSQL with an ambient transaction: the catalog table (and the type's table) may
            // exist only inside that transaction, so the upsert must run on it too — see execDDL.
            Connection tx = txConnection.get();
            boolean inTx = tx != null && currentDSType == DSType.POSTGRES;
            con = inTx ? tx : newConnection();
            java.sql.Savepoint sp = inTx ? con.setSavepoint() : null;
            try {
                upd = con.prepareStatement("UPDATE " + H2PUtil.q(META_CATALOG_TABLE) + " SET "
                        + H2PUtil.q("class_type") + " = ? WHERE " + H2PUtil.q("table_name") + " = ?");
                upd.setString(1, classType);
                upd.setString(2, tableName(nvce));
                if (upd.executeUpdate() == 0) {
                    java.sql.Savepoint sp2 = inTx ? con.setSavepoint() : null;
                    try {
                        ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(META_CATALOG_TABLE) + " ("
                                + H2PUtil.q("table_name") + ", " + H2PUtil.q("class_type") + ") VALUES (?, ?)");
                        ins.setString(1, tableName(nvce));
                        ins.setString(2, classType);
                        ins.executeUpdate();
                        if (sp2 != null) con.releaseSavepoint(sp2);
                    } catch (SQLException e) {
                        if (sp2 != null) con.rollback(sp2);
                        // Concurrent registrar won the race — the row exists now, which is all we need.
                        if (!"23505".equals(e.getSQLState())) throw e;
                    }
                }
                if (sp != null) con.releaseSavepoint(sp);
            } catch (SQLException e) {
                if (sp != null) {
                    try {
                        con.rollback(sp); // keep the ambient transaction usable
                    } catch (SQLException ignore) {
                        // surface the original failure
                    }
                }
                throw e;
            }
        } catch (Exception e) {
            catalogSynced.remove(key); // retry on the next touch of this type
            if (log.isEnabled())
                log.getLogger().log(Level.WARNING, "meta catalog registration failed: " + nvce.getName(), e);
        } finally {
            close(ins, upd, con);
        }
    }

    /**
     * Every entity type known to this store: the session registry ({@link H2PMetaManager}) unioned
     * with the persistent {@code sys_meta_catalog} rows resolved via {@link #resolveNVCE}, so a fresh
     * JVM sees types it has never touched. Catalog rows whose class is not on this JVM's classpath
     * are skipped with a warning. Databases created before the catalog existed only have rows for
     * types touched since — pass explicit types to {@code dump} for those.
     */
    List<NVConfigEntity> discoverStoreTypes() {
        Map<String, NVConfigEntity> byKey = new LinkedHashMap<>();
        for (String t : metaManager.getTables()) {
            NVConfigEntity nvce = metaManager.lookup(t);
            if (nvce != null) byKey.putIfAbsent(nvce.getName().toLowerCase(), nvce);
        }
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = newConnection();
            if (rawTableExists(con, META_CATALOG_TABLE)) {
                st = con.createStatement();
                rs = st.executeQuery("SELECT " + H2PUtil.q("table_name") + ", " + H2PUtil.q("class_type")
                        + " FROM " + H2PUtil.q(META_CATALOG_TABLE));
                while (rs.next()) {
                    String classType = rs.getString(2);
                    NVConfigEntity nvce = resolveNVCE(classType);
                    if (nvce != null) byKey.putIfAbsent(nvce.getName().toLowerCase(), nvce);
                    else if (log.isEnabled())
                        log.getLogger().warning("catalog type not resolvable on this classpath: " + classType);
                }
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, st, con);
        }
        return new ArrayList<>(byKey.values());
    }

    // Class-name / meta-type-name -> NVConfigEntity, including the reflective (Class.forName) resolutions,
    // which H2PMetaManager can't serve because it is keyed by meta-type name only.
    private final Map<String, NVConfigEntity> nvceByTypeName = new ConcurrentHashMap<>();

    /** Resolve an NVConfigEntity from either a Java class name or a registered meta-type name. */
    NVConfigEntity resolveNVCE(String typeName) {
        if (typeName == null) return null;
        NVConfigEntity cached = nvceByTypeName.get(typeName);
        if (cached != null) return cached;
        NVConfigEntity nvce = metaManager.lookup(typeName);
        if (nvce == null) {
            try {
                Class<?> c = Class.forName(typeName);
                NVEntity e = (NVEntity) c.getDeclaredConstructor().newInstance();
                nvce = (NVConfigEntity) e.getNVConfig();
                metaManager.register(nvce);
            } catch (Throwable t) {
                if (log.isEnabled()) log.getLogger().log(Level.WARNING, "resolveNVCE failed: " + typeName, t);
                return null;
            }
        }
        nvceByTypeName.put(typeName, nvce);
        return nvce;
    }

    // ---------- Access control ----------

    /** Column of the owner of a row, as {@link AttrInfo#lowerName} spells it. */
    private static final String OWNER_COLUMN = MetaToken.SUBJECT_GUID.getName().toLowerCase();

    /**
     * True when the calling thread's operations are access-checked: a {@link SecurityController} is
     * configured and the thread is not inside its system context.
     */
    private boolean accessChecked() {
        SecurityController sc = crypto.controller();
        return sc != null && !sc.isSystemContext();
    }

    /**
     * The controller's verdict on one resource for the bound subject: the owner rights
     * ({@code ownerGUID}, null for a row without owner) or a grant on the resource itself. Never
     * compares owners here.
     */
    private boolean permitted(String guid, String ownerGUID, CRUD crud) {
        return crypto.accessAllowed(guid, ownerGUID, crud);
    }

    /** True when the type stores a {@code subject_guid} column (every type on the common base does). */
    private boolean hasOwnerColumn(NVConfigEntity nvce) {
        for (AttrInfo ai : attrInfos(nvce)) {
            if (ai.isColumn() && OWNER_COLUMN.equals(ai.lowerName)) return true;
        }
        return false;
    }

    private static String ownerOf(Object column) {
        return column instanceof UUID ? IDGs.UUIDV7.encode((UUID) column) : null;
    }

    /** A stored row as the write path needs it: it exists, and who owns it ({@code owner} null = no owner). */
    private static final class StoredRow {
        final String owner;

        StoredRow(String owner) {
            this.owner = owner;
        }
    }

    /**
     * The stored row with that GUID, or null when there is none. One round trip: it is the existence
     * probe of the write path and also yields the <b>stored</b> owner — an update or a delete is
     * judged against it, never against the {@code subject_guid} of the object the caller sent.
     */
    private StoredRow storedRow(Connection con, NVConfigEntity nvce, String guid) throws SQLException {
        boolean owned = hasOwnerColumn(nvce);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT " + (owned ? H2PUtil.q(MetaToken.SUBJECT_GUID.getName()) : "1")
                    + " FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            ps.setObject(1, IDGs.UUIDV7.decode(guid));
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            return new StoredRow(owned ? ownerOf(rs.getObject(1)) : null);
        } finally {
            close(rs, ps);
        }
    }

    /**
     * A new row: without an owner it gets the bound subject, then the caller needs {@code create} on
     * that owner — its own rows through the self permission, somebody else's only with a grant such
     * as {@code resource:*:*:create}. Nobody bound ⇒ refused.
     */
    private void checkCreate(NVConfigEntity nvce, NVEntity nve) {
        SecurityController sc = crypto.controller();
        if (sc == null || sc.isSystemContext()) return;
        if (SUS.isEmpty(nve.getSubjectGUID()) && hasOwnerColumn(nvce)) {
            String caller = sc.currentSubjectGUID();
            if (caller != null) nve.setSubjectGUID(caller);
        }
        String owner = SUS.isEmpty(nve.getSubjectGUID()) ? null : nve.getSubjectGUID();
        if (!(owner != null ? permitted(owner, owner, CRUD.CREATE) : permitted(nve.getGUID(), null, CRUD.CREATE))) {
            throw new AccessSecurityException("Not permitted to create " + nvce.getName()
                    + (owner != null ? " owned by " + owner : ""), Reason.UNAUTHORIZED);
        }
    }

    /**
     * An existing row: the caller needs the verb on it, judged against the stored owner. An update
     * never changes the owner: an object without one keeps the stored owner, a different one is
     * refused.
     */
    private void checkWrite(NVConfigEntity nvce, NVEntity nve, StoredRow stored, CRUD crud) {
        if (!accessChecked()) return;
        if (!permitted(nve.getGUID(), stored.owner, crud)) {
            throw new AccessSecurityException("Not permitted to " + crud.name().toLowerCase() + " "
                    + nvce.getName() + " " + nve.getGUID(), Reason.UNAUTHORIZED);
        }
        if (crud == CRUD.UPDATE && hasOwnerColumn(nvce)) {
            if (SUS.isEmpty(nve.getSubjectGUID())) {
                if (stored.owner != null) nve.setSubjectGUID(stored.owner);
            } else if (!nve.getSubjectGUID().equalsIgnoreCase(stored.owner)) {
                throw new AccessSecurityException("The owner of " + nvce.getName() + " " + nve.getGUID()
                        + " can not be changed by an update", Reason.UNAUTHORIZED);
            }
        }
    }

    /**
     * State of one read call: the entities built so far (cycle guard and dedup) and the access
     * verdicts already obtained, so one owner is asked about once per call, not once per row.
     */
    private final class ReadCtx {
        /** Built entities by GUID; a row denied to the caller is remembered with a null value. */
        final Map<String, NVEntity> cache = new HashMap<>();
        /** Owner GUID → the caller may read what that owner owns (the owner token). */
        private final Map<String, Boolean> ownerRead = new HashMap<>();
        final boolean checked = accessChecked();

        boolean mayRead(String guid, String ownerGUID) {
            if (!checked) return true;
            if (guid == null) return false;
            if (ownerGUID != null) {
                Boolean owner = ownerRead.get(ownerGUID);
                if (owner == null) {
                    // resource:<owner>:<caller>:read — the same answer for every row of that owner
                    owner = permitted(ownerGUID, ownerGUID, CRUD.READ);
                    ownerRead.put(ownerGUID, owner);
                }
                if (owner) return true;
            }
            // resource:<guid>:<caller>:read — a share of this row
            return permitted(guid, null, CRUD.READ);
        }
    }

    private void bindScalar(PreparedStatement ps, int index, NVConfig nvc, Object value) throws SQLException {
        if (H2PUtil.isUUIDField(nvc)) {
            if (value == null || (value instanceof String && ((String) value).isEmpty())) {
                ps.setObject(index, null);
            } else if (value instanceof UUID) {
                ps.setObject(index, value);
            } else {
                ps.setObject(index, IDGs.UUIDV7.decode(value.toString()));
            }
        } else if (value == null) {
            ps.setObject(index, null);
        } else if (value instanceof Enum) {
            ps.setString(index, ((Enum<?>) value).name());
        } else if (value instanceof Boolean) {
            ps.setBoolean(index, (Boolean) value);
        } else if (value instanceof Date) {
            ps.setLong(index, ((Date) value).getTime());
        } else if (value instanceof Number) {
            ps.setObject(index, value);
        } else if (value instanceof String) {
            ps.setString(index, (String) value);
        } else {
            ps.setString(index, value.toString());
        }
    }

    // ---------- Insert / update / patch ----------

    /**
     * Per-write-operation context. {@code seen} guards the child recursion so a cyclic entity graph
     * terminates; {@code inFlight} tracks entities whose row INSERT hasn't executed yet (still on the
     * call stack) — an FK column or join row pointing at one of those can't be written yet, so it is
     * deferred and applied once the whole graph is on disk ({@link #applyFixups}).
     */
    private final class WriteCtx {
        final Set<String> seen = new HashSet<>();
        final Set<String> inFlight = new HashSet<>();
        private final List<String[]> refFixups = new ArrayList<>();   // {table, column, rowGuid, refGuid}
        private final List<Object[]> joinFixups = new ArrayList<>();  // {joinTable, parentGuid, childGuid, ord}
        /**
         * Stored records (packed bytes) of the entity being updated, per lower-cased masked attribute
         * name — lets a masked read written back ({@code ****1234}) keep the stored ciphertext.
         * Filled by {@link #prepareCrypto} on update/patch only.
         */
        private Map<String, byte[]> storedRecords;

        byte[] storedRecord(String lowerName) {
            return storedRecords != null ? storedRecords.get(lowerName) : null;
        }

        void deferRef(String table, String column, String rowGuid, String refGuid) {
            refFixups.add(new String[]{table, column, rowGuid, refGuid});
        }

        void deferJoin(String joinTable, UUID parentGuid, UUID childGuid, int ord) {
            joinFixups.add(new Object[]{joinTable, parentGuid, childGuid, ord});
        }

        void applyFixups(Connection con) throws SQLException {
            for (String[] f : refFixups) {
                PreparedStatement ps = null;
                try {
                    ps = con.prepareStatement("UPDATE " + H2PUtil.q(f[0]) + " SET " + H2PUtil.q(f[1])
                            + " = ? WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
                    ps.setObject(1, IDGs.UUIDV7.decode(f[3]));
                    ps.setObject(2, IDGs.UUIDV7.decode(f[2]));
                    ps.executeUpdate();
                } finally {
                    close(ps);
                }
            }
            for (Object[] f : joinFixups) {
                PreparedStatement ps = null;
                try {
                    ps = con.prepareStatement("INSERT INTO " + H2PUtil.q((String) f[0]) + " ("
                            + H2PUtil.q("parent_guid") + ", " + H2PUtil.q("child_guid") + ", " + H2PUtil.q("ord")
                            + ") VALUES (?, ?, ?)");
                    ps.setObject(1, f[1]);
                    ps.setObject(2, f[2]);
                    ps.setInt(3, (Integer) f[3]);
                    ps.executeUpdate();
                } finally {
                    close(ps);
                }
            }
            refFixups.clear();
            joinFixups.clear();
        }
    }

    @Override
    public <V extends NVEntity> V insert(V nve)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", nve);
        Connection con = null;
        try {
            con = acquire();
            WriteCtx ctx = new WriteCtx();
            V ret = upsert(con, nve, ctx, false);
            ctx.applyFixups(con);
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    /**
     * {@code insert} and {@code update} are the same GUID-keyed upsert: a stored row with the
     * entity's GUID is updated, a missing one inserted. The row is probed once ({@link #storedRow}).
     *
     * @param reference true when {@code nve} is written as a referenced entity of the one being
     *                  saved: under access control a row the caller may not update is then linked,
     *                  not rewritten, provided the caller may read it
     */
    private <V extends NVEntity> V upsert(Connection con, V nve, WriteCtx ctx, boolean reference) throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        ensureTable(nvce);
        StoredRow stored = SUS.isEmpty(nve.getGUID()) ? null : storedRow(con, nvce, nve.getGUID());
        if (stored == null) {
            return insertRow(con, nve, nvce, ctx);
        }
        if (reference && accessChecked() && !permitted(nve.getGUID(), stored.owner, CRUD.UPDATE)) {
            if (!permitted(nve.getGUID(), stored.owner, CRUD.READ)) {
                throw new AccessSecurityException("Not permitted to reference " + nvce.getName() + " " + nve.getGUID(),
                        Reason.UNAUTHORIZED);
            }
            ctx.seen.add(nve.getGUID());
            return nve;
        }
        return updateRow(con, nve, nvce, ctx, stored);
    }

    private <V extends NVEntity> V insertRow(Connection con, V nve, NVConfigEntity nvce, WriteCtx ctx) throws SQLException {
        SecurityController sc = crypto.controller();
        if (sc != null) sc.associateNVEntityToSubjectGUID(nve, null);
        if (SUS.isEmpty(nve.getGUID())) nve.setGUID(IDGs.UUIDV7.genID());
        checkCreate(nvce, nve);
        MetaUtil.initTimeStamp(nve);

        ctx.seen.add(nve.getGUID());
        ctx.inFlight.add(nve.getGUID());

        List<AttrInfo> infos = attrInfos(nvce);
        prepareCrypto(con, nve, infos, ctx, false); // entity key before any statement of this row
        insertChildren(con, nve, infos, ctx); // referenced entities first (FK targets must exist)

        List<AttrInfo> cols = columnAttrs(infos);
        String sql = insertSQLCache.computeIfAbsent(nvce.getName().toLowerCase(), k -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(H2PUtil.q(tableName(nvce)))
                    .append(" (").append(H2PUtil.q(MetaToken.GUID.getName()));
            for (AttrInfo ai : cols) sb.append(", ").append(H2PUtil.q(ai.name));
            sb.append(") VALUES (?");
            for (int i = 0; i < cols.size(); i++) sb.append(", ?");
            return sb.append(')').toString();
        });

        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(sql);
            int idx = 1;
            ps.setObject(idx++, IDGs.UUIDV7.decode(nve.getGUID()));
            for (AttrInfo ai : cols) bindColumn(ps, idx++, ai, nve, ctx);
            ps.executeUpdate();
        } finally {
            close(ps);
        }
        ctx.inFlight.remove(nve.getGUID()); // row exists now — FK references to it can bind directly

        syncJoins(con, nve, infos, false, ctx); // link rows for entity collections
        return nve;
    }

    private static List<AttrInfo> columnAttrs(List<AttrInfo> infos) {
        List<AttrInfo> c = new ArrayList<>();
        for (AttrInfo ai : infos) if (ai.isColumn()) c.add(ai);
        return c;
    }

    /**
     * The crypto pass before a row is written (see {@link H2PFieldCrypto}). Runs only for types that
     * declare ENCRYPT* attributes or hold ENCRYPT* pairs in a schemaless container:
     * <ol>
     * <li>a half configuration (controller xor key maker) is refused when a value would have to be
     * sealed — never silently plaintext;</li>
     * <li>when active, the entity's key is minted on the first sealed write (wrapped under the owner's
     * subject key, which must exist);</li>
     * <li>on update/patch the stored records of the masked attributes are preloaded so a masked
     * value written back keeps the stored ciphertext.</li>
     * </ol>
     * Without any configuration the write proceeds in plaintext, as before.
     */
    private void prepareCrypto(Connection con, NVEntity nve, List<AttrInfo> infos, WriteCtx ctx, boolean stored)
            throws SQLException {
        boolean sealing = false;
        List<AttrInfo> masked = null;
        for (AttrInfo ai : infos) {
            if (ai.encrypted) {
                NVBase<?> nvb = nve.lookup(ai.name);
                if (nvb != null && H2PFieldCrypto.needsSealing(nvb.getValue())) sealing = true;
                if (ai.masked) {
                    if (masked == null) masked = new ArrayList<>();
                    masked.add(ai);
                }
            } else if (ai.kind == H2PUtil.AttrKind.SCHEMALESS && H2PFieldCrypto.hasEncryptedPairs(nve.lookup(ai.name))) {
                sealing = true;
            }
        }
        if (!sealing) return;
        crypto.requireConsistent(nve.getNVConfig().getName() + " " + nve.getGUID());
        if (!crypto.active()) return; // neither configured: clear text as UTF-8 bytes (dev/tests)
        crypto.ensureEntityKey(nve);
        if (stored && masked != null) {
            ctx.storedRecords = readBinaryColumns(con, (NVConfigEntity) nve.getNVConfig(), nve.getGUID(), masked);
        }
    }

    /** The current bytes of the given {@code bytea} columns of one row, keyed by lower-cased attribute name. */
    private Map<String, byte[]> readBinaryColumns(Connection con, NVConfigEntity nvce, String guid, List<AttrInfo> cols)
            throws SQLException {
        Map<String, byte[]> ret = new HashMap<>();
        if (cols.isEmpty()) return ret;
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(H2PUtil.q(cols.get(i).name));
        }
        sql.append(" FROM ").append(H2PUtil.q(tableName(nvce)))
                .append(" WHERE ").append(H2PUtil.q(MetaToken.GUID.getName())).append(" = ?");
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql.toString());
            ps.setObject(1, IDGs.UUIDV7.decode(guid));
            rs = ps.executeQuery();
            if (rs.next()) {
                for (int i = 0; i < cols.size(); i++) {
                    byte[] v = rs.getBytes(i + 1);
                    if (v != null) ret.put(cols.get(i).lowerName, v);
                }
            }
        } finally {
            close(rs, ps);
        }
        return ret;
    }

    /** The encrypted (ENCRYPT*) attributes of a type, in declaration order. */
    private List<AttrInfo> encryptedAttrs(NVConfigEntity nvce) {
        List<AttrInfo> ret = new ArrayList<>();
        for (AttrInfo ai : attrInfos(nvce)) if (ai.encrypted) ret.add(ai);
        return ret;
    }

    /**
     * The stored bytes of a row's encrypted columns, keyed by attribute name — for the dump, which
     * moves them beside the entity line instead of through the entity. Empty for a type without
     * encrypted attributes.
     */
    Map<String, byte[]> readEncryptedColumns(NVConfigEntity nvce, String guid) {
        List<AttrInfo> cols = encryptedAttrs(nvce);
        if (cols.isEmpty()) return new HashMap<>();
        Connection con = null;
        try {
            con = acquire();
            Map<String, byte[]> byLower = readBinaryColumns(con, nvce, guid, cols);
            Map<String, byte[]> ret = new LinkedHashMap<>();
            for (AttrInfo ai : cols) {
                byte[] v = byLower.get(ai.lowerName);
                if (v != null) ret.put(ai.name, v);
            }
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    /**
     * Writes stored bytes straight into a row's encrypted columns (restore), bypassing the entity
     * and its filters: the record is opaque to this store and only meaningful under the same master
     * key. Unknown or non-encrypted attribute names are rejected.
     */
    void writeEncryptedColumns(NVConfigEntity nvce, String guid, Map<String, byte[]> columns) {
        if (columns == null || columns.isEmpty()) return;
        Map<String, AttrInfo> allowed = new HashMap<>();
        for (AttrInfo ai : encryptedAttrs(nvce)) allowed.put(ai.lowerName, ai);
        StringBuilder sql = new StringBuilder("UPDATE ").append(H2PUtil.q(tableName(nvce))).append(" SET ");
        List<byte[]> values = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : columns.entrySet()) {
            AttrInfo ai = allowed.get(e.getKey().toLowerCase());
            if (ai == null) {
                throw new IllegalArgumentException("not an encrypted attribute of " + nvce.getName() + ": " + e.getKey());
            }
            if (!values.isEmpty()) sql.append(", ");
            sql.append(H2PUtil.q(ai.name)).append(" = ?");
            values.add(e.getValue());
        }
        sql.append(" WHERE ").append(H2PUtil.q(MetaToken.GUID.getName())).append(" = ?");
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = acquire();
            ps = con.prepareStatement(sql.toString());
            int idx = 1;
            for (byte[] v : values) ps.setBytes(idx++, v);
            ps.setObject(idx, IDGs.UUIDV7.decode(guid));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
    }

    private static Object valueOf(NVEntity nve, NVConfig nvc) {
        NVBase<?> nvb = nve.lookup(nvc.getName());
        return nvb != null ? nvb.getValue() : null;
    }

    /** Insert (or update) every referenced entity so FK targets exist before the parent row. */
    @SuppressWarnings("unchecked")
    private void insertChildren(Connection con, NVEntity nve, List<AttrInfo> infos, WriteCtx ctx) throws SQLException {
        for (AttrInfo ai : infos) {
            if (ai.kind == H2PUtil.AttrKind.ENTITY_REF) {
                NVEntity child = (NVEntity) valueOf(nve, ai.nvc);
                if (child != null) writeChild(con, child, ctx);
            } else if (ai.kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
                if (av != null) {
                    for (NVEntity child : av.values()) {
                        if (child != null) writeChild(con, child, ctx);
                    }
                }
            }
        }
    }

    /** Write one referenced entity unless this operation already wrote (or is writing) it — cycle guard. */
    private void writeChild(Connection con, NVEntity child, WriteCtx ctx) throws SQLException {
        if (SUS.isEmpty(child.getGUID())) child.setGUID(IDGs.UUIDV7.genID());
        if (ctx.seen.contains(child.getGUID())) return;
        upsert(con, child, ctx, true);
    }

    private void bindColumn(PreparedStatement ps, int idx, AttrInfo ai, NVEntity nve, WriteCtx ctx) throws SQLException {
        NVBase<?> nvb = nve.lookup(ai.name);
        Object value = nvb != null ? nvb.getValue() : null;
        switch (ai.kind) {
            case SCALAR:
                if (ai.encrypted) {
                    // bytea column: the packed record when encrypting (or the kept ciphertext of a masked
                    // write-back), the clear text as UTF-8 bytes otherwise — see H2PFieldCrypto
                    byte[] bytes = crypto.active()
                            ? crypto.encryptScalar(nve, ai.nvc, ai.masked, nvb, ctx != null ? ctx.storedRecord(ai.lowerName) : null)
                            : H2PFieldCrypto.plaintextBytes(value);
                    if (bytes == null) ps.setObject(idx, null);
                    else ps.setBytes(idx, bytes);
                } else if (nvb instanceof NVNumber) {
                    // NVNumber (e.g. Range start/end) carries a runtime numeric type — tag it so int/long/… survive.
                    ps.setString(idx, value == null ? null : H2PUtil.encodeNumber((Number) value));
                } else {
                    bindScalar(ps, idx, ai.nvc, value);
                }
                break;
            case BLOB:
                if (value == null) ps.setObject(idx, null);
                else ps.setBytes(idx, (byte[]) value);
                break;
            case ENTITY_REF: {
                NVEntity child = (NVEntity) value;
                if (child == null || SUS.isEmpty(child.getGUID())) {
                    ps.setObject(idx, null);
                } else if (ctx != null && ctx.inFlight.contains(child.getGUID())) {
                    // Cycle: the referenced row is an ancestor still being inserted — bind NULL now,
                    // patch the FK column after the whole graph is on disk (WriteCtx.applyFixups).
                    ps.setObject(idx, null);
                    ctx.deferRef(tableName((NVConfigEntity) nve.getNVConfig()), ai.name,
                            nve.getGUID(), child.getGUID());
                } else {
                    ps.setObject(idx, IDGs.UUIDV7.decode(child.getGUID()));
                }
                break;
            }
            case SCHEMALESS:
                // ENCRYPT* pairs inside the container are sealed on a copy (the caller keeps its plaintext)
                dialect.bindSchemaless(ps, idx, crypto.encodeSchemaless(nve, nvb, encodeSchemaless(nvb)));
                break;
            default:
                ps.setObject(idx, null);
                break;
        }
    }

    /** JSON for a schemaless container. Enum lists convert to names (Gson can't reflect enums). */
    private static String encodeSchemaless(NVBase<?> nvb) {
        if (nvb == null) return null;
        if (nvb instanceof NVEnumList) {
            List<String> names = new ArrayList<>();
            for (Object en : ((NVEnumList) nvb).getValue()) {
                names.add(((Enum<?>) en).name());
            }
            return GSONUtil.toJSONDefault(names);
        }
        return GSONUtil.toJSONDefault(nvb);
    }

    /** Rewrite an entity's collection join rows (delete-then-insert on update; insert-only on insert). */
    @SuppressWarnings("unchecked")
    private void syncJoins(Connection con, NVEntity nve, List<AttrInfo> infos, boolean deleteFirst, WriteCtx ctx)
            throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        UUID parent = IDGs.UUIDV7.decode(nve.getGUID());
        for (AttrInfo ai : infos) {
            if (ai.kind != H2PUtil.AttrKind.ENTITY_COLLECTION) continue;
            String jt = joinTableName(nvce, ai);
            if (deleteFirst) {
                PreparedStatement del = null;
                try {
                    del = con.prepareStatement("DELETE FROM " + H2PUtil.q(jt)
                            + " WHERE " + H2PUtil.q("parent_guid") + " = ?");
                    del.setObject(1, parent);
                    del.executeUpdate();
                } finally {
                    close(del);
                }
            }
            ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
            if (av == null) continue;
            // One statement for the whole collection, sent as a single batch (was: prepare + round trip per child).
            PreparedStatement ins = null;
            try {
                int ord = 0;
                for (NVEntity child : av.values()) {
                    if (child == null || SUS.isEmpty(child.getGUID())) continue;
                    if (ctx != null && ctx.inFlight.contains(child.getGUID())) {
                        // Cycle: child row not on disk yet — defer the join row, keep its position.
                        ctx.deferJoin(jt, parent, IDGs.UUIDV7.decode(child.getGUID()), ord++);
                        continue;
                    }
                    if (ins == null) {
                        ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(jt) + " ("
                                + H2PUtil.q("parent_guid") + ", " + H2PUtil.q("child_guid") + ", " + H2PUtil.q("ord")
                                + ") VALUES (?, ?, ?)");
                    }
                    ins.setObject(1, parent);
                    ins.setObject(2, IDGs.UUIDV7.decode(child.getGUID()));
                    ins.setInt(3, ord++);
                    ins.addBatch();
                }
                if (ins != null) ins.executeBatch();
            } finally {
                close(ins);
            }
        }
    }

    // ---------- Row read ----------

    @FunctionalInterface
    private interface SqlBinder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    /**
     * Run a SELECT with an optional WHERE, materialize rows, then build entities (resolving
     * refs/joins). {@code ctx} is the per-call read state: the entity cache keyed by GUID — the cycle
     * guard for mutually-referencing rows, and a dedup for repeated child fetches within one
     * operation — and the access verdicts; a row the caller may not read is not returned.
     * {@code projection} (lowercased attribute names, null = all) limits the selected columns and
     * the resolved collections — {@code guid} is always selected. {@code capResults} applies the
     * {@link H2PParam#MAX_SELECT_RESULTS} valve — only ever true for open-ended predicate searches;
     * guid-IN lookups ({@link #innerSearchByIDs}) are already bounded by their id list and must
     * NEVER be capped, or collection resolution / batch pages would silently drop rows.
     */
    private List<NVEntity> select(Connection con, NVConfigEntity nvce, String whereClause, SqlBinder binder,
                                  ReadCtx ctx, Set<String> projection, boolean capResults)
            throws SQLException {
        List<NVEntity> ret = new ArrayList<>();
        if (nvce == null || !tableExists(con, nvce)) return ret;
        String colList = "*";
        if (projection != null) {
            StringBuilder sb = new StringBuilder(H2PUtil.q(MetaToken.GUID.getName()));
            for (AttrInfo ai : attrInfos(nvce)) {
                // the owner column is always read when access is checked: the verdict needs it
                if (ai.isColumn() && (projection.contains(ai.lowerName) || (ctx.checked && OWNER_COLUMN.equals(ai.lowerName)))) {
                    sb.append(", ").append(H2PUtil.q(ai.name));
                }
            }
            colList = sb.toString();
        }
        String sql = "SELECT " + colList + " FROM " + H2PUtil.q(tableName(nvce))
                + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "");
        // Safety valve (opt-in via MAX_SELECT_RESULTS): cap unbounded search materialization.
        if (capResults) {
            int maxResults = intParam(H2PParam.MAX_SELECT_RESULTS, 0);
            if (maxResults > 0) sql += " LIMIT " + maxResults;
        }
        List<Map<String, Object>> rows;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            if (binder != null) binder.bind(ps);
            rs = ps.executeQuery();
            rows = materialize(rs);
        } finally {
            close(rs, ps);
        }
        for (Map<String, Object> row : rows) {
            NVEntity built = buildEntity(con, nvce, row, ctx, projection);
            if (built != null) ret.add(built); // null: the caller may not read that row
        }
        return ret;
    }

    private static List<Map<String, Object>> materialize(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        // Labels are fixed for the whole result set — resolve+lowercase once, not once per cell.
        String[] labels = new String[n];
        for (int i = 0; i < n; i++) labels[i] = md.getColumnLabel(i + 1).toLowerCase();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) row.put(labels[i], rs.getObject(i + 1));
            rows.add(row);
        }
        return rows;
    }

    /**
     * Builds one entity from its row, or returns null when the caller may not read it. The access
     * verdict comes first — right after the row's GUID and owner, before any value is decrypted or
     * any reference resolved.
     */
    @SuppressWarnings("unchecked")
    private NVEntity buildEntity(Connection con, NVConfigEntity nvce, Map<String, Object> row,
                                 ReadCtx ctx, Set<String> projection) throws SQLException {
        NVEntity nve;
        try {
            nve = (NVEntity) nvce.getMetaTypeBase().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new APIException("cannot instantiate " + nvce.getName() + ": " + e.getMessage());
        }
        Object g = row.get(MetaToken.GUID.getName());
        if (g instanceof UUID) nve.setGUID(IDGs.UUIDV7.encode((UUID) g));
        if (ctx.checked && !ctx.mayRead(nve.getGUID(), ownerOf(row.get(OWNER_COLUMN)))) {
            if (nve.getGUID() != null) ctx.cache.put(nve.getGUID(), null); // denied: remembered, not re-queried
            return null;
        }
        // Register before resolving references: a child referencing back to this row must find it
        // here instead of re-querying (infinite recursion on cyclic graphs).
        if (nve.getGUID() != null) ctx.cache.put(nve.getGUID(), nve);

        List<AttrInfo> infos = attrInfos(nvce);
        // Encrypted scalars and schemaless containers with sealed pairs are opened AFTER the loop:
        // the controller needs the row's guid + subject_guid, which are plain scalars set by the loop.
        List<AttrInfo> deferredCrypto = null;
        for (AttrInfo ai : infos) {
            String col = ai.lowerName;
            switch (ai.kind) {
                case SCALAR:
                    if (ai.encrypted) {
                        if (row.get(col) != null) {
                            if (deferredCrypto == null) deferredCrypto = new ArrayList<>();
                            deferredCrypto.add(ai);
                        }
                        break;
                    }
                    setScalar(nve, ai, row.get(col));
                    break;
                case BLOB: {
                    Object b = row.get(col);
                    if (b instanceof byte[]) ((NVBlob) nve.lookup(ai.name)).setValue((byte[]) b);
                    break;
                }
                case ENTITY_REF: {
                    Object ref = row.get(col);
                    if (ref instanceof UUID) {
                        String refId = IDGs.UUIDV7.encode((UUID) ref);
                        NVEntity child = ctx.cache.get(refId);
                        if (child == null) {
                            List<NVEntity> found = innerSearchByIDs(con, childNVCE(ai), null, ctx, refId);
                            child = found.isEmpty() ? null : found.get(0);
                        }
                        if (child != null) ((NVEntityReference) nve.lookup(ai.name)).setValue(child);
                    }
                    break;
                }
                case SCHEMALESS: {
                    String json = dialect.readSchemaless(row.get(col)); // String (H2) or PGobject jsonb (PG)
                    if (json != null) {
                        decodeSchemaless(json, ai, nve);
                        NVBase<?> decoded = nve.lookup(ai.name);
                        if (decoded != null && H2PFieldCrypto.hasSealedPairs(decoded)) {
                            if (deferredCrypto == null) deferredCrypto = new ArrayList<>();
                            deferredCrypto.add(ai);
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        }
        if (deferredCrypto != null) {
            for (AttrInfo ai : deferredCrypto) {
                if (ai.kind == H2PUtil.AttrKind.SCALAR) {
                    Object v = row.get(ai.lowerName);
                    crypto.decryptScalar(nve, ai.nvc, ai.masked, v instanceof byte[] ? (byte[]) v : null);
                } else {
                    crypto.decryptSchemaless(nve, nve.lookup(ai.name));
                }
            }
        }

        // Entity collections resolved via join tables: the whole collection is fetched with a single
        // IN (...) query, then re-ordered to the join table's "ord" (was one SELECT per child).
        for (AttrInfo ai : infos) {
            if (ai.kind != H2PUtil.AttrKind.ENTITY_COLLECTION) continue;
            if (projection != null && !projection.contains(ai.lowerName)) continue; // not projected
            List<UUID> childGuids = selectJoinChildren(con, nvce, ai, (UUID) g);
            if (childGuids.isEmpty()) continue;
            ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
            String[] ids = new String[childGuids.size()];
            for (int i = 0; i < ids.length; i++) ids[i] = IDGs.UUIDV7.encode(childGuids.get(i));
            Map<String, NVEntity> byGUID = new LinkedHashMap<>();
            for (NVEntity child : this.<NVEntity>innerSearchByIDs(con, childNVCE(ai), null, ctx, ids)) {
                byGUID.put(child.getGUID(), child);
            }
            for (String id : ids) {
                NVEntity child = byGUID.get(id);
                if (child != null) av.add(child);
            }
        }
        return nve;
    }

    @SuppressWarnings("unchecked")
    private void setScalar(NVEntity nve, AttrInfo ai, Object col) {
        if (col == null) return;
        NVBase<?> nvb = nve.lookup(ai.name);
        if (nvb == null) return;
        if (H2PUtil.isUUIDField(ai.nvc)) {
            if (col instanceof UUID) ((NVBase<Object>) nvb).setValue(IDGs.UUIDV7.encode((UUID) col));
            return;
        }
        if (nvb instanceof NVNumber) ((NVNumber) nvb).setValue(H2PUtil.decodeNumber(col.toString()));
        else if (nvb instanceof NVEnum)
            ((NVEnum) nvb).setValue(SUS.enumValue(ai.nvc.getMetaType(), col.toString()));
        else if (nvb instanceof NVBoolean) ((NVBoolean) nvb).setValue((Boolean) col);
        else if (nvb instanceof NVInt) ((NVInt) nvb).setValue(((Number) col).intValue());
        else if (nvb instanceof NVLong) ((NVLong) nvb).setValue(((Number) col).longValue());
        else if (nvb instanceof NVFloat) ((NVFloat) nvb).setValue(((Number) col).floatValue());
        else if (nvb instanceof NVDouble) ((NVDouble) nvb).setValue(((Number) col).doubleValue());
        else ((NVBase<Object>) nvb).setValue(col.toString());
    }

    /** Reconstruct a schemaless attribute from its JSON column. Enum lists rebuild via the enum class. */
    @SuppressWarnings("unchecked")
    private void decodeSchemaless(String json, AttrInfo ai, NVEntity nve) {
        NVBase<?> target = nve.lookup(ai.name);
        if (target == null) {
            // Meta/class drift: the NVConfigEntity declares the attribute but the instance lacks its
            // NVBase — skip the column instead of NPEing (same degradation as setScalar's null path).
            if (log.isEnabled()) log.getLogger().warning("schemaless attribute not on instance, skipped: "
                    + nve.getNVConfig().getName() + "." + ai.name);
            return;
        }
        if (target instanceof NVEnumList) {
            String[] names = GSONUtil.fromJSONDefault(json, String[].class);
            NVEnumList el = (NVEnumList) target;
            for (String nm : names) {
                el.getValue().add((Enum<?>) SUS.enumValue(ai.nvc.getMetaTypeBase(), nm));
            }
            return;
        }
        NVBase<?> parsed = GSONUtil.fromJSONDefault(json, target.getClass());
        parsed.setName(ai.name);
        // JSON doesn't encode a nested map's own name; restore it so the value re-serializes cleanly.
        if (parsed instanceof NamedValue && target instanceof NamedValue) {
            ((NamedValue<?>) parsed).getProperties().setName(((NamedValue<?>) target).getProperties().getName());
        }
        nve.getAttributes().put(ai.name, parsed);
    }

    private List<UUID> selectJoinChildren(Connection con, NVConfigEntity nvce, AttrInfo ai, UUID parentGuid)
            throws SQLException {
        List<UUID> ret = new ArrayList<>();
        if (parentGuid == null) return ret;
        String jt = joinTableName(nvce, ai);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT " + H2PUtil.q("child_guid") + " FROM " + H2PUtil.q(jt)
                    + " WHERE " + H2PUtil.q("parent_guid") + " = ? ORDER BY " + H2PUtil.q("ord"));
            ps.setObject(1, parentGuid);
            rs = ps.executeQuery();
            while (rs.next()) {
                Object c = rs.getObject(1);
                if (c instanceof UUID) ret.add((UUID) c);
            }
        } finally {
            close(rs, ps);
        }
        return ret;
    }

    @Override
    public <V extends NVEntity> V update(V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Can't update null nve", nve);
        Connection con = null;
        try {
            con = acquire();
            WriteCtx ctx = new WriteCtx();
            V ret = upsert(con, nve, ctx, false);
            ctx.applyFixups(con);
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    private <V extends NVEntity> V updateRow(Connection con, V nve, NVConfigEntity nvce, WriteCtx ctx, StoredRow stored)
            throws SQLException {
        checkWrite(nvce, nve, stored, CRUD.UPDATE);
        ctx.seen.add(nve.getGUID());
        MetaUtil.initTimeStamp(nve);

        List<AttrInfo> infos = attrInfos(nvce);
        prepareCrypto(con, nve, infos, ctx, true); // entity key + stored records of masked attributes
        // Opt-in orphan cleanup: remember the stored children before the update rewrites them.
        List<ChildRef> before = orphanCleanupEnabled()
                ? collectDbChildren(con, nvce, IDGs.UUIDV7.decode(nve.getGUID()), infos) : null;
        insertChildren(con, nve, infos, ctx); // new/changed referenced entities

        List<AttrInfo> cols = columnAttrs(infos);
        String sql = updateSQLCache.computeIfAbsent(nvce.getName().toLowerCase(), k -> {
            StringBuilder sb = new StringBuilder("UPDATE ").append(H2PUtil.q(tableName(nvce))).append(" SET ");
            boolean first = true;
            for (AttrInfo ai : cols) {
                if (!first) sb.append(", ");
                sb.append(H2PUtil.q(ai.name)).append(" = ?");
                first = false;
            }
            return sb.append(" WHERE ").append(H2PUtil.q(MetaToken.GUID.getName())).append(" = ?").toString();
        });

        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(sql);
            int idx = 1;
            for (AttrInfo ai : cols) bindColumn(ps, idx++, ai, nve, ctx);
            ps.setObject(idx, IDGs.UUIDV7.decode(nve.getGUID()));
            ps.executeUpdate();
        } finally {
            close(ps);
        }

        syncJoins(con, nve, infos, true, ctx); // resync collection links

        if (before != null && !before.isEmpty()) {
            deleteDetachedChildren(con, nve, infos, before);
        }
        return nve;
    }

    private boolean orphanCleanupEnabled() {
        APIConfigInfo aci = getAPIConfigInfo();
        String v = aci != null ? aci.getProperties().getValue(H2PParam.ORPHAN_CLEANUP) : null;
        return v != null && Boolean.parseBoolean(v.trim());
    }

    /**
     * Orphan cleanup (opt-in via {@link H2PParam#ORPHAN_CLEANUP}): delete previously referenced
     * child rows the update just detached (replaced single refs, children removed from
     * collections). A child still referenced elsewhere is kept ({@link #deleteChildSafely}).
     */
    @SuppressWarnings("unchecked")
    private void deleteDetachedChildren(Connection con, NVEntity nve, List<AttrInfo> infos,
                                        List<ChildRef> before) throws SQLException {
        Set<UUID> current = new HashSet<>();
        for (AttrInfo ai : infos) {
            if (ai.kind == H2PUtil.AttrKind.ENTITY_REF) {
                NVEntity child = (NVEntity) valueOf(nve, ai.nvc);
                if (child != null && !SUS.isEmpty(child.getGUID())) current.add(IDGs.UUIDV7.decode(child.getGUID()));
            } else if (ai.kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
                if (av != null) {
                    for (NVEntity child : av.values()) {
                        if (child != null && !SUS.isEmpty(child.getGUID())) current.add(IDGs.UUIDV7.decode(child.getGUID()));
                    }
                }
            }
        }
        Set<UUID> visited = new HashSet<>();
        for (ChildRef c : before) {
            if (!current.contains(c.guid)) {
                deleteChildSafely(con, c.nvce, c.guid, visited);
            }
        }
    }

    /**
     * Partial update. Mirrors {@code SyncMongoDS.patch} semantics: {@code nvConfigNames} with
     * {@code includeParam=true} is the exact set of attributes to write; with {@code includeParam=false}
     * it is the set to exclude; empty means full update. {@code updateTS} touches the timestamps,
     * {@code sync} serializes concurrent patches on this instance, {@code updateRefOnly} binds the
     * existing GUIDs of referenced entities without writing the referenced rows themselves.
     * A null/empty GUID falls through to insert; a GUID that doesn't exist is an error.
     */
    @Override
    public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateRefOnly,
                                        boolean includeParam, String... nvConfigNames)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nve);
        if (sync) lock.lock();
        try {
            Connection con = null;
            try {
                con = acquire();
                NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
                ensureTable(nvce);

                WriteCtx ctx = new WriteCtx();
                if (SUS.isEmpty(nve.getGUID())) {
                    V ret = insertRow(con, nve, nvce, ctx); // associates the new row with the bound subject
                    ctx.applyFixups(con);
                    return ret;
                }
                StoredRow stored = storedRow(con, nvce, nve.getGUID());
                if (stored == null) {
                    throw new APIException("Can not patch a missing object " + nve.getGUID());
                }
                checkWrite(nvce, nve, stored, CRUD.UPDATE);
                ctx.seen.add(nve.getGUID());
                if (updateTS) MetaUtil.initTimeStamp(nve);

                // Resolve the attribute subset to write.
                List<AttrInfo> infos = attrInfos(nvce);
                List<AttrInfo> subset;
                if (nvConfigNames != null && nvConfigNames.length > 0) {
                    Set<String> names = new HashSet<>();
                    for (String n : nvConfigNames) {
                        n = SUS.trimOrNull(n);
                        if (n != null) names.add(n.toLowerCase());
                    }
                    subset = new ArrayList<>();
                    for (AttrInfo ai : infos) {
                        boolean named = names.contains(ai.lowerName);
                        if (includeParam ? named : !named) subset.add(ai);
                    }
                } else {
                    subset = infos; // no names -> full update
                }
                prepareCrypto(con, nve, subset, ctx, true); // entity key + stored records of masked attributes

                if (!updateRefOnly) {
                    insertChildren(con, nve, subset, ctx); // write referenced entities within the subset
                }

                List<AttrInfo> cols = columnAttrs(subset);
                if (!cols.isEmpty()) {
                    StringBuilder sb = new StringBuilder("UPDATE ").append(H2PUtil.q(tableName(nvce))).append(" SET ");
                    boolean first = true;
                    for (AttrInfo ai : cols) {
                        if (!first) sb.append(", ");
                        sb.append(H2PUtil.q(ai.name)).append(" = ?");
                        first = false;
                    }
                    sb.append(" WHERE ").append(H2PUtil.q(MetaToken.GUID.getName())).append(" = ?");
                    PreparedStatement ps = null;
                    try {
                        ps = con.prepareStatement(sb.toString());
                        int idx = 1;
                        for (AttrInfo ai : cols) bindColumn(ps, idx++, ai, nve, ctx);
                        ps.setObject(idx, IDGs.UUIDV7.decode(nve.getGUID()));
                        ps.executeUpdate();
                    } finally {
                        close(ps);
                    }
                }

                syncJoins(con, nve, subset, true, ctx); // resync only the subset's collections
                ctx.applyFixups(con);
                return nve;
            } catch (SQLException e) {
                throw mapOrWrap(e);
            } finally {
                close(con);
            }
        } finally {
            if (sync) lock.unlock();
        }
    }

    // ---------- Delete ----------

    @Override
    @SuppressWarnings("unchecked")
    public <V extends NVEntity> boolean delete(V nve, boolean withReference)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        if (nve == null) return false;
        Connection con = null;
        try {
            con = acquire();
            return innerDelete(con, nve, withReference);
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    /**
     * Cascade delete on one connection. The children to cascade to are resolved from the
     * <b>database</b> (the stored row's FK columns and join tables), never from the in-memory
     * object — a shell entity (children not loaded) cascades exactly like a fully loaded one.
     */
    private boolean innerDelete(Connection con, NVEntity nve, boolean withReference) throws SQLException {
        if (nve == null || SUS.isEmpty(nve.getGUID())) return false;
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        return deleteByGuid(con, nvce, IDGs.UUIDV7.decode(nve.getGUID()), withReference, new HashSet<>());
    }

    /** A child row reference collected from the DB before its parent row is deleted. */
    private static final class ChildRef {
        final NVConfigEntity nvce;
        final UUID guid;

        ChildRef(NVConfigEntity nvce, UUID guid) {
            this.nvce = nvce;
            this.guid = guid;
        }
    }

    /** This row's referenced children as stored: non-null ENTITY_REF FK columns + join-table rows. */
    private List<ChildRef> collectDbChildren(Connection con, NVConfigEntity nvce, UUID guid,
                                             List<AttrInfo> infos) throws SQLException {
        List<ChildRef> ret = new ArrayList<>();
        List<AttrInfo> refs = new ArrayList<>();
        for (AttrInfo ai : infos) {
            if (ai.kind == H2PUtil.AttrKind.ENTITY_REF && childNVCE(ai) != null) refs.add(ai);
        }
        if (!refs.isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT ");
            for (int i = 0; i < refs.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(H2PUtil.q(refs.get(i).name));
            }
            sql.append(" FROM ").append(H2PUtil.q(tableName(nvce)))
                    .append(" WHERE ").append(H2PUtil.q(MetaToken.GUID.getName())).append(" = ?");
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = con.prepareStatement(sql.toString());
                ps.setObject(1, guid);
                rs = ps.executeQuery();
                if (rs.next()) {
                    for (int i = 0; i < refs.size(); i++) {
                        Object v = rs.getObject(i + 1);
                        if (v instanceof UUID) ret.add(new ChildRef(childNVCE(refs.get(i)), (UUID) v));
                    }
                }
            } finally {
                close(rs, ps);
            }
        }
        for (AttrInfo ai : infos) {
            if (ai.kind != H2PUtil.AttrKind.ENTITY_COLLECTION) continue;
            NVConfigEntity child = childNVCE(ai);
            if (child == null) continue;
            for (UUID c : selectJoinChildren(con, nvce, ai, guid)) ret.add(new ChildRef(child, c));
        }
        return ret;
    }

    /**
     * DB-driven cascade delete. {@code visited} guards cyclic reference chains. Children are
     * collected from the stored row before it is deleted (its join rows go with it via
     * {@code ON DELETE CASCADE}); each child then cascades recursively via
     * {@link #deleteChildSafely} — a child still referenced elsewhere (shared) is kept.
     */
    private boolean deleteByGuid(Connection con, NVConfigEntity nvce, UUID guid, boolean withReference,
                                 Set<UUID> visited) throws SQLException {
        if (nvce == null || guid == null || !visited.add(guid)) return false;
        if (!tableExists(con, nvce)) return false;
        if (accessChecked()) {
            String id = IDGs.UUIDV7.encode(guid);
            StoredRow stored = storedRow(con, nvce, id);
            if (stored == null) return false;
            if (!permitted(id, stored.owner, CRUD.DELETE)) {
                throw new AccessSecurityException("Not permitted to delete " + nvce.getName() + " " + id, Reason.UNAUTHORIZED);
            }
        }

        List<AttrInfo> infos = attrInfos(nvce);
        List<ChildRef> children = withReference ? collectDbChildren(con, nvce, guid, infos) : null;

        boolean deleted;
        PreparedStatement ps = null;
        try {
            // Delete the row first; ON DELETE CASCADE clears its collection join rows, and its own
            // FK references to the children disappear with it.
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            ps.setObject(1, guid);
            deleted = ps.executeUpdate() > 0;
        } finally {
            close(ps);
        }

        if (deleted) {
            deleteEntityKeys(con, nvce, guid);
            if (children != null) {
                for (ChildRef c : children) {
                    deleteChildSafely(con, c.nvce, c.guid, visited);
                }
            }
        }
        return deleted;
    }

    /**
     * Removes the {@code EncapsulatedKey} row(s) protecting a deleted entity ({@code reference_guid} =
     * its GUID). Direct SQL on the key table when it exists; no-op for the key table itself. The key
     * maker's lookup cache may keep the wrapped row until the JVM restarts — harmless, nothing is
     * left to open with it.
     */
    private void deleteEntityKeys(Connection con, NVConfigEntity nvce, UUID guid) throws SQLException {
        if (H2PFieldCrypto.KEY_TABLE.equalsIgnoreCase(nvce.getName())) return;
        if (!rawTableExists(con, H2PFieldCrypto.KEY_TABLE)) return;
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(H2PFieldCrypto.KEY_TABLE)
                    + " WHERE " + H2PUtil.q(H2PFieldCrypto.KEY_REFERENCE_COLUMN) + " = ?");
            ps.setObject(1, guid);
            ps.executeUpdate();
        } finally {
            close(ps);
        }
    }

    /**
     * Cascade into one child; a child still referenced by another row (shared) raises an FK
     * violation — it is kept and the cascade continues. Inside a transaction the attempt is wrapped
     * in a SAVEPOINT (PostgreSQL aborts the whole tx on any failed statement otherwise). Under access
     * control a child the caller may not delete is kept the same way.
     */
    private boolean deleteChildSafely(Connection con, NVConfigEntity childNvce, UUID childGuid,
                                      Set<UUID> visited) throws SQLException {
        java.sql.Savepoint sp = !con.getAutoCommit() ? con.setSavepoint() : null;
        try {
            boolean r = deleteByGuid(con, childNvce, childGuid, true, visited);
            if (sp != null) con.releaseSavepoint(sp);
            return r;
        } catch (AccessSecurityException e) {
            if (sp != null) con.rollback(sp);
            if (log.isEnabled()) {
                log.getLogger().log(Level.FINE, "child kept (delete not permitted): " + childNvce.getName() + " " + childGuid);
            }
            return false;
        } catch (SQLException e) {
            if (isFkViolation(e)) {
                if (sp != null) con.rollback(sp);
                if (log.isEnabled()) {
                    log.getLogger().log(Level.FINE,
                            "shared child kept (still referenced): " + childNvce.getName() + " " + childGuid);
                }
                return false;
            }
            throw e;
        }
    }

    /** FK violation SQLStates: 23503 (PostgreSQL, and H2 child-exists) / 23506 (H2 parent-missing). */
    private static boolean isFkViolation(SQLException e) {
        String s = e.getSQLState();
        return "23503".equals(s) || "23506".equals(s);
    }

    @Override
    public <V extends NVEntity> boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("nvce and queryCriteria can not be null", nvce, queryCriteria);
        if (queryCriteria.length == 0) {
            throw new IllegalArgumentException("queryCriteria can not be empty; use a full-table delete explicitly");
        }
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = acquire();
            if (!tableExists(con, nvce)) return false;
            String where = H2PQueryFormatter.formatWhere(queryCriteria);
            if (accessChecked()) {
                return deleteCheckedByCriteria(con, nvce, where, queryCriteria);
            }
            // Entities may own EncapsulatedKey rows: collect the matching GUIDs first so the key
            // rows can go with them (the key table is the only case where this pass is needed).
            List<UUID> guids = null;
            if (!H2PFieldCrypto.KEY_TABLE.equalsIgnoreCase(nvce.getName()) && rawTableExists(con, H2PFieldCrypto.KEY_TABLE)) {
                guids = new ArrayList<>();
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement("SELECT " + H2PUtil.q(MetaToken.GUID.getName()) + " FROM "
                            + H2PUtil.q(tableName(nvce)) + " WHERE " + where);
                    H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        Object g = rs.getObject(1);
                        if (g instanceof UUID) guids.add((UUID) g);
                    }
                } finally {
                    close(rs, ps);
                    ps = null;
                }
            }
            String sql = "DELETE FROM " + H2PUtil.q(tableName(nvce)) + " WHERE " + where;
            ps = con.prepareStatement(sql);
            H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
            boolean deleted = ps.executeUpdate() > 0;
            if (deleted && guids != null) {
                for (UUID g : guids) deleteEntityKeys(con, nvce, g);
            }
            return deleted;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
    }

    /**
     * Criteria delete under access control. Matching rows the caller may not read are skipped — they
     * are invisible to it, so neither deleted nor reported. A row it may read but not delete aborts
     * the call before anything is deleted.
     */
    private boolean deleteCheckedByCriteria(Connection con, NVConfigEntity nvce, String where, QueryMarker... queryCriteria)
            throws SQLException {
        boolean owned = hasOwnerColumn(nvce);
        ReadCtx ctx = new ReadCtx();
        List<UUID> guids = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT " + H2PUtil.q(MetaToken.GUID.getName())
                    + (owned ? ", " + H2PUtil.q(MetaToken.SUBJECT_GUID.getName()) : "")
                    + " FROM " + H2PUtil.q(tableName(nvce)) + " WHERE " + where);
            H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
            rs = ps.executeQuery();
            while (rs.next()) {
                Object g = rs.getObject(1);
                if (!(g instanceof UUID)) continue;
                String id = IDGs.UUIDV7.encode((UUID) g);
                String owner = owned ? ownerOf(rs.getObject(2)) : null;
                if (!ctx.mayRead(id, owner)) continue;
                if (!permitted(id, owner, CRUD.DELETE)) {
                    throw new AccessSecurityException("Not permitted to delete " + nvce.getName() + " " + id, Reason.UNAUTHORIZED);
                }
                guids.add((UUID) g);
            }
        } finally {
            close(rs, ps);
        }
        if (guids.isEmpty()) return false;
        try {
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            for (UUID g : guids) {
                ps.setObject(1, g);
                ps.addBatch();
            }
            ps.executeBatch();
        } finally {
            close(ps);
        }
        for (UUID g : guids) deleteEntityKeys(con, nvce, g);
        return true;
    }

    // ---------- Search ----------

    @Override
    public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        return innerSearch(nvce, null, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        return innerSearch(resolveNVCE(className), null, fieldNames, queryCriteria);
    }

    /** Projection set (lowercased attribute names) from a fieldNames list; null = all fields. */
    private static Set<String> toProjection(List<String> fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) return null;
        Set<String> ret = new HashSet<>();
        for (String fn : fieldNames) {
            fn = SUS.trimOrNull(fn);
            if (fn != null) ret.add(fn.toLowerCase());
        }
        return ret.isEmpty() ? null : ret;
    }

    /** Core search: optional subject_guid (userID) filter AND optional criteria AND optional projection. */
    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearch(NVConfigEntity nvce, String userID,
                                                     List<String> fieldNames, QueryMarker... queryCriteria) {
        List<V> ret = new ArrayList<>();
        if (nvce == null) return ret;
        Connection con = null;
        try {
            con = acquire();
            String where = H2PQueryFormatter.formatWhere(queryCriteria);
            final boolean hasUser = userID != null;
            boolean hasWhere = !where.isEmpty();
            StringBuilder w = new StringBuilder();
            if (hasUser) w.append(H2PUtil.q(MetaToken.SUBJECT_GUID.getName())).append(" = ?");
            if (hasUser && hasWhere) w.append(" AND (").append(where).append(')');
            else if (hasWhere) w.append(where);

            for (NVEntity e : select(con, nvce, w.toString(), ps -> {
                int idx = 1;
                if (hasUser) ps.setObject(idx++, IDGs.UUIDV7.decode(userID));
                H2PQueryFormatter.bindWhere(ps, idx, nvce, crypto.active(), queryCriteria);
            }, new ReadCtx(), toProjection(fieldNames), true)) {
                ret.add((V) e);
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
        return ret;
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        Connection con = null;
        try {
            con = acquire();
            return innerSearchByIDs(con, nvce, null, new ReadCtx(), ids);
        } finally {
            close(con);
        }
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(String className, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        Connection con = null;
        try {
            con = acquire();
            return innerSearchByIDs(con, resolveNVCE(className), null, new ReadCtx(), ids);
        } finally {
            close(con);
        }
    }

    /**
     * Fetch entities by GUID, serving already-built instances from the per-call {@code ctx} cache and
     * querying only the missing ones. When {@code userID} is non-null the query is additionally
     * scoped to {@code subject_guid = userID}. Result order follows {@code ids}; missing, filtered
     * and access-denied ids are simply absent.
     */
    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearchByIDs(Connection con, NVConfigEntity nvce, String userID,
                                                          ReadCtx ctx, String... ids) {
        List<V> ret = new ArrayList<>();
        if (nvce == null || ids == null || ids.length == 0) return ret;
        Map<String, NVEntity> effectiveCache = ctx.cache;
        List<String> order = new ArrayList<>();
        List<UUID> toFetch = new ArrayList<>();
        for (String id : ids) {
            if (id == null) continue;
            UUID u = IDGs.UUIDV7.decode(id);
            String norm = IDGs.UUIDV7.encode(u); // canonical form — must match buildEntity's cache key
            order.add(norm);
            if (!effectiveCache.containsKey(norm)) toFetch.add(u);
        }
        if (order.isEmpty()) return ret;
        if (!toFetch.isEmpty()) {
            StringBuilder in = new StringBuilder(H2PUtil.q(MetaToken.GUID.getName())).append(" IN (");
            for (int i = 0; i < toFetch.size(); i++) in.append(i == 0 ? "?" : ", ?");
            in.append(')');
            final boolean hasUser = userID != null;
            if (hasUser) in.append(" AND ").append(H2PUtil.q(MetaToken.SUBJECT_GUID.getName())).append(" = ?");
            try {
                // capResults=false: the result is already bounded by the id list — capping here
                // would silently drop collection children and batch-page rows.
                select(con, nvce, in.toString(), ps -> {
                    int idx = 1;
                    for (UUID u : toFetch) ps.setObject(idx++, u);
                    if (hasUser) ps.setObject(idx, IDGs.UUIDV7.decode(userID));
                }, ctx, null, false); // built entities land in the cache, keyed by GUID
            } catch (SQLException e) {
                throw mapOrWrap(e);
            }
        }
        for (String norm : order) {
            NVEntity e = effectiveCache.get(norm);
            if (e != null) ret.add((V) e);
        }
        return ret;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, NVConfigEntity nvce,
                                                   List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        return innerSearch(nvce, userID, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, String className,
                                                   List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        return innerSearch(resolveNVCE(className), userID, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        Connection con = null;
        try {
            con = acquire();
            // Scoped to the subject: an id belonging to another subject_guid is filtered out.
            return innerSearchByIDs(con, nvce, userID, new ReadCtx(), ids);
        } finally {
            close(con);
        }
    }

    @Override
    public long countMatch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("NVConfigEntity is null", nvce);
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (!tableExists(con, nvce)) return 0;
            String where = H2PQueryFormatter.formatWhere(queryCriteria);
            if (accessChecked()) {
                // only the rows the caller may read are counted: one verdict per matching row
                boolean owned = hasOwnerColumn(nvce);
                StringBuilder sql = new StringBuilder("SELECT ").append(H2PUtil.q(MetaToken.GUID.getName()));
                if (owned) sql.append(", ").append(H2PUtil.q(MetaToken.SUBJECT_GUID.getName()));
                sql.append(" FROM ").append(H2PUtil.q(tableName(nvce)));
                if (!where.isEmpty()) sql.append(" WHERE ").append(where);
                ps = con.prepareStatement(sql.toString());
                H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
                rs = ps.executeQuery();
                ReadCtx ctx = new ReadCtx();
                long count = 0;
                while (rs.next()) {
                    Object g = rs.getObject(1);
                    if (g instanceof UUID && ctx.mayRead(IDGs.UUIDV7.encode((UUID) g), owned ? ownerOf(rs.getObject(2)) : null)) {
                        count++;
                    }
                }
                return count;
            }
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(H2PUtil.q(tableName(nvce)));
            if (!where.isEmpty()) sql.append(" WHERE ").append(where);
            ps = con.prepareStatement(sql.toString());
            H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
            rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {
        NVConfigEntity nvce = resolveNVCE(metaTypeName);
        if (nvce == null || objectId == null) return null;
        String id = objectId instanceof UUID ? IDGs.UUIDV7.encode((UUID) objectId) : objectId.toString();
        Connection con = null;
        try {
            con = acquire();
            List<NVEntity> found = innerSearchByIDs(con, nvce, null, new ReadCtx(), id);
            return (NT) (found.isEmpty() ? null : found.get(0));
        } finally {
            close(con);
        }
    }

    @Override
    public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection) {
        return lookupByReferenceID(metaTypeName, objectId);
    }

    // ---------- Batch search ----------

    /**
     * Builds the pagination report: the complete, deterministic list of every matching guid.
     * NEVER capped — {@code batchSearch}/{@code nextBatch} IS the datastore-agnostic user-space
     * pagination mechanism ({@code MAX_SELECT_RESULTS} guards full-entity materialization; a
     * guid-only report row is cheap, and the caller pages the actual data via {@link #nextBatch}
     * at the size of its own choosing). Under access control the report lists only the rows the
     * caller may read.
     */
    @Override
    public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("NVConfigEntity is null.", nvce);
        List<T> list = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (tableExists(con, nvce)) {
                ReadCtx ctx = accessChecked() ? new ReadCtx() : null;
                boolean owned = ctx != null && hasOwnerColumn(nvce);
                StringBuilder sql = new StringBuilder("SELECT ").append(H2PUtil.q(MetaToken.GUID.getName()));
                if (owned) sql.append(", ").append(H2PUtil.q(MetaToken.SUBJECT_GUID.getName()));
                sql.append(" FROM ").append(H2PUtil.q(tableName(nvce)));
                String where = H2PQueryFormatter.formatWhere(queryCriteria);
                if (!where.isEmpty()) sql.append(" WHERE ").append(where);
                // Deterministic report order (UUID v7 is time-ordered) so nextBatch pages are stable.
                sql.append(" ORDER BY ").append(H2PUtil.q(MetaToken.GUID.getName()));
                ps = con.prepareStatement(sql.toString());
                H2PQueryFormatter.bindWhere(ps, 1, nvce, crypto.active(), queryCriteria);
                rs = ps.executeQuery();
                while (rs.next()) {
                    UUID guid = rs.getObject(1, UUID.class);
                    if (ctx != null && (guid == null
                            || !ctx.mayRead(IDGs.UUIDV7.encode(guid), owned ? ownerOf(rs.getObject(2)) : null))) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    T id = (T) guid;
                    list.add(id);
                }
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }

        APISearchResult<T> results = new APISearchResult<>();
        results.setNVConfigEntity(nvce);
        results.setReportID(UUID.randomUUID().toString());
        results.setMatchIDs(list);
        results.setCreationTime(System.currentTimeMillis());
        results.setLastTimeUpdated(System.currentTimeMillis());
        results.setLastTimeRead(System.currentTimeMillis());
        return results;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(String className, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        NVConfigEntity nvce = resolveNVCE(className);
        if (nvce == null) throw new IllegalArgumentException("Class " + className + " not supported.");
        return batchSearch(nvce, queryCriteria);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> reportResults,
                                                               int startIndex, int batchSize)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        APIBatchResult<V> batch = new APIBatchResult<>();
        batch.setReportID(reportResults.getReportID());
        batch.setTotalMatches(reportResults.size());

        if (startIndex >= reportResults.size()) {
            return null;
        }
        int endIndex;
        if (batchSize == -1 || (startIndex + batchSize >= reportResults.size())) {
            endIndex = reportResults.size();
        } else {
            endIndex = startIndex + batchSize;
        }
        batch.setRange(startIndex, endIndex);

        List<T> sub = reportResults.getMatchIDs().subList(startIndex, endIndex);
        String[] ids = new String[sub.size()];
        for (int i = 0; i < sub.size(); i++) {
            Object id = sub.get(i);
            ids[i] = id instanceof UUID ? IDGs.UUIDV7.encode((UUID) id) : String.valueOf(id);
        }
        Connection con = null;
        try {
            con = acquire();
            List<NVEntity> nveList = innerSearchByIDs(con, reportResults.getNVConfigEntity(), null, new ReadCtx(), ids);
            batch.setBatch(nveList);
        } finally {
            close(con);
        }
        return batch;
    }

    // ---------- DynamicEnumMap ----------

    private void ensureDEMTable() {
        execDDL("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(DEM_TABLE) + " ("
                + H2PUtil.q("name") + " VARCHAR PRIMARY KEY, "
                + H2PUtil.q("dem_data") + " VARCHAR)");
    }

    @Override
    public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null DynamicEnumMap", dynamicEnumMap);
        Connection con = null;
        PreparedStatement upd = null;
        PreparedStatement ins = null;
        try {
            con = acquire();
            ensureDEMTable();
            String json = GSONUtil.toJSONDynamicEnumMap(dynamicEnumMap);
            // Portable upsert (UPDATE then INSERT-if-absent) — avoids H2-only MERGE / Postgres ON CONFLICT.
            upd = con.prepareStatement("UPDATE " + H2PUtil.q(DEM_TABLE) + " SET "
                    + H2PUtil.q("dem_data") + " = ? WHERE " + H2PUtil.q("name") + " = ?");
            upd.setString(1, json);
            upd.setString(2, dynamicEnumMap.getName());
            if (upd.executeUpdate() == 0) {
                try {
                    ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(DEM_TABLE) + " ("
                            + H2PUtil.q("name") + ", " + H2PUtil.q("dem_data") + ") VALUES (?, ?)");
                    ins.setString(1, dynamicEnumMap.getName());
                    ins.setString(2, json);
                    ins.executeUpdate();
                } catch (SQLException e) {
                    // Concurrent inserter won the race — the row exists now, retry the UPDATE.
                    if (!"23505".equals(e.getSQLState())) throw e;
                    upd.executeUpdate();
                }
            }
            return dynamicEnumMap;
        } catch (Exception e) {
            throw mapOrWrap(e);
        } finally {
            close(ins, upd, con);
        }
    }

    @Override
    public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        return insertDynamicEnumMap(dynamicEnumMap);
    }

    @Override
    public DynamicEnumMap searchDynamicEnumMapByName(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null name", name);
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (!rawTableExists(con, DEM_TABLE)) return null;
            ps = con.prepareStatement("SELECT " + H2PUtil.q("dem_data") + " FROM " + H2PUtil.q(DEM_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            ps.setString(1, name);
            rs = ps.executeQuery();
            return rs.next() ? GSONUtil.fromJSONDynamicEnumMap(rs.getString(1)) : null;
        } catch (Exception e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
    }

    @Override
    public void deleteDynamicEnumMap(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null name", name);
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = acquire();
            if (!rawTableExists(con, DEM_TABLE)) return;
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(DEM_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            ps.setString(1, name);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
    }

    @Override
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        List<DynamicEnumMap> ret = new ArrayList<>();
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (!rawTableExists(con, DEM_TABLE)) return ret;
            stmt = con.createStatement();
            rs = stmt.executeQuery("SELECT " + H2PUtil.q("dem_data") + " FROM " + H2PUtil.q(DEM_TABLE));
            while (rs.next()) {
                ret.add(GSONUtil.fromJSONDynamicEnumMap(rs.getString(1)));
            }
        } catch (Exception e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, stmt, con);
        }
        return ret;
    }

    boolean rawTableExists(Connection con, String table) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME)=UPPER(?)"
                            + " AND TABLE_TYPE='BASE TABLE'"
                            + " AND TABLE_SCHEMA = CURRENT_SCHEMA");
            ps.setString(1, table);
            rs = ps.executeQuery();
            return rs.next();
        } finally {
            close(rs, ps);
        }
    }

    // ---------- Sequences ----------

    private void ensureSequenceTable() {
        execDDL("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(SEQ_TABLE) + " ("
                + H2PUtil.q("name") + " VARCHAR PRIMARY KEY, "
                + H2PUtil.q("seq_value") + " BIGINT, "
                + H2PUtil.q("increment_value") + " BIGINT)");
    }

    @Override
    public LongSequence createSequence(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        return createSequence(sequenceName, 0, 1);
    }

    @Override
    public LongSequence createSequence(String sequenceName, long startValue, long defaultIncrement)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        String seq = sequenceName.toLowerCase();
        // Sequences are non-transactional: always run on a dedicated auto-commit connection, never the
        // ambient tx connection (a rolled-back tx must not undo — and its row locks must not pin — a sequence).
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = newConnection();
            ensureSequenceTable();
            if (!sequenceExists(con, seq)) {
                try {
                    ps = con.prepareStatement("INSERT INTO " + H2PUtil.q(SEQ_TABLE) + " ("
                            + H2PUtil.q("name") + ", " + H2PUtil.q("seq_value") + ", " + H2PUtil.q("increment_value")
                            + ") VALUES (?, ?, ?)");
                    ps.setString(1, seq);
                    ps.setLong(2, startValue);
                    ps.setLong(3, defaultIncrement);
                    ps.executeUpdate();
                } catch (SQLException e) {
                    // Lost the seed race to a concurrent creator — the row exists now, which is all we need.
                    if (!"23505".equals(e.getSQLState())) throw e;
                }
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
        LongSequence ls = new LongSequence();
        ls.setName(seq);
        ls.setSequenceValue(startValue);
        ls.setDefaultIncrement(defaultIncrement);
        return ls;
    }

    private boolean sequenceExists(Connection con, String seq) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT 1 FROM " + H2PUtil.q(SEQ_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            ps.setString(1, seq);
            rs = ps.executeQuery();
            return rs.next();
        } finally {
            close(rs, ps);
        }
    }

    @Override
    public void deleteSequence(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = newConnection(); // sequences are non-transactional (see createSequence)
            if (!rawTableExists(con, SEQ_TABLE)) return;
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(SEQ_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            ps.setString(1, sequenceName.toLowerCase());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
    }

    @Override
    public long currentSequenceValue(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = newConnection(); // sequences are non-transactional (see createSequence)
            if (!rawTableExists(con, SEQ_TABLE)) return 0;
            ps = con.prepareStatement("SELECT " + H2PUtil.q("seq_value") + " FROM " + H2PUtil.q(SEQ_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            ps.setString(1, sequenceName.toLowerCase());
            rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
    }

    @Override
    public long nextSequenceValue(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        return incrementSequence(sequenceName.toLowerCase(), null);
    }

    @Override
    public long nextSequenceValue(String sequenceName, long increment)
            throws NullPointerException, IllegalArgumentException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        return incrementSequence(sequenceName.toLowerCase(), increment);
    }

    /**
     * Atomically advance a sequence and return the new value: {@code SELECT ... FOR UPDATE} +
     * {@code UPDATE} in one short DB transaction on a dedicated connection. The row lock makes the
     * increment safe across threads, pooled connections and JVMs (a JVM-local lock can't); the
     * dedicated connection keeps the op out of the ambient ThreadLocal transaction — a rolled-back
     * tx must not undo the increment, and its uncommitted row lock must not block other callers.
     *
     * @param increment null = use the sequence's stored increment_value
     */
    private long incrementSequence(String seq, Long increment) {
        Connection con = null;
        PreparedStatement sel = null;
        PreparedStatement upd = null;
        ResultSet rs = null;
        try {
            con = newConnection();
            ensureSequenceTable();
            con.setAutoCommit(false);
            try {
                sel = con.prepareStatement("SELECT " + H2PUtil.q("seq_value") + ", " + H2PUtil.q("increment_value")
                        + " FROM " + H2PUtil.q(SEQ_TABLE) + " WHERE " + H2PUtil.q("name") + " = ? FOR UPDATE");
                sel.setString(1, seq);
                rs = sel.executeQuery();
                if (!rs.next()) {
                    // Sequence missing: seed it (own connection, seed-race safe) and re-lock the row.
                    con.rollback();
                    createSequence(seq);
                    close(rs);
                    rs = sel.executeQuery();
                    if (!rs.next()) throw new APIException("sequence not found: " + seq);
                }
                long inc = increment != null ? increment : rs.getLong(2);
                long next = rs.getLong(1) + inc;
                upd = con.prepareStatement("UPDATE " + H2PUtil.q(SEQ_TABLE) + " SET "
                        + H2PUtil.q("seq_value") + " = ? WHERE " + H2PUtil.q("name") + " = ?");
                upd.setLong(1, next);
                upd.setString(2, seq);
                upd.executeUpdate();
                con.commit();
                return next;
            } catch (SQLException | RuntimeException e) {
                try {
                    con.rollback();
                } catch (SQLException ignore) {
                    // surface the original failure
                }
                throw e;
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            if (con != null) {
                try {
                    con.setAutoCommit(true);
                } catch (SQLException ignore) {
                    // closing anyway
                }
            }
            close(rs, sel, upd, con);
        }
    }

    // ---------- File storage (APIDocumentStore) — versioned, dual-target ----------
    //
    // Metadata is a regular FileInfoDAO row (existing normalized CRUD, table file_info_dao);
    // content lives in sys_file_version — one bytea row per version, keyed (file_guid, version)
    // with version numbers monotonic per file (MAX+1, never reused, 23505-retry on concurrent
    // updates: last-write-wins with full history). sys_file_head points at the current version;
    // rollback just repoints it (no content copy). Identical SQL on H2 and PostgreSQL — no
    // dialect divergence. FILE_VERSIONS_MAX (opt-in) prunes old versions, never the head's.

    /** Out-of-band DDL for the file tables (metadata table first — the FKs reference it). */
    void ensureFileTables() {
        if (fileTablesEnsured) {
            return;
        }
        ensureTable(FileInfo.NVC_FILE_INFO);
        String fileTable = H2PUtil.q(FileInfo.NVC_FILE_INFO.getName());
        String guidCol = H2PUtil.q(MetaToken.GUID.getName());
        execDDL("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(FILE_VERSION_TABLE) + " ("
                + H2PUtil.q("file_guid") + " uuid NOT NULL REFERENCES " + fileTable + "(" + guidCol + ") ON DELETE CASCADE, "
                + H2PUtil.q("version") + " BIGINT NOT NULL, "
                + H2PUtil.q("length") + " BIGINT NOT NULL, "
                + H2PUtil.q("created_ts") + " BIGINT NOT NULL, "
                + H2PUtil.q("data") + " bytea NOT NULL, "
                + H2PUtil.q(FILE_ENC_COLUMN) + " SMALLINT DEFAULT 0 NOT NULL, "
                + "PRIMARY KEY (" + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version") + "))");
        // pre-existing tables (created before encryption at rest): additive, portable to both engines
        execDDL("ALTER TABLE " + H2PUtil.q(FILE_VERSION_TABLE) + " ADD COLUMN IF NOT EXISTS "
                + H2PUtil.q(FILE_ENC_COLUMN) + " SMALLINT DEFAULT 0 NOT NULL");
        execDDL("CREATE TABLE IF NOT EXISTS " + H2PUtil.q(FILE_HEAD_TABLE) + " ("
                + H2PUtil.q("file_guid") + " uuid PRIMARY KEY REFERENCES " + fileTable + "(" + guidCol + ") ON DELETE CASCADE, "
                + H2PUtil.q("current_version") + " BIGINT NOT NULL)");
        fileTablesEnsured = true;
    }

    /**
     * The owner ({@code subject_guid}) of a stored file, read from its metadata row — the caller's
     * {@link APIFileInfoMap} may be a shell carrying only the GUID. Null when unknown.
     */
    private String fileOwner(Connection con, UUID fileGuid) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT " + H2PUtil.q(MetaToken.SUBJECT_GUID.getName())
                    + " FROM " + H2PUtil.q(FileInfo.NVC_FILE_INFO.getName())
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            ps.setObject(1, fileGuid);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object o = rs.getObject(1);
                return o instanceof UUID ? IDGs.UUIDV7.encode((UUID) o) : (o != null ? o.toString() : null);
            }
            return null;
        } finally {
            close(rs, ps);
        }
    }

    /**
     * Access check for a file operation: the controller decides (owner through its self permission,
     * or a grant), judged against the file's <b>stored</b> owner — the caller's object is never
     * trusted for it. Read denial is reported to the caller as "nothing" ({@code false});
     * write/delete denial throws.
     */
    private boolean fileAccess(Connection con, UUID fileGuid, FileInfo info, CRUD crud) throws SQLException {
        String owner = fileOwner(con, fileGuid);
        if (owner != null && info != null && SUS.isEmpty(info.getSubjectGUID())) info.setSubjectGUID(owner);
        return crypto.accessAllowed(IDGs.UUIDV7.encode(fileGuid), owner, crud);
    }

    /** @return the file's guid as a UUID; throws when the map has no GUID (never stored). */
    private static UUID fileGuid(APIFileInfoMap map) {
        SUS.checkIfNulls("Null file info", map.getOriginalFileInfo());
        String guid = map.getOriginalFileInfo().getGUID();
        if (SUS.isEmpty(guid)) {
            throw new APIException("File has no GUID (was never stored): " + map.getOriginalFileInfo().getName());
        }
        return IDGs.UUIDV7.decode(guid);
    }

    /**
     * Stores the stream as the file's next version and makes it current. First store = version 1;
     * every subsequent call bumps the version (monotonic, never reused). Metadata row, version row
     * and head move as one atomic unit: the op joins the caller's ambient transaction when one is
     * active, otherwise it runs its own local transaction — a failure leaves no orphaned
     * metadata/content (cf. {@code XlogistxMongoDataStore.createFile}'s GridFS rollback).
     */
    @Override
    public APIFileInfoMap createFile(String folderID, APIFileInfoMap file, InputStream is, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", file, is);
        FileInfo info = file.getOriginalFileInfo();
        SUS.checkIfNulls("Null file info", info);
        try {
            byte[] content = IOUtil.inputStreamToByteArray(is, false).toByteArray();
            info.setLength(content.length);
            ensureFileTables(); // out-of-band DDL — before joining/starting any transaction
            crypto.requireConsistent("file " + info.getName()); // controller xor key maker: refuse
            boolean encrypt = crypto.active();
            boolean localTx = getTransactionConnection() == null;
            if (localTx) beginTransaction();
            try {
                if (encrypt) {
                    // Encryption at rest: every file is a VX container under the file's entity key.
                    // Overwriting an existing file needs UPDATE on it (owner self permission or grant),
                    // judged against its stored owner, which a shell object then takes over; a new file
                    // is owned by the bound subject (association) or the caller-supplied subject_guid.
                    if (!SUS.isEmpty(info.getGUID())) {
                        Connection c0 = null;
                        try {
                            c0 = acquire();
                            UUID g0 = IDGs.UUIDV7.decode(info.getGUID());
                            if (storedRow(c0, FileInfo.NVC_FILE_INFO, info.getGUID()) != null
                                    && !fileAccess(c0, g0, info, CRUD.UPDATE)) {
                                throw new AccessSecurityException("Not permitted to update file " + info.getGUID());
                            }
                        } catch (SQLException e) {
                            throw mapOrWrap(e);
                        } finally {
                            close(c0);
                        }
                    }
                    SecurityController sc = crypto.controller();
                    sc.associateNVEntityToSubjectGUID(info, null);
                    if (SUS.isEmpty(info.getSubjectGUID())) {
                        throw new AccessSecurityException("encrypted file without subject_guid (no bound subject): " + info.getName());
                    }
                }
                insert(info); // existing CRUD: null GUID -> insert (assigns UUID v7), known GUID -> update
                UUID guid = IDGs.UUIDV7.decode(info.getGUID());
                byte[] stored = content;
                if (encrypt) {
                    crypto.ensureEntityKey(info);
                    stored = crypto.encryptFile(info, content);
                }
                Connection con = null;
                try {
                    con = acquire();
                    long version = insertFileVersion(con, guid, stored, content.length, encrypt ? FILE_ENC_VX : FILE_ENC_PLAIN);
                    setFileHead(con, guid, version);
                    pruneFileVersions(con, guid);
                } catch (SQLException e) {
                    throw mapOrWrap(e);
                } finally {
                    close(con);
                }
                if (localTx) endTransaction();
            } catch (RuntimeException e) {
                if (localTx) {
                    try {
                        abortTransaction();
                    } catch (RuntimeException ignore) {
                        // surface the original failure
                    }
                }
                throw e;
            }
            if (log.isEnabled()) log.getLogger().info(info.getName());
            return file;
        } finally {
            if (closeStream) SharedIOUtil.close(is);
        }
    }

    /**
     * Streams the file's current (head) version. A version the bound subject may not read (no
     * ownership, no grant) writes nothing and returns {@code null} — encrypted or not, whenever a
     * {@link SecurityController} is configured.
     */
    @Override
    public APIFileInfoMap readFile(APIFileInfoMap map, OutputStream os, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", map, os);
        try {
            return writeVersionTo(map, null, os) ? map : null;
        } finally {
            if (closeStream) SharedIOUtil.close(os);
        }
    }

    /** Streams one specific stored version of the file; {@code null} when the read is denied. */
    @Override
    public APIFileInfoMap readFile(APIFileInfoMap map, long version, OutputStream os, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", map, os);
        try {
            return writeVersionTo(map, version, os) ? map : null;
        } finally {
            if (closeStream) SharedIOUtil.close(os);
        }
    }

    /**
     * Writes one stored version's content ({@code null} = the head version) to {@code os}. With a
     * controller configured the bound subject needs READ on the file first. A plaintext version
     * ({@code enc = 0}, legacy or written without encryption) is then copied as-is; a VX version
     * ({@code enc = 1}) is opened under the file's entity key.
     *
     * @return false when the read was denied (nothing written)
     */
    private boolean writeVersionTo(APIFileInfoMap map, Long version, OutputStream os) throws IOException {
        UUID guid = fileGuid(map);
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (!rawTableExists(con, FILE_VERSION_TABLE)) {
                throw new APIException("File not found: " + map.getOriginalFileInfo().getName());
            }
            if (version == null) {
                ps = con.prepareStatement("SELECT v." + H2PUtil.q("data") + ", v." + H2PUtil.q(FILE_ENC_COLUMN)
                        + " FROM " + H2PUtil.q(FILE_VERSION_TABLE) + " v JOIN " + H2PUtil.q(FILE_HEAD_TABLE) + " h"
                        + " ON h." + H2PUtil.q("file_guid") + " = v." + H2PUtil.q("file_guid")
                        + " AND h." + H2PUtil.q("current_version") + " = v." + H2PUtil.q("version")
                        + " WHERE v." + H2PUtil.q("file_guid") + " = ?");
                ps.setObject(1, guid);
            } else {
                ps = con.prepareStatement("SELECT " + H2PUtil.q("data") + ", " + H2PUtil.q(FILE_ENC_COLUMN)
                        + " FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                        + " WHERE " + H2PUtil.q("file_guid") + " = ? AND " + H2PUtil.q("version") + " = ?");
                ps.setObject(1, guid);
                ps.setLong(2, version);
            }
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw new APIException("File " + (version != null ? "version " + version + " " : "")
                        + "not found: " + map.getOriginalFileInfo().getName());
            }
            byte[] data = rs.getBytes(1);
            int enc = rs.getInt(2);
            FileInfo info = map.getOriginalFileInfo();
            if (crypto.controller() != null && !fileAccess(con, guid, info, CRUD.READ)) {
                if (log.isEnabled()) log.getLogger().info("read denied for file " + guid);
                return false;
            }
            if (enc == FILE_ENC_PLAIN) {
                os.write(data);
                os.flush();
                return true;
            }
            if (enc != FILE_ENC_VX) {
                throw new APIException("Unknown file content encoding " + enc + " for " + map.getOriginalFileInfo().getName());
            }
            if (!crypto.active()) {
                throw new APIException("File " + map.getOriginalFileInfo().getName()
                        + " is encrypted at rest; this store has no SecurityController + KeyMaker to open it");
            }
            crypto.decryptFile(info, data, os);
            os.flush();
            return true;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
    }

    /** Overwrites the file: stores the stream as the next version and moves the head to it. */
    @Override
    public APIFileInfoMap updateFile(APIFileInfoMap map, InputStream is, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        // createFile already versions + repoints the head — the versioned equivalent of the
        // Mongo stores' delete-then-recreate.
        return createFile(null, map, is, closeStream);
    }

    /** Deletes the file: metadata row + (via FK ON DELETE CASCADE) every version row and the head. */
    @Override
    public void deleteFile(APIFileInfoMap map)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", map);
        FileInfo info = map.getOriginalFileInfo();
        UUID guid = fileGuid(map); // validates presence of a GUID
        if (crypto.active()) {
            // encryption at rest: deleting needs DELETE on the file (owner self permission or grant)
            Connection con = null;
            try {
                con = acquire();
                if (storedRow(con, FileInfo.NVC_FILE_INFO, info.getGUID()) != null && !fileAccess(con, guid, info, CRUD.DELETE)) {
                    throw new AccessSecurityException("Not permitted to delete file " + info.getGUID());
                }
            } catch (SQLException e) {
                throw mapOrWrap(e);
            } finally {
                close(con);
            }
        }
        delete(info, false); // FK cascade removes versions + head; deleteByGuid removes the entity key
        if (log.isEnabled()) log.getLogger().info(info.getName());
    }

    /** Lists the stored versions of a file, newest first (version, length, created_ts, current). */
    @Override
    public List<NVGenericMap> fileVersions(APIFileInfoMap map)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        UUID guid = fileGuid(map);
        List<NVGenericMap> ret = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (!rawTableExists(con, FILE_VERSION_TABLE)) {
                return ret;
            }
            if (crypto.controller() != null && !fileAccess(con, guid, map.getOriginalFileInfo(), CRUD.READ)) {
                return ret; // not the caller's to see
            }
            long head = currentFileVersion(con, guid);
            ps = con.prepareStatement("SELECT " + H2PUtil.q("version") + ", " + H2PUtil.q("length") + ", "
                    + H2PUtil.q("created_ts") + ", " + H2PUtil.q(FILE_ENC_COLUMN) + " FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                    + " WHERE " + H2PUtil.q("file_guid") + " = ? ORDER BY " + H2PUtil.q("version") + " DESC");
            ps.setObject(1, guid);
            rs = ps.executeQuery();
            while (rs.next()) {
                long v = rs.getLong(1);
                ret.add(new NVGenericMap()
                        .build(new NVLong("version", v))
                        .build(new NVLong("length", rs.getLong(2)))
                        .build(new NVLong("created_ts", rs.getLong(3)))
                        .build(new NVBoolean("current", v == head))
                        .build(new NVBoolean("encrypted", rs.getInt(4) == FILE_ENC_VX)));
            }
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
    }

    /**
     * Rolls the file back: the given stored version becomes current by repointing the head — no
     * content is copied and no history is rewritten (a later update continues above the highest
     * stored version number). The restored length is reflected on the metadata row.
     */
    @Override
    public APIFileInfoMap rollbackFile(APIFileInfoMap map, long version)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        SUS.checkIfNulls("Null value", map);
        FileInfo info = map.getOriginalFileInfo();
        UUID guid = fileGuid(map);
        boolean localTx = getTransactionConnection() == null;
        if (localTx) beginTransaction();
        try {
            long restoredLength;
            Connection con = null;
            PreparedStatement sel = null;
            ResultSet rs = null;
            try {
                con = acquire();
                if (crypto.controller() != null && !fileAccess(con, guid, info, CRUD.UPDATE)) {
                    throw new AccessSecurityException("Not permitted to roll back file " + info.getGUID());
                }
                sel = con.prepareStatement("SELECT " + H2PUtil.q("length")
                        + " FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                        + " WHERE " + H2PUtil.q("file_guid") + " = ? AND " + H2PUtil.q("version") + " = ?");
                sel.setObject(1, guid);
                sel.setLong(2, version);
                rs = sel.executeQuery();
                if (!rs.next()) {
                    throw new APIException("File version " + version + " not found: " + info.getName());
                }
                restoredLength = rs.getLong(1);
                setFileHead(con, guid, version);
            } catch (SQLException e) {
                throw mapOrWrap(e);
            } finally {
                close(rs, sel, con);
            }
            info.setLength(restoredLength);
            update(info);
            if (localTx) endTransaction();
            if (log.isEnabled()) log.getLogger().info(info.getName() + " -> version " + version);
            return map;
        } catch (RuntimeException e) {
            if (localTx) {
                try {
                    abortTransaction();
                } catch (RuntimeException ignore) {
                    // surface the original failure
                }
            }
            throw e;
        }
    }

    /** Not implemented — parity with the Mongo document stores (folders are FULL_PATH_NAME strings). */
    @Override
    public APIFileInfoMap createFolder(String folderFullPath)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        return null;
    }

    /** Not implemented — parity with the Mongo document stores. */
    @Override
    public Map<String, APIFileInfoMap> discover() throws IOException, AccessSecurityException, APIException {
        return null;
    }

    /** Not implemented — parity with the Mongo document stores. */
    @Override
    public List<APIFileInfoMap> search(String... args)
            throws NullPointerException, IllegalArgumentException, IOException, AccessSecurityException, APIException {
        return null;
    }

    /**
     * Inserts the file's next version row: {@code MAX(version)+1}, retried when a concurrent
     * updater takes the same number (23505 — last-write-wins, both versions are kept). Inside a
     * transaction the INSERT is SAVEPOINT-wrapped: PostgreSQL aborts the whole tx on any failed
     * statement, and the retry must survive the collision.
     */
    /**
     * Stores one version row: {@code content} as written to the column (plaintext, or a VX container
     * when {@code enc} is {@link #FILE_ENC_VX}); {@code length} is always the plaintext length.
     */
    private long insertFileVersion(Connection con, UUID fileGuid, byte[] content, long plainLength, int enc) throws SQLException {
        long now = System.currentTimeMillis();
        while (true) {
            long next;
            PreparedStatement sel = null;
            ResultSet rs = null;
            try {
                sel = con.prepareStatement("SELECT COALESCE(MAX(" + H2PUtil.q("version") + "), 0) + 1"
                        + " FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                        + " WHERE " + H2PUtil.q("file_guid") + " = ?");
                sel.setObject(1, fileGuid);
                rs = sel.executeQuery();
                rs.next();
                next = rs.getLong(1);
            } finally {
                close(rs, sel);
            }
            java.sql.Savepoint sp = !con.getAutoCommit() ? con.setSavepoint() : null;
            PreparedStatement ins = null;
            try {
                ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(FILE_VERSION_TABLE) + " ("
                        + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version") + ", " + H2PUtil.q("length") + ", "
                        + H2PUtil.q("created_ts") + ", " + H2PUtil.q("data") + ", " + H2PUtil.q(FILE_ENC_COLUMN)
                        + ") VALUES (?, ?, ?, ?, ?, ?)");
                ins.setObject(1, fileGuid);
                ins.setLong(2, next);
                ins.setLong(3, plainLength);
                ins.setLong(4, now);
                ins.setBytes(5, content);
                ins.setInt(6, enc);
                ins.executeUpdate();
                if (sp != null) con.releaseSavepoint(sp);
                return next;
            } catch (SQLException e) {
                if (!"23505".equals(e.getSQLState())) throw e;
                if (sp != null) con.rollback(sp);
                // a concurrent updater took this version number — recompute and retry
            } finally {
                close(ins);
            }
        }
    }

    /** Points the head at a version — portable UPDATE-then-INSERT upsert (DEM pattern), last-write-wins. */
    private void setFileHead(Connection con, UUID fileGuid, long version) throws SQLException {
        PreparedStatement upd = null;
        PreparedStatement ins = null;
        try {
            upd = con.prepareStatement("UPDATE " + H2PUtil.q(FILE_HEAD_TABLE) + " SET "
                    + H2PUtil.q("current_version") + " = ? WHERE " + H2PUtil.q("file_guid") + " = ?");
            upd.setLong(1, version);
            upd.setObject(2, fileGuid);
            if (upd.executeUpdate() == 0) {
                java.sql.Savepoint sp = !con.getAutoCommit() ? con.setSavepoint() : null;
                try {
                    ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(FILE_HEAD_TABLE) + " ("
                            + H2PUtil.q("file_guid") + ", " + H2PUtil.q("current_version") + ") VALUES (?, ?)");
                    ins.setObject(1, fileGuid);
                    ins.setLong(2, version);
                    ins.executeUpdate();
                    if (sp != null) con.releaseSavepoint(sp);
                } catch (SQLException e) {
                    // Concurrent creator won the race — the row exists now, retry the UPDATE.
                    if (!"23505".equals(e.getSQLState())) throw e;
                    if (sp != null) con.rollback(sp);
                    upd.executeUpdate();
                }
            }
        } finally {
            close(ins, upd);
        }
    }

    /** @return the head version for a file, or -1 when it has none. */
    private long currentFileVersion(Connection con, UUID fileGuid) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT " + H2PUtil.q("current_version")
                    + " FROM " + H2PUtil.q(FILE_HEAD_TABLE)
                    + " WHERE " + H2PUtil.q("file_guid") + " = ?");
            ps.setObject(1, fileGuid);
            rs = ps.executeQuery();
            return rs.next() ? rs.getLong(1) : -1;
        } finally {
            close(rs, ps);
        }
    }

    /**
     * Enforces {@link H2PParam#FILE_VERSIONS_MAX} (opt-in, > 0): deletes versions older than the
     * newest n for the file — except the version the head points at (a rolled-back head must
     * always stay readable).
     */
    private void pruneFileVersions(Connection con, UUID fileGuid) throws SQLException {
        int max = intParam(H2PParam.FILE_VERSIONS_MAX, 0);
        if (max <= 0) {
            return;
        }
        long cutoff;
        PreparedStatement sel = null;
        ResultSet rs = null;
        try {
            sel = con.prepareStatement("SELECT " + H2PUtil.q("version")
                    + " FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                    + " WHERE " + H2PUtil.q("file_guid") + " = ?"
                    + " ORDER BY " + H2PUtil.q("version") + " DESC LIMIT 1 OFFSET " + (max - 1));
            sel.setObject(1, fileGuid);
            rs = sel.executeQuery();
            if (!rs.next()) {
                return; // fewer than max versions stored — nothing to prune
            }
            cutoff = rs.getLong(1);
        } finally {
            close(rs, sel);
        }
        PreparedStatement del = null;
        try {
            del = con.prepareStatement("DELETE FROM " + H2PUtil.q(FILE_VERSION_TABLE)
                    + " WHERE " + H2PUtil.q("file_guid") + " = ? AND " + H2PUtil.q("version") + " < ?"
                    + " AND " + H2PUtil.q("version") + " NOT IN (SELECT " + H2PUtil.q("current_version")
                    + " FROM " + H2PUtil.q(FILE_HEAD_TABLE)
                    + " WHERE " + H2PUtil.q("file_guid") + " = ?)");
            del.setObject(1, fileGuid);
            del.setLong(2, cutoff);
            del.setObject(3, fileGuid);
            del.executeUpdate();
        } finally {
            close(del);
        }
    }

    // ---------- Dump / restore (portable JSONL) ----------
    //
    // Engine-portable JSON export/import (H2PDumpRestore): entities via GSONUtil (self-describing
    // "class_type"), DEM, sequences and versioned file content in one JSONL stream. Intended for
    // migration/interchange (H2 <-> PostgreSQL, cross-store) — for same-engine backup prefer the
    // native tools (H2 SCRIPT TO / pg_dump).

    /** How {@link #restore} treats data already in the store. */
    public enum RestoreMode {
        /**
         * Upsert every dump record over the existing data (guid-keyed — {@code insert} routes
         * existing GUIDs to update); nothing is deleted. Idempotent: re-running a partially
         * completed restore converges. Sequences are only ever raised, never lowered.
         */
        MERGE,
        /**
         * First clear every discoverable entity table (FK columns nulled, join tables and file
         * content included), DEM and sequences, then load the dump. The store ends up equal to
         * the dump's content.
         */
        WIPE_AND_LOAD
    }

    /**
     * Dumps every stored entity of one type as JSONL (one {@code {"k":"entity","v":{...}}} envelope
     * per line) to {@code out} — streamed via the batch-search paging path, so memory stays bounded
     * by one batch. Entities whose reference graph is cyclic cannot be represented in JSON
     * (references are inlined) and are skipped with a warning. The stream is flushed but not closed.
     *
     * @return the number of entities written
     */
    public long dump(NVConfigEntity nvce, OutputStream out) throws APIException {
        SUS.checkIfNulls("Null type or stream", nvce, out);
        requireSystemContext("dump");
        crypto.setRaw(true); // a backup carries the stored records, never the plaintext of whoever runs it
        try {
            return new H2PDumpRestore(this).dumpType(nvce, out);
        } finally {
            crypto.setRaw(false);
        }
    }

    /**
     * Convenience form of {@link #dump(NVConfigEntity, OutputStream)} for small tables: every stored
     * entity of the type as one JSON array string (cyclic entities skipped). The whole result is
     * materialized in memory — use the streaming form for large tables.
     */
    public String dumpToJSON(NVConfigEntity nvce) throws APIException {
        SUS.checkIfNulls("Null type", nvce);
        requireSystemContext("dump");
        crypto.setRaw(true);
        try {
            return new H2PDumpRestore(this).dumpTypeToJSONArray(nvce);
        } finally {
            crypto.setRaw(false);
        }
    }

    /** Whole-store dump including file content — see {@link #dump(OutputStream, boolean, NVConfigEntity...)}. */
    public NVGenericMap dump(OutputStream out, NVConfigEntity... types) throws APIException {
        return dump(out, true, types);
    }

    /**
     * Dumps the whole store as JSONL: a header line, then every entity of every type, DEM entries,
     * sequences and (when {@code includeFiles}) the versioned file content ({@code sys_file_version}/
     * {@code sys_file_head}, content base64). With no explicit {@code types} the type set is
     * discovered from {@code sys_meta_catalog} ∪ the session registry — databases created before the
     * catalog existed only have rows for types touched since, so pass the types explicitly for those.
     * The stream is flushed but not closed.
     *
     * @return per-kind counts ({@code types} nested map, {@code dem}, {@code sequences},
     *         {@code file_versions}, {@code file_heads}, {@code cycles_skipped})
     */
    public NVGenericMap dump(OutputStream out, boolean includeFiles, NVConfigEntity... types) throws APIException {
        SUS.checkIfNulls("Null stream", out);
        requireSystemContext("dump");
        crypto.setRaw(true); // encrypted attributes travel as their stored records (same master key to restore)
        try {
            return new H2PDumpRestore(this).dumpStore(out, includeFiles, types);
        } finally {
            crypto.setRaw(false);
        }
    }

    /** Zip-container dump including file content — see {@link #dumpZip(OutputStream, boolean, NVConfigEntity...)}. */
    public NVGenericMap dumpZip(OutputStream out, NVConfigEntity... types) throws APIException {
        return dumpZip(out, true, types);
    }

    /**
     * Dumps the whole store as a <b>zip archive</b>: entry {@code dump.jsonl} holds the same JSONL
     * stream as {@link #dump(OutputStream, boolean, NVConfigEntity...)}, but file content is stored
     * as raw {@code files/<file_guid>/<version>} entries (deflate-compressed by the zip layer)
     * instead of inline base64 — the right form when file content dominates the store. A
     * {@code README.TXT} entry explains how to read the archive. Restore the archive with
     * {@link #restore} — it auto-detects the container. The stream is finalized ({@code finish()})
     * but not closed.
     *
     * @return the same per-kind counts as the JSONL dump
     */
    public NVGenericMap dumpZip(OutputStream out, boolean includeFiles, NVConfigEntity... types)
            throws APIException {
        SUS.checkIfNulls("Null stream", out);
        requireSystemContext("dump");
        crypto.setRaw(true);
        try {
            return new H2PDumpRestore(this).dumpZip(out, includeFiles, types);
        } finally {
            crypto.setRaw(false);
        }
    }

    /**
     * Restores a dump into this store, auto-detecting the container: a {@link #dumpZip} archive
     * ({@code PK} magic — {@code dump.jsonl} + raw content entries), or a plain JSONL stream
     * (whole-store with header, or a header-less per-type dump) with inline base64 file content.
     * Entities load in per-batch transactions and upsert by GUID — inlined children are
     * written by the same recursion as a live {@code insert}, so shared children dedup to one row
     * and no ordering between lines is required. Restoring an existing row runs the update path
     * (collection join rows are resynced). Entity classes named by {@code class_type} must be on
     * this JVM's classpath. A failure aborts the current batch and rethrows with the line number;
     * completed batches stay committed — re-running with {@link RestoreMode#MERGE} converges.
     *
     * @return per-kind counts ({@code entities}, {@code dem}, {@code sequences},
     *         {@code file_versions}, {@code file_heads})
     */
    public NVGenericMap restore(InputStream in, RestoreMode mode) throws APIException {
        SUS.checkIfNulls("Null stream or mode", in, mode);
        requireSystemContext("restore");
        return new H2PDumpRestore(this).restore(in, mode);
    }

    /**
     * A dump or a restore moves every row of the store. Under access control it would otherwise
     * carry only what the caller may read — a silently partial backup — so it is refused outside the
     * controller's system context.
     */
    private void requireSystemContext(String operation) {
        if (accessChecked()) {
            throw new AccessSecurityException(operation + " of an access-controlled store runs in the system context only"
                    + " (SecurityController.runAsSystem)", Reason.UNAUTHORIZED);
        }
    }

    // ---------- Error mapping ----------

    private APIException mapOrWrap(Exception e) {
        APIExceptionHandler exceptionHandler = getAPIExceptionHandler();
        if (exceptionHandler != null) {
            APIException mapped = exceptionHandler.mapException(e);
            if (mapped != null) return mapped;
        }
        APIException apiEx = new APIException(e.getMessage());
        apiEx.initCause(e);
        return apiEx;
    }
}
