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
import io.xlogistx.datastore.h2p.H2PDSCreator.H2PParam;
import org.zoxweb.server.api.APIServiceProviderBase;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.server.util.MetaUtil;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.LongSequence;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.io.SharedIOUtil;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.SecurityController;
import org.zoxweb.shared.util.*;

import java.sql.Connection;
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
 */
@SuppressWarnings("serial")
public class H2PDataStore extends APIServiceProviderBase<Connection, Connection>
        implements APIDataStore<Connection, Connection> {

    public static final LogWrapper log = new LogWrapper(H2PDataStore.class);

    private static final String SEQ_TABLE = "sys_long_sequence";
    private static final String DEM_TABLE = "dynamic_enum_map";

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

    public H2PDataStore() {
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

    @Override
    public Connection newConnection() throws APIException {
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

    /** Run a DDL statement on its own auto-committed connection (never the ambient transaction connection). */
    private void execDDL(String sql) {
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

    private static String tableName(NVConfigEntity nvce) {
        return H2PUtil.sqlName(nvce.getName());
    }

    /** Per-attribute storage plan, cached per entity type. */
    private static final class AttrInfo {
        final NVConfig nvc;
        final String name;
        /** {@code name.toLowerCase()} — the key rows are materialized under; precomputed (hot read path). */
        final String lowerName;
        final H2PUtil.AttrKind kind;
        /** Referenced type for ENTITY_REF / ENTITY_COLLECTION, resolved once — see {@link #childNVCE}. */
        volatile NVConfigEntity child;
        volatile boolean childUnresolvable;

        AttrInfo(NVConfig nvc) {
            this.nvc = nvc;
            this.name = nvc.getName();
            this.lowerName = this.name.toLowerCase();
            this.kind = H2PUtil.classify(nvc);
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
            List<AttrInfo> list = new ArrayList<>();
            for (NVConfig nvc : nvce.getAttributes()) {
                AttrInfo ai = new AttrInfo(nvc);
                if (ai.kind != H2PUtil.AttrKind.PK && ai.kind != H2PUtil.AttrKind.EXCLUDED) {
                    list.add(ai);
                }
            }
            return list;
        });
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
     * Create the entity's table (typed columns, {@code bytea} blobs, {@code uuid} FK columns for
     * single references, {@code varchar} JSON for schemaless), then — after recursively ensuring
     * referenced types' tables exist — add FOREIGN KEY constraints and create join tables for
     * entity collections. The bare table is registered before FKs are added, so cyclic type
     * references resolve.
     */
    private void ensureTable(NVConfigEntity nvce) {
        String key = nvce.getName().toLowerCase();
        if (createdTables.contains(key)) return;
        ddlLock.lock();
        try {
            if (createdTables.contains(key)) return;
            List<AttrInfo> infos = attrInfos(nvce);

            StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                    .append(H2PUtil.q(tableName(nvce))).append(" (")
                    .append(H2PUtil.q(MetaToken.GUID.getName())).append(" uuid PRIMARY KEY");
            for (AttrInfo ai : infos) {
                switch (ai.kind) {
                    case SCALAR:
                        sb.append(", ").append(H2PUtil.q(ai.name)).append(' ').append(H2PUtil.scalarColumnType(ai.nvc));
                        if (ai.nvc.isUnique()) sb.append(" UNIQUE");
                        break;
                    case BLOB:
                        sb.append(", ").append(H2PUtil.q(ai.name)).append(" bytea");
                        break;
                    case ENTITY_REF:
                        sb.append(", ").append(H2PUtil.q(ai.name)).append(" uuid");
                        break;
                    case SCHEMALESS:
                        sb.append(", ").append(H2PUtil.q(ai.name)).append(' ').append(dialect.schemalessColumnType());
                        break;
                    default: // ENTITY_COLLECTION -> join table, no column
                        break;
                }
            }
            sb.append(')');
            execDDL(sb.toString());
            metaManager.register(nvce);
            createdTables.add(key); // register bare table before FKs so cyclic refs resolve

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
        try {
            // Scoped to the connection's current schema — a same-named table in another schema of the
            // database must not count as ours (CURRENT_SCHEMA works on both H2 and PostgreSQL).
            ps = con.prepareStatement(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME)=UPPER(?)"
                            + " AND TABLE_TYPE='BASE TABLE'"
                            + " AND TABLE_SCHEMA = CURRENT_SCHEMA");
            ps.setString(1, tableName(nvce));
            rs = ps.executeQuery();
            boolean exists = rs.next();
            if (exists) {
                // Remember it: a JVM reading a pre-existing DB never runs ensureTable, so without this
                // every single select would pay an INFORMATION_SCHEMA round trip.
                metaManager.register(nvce);
                createdTables.add(key);
            }
            return exists;
        } finally {
            close(rs, ps);
        }
    }

    // Class-name / meta-type-name -> NVConfigEntity, including the reflective (Class.forName) resolutions,
    // which H2PMetaManager can't serve because it is keyed by meta-type name only.
    private final Map<String, NVConfigEntity> nvceByTypeName = new ConcurrentHashMap<>();

    /** Resolve an NVConfigEntity from either a Java class name or a registered meta-type name. */
    private NVConfigEntity resolveNVCE(String typeName) {
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

    private boolean existsByGuid(Connection con, NVConfigEntity nvce, String guid) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement("SELECT 1 FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            ps.setObject(1, IDGs.UUIDV7.decode(guid));
            rs = ps.executeQuery();
            return rs.next();
        } finally {
            close(rs, ps);
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("Null value", nve);
        Connection con = null;
        try {
            con = acquire();
            WriteCtx ctx = new WriteCtx();
            V ret = innerInsert(con, nve, ctx);
            ctx.applyFixups(con);
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    private <V extends NVEntity> V innerInsert(Connection con, V nve, WriteCtx ctx) throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        ensureTable(nvce);

        SecurityController sc = getAPIConfigInfo() != null ? getAPIConfigInfo().getSecurityController() : null;
        if (sc != null) sc.associateNVEntityToSubjectGUID(nve, null);
        if (SUS.isEmpty(nve.getGUID())) nve.setGUID(IDGs.UUIDV7.genID());
        MetaUtil.initTimeStamp(nve);

        if (existsByGuid(con, nvce, nve.getGUID())) {
            return innerUpdate(con, nve, ctx);
        }
        ctx.seen.add(nve.getGUID());
        ctx.inFlight.add(nve.getGUID());

        List<AttrInfo> infos = attrInfos(nvce);
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
        innerInsert(con, child, ctx);
    }

    private void bindColumn(PreparedStatement ps, int idx, AttrInfo ai, NVEntity nve, WriteCtx ctx) throws SQLException {
        NVBase<?> nvb = nve.lookup(ai.name);
        Object value = nvb != null ? nvb.getValue() : null;
        switch (ai.kind) {
            case SCALAR:
                if (nvb instanceof NVNumber) {
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
                dialect.bindSchemaless(ps, idx, encodeSchemaless(nvb));
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
     * refs/joins). {@code cache} is the per-call entity cache keyed by GUID — the cycle guard for
     * mutually-referencing rows, and a dedup for repeated child fetches within one operation.
     * {@code projection} (lowercased attribute names, null = all) limits the selected columns and
     * the resolved collections — {@code guid} is always selected.
     */
    private List<NVEntity> select(Connection con, NVConfigEntity nvce, String whereClause, SqlBinder binder,
                                  Map<String, NVEntity> cache, Set<String> projection) throws SQLException {
        List<NVEntity> ret = new ArrayList<>();
        if (nvce == null || !tableExists(con, nvce)) return ret;
        String colList = "*";
        if (projection != null) {
            StringBuilder sb = new StringBuilder(H2PUtil.q(MetaToken.GUID.getName()));
            for (AttrInfo ai : attrInfos(nvce)) {
                if (ai.isColumn() && projection.contains(ai.lowerName)) {
                    sb.append(", ").append(H2PUtil.q(ai.name));
                }
            }
            colList = sb.toString();
        }
        String sql = "SELECT " + colList + " FROM " + H2PUtil.q(tableName(nvce))
                + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "");
        // Safety valve (opt-in via MAX_SELECT_RESULTS): cap unbounded materialization.
        int maxResults = intParam(H2PParam.MAX_SELECT_RESULTS, 0);
        if (maxResults > 0) sql += " LIMIT " + maxResults;
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
        for (Map<String, Object> row : rows) ret.add(buildEntity(con, nvce, row, cache, projection));
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

    @SuppressWarnings("unchecked")
    private NVEntity buildEntity(Connection con, NVConfigEntity nvce, Map<String, Object> row,
                                 Map<String, NVEntity> cache, Set<String> projection) throws SQLException {
        NVEntity nve;
        try {
            nve = (NVEntity) nvce.getMetaTypeBase().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new APIException("cannot instantiate " + nvce.getName() + ": " + e.getMessage());
        }
        Object g = row.get(MetaToken.GUID.getName());
        if (g instanceof UUID) nve.setGUID(IDGs.UUIDV7.encode((UUID) g));
        // Register before resolving references: a child referencing back to this row must find it
        // here instead of re-querying (infinite recursion on cyclic graphs).
        if (cache != null && nve.getGUID() != null) cache.put(nve.getGUID(), nve);

        List<AttrInfo> infos = attrInfos(nvce);
        for (AttrInfo ai : infos) {
            String col = ai.lowerName;
            switch (ai.kind) {
                case SCALAR:
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
                        NVEntity child = cache != null ? cache.get(refId) : null;
                        if (child == null) {
                            List<NVEntity> found = innerSearchByIDs(con, childNVCE(ai), null, cache, refId);
                            child = found.isEmpty() ? null : found.get(0);
                        }
                        if (child != null) ((NVEntityReference) nve.lookup(ai.name)).setValue(child);
                    }
                    break;
                }
                case SCHEMALESS: {
                    String json = dialect.readSchemaless(row.get(col)); // String (H2) or PGobject jsonb (PG)
                    if (json != null) decodeSchemaless(json, ai, nve);
                    break;
                }
                default:
                    break;
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
            for (NVEntity child : this.<NVEntity>innerSearchByIDs(con, childNVCE(ai), null, cache, ids)) {
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
            ((NVEnum) nvb).setValue(SharedUtil.enumValue(ai.nvc.getMetaType(), col.toString()));
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
        if (target instanceof NVEnumList) {
            String[] names = GSONUtil.fromJSONDefault(json, String[].class);
            NVEnumList el = (NVEnumList) target;
            for (String nm : names) {
                el.getValue().add((Enum<?>) SharedUtil.enumValue(ai.nvc.getMetaTypeBase(), nm));
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
            V ret = innerUpdate(con, nve, ctx);
            ctx.applyFixups(con);
            return ret;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    private <V extends NVEntity> V innerUpdate(Connection con, V nve, WriteCtx ctx) throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        ensureTable(nvce);
        if (SUS.isEmpty(nve.getGUID()) || !existsByGuid(con, nvce, nve.getGUID())) {
            return innerInsert(con, nve, ctx);
        }
        ctx.seen.add(nve.getGUID());
        MetaUtil.initTimeStamp(nve);

        List<AttrInfo> infos = attrInfos(nvce);
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

                SecurityController sc = getAPIConfigInfo() != null ? getAPIConfigInfo().getSecurityController() : null;
                if (sc != null) sc.associateNVEntityToSubjectGUID(nve, null);

                WriteCtx ctx = new WriteCtx();
                if (SUS.isEmpty(nve.getGUID())) {
                    V ret = innerInsert(con, nve, ctx);
                    ctx.applyFixups(con);
                    return ret;
                }
                if (!existsByGuid(con, nvce, nve.getGUID())) {
                    throw new APIException("Can not patch a missing object " + nve.getGUID());
                }
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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

        if (deleted && children != null) {
            for (ChildRef c : children) {
                deleteChildSafely(con, c.nvce, c.guid, visited);
            }
        }
        return deleted;
    }

    /**
     * Cascade into one child; a child still referenced by another row (shared) raises an FK
     * violation — it is kept and the cascade continues. Inside a transaction the attempt is wrapped
     * in a SAVEPOINT (PostgreSQL aborts the whole tx on any failed statement otherwise).
     */
    private boolean deleteChildSafely(Connection con, NVConfigEntity childNvce, UUID childGuid,
                                      Set<UUID> visited) throws SQLException {
        java.sql.Savepoint sp = !con.getAutoCommit() ? con.setSavepoint() : null;
        try {
            boolean r = deleteByGuid(con, childNvce, childGuid, true, visited);
            if (sp != null) con.releaseSavepoint(sp);
            return r;
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("nvce and queryCriteria can not be null", nvce, queryCriteria);
        if (queryCriteria.length == 0) {
            throw new IllegalArgumentException("queryCriteria can not be empty; use a full-table delete explicitly");
        }
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = acquire();
            if (!tableExists(con, nvce)) return false;
            String sql = "DELETE FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PQueryFormatter.formatWhere(queryCriteria);
            ps = con.prepareStatement(sql);
            H2PQueryFormatter.bindWhere(ps, 1, nvce, queryCriteria);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(ps, con);
        }
    }

    // ---------- Search ----------

    @Override
    public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(nvce, null, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
                H2PQueryFormatter.bindWhere(ps, idx, nvce, queryCriteria);
            }, new HashMap<>(), toProjection(fieldNames))) {
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        Connection con = null;
        try {
            con = acquire();
            return innerSearchByIDs(con, nvce, null, new HashMap<>(), ids);
        } finally {
            close(con);
        }
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(String className, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        Connection con = null;
        try {
            con = acquire();
            return innerSearchByIDs(con, resolveNVCE(className), null, new HashMap<>(), ids);
        } finally {
            close(con);
        }
    }

    /**
     * Fetch entities by GUID, serving already-built instances from the per-call {@code cache} and
     * querying only the missing ones. When {@code userID} is non-null the query is additionally
     * scoped to {@code subject_guid = userID}. Result order follows {@code ids}; missing/filtered
     * ids are simply absent.
     */
    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearchByIDs(Connection con, NVConfigEntity nvce, String userID,
                                                          Map<String, NVEntity> cache, String... ids) {
        List<V> ret = new ArrayList<>();
        if (nvce == null || ids == null || ids.length == 0) return ret;
        Map<String, NVEntity> effectiveCache = cache != null ? cache : new HashMap<>();
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
                select(con, nvce, in.toString(), ps -> {
                    int idx = 1;
                    for (UUID u : toFetch) ps.setObject(idx++, u);
                    if (hasUser) ps.setObject(idx, IDGs.UUIDV7.decode(userID));
                }, effectiveCache, null); // built entities land in the cache, keyed by GUID
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(nvce, userID, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, String className,
                                                   List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(resolveNVCE(className), userID, fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        Connection con = null;
        try {
            con = acquire();
            // Scoped to the subject: an id belonging to another subject_guid is filtered out.
            return innerSearchByIDs(con, nvce, userID, new HashMap<>(), ids);
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
            StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM ").append(H2PUtil.q(tableName(nvce)));
            String where = H2PQueryFormatter.formatWhere(queryCriteria);
            if (!where.isEmpty()) sql.append(" WHERE ").append(where);
            ps = con.prepareStatement(sql.toString());
            H2PQueryFormatter.bindWhere(ps, 1, nvce, queryCriteria);
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
            List<NVEntity> found = innerSearchByIDs(con, nvce, null, new HashMap<>(), id);
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

    @Override
    public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("NVConfigEntity is null.", nvce);
        List<T> list = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            if (tableExists(con, nvce)) {
                StringBuilder sql = new StringBuilder("SELECT ").append(H2PUtil.q(MetaToken.GUID.getName()))
                        .append(" FROM ").append(H2PUtil.q(tableName(nvce)));
                String where = H2PQueryFormatter.formatWhere(queryCriteria);
                if (!where.isEmpty()) sql.append(" WHERE ").append(where);
                // Deterministic report order (UUID v7 is time-ordered) so nextBatch pages are stable.
                sql.append(" ORDER BY ").append(H2PUtil.q(MetaToken.GUID.getName()));
                ps = con.prepareStatement(sql.toString());
                H2PQueryFormatter.bindWhere(ps, 1, nvce, queryCriteria);
                rs = ps.executeQuery();
                while (rs.next()) {
                    @SuppressWarnings("unchecked")
                    T id = (T) rs.getObject(1, UUID.class);
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        NVConfigEntity nvce = resolveNVCE(className);
        if (nvce == null) throw new IllegalArgumentException("Class " + className + " not supported.");
        return batchSearch(nvce, queryCriteria);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> reportResults,
                                                               int startIndex, int batchSize)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
            List<NVEntity> nveList = innerSearchByIDs(con, reportResults.getNVConfigEntity(), null, new HashMap<>(), ids);
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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

    private boolean rawTableExists(Connection con, String table) throws SQLException {
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return createSequence(sequenceName, 0, 1);
    }

    @Override
    public LongSequence createSequence(String sequenceName, long startValue, long defaultIncrement)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        return incrementSequence(sequenceName.toLowerCase(), null);
    }

    @Override
    public long nextSequenceValue(String sequenceName, long increment)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
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
