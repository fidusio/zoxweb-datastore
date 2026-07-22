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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
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
public class H2PDataStore implements APIDataStore<Connection, Connection> {

    public static final LogWrapper log = new LogWrapper(H2PDataStore.class);

    private static final String SEQ_TABLE = "sys_long_sequence";
    private static final String DEM_TABLE = "dynamic_enum_map";

    private volatile APIConfigInfo apiConfig = null;
    private volatile boolean driverLoaded = false;
    private volatile APIExceptionHandler exceptionHandler = null;
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
    // HikariCP connection pool — built lazily for native PostgreSQL only; null for H2.
    private volatile HikariDataSource pool = null;
    // General-purpose per-instance cache (resolved-once connection params + other reuse). Cleared on
    // reconfigure in setAPIConfigInfo so cached values can't go stale across a new config.
    private final RegistrarMapDefault<Object, Object> cache = new RegistrarMapDefault<>();

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
    public APIConfigInfo getAPIConfigInfo() {
        return apiConfig;
    }

    @Override
    public void setAPIConfigInfo(APIConfigInfo configInfo) {
        this.apiConfig = configInfo;
        // Resolve the target engine once, at creation, and pick the matching schemaless dialect codec.
        this.currentDSType = H2PDSCreator.resolveDSType(configInfo);
        this.dialect = H2PDialect.forDSType(currentDSType);
        // New config -> drop any cached connection params (jdbc-url / user / password) so they
        // can't go stale; they are recomputed lazily on the next newConnection().
        cache.clear(false);
        // A new config may point at a different database, where none of these tables exist yet.
        createdTables.clear();
    }

    @Override
    public Connection connect() throws APIException {
        if (!driverLoaded) {
            try {
                lock.lock();
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
            // Native PostgreSQL is pooled (HikariCP); H2 opens a fresh connection per op (mem is ~free).
            // The H2 URL/user/password are computed once and cached (the cache is cleared on reconfigure).
            Connection conn = (currentDSType == DSType.POSTGRES)
                    ? pool().getConnection()
                    : DriverManager.getConnection(
                    cache.lookup("jdbc-url", k -> H2PParam.dataStoreURI(getAPIConfigInfo())),
                    cache.lookup(H2PParam.USER.getName(), k -> getAPIConfigInfo().getProperties().getValue(H2PParam.USER)),
                    cache.lookup("file-user-password", k -> H2PParam.dataStorePassword(getAPIConfigInfo())));
            synchronized (connections) {
                connections.add(conn);
            }
            return conn;
        } catch (SQLException e) {
            throw new APIException("Connection failed: " + e.getMessage());
        }
    }

    /**
     * Lazily-built HikariCP pool for native PostgreSQL. A pooled {@code connection.close()} returns
     * the connection to the pool, so the existing acquire/close/transaction machinery is unchanged —
     * only the physical connection source differs from the H2 (DriverManager) path.
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
                    cfg.setPoolName("h2p-" + (name != null ? name : "postgres"));
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
            SharedIOUtil.close(p); // shut the pool down (native PostgreSQL only)
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
        try {
            Connection con = newConnection();
            con.setAutoCommit(false);
            txConnection.set(con);
            return (T) con;
        } catch (SQLException e) {
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

    @Override
    public APIExceptionHandler getAPIExceptionHandler() {
        return exceptionHandler;
    }

    @Override
    public void setAPIExceptionHandler(APIExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public <T> T lookupProperty(GetName propertyName) {
        return null;
    }

    @Override
    public long lastTimeAccessed() {
        return 0;
    }

    @Override
    public long inactivityDuration() {
        return 0;
    }

    @Override
    public boolean isBusy() {
        return lock.tryLock() ? unlockAndReturn(false) : true;
    }

    private boolean unlockAndReturn(boolean v) {
        lock.unlock();
        return v;
    }

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
        return apiConfig != null ? H2PParam.dataStoreName(apiConfig) : null;
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
        return nvce.getName();
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

    /** Join table name for an entity-collection attribute: {@code <table>__<attr>}. */
    private static String joinTableName(NVConfigEntity nvce, AttrInfo ai) {
        return nvce.getName() + "__" + ai.name;
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
                            + H2PUtil.q("fk_" + nvce.getName() + "_" + ai.name)
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
     * The name is truncated to 63 chars so long table/attribute names stay within PostgreSQL's
     * identifier limit instead of being silently truncated by the server.
     */
    private void createIndex(String table, String... columns) {
        StringBuilder name = new StringBuilder("idx_").append(table);
        StringBuilder cols = new StringBuilder();
        for (String c : columns) {
            name.append('_').append(c);
            if (cols.length() > 0) cols.append(", ");
            cols.append(H2PUtil.q(c));
        }
        String idx = name.length() > 63 ? name.substring(0, 63) : name.toString();
        execDDLQuiet("CREATE INDEX IF NOT EXISTS " + H2PUtil.q(idx)
                + " ON " + H2PUtil.q(table) + " (" + cols + ")");
    }


    @Override
    public DSType getDSType() {
        return currentDSType;
    }

    /** Run DDL that may already have been applied (ADD CONSTRAINT / join table); log-and-ignore duplicates. */
    private void execDDLQuiet(String sql) {
        try {
            execDDL(sql);
        } catch (RuntimeException e) {
            if (log.isEnabled()) log.getLogger().log(Level.FINE, "execDDLQuiet ignored: " + sql, e);
        }
    }

    private boolean tableExists(Connection con, NVConfigEntity nvce) throws SQLException {
        String key = nvce.getName().toLowerCase();
        if (createdTables.contains(key)) return true;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME)=UPPER(?)"
                            + " AND TABLE_TYPE='BASE TABLE'"
                            + " AND LOWER(TABLE_SCHEMA) NOT IN ('pg_catalog','information_schema')");
            ps.setString(1, nvce.getName());
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

    @Override
    public <V extends NVEntity> V insert(V nve)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("Null value", nve);
        Connection con = null;
        try {
            con = acquire();
            return innerInsert(con, nve);
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    private <V extends NVEntity> V innerInsert(Connection con, V nve) throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        ensureTable(nvce);

        SecurityController sc = getAPIConfigInfo() != null ? getAPIConfigInfo().getSecurityController() : null;
        if (sc != null) sc.associateNVEntityToSubjectGUID(nve, null);
        if (SUS.isEmpty(nve.getGUID())) nve.setGUID(IDGs.UUIDV7.genID());
        MetaUtil.initTimeStamp(nve);

        if (existsByGuid(con, nvce, nve.getGUID())) {
            return innerUpdate(con, nve);
        }

        List<AttrInfo> infos = attrInfos(nvce);
        insertChildren(con, nve, infos); // referenced entities first (FK targets must exist)

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
            for (AttrInfo ai : cols) bindColumn(ps, idx++, ai, nve);
            ps.executeUpdate();
        } finally {
            close(ps);
        }

        syncJoins(con, nve, infos, false); // link rows for entity collections
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
    private void insertChildren(Connection con, NVEntity nve, List<AttrInfo> infos) throws SQLException {
        for (AttrInfo ai : infos) {
            if (ai.kind == H2PUtil.AttrKind.ENTITY_REF) {
                NVEntity child = (NVEntity) valueOf(nve, ai.nvc);
                if (child != null) innerInsert(con, child);
            } else if (ai.kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
                if (av != null) {
                    for (NVEntity child : av.values()) {
                        if (child != null) innerInsert(con, child);
                    }
                }
            }
        }
    }

    private void bindColumn(PreparedStatement ps, int idx, AttrInfo ai, NVEntity nve) throws SQLException {
        NVBase<?> nvb = nve.lookup(ai.name);
        Object value = nvb != null ? nvb.getValue() : null;
        switch (ai.kind) {
            case SCALAR:
                if (nvb instanceof NVNumber) {
                    // NVNumber (e.g. Range start/end) carries a runtime numeric type — tag it so int/long/… survive.
                    ps.setString(idx, value == null ? null : encodeNumber((Number) value));
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
                ps.setObject(idx, (child != null && !SUS.isEmpty(child.getGUID()))
                        ? IDGs.UUIDV7.decode(child.getGUID()) : null);
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

    private static String encodeNumber(Number n) {
        if (n instanceof Integer) return "int:" + n.intValue();
        if (n instanceof Long) return "long:" + n.longValue();
        if (n instanceof Float) return "float:" + n.floatValue();
        if (n instanceof Double) return "double:" + n.doubleValue();
        if (n instanceof java.math.BigDecimal) return "bigdec:" + n;
        return "double:" + n.doubleValue();
    }

    private static Number decodeNumber(String s) {
        int i = s.indexOf(':');
        if (i < 0) return null;
        String t = s.substring(0, i), v = s.substring(i + 1);
        switch (t) {
            case "int":
                return Integer.valueOf(v);
            case "long":
                return Long.valueOf(v);
            case "float":
                return Float.valueOf(v);
            case "double":
                return Double.valueOf(v);
            case "bigdec":
                return new java.math.BigDecimal(v);
            default:
                return Double.valueOf(v);
        }
    }

    /** Rewrite an entity's collection join rows (delete-then-insert on update; insert-only on insert). */
    @SuppressWarnings("unchecked")
    private void syncJoins(Connection con, NVEntity nve, List<AttrInfo> infos, boolean deleteFirst) throws SQLException {
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

    /** Run {@code SELECT *} with an optional WHERE, materialize rows, then build entities (resolving refs/joins). */
    private List<NVEntity> select(Connection con, NVConfigEntity nvce, String whereClause, SqlBinder binder)
            throws SQLException {
        List<NVEntity> ret = new ArrayList<>();
        if (nvce == null || !tableExists(con, nvce)) return ret;
        String sql = "SELECT * FROM " + H2PUtil.q(tableName(nvce))
                + (whereClause != null && !whereClause.isEmpty() ? " WHERE " + whereClause : "");
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
        for (Map<String, Object> row : rows) ret.add(buildEntity(con, nvce, row));
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
    private NVEntity buildEntity(Connection con, NVConfigEntity nvce, Map<String, Object> row) throws SQLException {
        NVEntity nve;
        try {
            nve = (NVEntity) nvce.getMetaTypeBase().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new APIException("cannot instantiate " + nvce.getName() + ": " + e.getMessage());
        }
        Object g = row.get(MetaToken.GUID.getName());
        if (g instanceof UUID) nve.setGUID(IDGs.UUIDV7.encode((UUID) g));

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
                        List<NVEntity> child = innerSearchByIDs(con, childNVCE(ai), IDGs.UUIDV7.encode((UUID) ref));
                        if (!child.isEmpty()) ((NVEntityReference) nve.lookup(ai.name)).setValue(child.get(0));
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
            List<UUID> childGuids = selectJoinChildren(con, nvce, ai, (UUID) g);
            if (childGuids.isEmpty()) continue;
            ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
            String[] ids = new String[childGuids.size()];
            for (int i = 0; i < ids.length; i++) ids[i] = IDGs.UUIDV7.encode(childGuids.get(i));
            Map<String, NVEntity> byGUID = new LinkedHashMap<>();
            for (NVEntity child : innerSearchByIDs(con, childNVCE(ai), ids)) byGUID.put(child.getGUID(), child);
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
        if (nvb instanceof NVNumber) ((NVNumber) nvb).setValue(decodeNumber(col.toString()));
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
            return innerUpdate(con, nve);
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(con);
        }
    }

    private <V extends NVEntity> V innerUpdate(Connection con, V nve) throws SQLException {
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        ensureTable(nvce);
        if (SUS.isEmpty(nve.getGUID()) || !existsByGuid(con, nvce, nve.getGUID())) {
            return innerInsert(con, nve);
        }
        MetaUtil.initTimeStamp(nve);

        List<AttrInfo> infos = attrInfos(nvce);
        insertChildren(con, nve, infos); // new/changed referenced entities

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
            for (AttrInfo ai : cols) bindColumn(ps, idx++, ai, nve);
            ps.setObject(idx, IDGs.UUIDV7.decode(nve.getGUID()));
            ps.executeUpdate();
        } finally {
            close(ps);
        }

        syncJoins(con, nve, infos, true); // resync collection links
        return nve;
    }

    @Override
    public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateRefOnly,
                                        boolean includeParam, String... nvConfigNames)
            throws NullPointerException, IllegalArgumentException, APIException {
        return update(nve);
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

    /** Cascade delete on one connection — the recursion must not re-acquire per referenced entity. */
    @SuppressWarnings("unchecked")
    private boolean innerDelete(Connection con, NVEntity nve, boolean withReference) throws SQLException {
        if (nve == null) return false;
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        if (!tableExists(con, nvce)) return false;
        PreparedStatement ps = null;
        boolean deleted;
        try {
            // Delete the parent row first; ON DELETE CASCADE clears its collection join rows.
            ps = con.prepareStatement("DELETE FROM " + H2PUtil.q(tableName(nvce))
                    + " WHERE " + H2PUtil.q(MetaToken.GUID.getName()) + " = ?");
            ps.setObject(1, IDGs.UUIDV7.decode(nve.getGUID()));
            deleted = ps.executeUpdate() > 0;
        } finally {
            close(ps);
        }

        if (deleted && withReference) {
            // Now that the parent no longer references them, delete the referenced entities.
            for (AttrInfo ai : attrInfos(nvce)) {
                if (ai.kind == H2PUtil.AttrKind.ENTITY_REF) {
                    NVEntity child = (NVEntity) valueOf(nve, ai.nvc);
                    if (child != null) innerDelete(con, child, true);
                } else if (ai.kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                    ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(ai.name);
                    if (av != null) {
                        for (NVEntity child : av.values()) {
                            if (child != null) innerDelete(con, child, true);
                        }
                    }
                }
            }
        }
        return deleted;
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
        return innerSearch(nvce, null, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(resolveNVCE(className), null, queryCriteria);
    }

    /** Core search: optional subject_guid (userID) filter AND optional criteria. */
    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearch(NVConfigEntity nvce, String userID,
                                                     QueryMarker... queryCriteria) {
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
            })) {
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
            return innerSearchByIDs(con, nvce, ids);
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
            return innerSearchByIDs(con, resolveNVCE(className), ids);
        } finally {
            close(con);
        }
    }

    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearchByIDs(Connection con, NVConfigEntity nvce, String... ids) {
        List<V> ret = new ArrayList<>();
        if (nvce == null || ids == null || ids.length == 0) return ret;
        List<UUID> uuids = new ArrayList<>();
        for (String id : ids) {
            if (id != null) uuids.add(IDGs.UUIDV7.decode(id));
        }
        if (uuids.isEmpty()) return ret;
        StringBuilder in = new StringBuilder(H2PUtil.q(MetaToken.GUID.getName())).append(" IN (");
        for (int i = 0; i < uuids.size(); i++) in.append(i == 0 ? "?" : ", ?");
        in.append(')');
        try {
            for (NVEntity e : select(con, nvce, in.toString(), ps -> {
                for (int i = 0; i < uuids.size(); i++) ps.setObject(i + 1, uuids.get(i));
            })) {
                ret.add((V) e);
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        }
        return ret;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, NVConfigEntity nvce,
                                                   List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(nvce, userID, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, String className,
                                                   List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return innerSearch(resolveNVCE(className), userID, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return searchByID(nvce, ids);
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
            List<NVEntity> found = innerSearchByIDs(con, nvce, id);
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
            List<NVEntity> nveList = innerSearchByIDs(con, reportResults.getNVConfigEntity(), ids);
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
                ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(DEM_TABLE) + " ("
                        + H2PUtil.q("name") + ", " + H2PUtil.q("dem_data") + ") VALUES (?, ?)");
                ins.setString(1, dynamicEnumMap.getName());
                ins.setString(2, json);
                ins.executeUpdate();
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
                            + " AND LOWER(TABLE_SCHEMA) NOT IN ('pg_catalog','information_schema')");
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
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = acquire();
            ensureSequenceTable();
            if (!sequenceExists(con, seq)) {
                ps = con.prepareStatement("INSERT INTO " + H2PUtil.q(SEQ_TABLE) + " ("
                        + H2PUtil.q("name") + ", " + H2PUtil.q("seq_value") + ", " + H2PUtil.q("increment_value")
                        + ") VALUES (?, ?, ?)");
                ps.setString(1, seq);
                ps.setLong(2, startValue);
                ps.setLong(3, defaultIncrement);
                ps.executeUpdate();
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
            con = acquire();
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
            con = acquire();
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
        long inc = 1;
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = acquire();
            ensureSequenceTable();
            String seq = sequenceName.toLowerCase();
            if (!sequenceExists(con, seq)) {
                createSequence(sequenceName);
            } else {
                ps = con.prepareStatement("SELECT " + H2PUtil.q("increment_value") + " FROM " + H2PUtil.q(SEQ_TABLE)
                        + " WHERE " + H2PUtil.q("name") + " = ?");
                ps.setString(1, seq);
                rs = ps.executeQuery();
                if (rs.next()) inc = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, ps, con);
        }
        return nextSequenceValue(sequenceName, inc);
    }

    @Override
    public long nextSequenceValue(String sequenceName, long increment)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("Null sequence name", sequenceName);
        String seq = sequenceName.toLowerCase();
        lock.lock();
        Connection con = null;
        PreparedStatement upd = null;
        PreparedStatement sel = null;
        ResultSet rs = null;
        try {
            con = acquire();
            ensureSequenceTable();
            if (!sequenceExists(con, seq)) {
                createSequence(sequenceName);
            }
            upd = con.prepareStatement("UPDATE " + H2PUtil.q(SEQ_TABLE) + " SET "
                    + H2PUtil.q("seq_value") + " = " + H2PUtil.q("seq_value") + " + ? WHERE " + H2PUtil.q("name") + " = ?");
            upd.setLong(1, increment);
            upd.setString(2, seq);
            upd.executeUpdate();

            sel = con.prepareStatement("SELECT " + H2PUtil.q("seq_value") + " FROM " + H2PUtil.q(SEQ_TABLE)
                    + " WHERE " + H2PUtil.q("name") + " = ?");
            sel.setString(1, seq);
            rs = sel.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            throw mapOrWrap(e);
        } finally {
            close(rs, sel, upd, con);
            lock.unlock();
        }
    }

    // ---------- Error mapping ----------

    private APIException mapOrWrap(Exception e) {
        if (exceptionHandler != null) {
            APIException mapped = exceptionHandler.mapException(e);
            if (mapped != null) return mapped;
        }
        APIException apiEx = new APIException(e.getMessage());
        apiEx.initCause(e);
        return apiEx;
    }
}
