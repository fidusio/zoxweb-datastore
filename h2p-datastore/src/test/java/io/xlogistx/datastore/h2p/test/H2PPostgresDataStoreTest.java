package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PExceptionHandler;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.zoxweb.datastore.test.DSConst;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore.DSType;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.http.HTTPAuthorization;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVEntityReferenceList;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.NamedValue;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live PostgreSQL integration test. Runs the same scenarios as the H2 suite against a real
 * PostgreSQL server, proving the identical code path on native PG — including native {@code jsonb}
 * schemaless columns ({@code NVGenericMap}, {@code NamedValue}), {@code bytea} blobs, FK-normalized
 * entity references, and JDBC transactions.
 *
 * <p>Configure via system properties (the whole class is skipped when {@code h2p.pg.url} is absent):
 * <pre>
 *   -Dh2p.pg.url=jdbc:postgresql://host:5432/db  -Dh2p.pg.user=…  -Dh2p.pg.password=…
 * </pre>
 *
 * <p>NOTE: PostgreSQL {@code jsonb} normalizes key order / whitespace, so schemaless round-trips are
 * asserted by <b>semantic value</b> (per key), not by raw-JSON-string equality. Data is UUID-suffixed
 * so re-runs against a persistent server stay isolated.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class H2PPostgresDataStoreTest {

    private static H2PDataStore ds;

    /** Target database name; auto-created if missing. Override with -Dh2p.pg.db. */
    private static final String DB_NAME = System.getenv("h2p.pg.db");

    @BeforeAll
    @SuppressWarnings("unused")
    public static void setup() throws Exception {
        // h2p.pg.url is the BASE endpoint, e.g. jdbc:postgresql://lax2.xlogistx.io:5432 (db optional).

        String raw = System.getenv("h2p.pg.url");
        System.out.println(raw + " " + DB_NAME);
        Assumptions.assumeTrue(raw != null && !raw.isEmpty(),
                "set -Dh2p.pg.url=jdbc:postgresql://host:port (+ h2p.pg.user / h2p.pg.password) to run the live PostgreSQL test");
        String user = System.getenv("h2p.pg.user");
        String password = System.getenv("h2p.pg.password");
        System.out.println(raw + " " + DB_NAME + " " + user + " " + password);

        Class.forName("org.postgresql.Driver");

        // Split off any db path to derive a maintenance URL (postgres) and the target DB URL.
        int schemeEnd = raw.indexOf("://");
        int pathStart = schemeEnd >= 0 ? raw.indexOf('/', schemeEnd + 3) : -1;
        String base = pathStart >= 0 ? raw.substring(0, pathStart) : raw;
        String maintenanceUrl = base + "/postgres";
        String targetUrl = base + "/" + DB_NAME;

        ensureDatabase(maintenanceUrl, user, password, DB_NAME);

        H2PDSCreator creator = new H2PDSCreator();
        APIConfigInfo cfg = creator.toAPIConfigInfo(targetUrl, user, password);
        cfg.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");

        ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        OPSecUtil.singleton();
        System.out.println("Live PostgreSQL target: " + targetUrl);
    }

    /** Create the test database if it does not already exist (CREATE DATABASE cannot run in a txn). */
    private static void ensureDatabase(String maintenanceUrl, String user, String password, String db)
            throws SQLException {
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
                    System.out.println("created database " + db);
                }
            }
        }
    }

    @Test
    @Order(1)
    public void dsTypeIsPostgres() {
        assertEquals(DSType.POSTGRES, ds.getDSType());
        Connection c = ds.connect();
        System.out.println("PG connection: " + c);
    }

    @Test
    @Order(2)
    public void nvGenericMapJsonbRoundTrip() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("pg-nvgm-" + UUID.randomUUID());
        pd.setDescription("live pg jsonb");
        pd.getProperties()
                .build("str", "hello")
                .build(new NVInt("n", 7))
                .build(new NVLong("big", 9_000_000_000L));
        ds.insert(pd);

        PropertyDAO read = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        assertEquals(pd.getGUID(), read.getGUID());
        // jsonb normalizes ordering/whitespace -> assert semantic content, not raw JSON string.
        assertEquals("hello", read.getProperties().getValue("str"));
        assertNotNull(read.getProperties().getValue("n"));
        assertNotNull(read.getProperties().getValue("big"));
        System.out.println("PG NVGenericMap(jsonb) OK: " + read.getGUID());
    }

    @Test
    @Order(3)
    public void namedValueJsonbRoundTrip() {
        HTTPAuthorization auth = HTTPAuthorization.createBasic("mario-" + UUID.randomUUID(), "password");
        auth = ds.insert(auth);
        HTTPAuthorization read = (HTTPAuthorization) ds
                .searchByID(HTTPAuthorization.class.getName(), auth.getGUID()).get(0);
        NamedValue<String> token = read.lookup(HTTPAuthorization.NVC_TOKEN.getName());
        assertNotNull(token);
        assertNotNull(token.getValue());
        System.out.println("PG NamedValue(jsonb) OK: " + GSONUtil.toJSONDefault(read));
    }

    @Test
    @Order(4)
    public void blobAndEntityReferenceRoundTrip() {
        // AllTypes carries a 64-byte blob (bytea).
        DSConst.AllTypes at = DSConst.AllTypes.autoBuilder();
        byte[] original = at.getBytes();

        DSConst.ComplexTypes ct = DSConst.ComplexTypes.buildComplex("pg-ref-" + UUID.randomUUID());
        ct.setAllTypes(at);
        ds.insert(ct);

        DSConst.ComplexTypes read = (DSConst.ComplexTypes) ds
                .searchByID(DSConst.ComplexTypes.class.getName(), ct.getGUID()).get(0);
        // Single FK reference resolves to its own row.
        assertNotNull(read.getAllTypes());
        assertEquals(ct.getAllTypes().getGUID(), read.getAllTypes().getGUID());
        assertArrayEquals(original, read.getAllTypes().getBytes(), "bytea blob must round-trip");
        // Reference list resolves via the join table.
        NVEntity[] refs = ((NVEntityReferenceList) read.lookup("array_of_all_types")).values();
        assertEquals(3, refs.length);
        System.out.println("PG bytea + FK references OK: " + read.getGUID());
    }

    @Test
    @Order(5)
    public void transactionCommitThenRollback() {
        PropertyDAO a = new PropertyDAO();
        a.setName("pg-tx-commit-" + UUID.randomUUID());
        ds.beginTransaction();
        try {
            ds.insert(a);
            ds.endTransaction();
        } catch (RuntimeException e) {
            ds.abortTransaction();
            throw e;
        }
        assertFalse(ds.searchByID(PropertyDAO.class.getName(), a.getGUID()).isEmpty(), "commit must persist");

        PropertyDAO b = new PropertyDAO();
        b.setName("pg-tx-rollback-" + UUID.randomUUID());
        ds.beginTransaction();
        try {
            ds.insert(b);
        } finally {
            ds.abortTransaction();
        }
        assertTrue(ds.searchByID(PropertyDAO.class.getName(), b.getGUID()).isEmpty(), "rollback must discard");
        System.out.println("PG transactions OK");
    }

    /** Last: list every base table in the target database (shows the normalized schema the suite created). */
    @Test
    @Order(6)
    public void listAllTables() {
        java.util.List<String> tables = new java.util.ArrayList<>();
        Connection c = ds.connect();
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT table_name FROM information_schema.tables"
                             + " WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'"
                             + " ORDER BY table_name")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException("listAllTables failed: " + e.getMessage(), e);
        }
        System.out.println("=== Tables in " + DB_NAME + " (" + tables.size() + ") ===");
        for (String t : tables) {
            System.out.println("  " + t);
        }
        assertFalse(tables.isEmpty(), "expected at least one table after the suite ran");
    }
}
