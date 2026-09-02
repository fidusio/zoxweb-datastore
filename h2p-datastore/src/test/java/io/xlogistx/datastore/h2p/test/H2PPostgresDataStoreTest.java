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
    private static final String DB_NAME = System.getProperty("h2p.pg.db", "testpostgres");

    // Kept for tests that need FRESH store instances against the same DB (schema evolution).
    private static String pgTargetUrl;
    private static String pgUser;
    private static String pgPassword;

    @BeforeAll
    @SuppressWarnings("unused")
    public static void setup() throws Exception {
        // h2p.pg.url is the BASE endpoint, e.g. jdbc:postgresql://lax2.xlogistx.io:5432 (db optional).

        String raw = System.getProperty("h2p.pg.url");
        Assumptions.assumeTrue(raw != null && !raw.isEmpty(),
                "set -Dh2p.pg.url=jdbc:postgresql://host:port (+ -Dh2p.pg.user / -Dh2p.pg.password) to run the live PostgreSQL test");
        String user = System.getProperty("h2p.pg.user");
        String password = System.getProperty("h2p.pg.password");

        Class.forName("org.postgresql.Driver");

        // Split off any db path to derive a maintenance URL (postgres) and the target DB URL.
        int schemeEnd = raw.indexOf("://");
        int pathStart = schemeEnd >= 0 ? raw.indexOf('/', schemeEnd + 3) : -1;
        String base = pathStart >= 0 ? raw.substring(0, pathStart) : raw;
        String maintenanceUrl = base + "/postgres";
        String targetUrl = base + "/" + DB_NAME;

        ensureDatabase(maintenanceUrl, user, password, DB_NAME);

        pgTargetUrl = targetUrl;
        pgUser = user;
        pgPassword = password;
        ds = newStore();
        OPSecUtil.singleton();
        System.out.println("Live PostgreSQL target: " + targetUrl);
    }

    /** A fresh datastore instance (fresh caches) against the target database. */
    private static H2PDataStore newStore() {
        APIConfigInfo cfg = new H2PDSCreator().toAPIConfigInfo(pgTargetUrl, pgUser, pgPassword);
        cfg.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");
        H2PDataStore store = new H2PDataStore();
        store.setAPIConfigInfo(cfg);
        store.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        return store;
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

    /** Versioned file storage on native PG: create/update/rollback + specific-version reads (bytea content). */
    @Test
    @Order(6)
    public void versionedFileStoreRoundTrip() throws java.io.IOException {
        byte[] v1 = new byte[64 * 1024], v2 = new byte[128 * 1024];
        new java.util.Random(42).nextBytes(v1);
        new java.util.Random(43).nextBytes(v2);

        org.zoxweb.shared.data.FileInfoDAO fid = new org.zoxweb.shared.data.FileInfoDAO();
        fid.setFullPathName("pg_file_" + UUID.randomUUID());
        fid.setFileType(org.zoxweb.shared.data.FileInfoDAO.FileType.FILE);
        fid.setCreationTime(System.currentTimeMillis());

        ds.createFile(null, fid, new java.io.ByteArrayInputStream(v1), true);
        ds.updateFile(fid, new java.io.ByteArrayInputStream(v2), true);

        java.io.ByteArrayOutputStream head = new java.io.ByteArrayOutputStream();
        ds.readFile(fid, head, true);
        assertArrayEquals(v2, head.toByteArray(), "head must be v2");

        java.io.ByteArrayOutputStream old = new java.io.ByteArrayOutputStream();
        ds.readFile(fid, 1, old, true);
        assertArrayEquals(v1, old.toByteArray(), "version 1 must stay readable");

        java.util.List<org.zoxweb.shared.util.NVGenericMap> versions = ds.fileVersions(fid);
        assertEquals(2, versions.size());
        assertEquals(2L, (long) versions.get(0).getValue("version"));
        assertTrue((boolean) versions.get(0).getValue("current"));

        ds.rollbackFile(fid, 1);
        java.io.ByteArrayOutputStream rolled = new java.io.ByteArrayOutputStream();
        ds.readFile(fid, rolled, true);
        assertArrayEquals(v1, rolled.toByteArray(), "after rollback head must be v1");
        assertEquals(v1.length, fid.getLength());

        ds.deleteFile(fid);
        assertTrue(ds.fileVersions(fid).isEmpty(), "delete must cascade to the version rows");
        System.out.println("PG versioned file store OK");
    }

    /**
     * The dump/restore headline scenario: a whole-store JSONL dump taken from an in-memory H2 store
     * restores into the live PostgreSQL store ({@code MERGE} — the shared test database is never
     * wiped). Entities (incl. an FK reference chain and jsonb schemaless), DEM, sequence and a
     * 2-version file all carry over.
     */
    @Test
    @Order(8)
    public void h2DumpRestoresIntoPostgres() throws java.io.IOException {
        H2PDataStore h2 = new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:pg_mig_src_" + Math.abs(UUID.randomUUID().hashCode())
                        + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL"));
        try {
            String tag = UUID.randomUUID().toString();
            PropertyDAO pd = new PropertyDAO();
            pd.setName("mig-pd-" + tag);
            pd.setDescription("h2 to pg migration");
            pd.getProperties().build("str", "hello").build(new NVInt("n", 7));
            h2.insert(pd);

            H2PRegressionTest.CyclicDAO parent = new H2PRegressionTest.CyclicDAO();
            parent.setName("mig-parent-" + tag);
            H2PRegressionTest.CyclicDAO child = new H2PRegressionTest.CyclicDAO();
            child.setName("mig-child-" + tag);
            parent.setPeer(child); // acyclic reference chain
            h2.insert(parent);

            org.zoxweb.shared.util.DynamicEnumMap dem =
                    new org.zoxweb.shared.util.DynamicEnumMap("mig_dem_" + Math.abs(tag.hashCode()));
            dem.addEnumValue(new org.zoxweb.shared.util.NVPair("k1", "v1"));
            h2.insertDynamicEnumMap(dem);

            String seqName = "mig_seq_" + Math.abs(tag.hashCode());
            h2.createSequence(seqName, 0, 1);
            for (int i = 0; i < 5; i++) h2.nextSequenceValue(seqName);

            byte[] v1 = ("file v1 " + tag).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] v2 = ("file v2 longer " + tag).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            org.zoxweb.shared.data.FileInfoDAO fid = new org.zoxweb.shared.data.FileInfoDAO();
            fid.setFullPathName("mig_file_" + tag);
            fid.setFileType(org.zoxweb.shared.data.FileInfoDAO.FileType.FILE);
            h2.createFile(null, fid, new java.io.ByteArrayInputStream(v1), true);
            h2.updateFile(fid, new java.io.ByteArrayInputStream(v2), true);

            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            org.zoxweb.shared.util.NVGenericMap dumpStats = h2.dump(bos);
            System.out.println("H2 dump stats: " + GSONUtil.toJSONDefault(dumpStats));

            org.zoxweb.shared.util.NVGenericMap restoreStats = ds.restore(
                    new java.io.ByteArrayInputStream(bos.toByteArray()),
                    H2PDataStore.RestoreMode.MERGE); // shared test DB — never WIPE_AND_LOAD here
            System.out.println("PG restore stats: " + GSONUtil.toJSONDefault(restoreStats));

            // Entities on native PG, schemaless now living in jsonb — assert semantic content.
            PropertyDAO pdRead = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
            assertEquals(pd.getName(), pdRead.getName());
            assertEquals("hello", pdRead.getProperties().getValue("str"));
            H2PRegressionTest.CyclicDAO parentRead = (H2PRegressionTest.CyclicDAO) ds
                    .searchByID(H2PRegressionTest.CyclicDAO.class.getName(), parent.getGUID()).get(0);
            assertNotNull(parentRead.getPeer(), "FK reference must survive H2 -> PG");
            assertEquals(child.getGUID(), parentRead.getPeer().getGUID());

            assertNotNull(ds.searchDynamicEnumMapByName(dem.getName()), "DEM must be restored on PG");
            assertEquals(5, ds.currentSequenceValue(seqName), "sequence value must carry over to PG");

            java.io.ByteArrayOutputStream head = new java.io.ByteArrayOutputStream();
            ds.readFile(fid, head, true);
            assertArrayEquals(v2, head.toByteArray(), "file head content must match on PG");
            java.io.ByteArrayOutputStream firstVersion = new java.io.ByteArrayOutputStream();
            ds.readFile(fid, 1, firstVersion, true);
            assertArrayEquals(v1, firstVersion.toByteArray(), "version 1 must be preserved on PG");
            assertEquals(2, ds.fileVersions(fid).size());

            // Close the loop: dump the migrated entities back OFF PostgreSQL into a fresh H2 store.
            java.io.ByteArrayOutputStream back = new java.io.ByteArrayOutputStream();
            long dumped = ds.dump(H2PRegressionTest.CyclicDAO.NVC_CYCLIC_DAO, back);
            assertTrue(dumped >= 2, "PG per-type dump must include the migrated entities");
            H2PDataStore h2Back = new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(
                    "jdbc:h2:mem:pg_mig_back_" + Math.abs(UUID.randomUUID().hashCode())
                            + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL"));
            try {
                h2Back.restore(new java.io.ByteArrayInputStream(back.toByteArray()),
                        H2PDataStore.RestoreMode.MERGE);
                H2PRegressionTest.CyclicDAO roundTripped = (H2PRegressionTest.CyclicDAO) h2Back
                        .searchByID(H2PRegressionTest.CyclicDAO.class.getName(), parent.getGUID()).get(0);
                assertEquals(child.getGUID(), roundTripped.getPeer().getGUID(),
                        "PG -> H2 restore must preserve the reference");
            } finally {
                h2Back.close();
            }
            System.out.println("H2 -> PG -> H2 dump/restore OK");
        } finally {
            h2.close();
        }
    }

    /**
     * Schema evolution on live PostgreSQL (same scenario as
     * {@code H2PRegressionTest.testSchemaEvolution}, fixtures reused): V1 creates the table and
     * writes; a fresh V2 store's first READ triggers the one-time additive sync (a real
     * {@code ALTER TABLE ADD COLUMN IF NOT EXISTS} on PG), old data survives, the added attribute
     * defaults on old rows and round-trips on new ones; a changed column type (String -> Long) is
     * rejected with SCHEMA TYPE MISMATCH. The table is dropped first so re-runs are deterministic.
     */
    @Test
    @Order(9)
    @SuppressWarnings("unchecked")
    public void schemaEvolutionOnLivePG() throws SQLException {
        Connection c = ds.connect();
        try (Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS \"regression_evolved_dao\"");
        } finally {
            c.close(); // pooled: returns to the pool
        }

        String oldGuid;
        H2PDataStore v1 = newStore();
        try {
            H2PRegressionTest.EvolvedV1 e1 = new H2PRegressionTest.EvolvedV1();
            e1.setName("pg-evolved-v1");
            ((org.zoxweb.shared.util.NVBase<String>) e1.lookup("keep_me")).setValue("original");
            v1.insert(e1);
            oldGuid = e1.getGUID();
        } finally {
            v1.close();
        }

        H2PDataStore v2 = newStore();
        try {
            H2PRegressionTest.EvolvedV2 old = (H2PRegressionTest.EvolvedV2)
                    v2.searchByID(H2PRegressionTest.EvolvedV2.NVC_E, oldGuid).get(0);
            assertEquals("original", old.lookup("keep_me").getValue(),
                    "pre-evolution data must survive the additive sync on live PG");
            assertEquals(
                    ((org.zoxweb.shared.util.NVBase<Long>)
                            new H2PRegressionTest.EvolvedV2().lookup("added_later")).getValue(),
                    ((org.zoxweb.shared.util.NVBase<Long>) old.lookup("added_later")).getValue(),
                    "an added attribute reads as default on pre-evolution rows");

            H2PRegressionTest.EvolvedV2 e2 = new H2PRegressionTest.EvolvedV2();
            e2.setName("pg-evolved-v2");
            ((org.zoxweb.shared.util.NVBase<String>) e2.lookup("keep_me")).setValue("newer");
            ((org.zoxweb.shared.util.NVBase<Long>) e2.lookup("added_later")).setValue(123L);
            v2.insert(e2);
            H2PRegressionTest.EvolvedV2 read = (H2PRegressionTest.EvolvedV2)
                    v2.searchByID(H2PRegressionTest.EvolvedV2.NVC_E, e2.getGUID()).get(0);
            assertEquals(Long.valueOf(123L),
                    ((org.zoxweb.shared.util.NVBase<Long>) read.lookup("added_later")).getValue(),
                    "the added attribute must round-trip on live PG");
        } finally {
            v2.close();
        }

        H2PDataStore bad = newStore();
        try {
            H2PRegressionTest.EvolvedBadType b = new H2PRegressionTest.EvolvedBadType();
            b.setName("pg-evolved-bad");
            org.zoxweb.shared.api.APIException mismatch = org.junit.jupiter.api.Assertions.assertThrows(
                    org.zoxweb.shared.api.APIException.class, () -> bad.insert(b),
                    "a changed column type must be rejected on live PG");
            assertTrue(mismatch.getMessage().contains("SCHEMA TYPE MISMATCH"),
                    "the rejection must be explicit: " + mismatch.getMessage());
        } finally {
            bad.close();
        }
        System.out.println("schema evolution verified on live PostgreSQL");
    }

    /** Last: list every base table in the target database (shows the normalized schema the suite created). */
    @Test
    @Order(10)
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
