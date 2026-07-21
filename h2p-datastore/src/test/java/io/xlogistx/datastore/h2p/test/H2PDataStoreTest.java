package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDialect;
import io.xlogistx.datastore.h2p.H2PExceptionHandler;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIDataStore.DSType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.datastore.test.CommonDataStoreTest;
import org.zoxweb.datastore.test.DSConst;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.http.HTTPAuthorization;
import org.zoxweb.shared.util.*;
import org.zoxweb.shared.util.Const.RelationalOperator;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full functional suite for {@link H2PDataStore}, run against in-memory H2 in PostgreSQL dialect
 * ({@code jdbc:h2:mem:...;MODE=PostgreSQL}) — no external server required. Covers CRUD, search,
 * FK-normalized entity references, blobs, schemaless JSON (NVGenericMap/NamedValue), Range, DEM,
 * sequences, transactions, URL/DSType/dialect wiring. The parallel {@code H2PPostgresDataStoreTest}
 * runs the same scenarios against a live PostgreSQL server when one is configured.
 */
public class H2PDataStoreTest {

    private static H2PDataStore h2DataStore;
    private static CommonDataStoreTest<Connection, Connection> cdst;

    // Single-URL configuration: in-memory H2 in PostgreSQL dialect (so the tests exercise the
    // exact SQL that runs against a real PostgreSQL server). DB_CLOSE_DELAY=-1 keeps it JVM-alive.
    public static final String DB_URL = "jdbc:h2:mem:h2_datastore_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    @BeforeAll
    public static void setup() {
        H2PDSCreator creator = new H2PDSCreator();
        APIConfigInfo configInfo = creator.toAPIConfigInfo(DB_URL);
        System.out.println("Config\n" + GSONUtil.toJSONDefault(configInfo, true));

        h2DataStore = new H2PDataStore();
        h2DataStore.setAPIConfigInfo(configInfo);
        h2DataStore.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        OPSecUtil.singleton();
        cdst = new CommonDataStoreTest<>(h2DataStore);
    }

    @Test
    public void testURLConfig() {
        // Single-URL configuration: no MODE/HOST/PORT/DB_NAME needed.
        H2PDSCreator creator = new H2PDSCreator();
        APIConfigInfo cfg = creator.toAPIConfigInfo("jdbc:h2:mem:h2_url_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        H2PDataStore ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        try {
            PropertyDAO pd = createPropertyDAO("url-cfg-" + UUID.randomUUID(), "url config", 5);
            ds.insert(pd);
            PropertyDAO read = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
            assertEquals(pd.getGUID(), read.getGUID());
            System.out.println("URL-config store OK: " + H2PDSCreator.H2PParam.dataStoreURI(cfg));
        } finally {
            ds.close();
        }
    }

    @Test
    public void testCompatibilityModeConfig() {
        // Component-based config: default TYPE=mem, default MODE=PostgreSQL.
        H2PDSCreator creator = new H2PDSCreator();
        APIConfigInfo cfg = creator.createEmptyConfigInfo();
        cfg.getProperties().build(H2PDSCreator.H2PParam.DB_NAME.getName(), "h2_pgmode_test");
        String uri = H2PDSCreator.H2PParam.dataStoreURI(cfg);
        assertTrue(uri.contains("MODE=PostgreSQL"), "default compatibility mode must be PostgreSQL: " + uri);

        H2PDataStore ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        try {
            PropertyDAO pd = createPropertyDAO("pg-mode-" + UUID.randomUUID(), "pg mode", 3);
            pd.getProperties().build(new NVLong("big", 9_000_000_000L));
            ds.insert(pd);
            PropertyDAO read = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
            assertEquals(pd.getGUID(), read.getGUID());
            assertEquals(Long.valueOf(9_000_000_000L), read.getProperties().getValue("big"));
            System.out.println("PostgreSQL-mode store OK: " + uri);
        } finally {
            ds.close();
        }
    }

    @Test
    public void testDSTypeAndDialectWiring() {
        // Live H2 store (default config) reports H2 with a varchar schemaless dialect.
        assertEquals(DSType.H2, h2DataStore.getDSType());
        assertEquals("varchar", H2PDialect.forDSType(DSType.H2).schemalessColumnType());

        H2PDSCreator creator = new H2PDSCreator();

        // Postgres via a full jdbc:postgresql URL.
        APIConfigInfo pgUrl = creator.toAPIConfigInfo("jdbc:postgresql://localhost:5432/mydb");
        assertEquals(DSType.POSTGRES, H2PDSCreator.resolveDSType(pgUrl));
        assertTrue(H2PDSCreator.H2PParam.dataStoreURI(pgUrl).startsWith("jdbc:postgresql://"));
        assertEquals("jsonb", H2PDialect.forDSType(DSType.POSTGRES).schemalessColumnType());

        // Postgres via driver + component config -> jdbc:postgresql URL with NO H2-only settings.
        APIConfigInfo pgComp = creator.createEmptyConfigInfo();
        pgComp.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");
        pgComp.getProperties().build(H2PDSCreator.H2PParam.HOST.getName(), "db.example.com");
        pgComp.getProperties().build(new NVInt(H2PDSCreator.H2PParam.PORT.getName(), 5432));
        pgComp.getProperties().build(H2PDSCreator.H2PParam.DB_NAME.getName(), "app");
        String pgUri = H2PDSCreator.H2PParam.dataStoreURI(pgComp);
        assertEquals("jdbc:postgresql://db.example.com:5432/app", pgUri);
        assertFalse(pgUri.contains("MODE="));
        assertFalse(pgUri.contains("DB_CLOSE_DELAY"));
        assertEquals(DSType.POSTGRES, H2PDSCreator.resolveDSType(pgComp));

        // Default H2 component config -> jdbc:h2 + MODE=PostgreSQL, resolves to H2.
        APIConfigInfo h2 = creator.createEmptyConfigInfo();
        String h2Uri = H2PDSCreator.H2PParam.dataStoreURI(h2);
        assertTrue(h2Uri.startsWith("jdbc:h2:"));
        assertTrue(h2Uri.contains("MODE=PostgreSQL"));
        assertEquals(DSType.H2, H2PDSCreator.resolveDSType(h2));

        System.out.println("DSType/dialect wiring OK: H2=varchar, POSTGRES=jsonb");
    }

    @Test
    public void connectTest() {
        Connection conn = h2DataStore.connect();
        System.out.println("H2 connection created: " + conn);
        System.out.println(h2DataStore.getStoreTables());
    }

    @Test
    public void testRangeInt() {
        Range<Integer> intRange = new Range<>(1, 1000);
        intRange.setName("INT_RANGE");
        h2DataStore.insert(intRange);
        System.out.println(intRange.getReferenceID() + " " + intRange.getGUID());

        Range<Integer> rIntRange = (Range<Integer>) h2DataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);
        System.out.println(rIntRange + " " + rIntRange.getStart().getClass() + " " + rIntRange.getEnd().getClass());

        cdst.rc.reset();
        cdst.rc.start();
        int length = 1000;
        for (int i = 0; i < length; i++) {
            rIntRange = (Range<Integer>) h2DataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);
            assert rIntRange != null;
        }
        cdst.rc.stop(length);
        System.out.println(cdst.rc);
    }

    @Test
    public void testGetAllRange() {
        cdst.rc.reset();
        cdst.rc.start();
        List<Range<Integer>> all = h2DataStore.userSearch(null, Range.NVC_RANGE, null);
        cdst.rc.stop(all.size());
        System.out.println(cdst.rc);

        List<String> idsList = new ArrayList<>();
        for (Range<Integer> r : all) {
            idsList.add(r.getGUID());
        }
        cdst.rc.reset();
        cdst.rc.start();
        all = h2DataStore.searchByID(Range.class.getName(), idsList.toArray(new String[0]));
        cdst.rc.stop(all.size());
        System.out.println(cdst.rc);
        for (Range r : all)
            System.out.println(SUS.toCanonicalID(',', r.getReferenceID(), r.getGUID()));
    }

    @Test
    public void insertNVGenericMapTest() {
        cdst.rc.reset();
        int length = 100;
        cdst.rc.start();
        for (int i = 0; i < length; i++) {
            h2DataStore.insert(createPropertyDAO("name " + i, "desc " + i, i));
        }
        cdst.rc.stop(length);
        System.out.println("insert " + cdst.rc);

        cdst.rc.reset().start();
        List<PropertyDAO> all = h2DataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all " + all.size() + " " + cdst.rc);
        for (PropertyDAO pd : all) {
            System.out.println(pd);
        }
    }

    @Test
    public void testGetALLNVGenericMap() {
        cdst.rc.reset().start();
        List<PropertyDAO> all = h2DataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all once " + all.size() + " " + cdst.rc);
        cdst.rc.reset().start();
        PropertyDAO result;
        for (PropertyDAO pd : all) {
            result = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
            assert pd.getGUID().equals(result.getGUID());
        }
        cdst.rc.stop(all.size());
        System.out.println("read all one by one: " + all.size() + " " + cdst.rc);
    }

    public static PropertyDAO createPropertyDAO(String name, String description, int val) {
        PropertyDAO ret = new PropertyDAO();
        ret.setName(name);
        ret.setDescription(description);
        ret.getProperties()
                .build("str", name)
                .build(new NVInt("int_val", val))
                .build(new NVFloat("float", (float) 5.78))
        ;
        return ret;
    }

    @Test
    public void testPassword() throws NoSuchAlgorithmException {
        cdst.testArgonPassword();
        cdst.testBCryptPassword();
        cdst.testUpdatePassword();
    }

    @Test
    public void testHMCI() {
        cdst.testHMCI();
    }

    @Test
    public void testComplicated() {
        cdst.insertAllType();
        cdst.insertComplexType();
        cdst.insertComplexType();
    }

    @Test
    public void testNVGenericMapList() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("nvgml_test");
        pd.setDescription("NVGenericMapList round-trip test");

        NVGenericMap map1 = new NVGenericMap("entry1");
        map1.build("key1", "value1");
        map1.build(new NVInt("num1", 42));

        NVGenericMap map2 = new NVGenericMap("entry2");
        map2.build("key2", "value2");
        map2.build(new NVFloat("num2", 3.14f));

        NVGenericMapList nvgml = new NVGenericMapList("map_list");
        nvgml.add(map1);
        nvgml.add(map2);

        pd.getProperties()
                .build("label", "test")
                .build(nvgml);

        h2DataStore.insert(pd);
        System.out.println("Inserted NVGenericMapList test: " + pd.getGUID());

        PropertyDAO result = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);

        NVGenericMapList resultList = result.getProperties().getNV("map_list");
        assert resultList != null : "NVGenericMapList should not be null";
        assert resultList.getValue().size() == 2 : "Expected 2 maps, got " + resultList.getValue().size();
        assert "value1".equals(resultList.getValue().get(0).getValue("key1")) : "key1 mismatch";
        assert "value2".equals(resultList.getValue().get(1).getValue("key2")) : "key2 mismatch";

        System.out.println("NVGenericMapList round-trip OK: " + GSONUtil.toJSONDefault(result.getProperties(), true));
    }

    @Test
    public void testNVGenericMapListUpdate() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("nvgml_update_test");
        pd.setDescription("NVGenericMapList update test");

        NVGenericMap map1 = new NVGenericMap("entry1");
        map1.build("original", "data");

        NVGenericMapList nvgml = new NVGenericMapList("map_list");
        nvgml.add(map1);

        pd.getProperties().build(nvgml);
        h2DataStore.insert(pd);

        // Update: add a second map to the list
        NVGenericMap map2 = new NVGenericMap("entry2");
        map2.build("added", "later");

        NVGenericMapList currentList = pd.getProperties().getNV("map_list");
        currentList.add(map2);

        h2DataStore.update(pd);

        // Read back and verify. Schemaless data is stored as JSON, so fidelity is asserted at the
        // JSON level (re-serializing the read-back value yields the same JSON as the original).
        PropertyDAO result = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        assertEquals(GSONUtil.toJSONDefault(pd.getProperties()), GSONUtil.toJSONDefault(result.getProperties()),
                "NVGenericMapList update must round-trip (JSON-stable)");
        System.out.println("NVGenericMapList update OK: " + GSONUtil.toJSONDefault(result.getProperties(), true));
    }

    @Test
    public void testNVGenericMapDiverseTypes() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("diverse_types_test");
        pd.setDescription("NVGenericMap diverse type round-trip");

        NVGenericMap nested = new NVGenericMap("nested_map");
        nested.build("nested_key", "nested_value");
        nested.build(new NVInt("nested_int", 100));

        NVGenericMap listEntry = new NVGenericMap("le");
        listEntry.build("le_key", "le_value");
        NVGenericMapList nvgml = new NVGenericMapList("generic_map_list");
        nvgml.add(listEntry);

        NVStringList strList = new NVStringList("string_list", "alpha", "beta", "gamma");

        pd.getProperties()
                .build("str_val", "hello")
                .build(new NVInt("int_val", 42))
                .build(new NVLong("long_val", 123456789L))
                .build(new NVDouble("double_val", 2.718))
                .build(new NVFloat("float_val", 1.5f))
                .build(nested)
                .build(nvgml)
                .build(strList);

        h2DataStore.insert(pd);

        PropertyDAO result = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        // Schemaless NVGenericMap is stored as JSON; assert a JSON-stable round-trip across every
        // diverse type (string, int, long, double, float, nested map, map-list, string-list).
        assertEquals(GSONUtil.toJSONDefault(pd.getProperties()), GSONUtil.toJSONDefault(result.getProperties()),
                "diverse NVGenericMap must round-trip (JSON-stable)");
        System.out.println("Diverse types OK: " + GSONUtil.toJSONDefault(result.getProperties(), true));
    }

    @Test
    public void deleteTest() {
        PropertyDAO pd = createPropertyDAO("delete-me-" + UUID.randomUUID(), "to be deleted", 9);
        h2DataStore.insert(pd);

        PropertyDAO result = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        System.out.println("To be deleted " + result.getGUID());

        h2DataStore.delete(result, false);
        PropertyDAO gone = h2DataStore.lookupByReferenceID(result.getNVConfig().getName(), pd.getGUID());
        assertNull(gone, "row must be gone after delete");
    }

    /**
     * ReservedID storage invariant at the API level: subject_guid persists as a UUID
     * and round-trips as its IDGs.UUIDV7 string encoding — on insert, entity read-back,
     * and query. (The raw-column-type assertion from the Mongo suite is storage-specific
     * and therefore omitted here.)
     */
    @Test
    public void testReservedIDUUIDInvariant() {
        String subjectGUID = IDGs.UUIDV7.genID();
        PropertyDAO pd = createPropertyDAO("uuid-invariant-" + UUID.randomUUID(), "reserved-id storage invariant", 7);
        pd.setSubjectGUID(subjectGUID);
        h2DataStore.insert(pd);

        // Entity round-trip: the java side must carry the UUIDV7 string encoding.
        PropertyDAO read = (PropertyDAO) h2DataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        assertEquals(pd.getGUID(), read.getGUID());
        assertEquals(subjectGUID, read.getSubjectGUID());

        // Query round-trip: a string subject_guid criterion must decode to UUID and match.
        List<PropertyDAO> found = h2DataStore.search(PropertyDAO.class.getName(), null,
                new QueryMatchString(MetaToken.SUBJECT_GUID, subjectGUID, RelationalOperator.EQUAL));
        assertFalse(found.isEmpty(), "query by subject_guid must match the UUID-stored field");
        assertEquals(pd.getGUID(), found.get(0).getGUID());
        System.out.println("reserved-id UUID invariant OK: " + pd.getGUID() + " / " + subjectGUID);
    }

    /**
     * Reference resolution: a single non-embedded NVEntityReference plus an
     * NVEntityReferenceList must all resolve on read-back with matching GUIDs.
     */
    @Test
    public void testEntityReferenceRoundTrip() {
        DSConst.ComplexTypes ct = DSConst.ComplexTypes.buildComplex("ref-round-trip-" + UUID.randomUUID());
        ct.setAllTypes(DSConst.AllTypes.autoBuilder());
        h2DataStore.insert(ct);

        DSConst.ComplexTypes read = (DSConst.ComplexTypes) h2DataStore
                .searchByID(DSConst.ComplexTypes.class.getName(), ct.getGUID()).get(0);

        assertNotNull(read.getAllTypes(), "non-embedded NVEntityReference must resolve on read");
        assertEquals(ct.getAllTypes().getGUID(), read.getAllTypes().getGUID());

        NVEntity[] originalRefs = ((NVEntityReferenceList) ct.lookup("array_of_all_types")).values();
        NVEntity[] readRefs = ((NVEntityReferenceList) read.lookup("array_of_all_types")).values();
        assertEquals(originalRefs.length, readRefs.length, "every referenced entity must resolve on read");
        for (NVEntity original : originalRefs) {
            boolean matched = false;
            for (NVEntity r : readRefs) {
                if (original.getGUID().equals(r.getGUID())) {
                    matched = true;
                    break;
                }
            }
            assert matched : "referenced entity " + original.getGUID() + " must be resolved";
        }
        System.out.println("entity reference round-trip OK: " + ct.getGUID()
                + " single=" + read.getAllTypes().getGUID() + " list=" + readRefs.length);
    }

    @Test
    public void testTransactionCommit() {
        PropertyDAO a = createPropertyDAO("tx-commit-a-" + UUID.randomUUID(), "a", 1);
        PropertyDAO b = createPropertyDAO("tx-commit-b-" + UUID.randomUUID(), "b", 2);

        h2DataStore.beginTransaction();
        try {
            h2DataStore.insert(a);
            h2DataStore.insert(b);
            // Read-your-own-writes: uncommitted rows are visible on the same (ambient) connection.
            assertFalse(h2DataStore.searchByID(PropertyDAO.class.getName(), a.getGUID()).isEmpty(),
                    "insert should be visible inside the transaction");
            h2DataStore.endTransaction(); // commit
        } catch (RuntimeException e) {
            h2DataStore.abortTransaction();
            throw e;
        }

        assertFalse(h2DataStore.searchByID(PropertyDAO.class.getName(), a.getGUID()).isEmpty(),
                "a should persist after commit");
        assertFalse(h2DataStore.searchByID(PropertyDAO.class.getName(), b.getGUID()).isEmpty(),
                "b should persist after commit");
        System.out.println("tx commit OK: " + a.getGUID() + ", " + b.getGUID());
    }

    @Test
    public void testTransactionRollback() {
        PropertyDAO a = createPropertyDAO("tx-rollback-a-" + UUID.randomUUID(), "a", 1);
        PropertyDAO b = createPropertyDAO("tx-rollback-b-" + UUID.randomUUID(), "b", 2);

        h2DataStore.beginTransaction();
        try {
            h2DataStore.insert(a);
            h2DataStore.insert(b);
            assertFalse(h2DataStore.searchByID(PropertyDAO.class.getName(), a.getGUID()).isEmpty(),
                    "insert should be visible inside the transaction");
        } finally {
            h2DataStore.abortTransaction(); // rollback
        }

        // After rollback neither row persists (fresh reads, no active transaction).
        assertTrue(h2DataStore.searchByID(PropertyDAO.class.getName(), a.getGUID()).isEmpty(),
                "a must not persist after rollback");
        assertTrue(h2DataStore.searchByID(PropertyDAO.class.getName(), b.getGUID()).isEmpty(),
                "b must not persist after rollback");
        System.out.println("tx rollback OK: neither " + a.getGUID() + " nor " + b.getGUID() + " persisted");
    }

    @Test
    public void testDynamicEnumMap() {
        String rawName = "colors-" + UUID.randomUUID();
        DynamicEnumMap dem = new DynamicEnumMap(rawName, new NVPair("RED", "red"), new NVPair("GREEN", "green"));
        h2DataStore.insertDynamicEnumMap(dem); // INSERT branch of the portable upsert

        // DEMs are keyed by dem.getName() (canonical-prefixed), matching the Mongo store's contract.
        DynamicEnumMap read = h2DataStore.searchDynamicEnumMapByName(dem.getName());
        assertNotNull(read, "DEM must round-trip");
        assertEquals(dem.getName(), read.getName());
        assertEquals(2, read.size());

        // Same key, more entries -> exercises the UPDATE branch of the portable upsert.
        DynamicEnumMap dem2 = new DynamicEnumMap(rawName,
                new NVPair("RED", "red"), new NVPair("GREEN", "green"), new NVPair("BLUE", "blue"));
        assertEquals(dem.getName(), dem2.getName());
        h2DataStore.updateDynamicEnumMap(dem2);
        read = h2DataStore.searchDynamicEnumMapByName(dem2.getName());
        assertEquals(3, read.size(), "DEM update must persist via portable upsert");
        System.out.println("DynamicEnumMap round-trip OK: " + read.size() + " entries");
    }

    @Test
    public void testNamedValue() {
        HTTPAuthorization auth = HTTPAuthorization.createGeneric("XlogistX-KEY", null, "SECRET-TOKEN-123");
        NamedValue<String> token = auth.lookup(HTTPAuthorization.NVC_TOKEN.getName());
        token.getProperties().build("scope", "read").build(new NVInt("ttl", 3600));
        auth = h2DataStore.insert(auth);

        HTTPAuthorization read = (HTTPAuthorization) h2DataStore
                .searchByID(HTTPAuthorization.class.getName(), auth.getGUID()).get(0);
        NamedValue<String> rToken = read.lookup(HTTPAuthorization.NVC_TOKEN.getName());
        assertNotNull(rToken, "NamedValue token must round-trip");
        assertEquals("SECRET-TOKEN-123", rToken.getValue(), "NamedValue value mismatch");
        assertEquals("read", rToken.getProperties().getValue("scope"), "NamedValue property (string) mismatch");
        assertEquals(Integer.valueOf(3600), rToken.getProperties().getValue("ttl"), "NamedValue property (int) mismatch");
        System.out.println("NamedValue round-trip OK: value + " + rToken.getProperties().size() + " properties");
    }

    @Test
    public void testNVBlob() {
        DSConst.AllTypes at = DSConst.AllTypes.autoBuilder();
        byte[] original = at.getBytes();
        assertNotNull(original, "auto-built AllTypes must carry a blob");
        assertEquals(64, original.length);

        h2DataStore.insert(at);
        DSConst.AllTypes read = (DSConst.AllTypes) h2DataStore
                .searchByID(DSConst.AllTypes.class.getName(), at.getGUID()).get(0);

        assertArrayEquals(original, read.getBytes(), "NVBlob bytes must round-trip exactly");
        System.out.println("NVBlob round-trip OK: " + original.length + " bytes");
    }

    /**
     * Encrypted H2 <b>file</b> DB round-trip: the cipher lives in the URL ({@code ;CIPHER=AES}) while
     * the secrets (user / password / file-password) are supplied separately through the 4-arg factory.
     * Verifies (1) {@code dataStorePassword} emits H2's {@code "<filePwd> <userPwd>"} form, (2) CRUD
     * works over the encrypted file, (3) the data persists and re-decrypts when the DB is reopened by a
     * fresh store, and (4) a wrong file password is rejected — proving the file is genuinely encrypted.
     *
     * <p>Note: this URL intentionally omits {@code DB_CLOSE_DELAY=-1} so the file DB actually closes
     * between store instances and the file password is re-validated on reopen.
     */
    @Test
    public void testEncryptedH2FileRoundTrip() {
        String dbPath = new File(System.getProperty("java.io.tmpdir"), "h2p_enc_" + UUID.randomUUID())
                .getAbsolutePath().replace('\\', '/');
        String url = "jdbc:h2:file:" + dbPath + ";CIPHER=AES;MODE=PostgreSQL";
        String user = "sa", pwd = "userPass", filePwd = "encPass";

        H2PDSCreator creator = new H2PDSCreator();
        try {
            // Cipher in the URL, secrets passed separately -> "<filePwd> <userPwd>" connection password.
            APIConfigInfo cfg = creator.toAPIConfigInfo(url, user, pwd, filePwd);
            assertEquals("encPass userPass", H2PDSCreator.H2PParam.dataStorePassword(cfg),
                    "encrypted H2 must combine file + user password");
            assertEquals(DSType.H2, H2PDSCreator.resolveDSType(cfg));

            // Phase 1: create the encrypted file, insert, read back within the same store.
            String guid;
            H2PDataStore ds1 = newStore(cfg);
            try {
                PropertyDAO pd = createPropertyDAO("enc-" + UUID.randomUUID(), "encrypted file db", 11);
                pd.getProperties().build("secret", "classified");
                ds1.insert(pd);
                guid = pd.getGUID();
                PropertyDAO read = (PropertyDAO) ds1.searchByID(PropertyDAO.class.getName(), guid).get(0);
                assertEquals(guid, read.getGUID());
                assertEquals("classified", read.getProperties().getValue("secret"));
            } finally {
                ds1.close();
            }

            // Phase 2: reopen with the same credentials -> the persisted, encrypted row re-decrypts.
            H2PDataStore ds2 = newStore(creator.toAPIConfigInfo(url, user, pwd, filePwd));
            try {
                PropertyDAO read = (PropertyDAO) ds2.searchByID(PropertyDAO.class.getName(), guid).get(0);
                assertEquals(guid, read.getGUID(), "encrypted file DB must persist across reopen");
                assertEquals("classified", read.getProperties().getValue("secret"));
            } finally {
                ds2.close();
            }

            // Phase 3: wrong file password must be rejected -> the file is genuinely encrypted.
            H2PDataStore ds3 = newStore(creator.toAPIConfigInfo(url, user, pwd, "wrongEnc"));
            try {
                assertThrows(Exception.class, ds3::connect,
                        "a wrong file password must fail to open the encrypted DB");
            } finally {
                ds3.close();
            }
            System.out.println("encrypted H2 file round-trip OK: " + dbPath);
            System.out.println(GSONUtil.toJSONDefault(cfg, true));
        } finally {
            deleteH2Files(dbPath);
        }
    }

    /**
     * {@link H2PUtil#parseJdbcURL} across the shapes this datastore uses: H2 mem/file/tcp and a bare
     * H2 path, PostgreSQL host/port/db (+ query options and multi-host), and the invalid-input guard.
     * Structural fields land at the top level; {@code ;}- or {@code ?&}-delimited settings go into a
     * nested {@code params} map, with {@code port} typed as an integer.
     */
    @Test
    public void testParseJdbcURL() {
        // H2 in-memory: type + database + ';' settings; no host/port.
        NVGenericMap mem = H2PUtil.parseJdbcURL("jdbc:h2:mem:h2_datastore_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        assertEquals("h2", mem.getValue(H2PUtil.JDBC_SUBPROTOCOL));
        assertEquals("mem", mem.getValue(H2PUtil.JDBC_TYPE));
        assertEquals("h2_datastore_test", mem.getValue(H2PUtil.JDBC_DATABASE));
        assertNull(mem.getValue(H2PUtil.JDBC_HOST), "mem URL has no host");
        NVGenericMap memParams = mem.getNV(H2PUtil.JDBC_PARAMS);
        assertNotNull(memParams);
        assertEquals("-1", memParams.getValue("DB_CLOSE_DELAY"));
        assertEquals("PostgreSQL", memParams.getValue("MODE"));

        // H2 file (encrypted): the file path is preserved verbatim (Windows drive-letter and all).
        NVGenericMap file = H2PUtil.parseJdbcURL("jdbc:h2:file:C:/data/secure;CIPHER=AES;MODE=PostgreSQL");
        assertEquals("file", file.getValue(H2PUtil.JDBC_TYPE));
        assertEquals("C:/data/secure", file.getValue(H2PUtil.JDBC_PATH));
        assertEquals("AES", ((NVGenericMap) file.getNV(H2PUtil.JDBC_PARAMS)).getValue("CIPHER"));

        // H2 tcp: network authority -> host + integer port + database.
        NVGenericMap tcp = H2PUtil.parseJdbcURL("jdbc:h2:tcp://localhost:9092/mydb;MODE=PostgreSQL");
        assertEquals("tcp", tcp.getValue(H2PUtil.JDBC_TYPE));
        assertEquals("localhost", tcp.getValue(H2PUtil.JDBC_HOST));
        assertEquals(Integer.valueOf(9092), tcp.getValue(H2PUtil.JDBC_PORT));
        assertEquals("mydb", tcp.getValue(H2PUtil.JDBC_DATABASE));

        // Bare H2 path form: just a path, no type/host/db.
        NVGenericMap bare = H2PUtil.parseJdbcURL("jdbc:h2:~/test");
        assertEquals("~/test", bare.getValue(H2PUtil.JDBC_PATH));
        assertNull(bare.getValue(H2PUtil.JDBC_TYPE));

        // PostgreSQL host/port/db, no settings.
        NVGenericMap pg = H2PUtil.parseJdbcURL("jdbc:postgresql://db.example.com:5432/app");
        assertEquals("postgresql", pg.getValue(H2PUtil.JDBC_SUBPROTOCOL));
        assertEquals("db.example.com", pg.getValue(H2PUtil.JDBC_HOST));
        assertEquals(Integer.valueOf(5432), pg.getValue(H2PUtil.JDBC_PORT));
        assertEquals("app", pg.getValue(H2PUtil.JDBC_DATABASE));
        assertNull(pg.getNV(H2PUtil.JDBC_PARAMS), "no '?' options -> no params map");

        // PostgreSQL query-style options + multi-host authority (kept raw, no single port split).
        NVGenericMap pgOpts = H2PUtil.parseJdbcURL("jdbc:postgresql://h1:5432,h2:5433/app?ssl=true&applicationName=x");
        assertEquals("h1:5432,h2:5433", pgOpts.getValue(H2PUtil.JDBC_HOST));
        assertNull(pgOpts.getValue(H2PUtil.JDBC_PORT), "multi-host authority must not split a single port");
        NVGenericMap pgParams = pgOpts.getNV(H2PUtil.JDBC_PARAMS);
        assertEquals("true", pgParams.getValue("ssl"));
        assertEquals("x", pgParams.getValue("applicationName"));

        // db-only PostgreSQL form.
        NVGenericMap pgDbOnly = H2PUtil.parseJdbcURL("jdbc:postgresql:mydb");
        assertEquals("mydb", pgDbOnly.getValue(H2PUtil.JDBC_DATABASE));
        assertNull(pgDbOnly.getValue(H2PUtil.JDBC_HOST));

        // Invalid input: null and non-jdbc URLs are rejected.
        assertThrows(IllegalArgumentException.class, () -> H2PUtil.parseJdbcURL(null));
        assertThrows(IllegalArgumentException.class, () -> H2PUtil.parseJdbcURL("h2:mem:x"));
        System.out.println(GSONUtil.toJSONDefault(file, true));
        System.out.println(GSONUtil.toJSONDefault(tcp, true));
        System.out.println("parseJdbcURL OK: mem/file/tcp/bare + postgres host+opts+multihost+db-only + guards");
    }

    private static H2PDataStore newStore(APIConfigInfo cfg) {
        H2PDataStore ds = new H2PDataStore();
        ds.setAPIConfigInfo(cfg);
        ds.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        return ds;
    }

    /** Remove the on-disk artifacts H2 creates for a file DB (mv/trace/lock). */
    private static void deleteH2Files(String dbPath) {
        for (String suffix : new String[]{".mv.db", ".trace.db", ".lock.db"}) {
            File f = new File(dbPath + suffix);
            if (f.exists() && !f.delete()) {
                f.deleteOnExit();
            }
        }
    }

    @Test
    public void testHTTPAuthorization() {
        HTTPAuthorization authorization = HTTPAuthorization.createGeneric("batata-auth", "no-bearer", UUID.randomUUID().toString());
        authorization = h2DataStore.insert(authorization);
        System.out.println("authentication " + GSONUtil.toJSONDefault(authorization, true));
        authorization = (HTTPAuthorization) h2DataStore.searchByID(HTTPAuthorization.NVC_HTTP_AUTHORIZATION, authorization.getGUID()).get(0);
        System.out.println("authentication " + GSONUtil.toJSONDefault(authorization, true));

        authorization = HTTPAuthorization.createBasic("mario", "password");
        authorization = h2DataStore.insert(authorization);
        authorization = (HTTPAuthorization) h2DataStore.searchByID(HTTPAuthorization.NVC_HTTP_AUTHORIZATION, authorization.getGUID()).get(0);
        System.out.println("authentication " + GSONUtil.toJSONDefault(authorization, true));
    }
}
