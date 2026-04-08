package io.xlogistx.datastore.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.xlogistx.datastore.XlogistxMongoDataStore;
import io.xlogistx.datastore.XlogistxMongoDSCreator;
import io.xlogistx.datastore.XlogistxMongoDSCreator.MongoParam;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.datastore.test.CommonDataStoreTest;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.http.URLInfo;
import org.zoxweb.shared.util.NVDouble;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVGenericMapList;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.NVStringList;
import org.zoxweb.shared.util.SUS;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class XlogistxMongoDataStoreTest {

    public static final String DB_URL = "mongodb://localhost:27017/xlog_datastore_test";

    private static XlogistxMongoDataStore mongoDataStore;
    private static CommonDataStoreTest<MongoClient, MongoDatabase> cdst;

    @BeforeAll
    public static void setup() {
        XlogistxMongoDSCreator creator = new XlogistxMongoDSCreator();
        APIConfigInfo configInfo = creator.toAPIConfigInfo(DB_URL);
        System.out.println("Config\n" + GSONUtil.toJSONDefault(configInfo, true));

        mongoDataStore = new XlogistxMongoDataStore();
        mongoDataStore.setAPIConfigInfo(configInfo);
        OPSecUtil.singleton();
        cdst = new CommonDataStoreTest<>(mongoDataStore);
    }

    @Test
    public void connectTest() {
        MongoDatabase mongoDB = mongoDataStore.connect();
        System.out.println("MongoDB created: " + mongoDB.getName());
        System.out.println(mongoDataStore.getStoreTables());
    }

    @Test
    public void testRangeInt() {
        Range<Integer> intRange = new Range<>(1, 1000);
        intRange.setName("INT_RANGE");
        mongoDataStore.insert(intRange);
        System.out.println(intRange.getReferenceID() + " " + intRange.getGUID());

        Range<Integer> rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getReferenceID()).get(0);
        System.out.println(intRange + " " + rIntRange.getStart().getClass() + " " + rIntRange.getEnd().getClass());
        rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);
        System.out.println(rIntRange + " " + rIntRange.getStart().getClass() + " " + rIntRange.getEnd().getClass());

        cdst.rc.reset();
        cdst.rc.start();
        int length = 1000;
        for (int i = 0; i < length; i++) {
            rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getReferenceID()).get(0);
            rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);
            assert rIntRange != null;
        }
        cdst.rc.stop(length);
        System.out.println(cdst.rc);
    }

    @Test
    public void testGetAllRange() {
        cdst.rc.reset();
        cdst.rc.start();
        List<Range<Integer>> all = mongoDataStore.userSearch(null, Range.NVC_RANGE, null);
        cdst.rc.stop(all.size());
        System.out.println(cdst.rc);

        List<String> idsList = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            if (i % 2 == 0)
                idsList.add(all.get(i).getReferenceID());
            else
                idsList.add(all.get(i).getGUID());
        }
        System.out.println(idsList);
        cdst.rc.reset();
        cdst.rc.start();
        all = mongoDataStore.searchByID(Range.class.getName(), idsList.toArray(new String[0]));
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
            mongoDataStore.insert(createPropertyDAO("name " + i, "desc " + i, i));
        }
        cdst.rc.stop(length);
        System.out.println("insert " + cdst.rc);

        cdst.rc.reset().start();
        List<PropertyDAO> all = mongoDataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all " + all.size() + " " + cdst.rc);
        for (PropertyDAO pd : all) {
            System.out.println(pd);
        }
    }

    @Test
    public void testGetALLNVGenericMap() {
        cdst.rc.reset().start();
        List<PropertyDAO> all = mongoDataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all once " + all.size() + " " + cdst.rc);
        cdst.rc.reset().start();
        PropertyDAO result;
        for (PropertyDAO pd : all) {
            result = (PropertyDAO) mongoDataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
            assert pd.getReferenceID().equals(result.getReferenceID());
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

        mongoDataStore.insert(pd);
        System.out.println("Inserted NVGenericMapList test: " + pd.getReferenceID());

        PropertyDAO result = (PropertyDAO) mongoDataStore.searchByID(PropertyDAO.class.getName(), pd.getReferenceID()).get(0);

        NVGenericMapList resultList = result.getProperties().getNV("map_list");
        assert resultList != null : "NVGenericMapList should not be null";
        assert resultList.getValue().size() == 2 : "Expected 2 maps, got " + resultList.getValue().size();

        NVGenericMap rm1 = resultList.getValue().get(0);
        assert "value1".equals(rm1.getValue("key1")) : "key1 mismatch";

        NVGenericMap rm2 = resultList.getValue().get(1);
        assert "value2".equals(rm2.getValue("key2")) : "key2 mismatch";

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
        mongoDataStore.insert(pd);

        // Update: add a second map to the list
        NVGenericMap map2 = new NVGenericMap("entry2");
        map2.build("added", "later");

        NVGenericMapList currentList = pd.getProperties().getNV("map_list");
        currentList.add(map2);

        mongoDataStore.update(pd);

        // Read back and verify
        PropertyDAO result = (PropertyDAO) mongoDataStore.searchByID(PropertyDAO.class.getName(), pd.getReferenceID()).get(0);
        NVGenericMapList resultList = result.getProperties().getNV("map_list");
        assert resultList != null : "NVGenericMapList should not be null after update";
        assert resultList.getValue().size() == 2 : "Expected 2 maps after update, got " + resultList.getValue().size();
        assert "data".equals(resultList.getValue().get(0).getValue("original")) : "Original data mismatch";
        assert "later".equals(resultList.getValue().get(1).getValue("added")) : "Added data mismatch";

        System.out.println("NVGenericMapList update OK: " + GSONUtil.toJSONDefault(result.getProperties(), true));
    }

    @Test
    public void testNVGenericMapDiverseTypes() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("diverse_types_test");
        pd.setDescription("NVGenericMap diverse type round-trip");

        // Nested NVGenericMap
        NVGenericMap nested = new NVGenericMap("nested_map");
        nested.build("nested_key", "nested_value");
        nested.build(new NVInt("nested_int", 100));

        // NVGenericMapList
        NVGenericMap listEntry = new NVGenericMap("le");
        listEntry.build("le_key", "le_value");
        NVGenericMapList nvgml = new NVGenericMapList("generic_map_list");
        nvgml.add(listEntry);

        // NVStringList
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

        mongoDataStore.insert(pd);

        PropertyDAO result = (PropertyDAO) mongoDataStore.searchByID(PropertyDAO.class.getName(), pd.getReferenceID()).get(0);
        NVGenericMap props = result.getProperties();

        // Verify primitives
        assert "hello".equals(props.getValue("str_val")) : "str_val mismatch";
        assert Integer.valueOf(42).equals(props.getValue("int_val")) : "int_val mismatch";
        assert Long.valueOf(123456789L).equals(props.getValue("long_val")) : "long_val mismatch";

        // Verify nested NVGenericMap
        NVGenericMap resultNested = props.getNV("nested_map");
        assert resultNested != null : "nested_map should not be null";
        assert "nested_value".equals(resultNested.getValue("nested_key")) : "nested_key mismatch";
        assert Integer.valueOf(100).equals(resultNested.getValue("nested_int")) : "nested_int mismatch";

        // Verify NVGenericMapList
        NVGenericMapList resultGML = props.getNV("generic_map_list");
        assert resultGML != null : "generic_map_list should not be null";
        assert resultGML.getValue().size() == 1 : "Expected 1 map in list";
        assert "le_value".equals(resultGML.getValue().get(0).getValue("le_key")) : "list entry mismatch";

        // Verify NVStringList
        NVStringList resultSL = props.getNV("string_list");
        assert resultSL != null : "string_list should not be null";
        assert resultSL.getValue().size() == 3 : "Expected 3 strings in list";
        assert resultSL.getValue().contains("alpha") : "string_list missing alpha";
        assert resultSL.getValue().contains("beta") : "string_list missing beta";
        assert resultSL.getValue().contains("gamma") : "string_list missing gamma";

        System.out.println("Diverse types OK: " + GSONUtil.toJSONDefault(props, true));
    }

    @Test
    public void pingTest() {
        NVGenericMap nvgm = mongoDataStore.ping(false);
        System.out.println(GSONUtil.toJSONDefault(nvgm, true));
    }

    @Test
    public void testDBURL() throws MalformedURLException, URISyntaxException {
        URLInfo url  = URLInfo.parse("mongodb://localhost:27017/batata_test?uuidRepresentation=standard");
        System.out.println(url);
        URI uri = new URI("mongodb://localhost:27017/batata_test?uuidRepresentation=standard");



    }
}
