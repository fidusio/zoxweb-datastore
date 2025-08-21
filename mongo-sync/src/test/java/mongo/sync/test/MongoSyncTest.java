package mongo.sync.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.datastore.test.CommonDataStoreTest;
import org.zoxweb.server.ds.mongo.sync.SyncMongoDS;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.RateCounter;
import org.zoxweb.shared.util.SUS;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class MongoSyncTest {

    private static SyncMongoDS mongoDataStore;
    //private final static RateCounter rc = new RateCounter("Test");
    private static CommonDataStoreTest<MongoClient, MongoDatabase> cdst;

    @BeforeAll
    public static void setup() {

        mongoDataStore = TestUtil.crateDataStore("test_local", "localhost", 27017);
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
        Range<Integer> intRange = new Range<Integer>(1, 1000);
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
    public void insertNVGenericMapTest()
    {
        cdst.rc.reset();
        int length = 100;
        cdst.rc.start();
        for(int i = 0; i < length; i++)
        {
            mongoDataStore.insert(createPropertyDAO("name " + i, "desc " + i, i));
        }
        cdst.rc.stop(length);
        System.out.println( "insert " + cdst.rc);

        cdst.rc.reset().start();
        List<PropertyDAO> all = mongoDataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all " + all.size() + " " + cdst.rc);
        for(PropertyDAO pd: all)
        {
            System.out.println(pd);
        }
    }

    @Test
    public void testGetALLNVGenericMap()
    {
        cdst.rc.reset().start();
        List<PropertyDAO> all = mongoDataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        cdst.rc.stop();
        System.out.println("read all once " + all.size() + " " + cdst.rc);
        cdst.rc.reset().start();
        PropertyDAO result;
        for(PropertyDAO pd : all)
        {
             result = (PropertyDAO) mongoDataStore.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
             assert pd.getReferenceID().equals(result.getReferenceID());
        }

        cdst.rc.stop(all.size());
        System.out.println("read all one by one: " + all.size() + " " + cdst.rc);
    }

    public static PropertyDAO createPropertyDAO(String name, String description, int val)
    {
        PropertyDAO ret = new PropertyDAO();
        ret.setName(name);
        ret.setDescription(description);
        ret.getProperties()
                .build("str", name)
                .build(new NVInt("int_val", val))
                .build(new NVFloat("float", (float)5.78))
        ;

        return ret;
    }


    @Test
    public void testPassword() throws NoSuchAlgorithmException {

        cdst.testArgonPassword();
        cdst.testBCryptPassword();
    }

    @Test
    public void testHMCI() {
        cdst.testHMCI();
    }
}
