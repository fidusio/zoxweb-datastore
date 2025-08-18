package mongo.sync.test;

import com.mongodb.client.MongoDatabase;
import io.xlogistx.opsec.OPSecUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.ds.mongo.sync.SyncMongoDS;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.RateCounter;
import org.zoxweb.shared.util.SUS;

import java.util.ArrayList;
import java.util.List;

public class MongoSyncTest {

    private static SyncMongoDS mongoDataStore;
    private final static RateCounter rc = new RateCounter("Test");

    @BeforeAll
    public static void setup() {

        mongoDataStore = TestUtil.crateDataStore("test_local", "localhost", 27017);
        OPSecUtil.singleton();
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


        rc.reset();
        rc.start();
        int length = 1000;
        for (int i = 0; i < length; i++) {
            rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getReferenceID()).get(0);
            rIntRange = (Range<Integer>) mongoDataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);
            assert rIntRange != null;

        }
        rc.stop(length);
        System.out.println(rc);

    }

    @Test
    public void testGetAllRange() {
        rc.reset();
        rc.start();
        List<Range<Integer>> all = mongoDataStore.userSearch(null, Range.NVC_RANGE, null);
        rc.stop(all.size());
        System.out.println(rc);

        List<String> idsList = new ArrayList<>();
        for (int i = 0; i < all.size(); i++) {
            if (i % 2 == 0)
                idsList.add(all.get(i).getReferenceID());
            else
                idsList.add(all.get(i).getGUID());
        }
        System.out.println(idsList);
        rc.reset();
        rc.start();
        all = mongoDataStore.searchByID(Range.class.getName(), idsList.toArray(new String[0]));
        rc.stop(all.size());
        System.out.println(rc);
        for (Range r : all)
            System.out.println(SUS.toCanonicalID(',', r.getReferenceID(), r.getGUID()));
    }


    @Test
    public void insertTest()
    {


        rc.reset();
        int length = 100;
        rc.start();
        for(int i = 0; i < length; i++)
        {
            mongoDataStore.insert(createPropertyDAO("name " + i, "desc " + i, i));
        }
        rc.stop(length);
        System.out.println( "insert " + rc);

        rc.reset().start();
        List<PropertyDAO> all = mongoDataStore.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
        rc.stop();
        System.out.println("read all " + all.size() + " " + rc);
        for(PropertyDAO pd: all)
        {
            System.out.println(pd);
        }




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
    public void testPassword()
    {

        rc.reset();
        rc.start();
        String[] hasherNames = {"bcrypt", "argon2id"};


        for(String hashName : hasherNames) {
            CredentialHasher<CIPassword> passwordHasher = SecUtil.SINGLETON.lookupCredentialHasher(hashName);
            CIPassword ciPassword = passwordHasher.hash("MyPassword!23");
            ciPassword = mongoDataStore.insert(ciPassword);
            CIPassword password = (CIPassword) mongoDataStore.searchByID(CIPassword.class.getName(), ciPassword.getGUID()).get(0);

            System.out.println(SUS.toCanonicalID(',',ciPassword.getName(), ciPassword.getReferenceID(), ciPassword.getGUID()));

            System.out.println(SUS.toCanonicalID(',',password.getName(), password.getReferenceID(), password.getGUID()));
            SecUtil.SINGLETON.validatePassword(password, "MyPassword!23");
        }
        rc.stop(hasherNames.length);



        System.out.println("argonPassword insert: " +rc);


    }
}
