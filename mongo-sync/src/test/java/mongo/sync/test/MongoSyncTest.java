package mongo.sync.test;

import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.ds.mongo.sync.SyncMongoDS;

public class MongoSyncTest {

    private static SyncMongoDS mongoDataStore;
    @BeforeAll
    public static void setup()
    {
        mongoDataStore = TestUtil.crateDataStore("test_local", "localhost", 27017);
    }

    @Test
    public void connectTest()
    {
        MongoDatabase mongoDB = mongoDataStore.connect();
        System.out.println("MongoDB created: " + mongoDB.getName());
        System.out.println(mongoDataStore.getStoreTables());
    }
}
