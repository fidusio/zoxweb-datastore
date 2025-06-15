package mongo.sync.test;

import org.zoxweb.server.ds.mongo.sync.SyncMongoDS;
import org.zoxweb.server.ds.mongo.sync.SyncMongoDSCreator;
import org.zoxweb.server.ds.mongo.sync.SyncMongoDSCreator.MongoParam;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.util.NVInt;

public class TestUtil {

    public static SyncMongoDS crateDataStore(String dataStoreName, String mongoDBHost, int mongoDBPort)
    {
        SyncMongoDSCreator smdsc = new SyncMongoDSCreator();
        APIConfigInfo apici = smdsc.createEmptyConfigInfo();
        apici.getProperties().build(MongoParam.DB_NAME, dataStoreName)
                .build(MongoParam.HOST, mongoDBHost)
                .build(new NVInt(MongoParam.PORT, mongoDBPort));

        SyncMongoDS ret = new SyncMongoDS();
        ret.setAPIConfigInfo(apici);
        return ret;
    }
}
