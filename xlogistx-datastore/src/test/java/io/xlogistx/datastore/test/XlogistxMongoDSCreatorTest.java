package io.xlogistx.datastore.test;

import io.xlogistx.datastore.XlogistxMongoDSCreator;
import io.xlogistx.datastore.XlogistxMongoDSCreator.MongoParam;
import org.junit.jupiter.api.Test;
import org.zoxweb.shared.api.APIConfigInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Offline tests for the connection-string building — no MongoDB required.
 */
public class XlogistxMongoDSCreatorTest {

    private final XlogistxMongoDSCreator creator = new XlogistxMongoDSCreator();

    @Test
    public void testUnauthenticatedURIRoundTrip() {
        APIConfigInfo cfg = creator.toAPIConfigInfo("mongodb://localhost:27017/mydb?replicaSet=rs0");
        String uri = MongoParam.dataStoreURI(cfg);
        assertEquals("mongodb://localhost:27017/mydb?uuidRepresentation=standard&replicaSet=rs0", uri);
        assertEquals("mydb", MongoParam.dataStoreName(cfg));
    }

    @Test
    public void testAuthenticatedURIRoundTrip() {
        APIConfigInfo cfg = creator.toAPIConfigInfo("mongodb://admin:secret@localhost:27017/mydb?replicaSet=rs0");
        assertEquals("admin", cfg.getProperties().getValue(MongoParam.USER));
        assertEquals("secret", cfg.getProperties().getValue(MongoParam.PASSWORD));
        String uri = MongoParam.dataStoreURI(cfg);
        assertEquals("mongodb://admin:secret@localhost:27017/mydb?uuidRepresentation=standard&replicaSet=rs0", uri,
                "credentials must survive the round trip (previously silently dropped)");
    }

    @Test
    public void testCredentialsPercentEncoding() {
        // Plain credentials with reserved characters set via config params...
        APIConfigInfo cfg = creator.createEmptyConfigInfo();
        cfg.getProperties().build(MongoParam.USER.getName(), "us er");
        cfg.getProperties().build(MongoParam.PASSWORD.getName(), "p@ss:w/rd%+");
        cfg.getProperties().build(MongoParam.DB_NAME.getName(), "db1");
        String uri = MongoParam.dataStoreURI(cfg);
        // ...must be percent-encoded exactly once in the connection string.
        assertTrue(uri.startsWith("mongodb://us%20er:p%40ss%3Aw%2Frd%25%2B@localhost:27017/db1?"), uri);

        // And an already-encoded URL decodes to the plain values (no double encoding on rebuild).
        APIConfigInfo parsed = creator.toAPIConfigInfo("mongodb://us%20er:p%40ss%3Aw%2Frd%25%2B@h1:27017/db1");
        assertEquals("us er", parsed.getProperties().getValue(MongoParam.USER));
        assertEquals("p@ss:w/rd%+", parsed.getProperties().getValue(MongoParam.PASSWORD));
    }

    @Test
    public void testEmptyDbNameOmittedFromPath() {
        APIConfigInfo cfg = creator.createEmptyConfigInfo();
        cfg.getProperties().build(MongoParam.DB_NAME.getName(), "");
        String uri = MongoParam.dataStoreURI(cfg);
        assertTrue(uri.startsWith("mongodb://localhost:27017/?"), uri);
        assertFalse(uri.contains("null"), "an unset db name must not serialize as 'null': " + uri);
    }
}
