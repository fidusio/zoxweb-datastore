package io.xlogistx.datastore.test;

import io.xlogistx.datastore.XlogistxMongoDataStore;
import org.junit.jupiter.api.Assumptions;

/**
 * Shared helpers for the live-Mongo test suites.
 */
public final class MongoTestUtil {

    private MongoTestUtil() {
    }

    /**
     * Skip the calling suite (JUnit {@link Assumptions}) when the configured MongoDB deployment is
     * not reachable — instead of failing every test with connection errors. The default test URL
     * carries {@code serverSelectionTimeoutMS=3000} so the probe fails fast.
     */
    public static void assumeMongoAvailable(XlogistxMongoDataStore ds, String url) {
        try {
            ds.ping(false);
        } catch (Throwable t) {
            Assumptions.assumeTrue(false,
                    "MongoDB not reachable at " + url + " — skipping suite (" + t + ")");
        }
    }
}
