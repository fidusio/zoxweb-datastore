package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDSCreator.H2PParam;
import io.xlogistx.datastore.h2p.H2PDataStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.data.FileInfoDAO;
import org.zoxweb.shared.util.NVGenericMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Versioned file storage ({@code APIDocumentStore}) on in-memory H2 in PostgreSQL mode:
 * create/read round-trips (1 KB and ~3 MB), version bumping on update, reading a specific
 * version, head-pointer rollback (no history rewrite, monotonic version numbers), concurrent
 * last-write-wins updates, the {@code FILE_VERSIONS_MAX} retention cap, cascade delete, and
 * ambient-transaction participation.
 */
public class H2PFileStoreTest {

    public static final String DB_URL = "jdbc:h2:mem:h2p_file_store_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    private static H2PDataStore ds;
    private static final Random RAND = new Random(20260729);

    @BeforeAll
    public static void setup() {
        ds = new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(DB_URL));
    }

    private static FileInfoDAO newFileInfo(String name) {
        FileInfoDAO fid = new FileInfoDAO();
        fid.setFullPathName(name);
        fid.setFileType(FileInfoDAO.FileType.FILE);
        fid.setCreationTime(System.currentTimeMillis());
        return fid;
    }

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        RAND.nextBytes(b);
        return b;
    }

    private static byte[] readBack(H2PDataStore store, FileInfoDAO fid) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        store.readFile(fid, bos, true);
        return bos.toByteArray();
    }

    private static byte[] readBackVersion(H2PDataStore store, FileInfoDAO fid, long version) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        store.readFile(fid, version, bos, true);
        return bos.toByteArray();
    }

    private static long version(NVGenericMap nvm) {
        return nvm.getValue("version");
    }

    private static boolean isCurrent(NVGenericMap nvm) {
        return nvm.getValue("current");
    }

    @Test
    public void testRoundTripSmallAndLarge() throws IOException {
        for (int size : new int[]{1024, 3 * 1024 * 1024}) {
            byte[] content = randomBytes(size);
            FileInfoDAO fid = newFileInfo("rt_" + size + "_" + UUID.randomUUID());
            ds.createFile(null, fid, new ByteArrayInputStream(content), true);

            assertNotNull(fid.getGUID(), "createFile must assign a GUID");
            assertEquals(size, fid.getLength(), "length must be set from the stream");
            assertArrayEquals(content, readBack(ds, fid), "read-back must match (" + size + " bytes)");

            // The metadata is a regular entity row, readable through the normal search path.
            FileInfoDAO meta = (FileInfoDAO) ds.searchByID(FileInfoDAO.class.getName(), fid.getGUID()).get(0);
            assertEquals(fid.getName(), meta.getName());
            assertEquals(size, meta.getLength());

            List<NVGenericMap> versions = ds.fileVersions(fid);
            assertEquals(1, versions.size());
            assertEquals(1, version(versions.get(0)));
            assertTrue(isCurrent(versions.get(0)), "the single version must be current");
        }
    }

    @Test
    public void testUpdateBumpsVersionAndReadSpecific() throws IOException {
        byte[] v1 = randomBytes(2048), v2 = randomBytes(4096), v3 = randomBytes(1024);
        FileInfoDAO fid = newFileInfo("versions_" + UUID.randomUUID());
        ds.createFile(null, fid, new ByteArrayInputStream(v1), true);
        ds.updateFile(fid, new ByteArrayInputStream(v2), true);
        ds.updateFile(fid, new ByteArrayInputStream(v3), true);

        assertArrayEquals(v3, readBack(ds, fid), "head read must return the latest content");
        assertEquals(v3.length, fid.getLength());
        assertArrayEquals(v1, readBackVersion(ds, fid, 1));
        assertArrayEquals(v2, readBackVersion(ds, fid, 2));

        List<NVGenericMap> versions = ds.fileVersions(fid); // newest first
        assertEquals(3, versions.size());
        assertEquals(3, version(versions.get(0)));
        assertTrue(isCurrent(versions.get(0)));
        assertFalse(isCurrent(versions.get(1)));
        assertFalse(isCurrent(versions.get(2)));

        assertThrows(APIException.class, () -> readBackVersion(ds, fid, 42), "unknown version must throw");
    }

    @Test
    public void testRollbackMovesHeadWithoutRewritingHistory() throws IOException {
        byte[] v1 = randomBytes(1500), v2 = randomBytes(2500);
        FileInfoDAO fid = newFileInfo("rollback_" + UUID.randomUUID());
        ds.createFile(null, fid, new ByteArrayInputStream(v1), true);
        ds.updateFile(fid, new ByteArrayInputStream(v2), true);

        ds.rollbackFile(fid, 1);
        assertArrayEquals(v1, readBack(ds, fid), "after rollback the head read must return v1 content");
        assertEquals(v1.length, fid.getLength(), "rollback must restore the metadata length");

        List<NVGenericMap> versions = ds.fileVersions(fid);
        assertEquals(2, versions.size(), "rollback must not add or remove versions");
        assertTrue(isCurrent(versions.get(1)), "v1 must be current after the rollback");
        assertFalse(isCurrent(versions.get(0)));

        // A later update continues above the highest stored version — numbers are never reused.
        byte[] v3 = randomBytes(3000);
        ds.updateFile(fid, new ByteArrayInputStream(v3), true);
        assertArrayEquals(v3, readBack(ds, fid));
        List<NVGenericMap> after = ds.fileVersions(fid);
        assertEquals(3, after.size());
        assertEquals(3, version(after.get(0)), "post-rollback update must become version 3 (monotonic)");
        assertTrue(isCurrent(after.get(0)));

        assertThrows(APIException.class, () -> ds.rollbackFile(fid, 99), "rollback to an unknown version must throw");
    }

    @Test
    public void testConcurrentUpdatesLastWriteWinsWithHistory() throws Exception {
        final byte[] initial = randomBytes(512);
        final FileInfoDAO fid = newFileInfo("concurrent_" + UUID.randomUUID());
        ds.createFile(null, fid, new ByteArrayInputStream(initial), true);
        final String guid = fid.getGUID();
        final String fullPath = fid.getFullPathName();

        final int threads = 4, perThread = 5;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int seed = t;
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // Each worker uses its own DAO instance (NVEntity is not thread-safe),
                        // all pointing at the same stored file via the shared GUID.
                        FileInfoDAO mine = newFileInfo(fullPath);
                        mine.setGUID(guid);
                        ds.updateFile(mine, new ByteArrayInputStream(randomBytes(256 + seed)), true);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers[t].start();
        }
        start.countDown();
        for (Thread w : workers) w.join(60_000);
        if (failure.get() != null) throw new AssertionError("worker failed", failure.get());

        List<NVGenericMap> versions = ds.fileVersions(fid);
        assertEquals(1 + threads * perThread, versions.size(), "every concurrent update must keep its own version");
        Set<Long> distinct = ConcurrentHashMap.newKeySet();
        for (NVGenericMap v : versions) distinct.add(version(v));
        assertEquals(versions.size(), distinct.size(), "version numbers must be unique");
        assertEquals(1 + threads * perThread, version(versions.get(0)), "head must be the highest version");
        assertTrue(isCurrent(versions.get(0)));
        assertNotNull(readBack(ds, fid)); // head content readable
    }

    @Test
    public void testRetentionCapPrunesOldVersions() throws IOException {
        // Dedicated store with FILE_VERSIONS_MAX=2 (separate DB so the cap doesn't leak into other tests).
        APIConfigInfo cfg = H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:h2p_file_cap_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        cfg.getProperties().build(H2PParam.FILE_VERSIONS_MAX.getName(), "2");
        H2PDataStore capped = new H2PDSCreator().createAPI(null, cfg);

        byte[] v3 = randomBytes(300), v4 = randomBytes(400);
        FileInfoDAO fid = newFileInfo("capped_" + UUID.randomUUID());
        capped.createFile(null, fid, new ByteArrayInputStream(randomBytes(100)), true); // v1
        capped.updateFile(fid, new ByteArrayInputStream(randomBytes(200)), true);       // v2
        capped.updateFile(fid, new ByteArrayInputStream(v3), true);                     // v3
        capped.updateFile(fid, new ByteArrayInputStream(v4), true);                     // v4

        List<NVGenericMap> versions = capped.fileVersions(fid);
        assertEquals(2, versions.size(), "only the newest 2 versions may survive");
        assertEquals(4, version(versions.get(0)));
        assertEquals(3, version(versions.get(1)));
        assertArrayEquals(v4, readBack(capped, fid));
        assertArrayEquals(v3, readBackVersion(capped, fid, 3));
        assertThrows(APIException.class, () -> readBackVersion(capped, fid, 1), "pruned version must be gone");
        capped.close();
    }

    @Test
    public void testDeleteFileCascades() throws IOException {
        FileInfoDAO fid = newFileInfo("delete_" + UUID.randomUUID());
        ds.createFile(null, fid, new ByteArrayInputStream(randomBytes(1024)), true);
        ds.updateFile(fid, new ByteArrayInputStream(randomBytes(2048)), true);

        ds.deleteFile(fid);

        assertTrue(ds.fileVersions(fid).isEmpty(), "all version rows must be gone");
        assertThrows(APIException.class, () -> readBack(ds, fid), "head read must fail after delete");
        assertTrue(ds.searchByID(FileInfoDAO.class.getName(), fid.getGUID()).isEmpty(),
                "the metadata row must be gone");
    }

    @Test
    public void testTransactionParticipation() throws IOException {
        // Abort: nothing persists — neither metadata nor content.
        FileInfoDAO aborted = newFileInfo("tx_abort_" + UUID.randomUUID());
        ds.beginTransaction();
        try {
            ds.createFile(null, aborted, new ByteArrayInputStream(randomBytes(1024)), true);
        } finally {
            ds.abortTransaction();
        }
        assertTrue(ds.searchByID(FileInfoDAO.class.getName(), aborted.getGUID()).isEmpty(),
                "aborted metadata must not persist");
        assertThrows(APIException.class, () -> readBack(ds, aborted), "aborted content must not persist");

        // Commit: the same flow persists.
        byte[] content = randomBytes(1024);
        FileInfoDAO committed = newFileInfo("tx_commit_" + UUID.randomUUID());
        ds.beginTransaction();
        try {
            ds.createFile(null, committed, new ByteArrayInputStream(content), true);
            ds.endTransaction();
        } catch (RuntimeException e) {
            ds.abortTransaction();
            throw e;
        }
        assertArrayEquals(content, readBack(ds, committed));
    }
}
