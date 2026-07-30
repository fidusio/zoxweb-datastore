package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PDataStore.RestoreMode;
import io.xlogistx.datastore.h2p.test.H2PRegressionTest.CyclicDAO;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.data.FileInfoDAO;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVPair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JSON dump/restore ({@code H2PDumpRestore}) on in-memory H2 in PostgreSQL mode: per-type and
 * whole-store round trips into a fresh store, shared-child GUID dedup, the cycle skip policy,
 * the {@code MAX_SELECT_RESULTS} batch clamp, MERGE vs WIPE_AND_LOAD semantics, and cold-start
 * type discovery through {@code sys_meta_catalog}.
 *
 * <p>Each test builds its own store(s) on a unique in-memory URL so content is fully controlled.
 */
public class H2PDumpRestoreTest {

    private static H2PDataStore newStore(String dbName) {
        return new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL"));
    }

    private static String db(String prefix) {
        return prefix + "_" + Math.abs(UUID.randomUUID().hashCode());
    }

    private static PropertyDAO newPropertyDAO(String name, int val) {
        PropertyDAO ret = new PropertyDAO();
        ret.setName(name);
        ret.setDescription("desc of " + name);
        ret.getProperties()
                .build("str", name)
                .build(new NVInt("int_val", val));
        return ret;
    }

    /** The module's fidelity standard: two entities are equal when they re-serialize identically. */
    private static void assertSameJSON(NVEntity expected, NVEntity actual) throws IOException {
        assertEquals(GSONUtil.toJSON(expected, true, false, true, null),
                GSONUtil.toJSON(actual, true, false, true, null));
    }

    @Test
    public void testPerTypeDumpAndRestoreRoundTrip() throws IOException {
        H2PDataStore source = newStore(db("dump_src"));
        H2PDataStore target = newStore(db("dump_dst"));
        try {
            for (int i = 0; i < 10; i++) {
                source.insert(newPropertyDAO("per-type-" + i, i));
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long written = source.dump(PropertyDAO.NVC_PROPERTY_DAO, bos);
            assertEquals(10, written);

            // A per-type dump is a header-less JSONL stream — restore accepts it as-is.
            NVGenericMap stats = target.restore(new ByteArrayInputStream(bos.toByteArray()), RestoreMode.MERGE);
            assertEquals(Long.valueOf(10), stats.getValue("entities"));

            List<PropertyDAO> restored = target.userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null);
            assertEquals(10, restored.size());
            for (PropertyDAO r : restored) {
                PropertyDAO orig = (PropertyDAO) source.searchByID(PropertyDAO.class.getName(), r.getGUID()).get(0);
                assertSameJSON(orig, r);
            }
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testDumpToJSONArray() {
        H2PDataStore source = newStore(db("dump_arr"));
        try {
            for (int i = 0; i < 3; i++) {
                source.insert(newPropertyDAO("array-" + i, i));
            }
            String json = source.dumpToJSON(PropertyDAO.NVC_PROPERTY_DAO);
            List<NVEntity> parsed = GSONUtil.fromJSONArray(json, null);
            assertEquals(3, parsed.size());
            for (NVEntity nve : parsed) {
                assertTrue(nve instanceof PropertyDAO, "class_type must reconstruct the concrete type");
                assertFalse(source.searchByID(PropertyDAO.class.getName(), nve.getGUID()).isEmpty());
            }
        } finally {
            source.close();
        }
    }

    @Test
    public void testFullStoreRoundTrip() throws IOException {
        H2PDataStore source = newStore(db("full_src"));
        H2PDataStore target = newStore(db("full_dst"));
        try {
            // Entities of several types, one with an entity reference.
            for (int i = 0; i < 5; i++) {
                source.insert(newPropertyDAO("full-" + i, i));
            }
            Range<Integer> range = new Range<>(1, 100);
            range.setName("full-range");
            source.insert(range);
            CyclicDAO parent = new CyclicDAO();
            parent.setName("full-parent");
            CyclicDAO child = new CyclicDAO();
            child.setName("full-child");
            parent.setPeer(child); // acyclic reference chain
            source.insert(parent);

            // DEM + sequence.
            DynamicEnumMap dem = new DynamicEnumMap("dump_dem_" + Math.abs(UUID.randomUUID().hashCode()));
            dem.addEnumValue(new NVPair("k1", "v1"));
            source.insertDynamicEnumMap(dem);
            String seqName = "dump_seq_" + Math.abs(UUID.randomUUID().hashCode());
            source.createSequence(seqName, 0, 1);
            for (int i = 0; i < 7; i++) source.nextSequenceValue(seqName);

            // A 2-version file rolled back to version 1 (head != highest version).
            byte[] v1 = "file content v1".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "file content v2 - longer".getBytes(StandardCharsets.UTF_8);
            FileInfoDAO fid = new FileInfoDAO();
            fid.setFullPathName("dump_file_" + UUID.randomUUID());
            fid.setFileType(FileInfoDAO.FileType.FILE);
            source.createFile(null, fid, new ByteArrayInputStream(v1), true);
            source.updateFile(fid, new ByteArrayInputStream(v2), true);
            source.rollbackFile(fid, 1);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NVGenericMap dumpStats = source.dump(bos);
            NVGenericMap typeCounts = dumpStats.getNV("types");
            assertEquals(Long.valueOf(5), typeCounts.getValue(PropertyDAO.NVC_PROPERTY_DAO.getName()));
            assertEquals(Long.valueOf(2), typeCounts.getValue(CyclicDAO.NVC_CYCLIC_DAO.getName()));
            assertEquals(Long.valueOf(1), dumpStats.getValue("dem"));
            assertEquals(Long.valueOf(1), dumpStats.getValue("sequences"));
            assertEquals(Long.valueOf(2), dumpStats.getValue("file_versions"));
            assertEquals(Long.valueOf(1), dumpStats.getValue("file_heads"));
            assertEquals(Long.valueOf(0), dumpStats.getValue("cycles_skipped"));

            NVGenericMap restoreStats = target.restore(
                    new ByteArrayInputStream(bos.toByteArray()), RestoreMode.WIPE_AND_LOAD);
            // 5 PropertyDAO + 1 Range + 2 CyclicDAO + 1 FileInfoDAO
            assertEquals(Long.valueOf(9), restoreStats.getValue("entities"));

            // Entities: JSON-identical to the source's stored form.
            for (PropertyDAO r : target.<PropertyDAO>userSearch(null, PropertyDAO.NVC_PROPERTY_DAO, null)) {
                assertSameJSON(
                        (NVEntity) source.searchByID(PropertyDAO.class.getName(), r.getGUID()).get(0), r);
            }
            CyclicDAO parentRead = (CyclicDAO) target.searchByID(CyclicDAO.class.getName(), parent.getGUID()).get(0);
            assertNotNull(parentRead.getPeer(), "entity reference must survive the round trip");
            assertEquals(child.getGUID(), parentRead.getPeer().getGUID());
            assertSameJSON((NVEntity) source.searchByID(Range.class.getName(), range.getGUID()).get(0),
                    (NVEntity) target.searchByID(Range.class.getName(), range.getGUID()).get(0));

            // DEM equality: compare both stores' read-back (same decode path) by re-serialized JSON.
            DynamicEnumMap demRead = target.searchDynamicEnumMapByName(dem.getName());
            assertNotNull(demRead, "DEM must be restored");
            assertEquals(GSONUtil.toJSONDynamicEnumMap(source.searchDynamicEnumMapByName(dem.getName())),
                    GSONUtil.toJSONDynamicEnumMap(demRead));
            assertEquals(7, target.currentSequenceValue(seqName), "sequence value must carry over");
            assertEquals(8, target.nextSequenceValue(seqName), "sequence must continue, not collide");

            // File content: both versions present, head still rolled back to v1.
            ByteArrayOutputStream head = new ByteArrayOutputStream();
            target.readFile(fid, head, true);
            assertArrayEquals(v1, head.toByteArray(), "head must point at the rolled-back version");
            ByteArrayOutputStream v2Read = new ByteArrayOutputStream();
            target.readFile(fid, 2, v2Read, true);
            assertArrayEquals(v2, v2Read.toByteArray(), "non-head version must be preserved");
            List<NVGenericMap> versions = target.fileVersions(fid);
            assertEquals(2, versions.size());
            assertTrue((Boolean) versions.get(1).getValue("current"), "version 1 must be current");
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testSharedChildDedup() {
        H2PDataStore source = newStore(db("shared_src"));
        H2PDataStore target = newStore(db("shared_dst"));
        try {
            CyclicDAO shared = new CyclicDAO();
            shared.setName("shared-child");
            CyclicDAO p1 = new CyclicDAO();
            p1.setName("shared-p1");
            p1.setPeer(shared);
            CyclicDAO p2 = new CyclicDAO();
            p2.setName("shared-p2");
            p2.setPeer(shared);
            source.insert(p1);
            source.insert(p2);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            source.dump(bos);
            target.restore(new ByteArrayInputStream(bos.toByteArray()), RestoreMode.MERGE);

            // The child was dumped 3 times (inlined in each parent + its own line) but must land once.
            assertEquals(3, target.countMatch(CyclicDAO.NVC_CYCLIC_DAO),
                    "2 parents + 1 shared child — inlined duplicates must dedup by GUID");
            CyclicDAO r1 = (CyclicDAO) target.searchByID(CyclicDAO.class.getName(), p1.getGUID()).get(0);
            CyclicDAO r2 = (CyclicDAO) target.searchByID(CyclicDAO.class.getName(), p2.getGUID()).get(0);
            assertEquals(shared.getGUID(), r1.getPeer().getGUID());
            assertEquals(shared.getGUID(), r2.getPeer().getGUID());
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testCyclicEntitiesAreSkipped() {
        H2PDataStore source = newStore(db("cycle_src"));
        try {
            CyclicDAO a = new CyclicDAO();
            a.setName("cycle-a");
            CyclicDAO b = new CyclicDAO();
            b.setName("cycle-b");
            a.setPeer(b);
            b.setPeer(a);
            source.insert(a);
            CyclicDAO self = new CyclicDAO();
            self.setName("cycle-self");
            self.setPeer(self);
            source.insert(self);
            CyclicDAO plain = new CyclicDAO();
            plain.setName("acyclic");
            source.insert(plain);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long written = source.dump(CyclicDAO.NVC_CYCLIC_DAO, bos);
            assertEquals(1, written, "only the acyclic entity is JSON-representable");

            NVGenericMap stats = source.dump(new ByteArrayOutputStream());
            assertEquals(Long.valueOf(3), stats.getValue("cycles_skipped"),
                    "a<->b and the self-reference must be counted as skipped");
        } finally {
            source.close();
        }
    }

    @Test
    public void testMaxSelectResultsBatchClamp() {
        APIConfigInfo cfg = H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:" + db("clamp") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        cfg.getProperties().build(H2PDSCreator.H2PParam.MAX_SELECT_RESULTS.getName(), "3");
        H2PDataStore capped = new H2PDSCreator().createAPI(null, cfg);
        try {
            for (int i = 0; i < 10; i++) {
                capped.insert(newPropertyDAO("clamp-" + i, i));
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long written = capped.dump(PropertyDAO.NVC_PROPERTY_DAO, bos);
            assertEquals(10, written,
                    "the dump pages must clamp to MAX_SELECT_RESULTS instead of being LIMIT-truncated");
        } finally {
            capped.close();
        }
    }

    @Test
    public void testMergeKeepsExtraRowsWipeRemovesThem() {
        H2PDataStore source = newStore(db("mode_src"));
        H2PDataStore target = newStore(db("mode_dst"));
        try {
            source.insert(newPropertyDAO("dumped", 1));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            source.dump(bos);
            byte[] dump = bos.toByteArray();

            PropertyDAO extra = newPropertyDAO("pre-existing", 99);
            target.insert(extra);

            target.restore(new ByteArrayInputStream(dump), RestoreMode.MERGE);
            assertEquals(2, target.countMatch(PropertyDAO.NVC_PROPERTY_DAO),
                    "MERGE must keep the pre-existing row");
            assertFalse(target.searchByID(PropertyDAO.class.getName(), extra.getGUID()).isEmpty());

            target.restore(new ByteArrayInputStream(dump), RestoreMode.WIPE_AND_LOAD);
            assertEquals(1, target.countMatch(PropertyDAO.NVC_PROPERTY_DAO),
                    "WIPE_AND_LOAD must clear rows not present in the dump");
            assertTrue(target.searchByID(PropertyDAO.class.getName(), extra.getGUID()).isEmpty());
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testColdStartCatalogDiscovery() {
        String dbName = db("coldstart");
        H2PDataStore first = newStore(dbName);
        try {
            first.insert(newPropertyDAO("cold-1", 1));
            Range<Integer> range = new Range<>(5, 10);
            range.setName("cold-range");
            first.insert(range);
        } finally {
            first.close();
        }

        // A fresh instance on the same database: its session registry is empty, so a no-arg dump
        // can only find the types through the persistent sys_meta_catalog.
        H2PDataStore second = newStore(dbName);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NVGenericMap stats = second.dump(bos);
            NVGenericMap typeCounts = stats.getNV("types");
            assertEquals(Long.valueOf(1), typeCounts.getValue(PropertyDAO.NVC_PROPERTY_DAO.getName()),
                    "cold-start dump must discover property_dao via the catalog");
            assertEquals(Long.valueOf(1), typeCounts.getValue(Range.NVC_RANGE.getName()),
                    "cold-start dump must discover range via the catalog");
        } finally {
            second.close();
        }
    }

    @Test
    public void testZipDumpAndRestoreRoundTrip() throws IOException {
        H2PDataStore source = newStore(db("zip_src"));
        H2PDataStore target = newStore(db("zip_dst"));
        try {
            source.insert(newPropertyDAO("zip-entity", 1));
            byte[] v1 = "zip file v1".getBytes(StandardCharsets.UTF_8);
            byte[] v2 = "zip file v2 - longer content".getBytes(StandardCharsets.UTF_8);
            FileInfoDAO fid = new FileInfoDAO();
            fid.setFullPathName("zip_file_" + UUID.randomUUID());
            fid.setFileType(FileInfoDAO.FileType.FILE);
            source.createFile(null, fid, new ByteArrayInputStream(v1), true);
            source.updateFile(fid, new ByteArrayInputStream(v2), true);
            source.rollbackFile(fid, 1); // head != highest version

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            NVGenericMap dumpStats = source.dumpZip(bos);
            byte[] zip = bos.toByteArray();
            assertEquals(Long.valueOf(2), dumpStats.getValue("file_versions"));
            assertTrue(zip.length > 2 && zip[0] == 'P' && zip[1] == 'K', "must be a zip archive");

            // Container layout: dump.jsonl first (entry pointers, no base64), then the raw content.
            java.util.List<String> entries = new java.util.ArrayList<>();
            String jsonl = null;
            try (java.util.zip.ZipInputStream zis =
                         new java.util.zip.ZipInputStream(new ByteArrayInputStream(zip))) {
                java.util.zip.ZipEntry e;
                while ((e = zis.getNextEntry()) != null) {
                    entries.add(e.getName());
                    if ("dump.jsonl".equals(e.getName())) {
                        ByteArrayOutputStream sb = new ByteArrayOutputStream();
                        byte[] buf = new byte[8192];
                        int n;
                        while ((n = zis.read(buf)) > 0) sb.write(buf, 0, n);
                        jsonl = sb.toString("UTF-8");
                    }
                }
            }
            assertEquals("dump.jsonl", entries.get(0), "JSONL entry must lead the archive");
            assertEquals(3, entries.size(), "dump.jsonl + one content entry per stored version");
            assertNotNull(jsonl);
            assertTrue(jsonl.contains("\"entry\":\"files/" + fid.getGUID() + "/1\""));
            assertFalse(jsonl.contains("\"content\""), "zip dump must not inline base64 content");

            // restore() auto-detects the zip container.
            NVGenericMap restoreStats = target.restore(new ByteArrayInputStream(zip), RestoreMode.MERGE);
            assertEquals(Long.valueOf(2), restoreStats.getValue("file_versions"));

            ByteArrayOutputStream head = new ByteArrayOutputStream();
            target.readFile(fid, head, true);
            assertArrayEquals(v1, head.toByteArray(), "rolled-back head must survive the zip round trip");
            ByteArrayOutputStream second = new ByteArrayOutputStream();
            target.readFile(fid, 2, second, true);
            assertArrayEquals(v2, second.toByteArray());
            assertEquals(1, target.countMatch(PropertyDAO.NVC_PROPERTY_DAO));
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testZipRestoreFailsOnMissingContentEntry() throws IOException {
        H2PDataStore source = newStore(db("zip_miss_src"));
        H2PDataStore target = newStore(db("zip_miss_dst"));
        try {
            FileInfoDAO fid = new FileInfoDAO();
            fid.setFullPathName("zip_miss_" + UUID.randomUUID());
            fid.setFileType(FileInfoDAO.FileType.FILE);
            source.createFile(null, fid,
                    new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)), true);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            source.dumpZip(bos);

            // Truncate the archive down to dump.jsonl only — the referenced content entry is gone.
            ByteArrayOutputStream truncated = new ByteArrayOutputStream();
            try (java.util.zip.ZipInputStream zis =
                         new java.util.zip.ZipInputStream(new ByteArrayInputStream(bos.toByteArray()));
                 java.util.zip.ZipOutputStream zos = new java.util.zip.ZipOutputStream(truncated)) {
                java.util.zip.ZipEntry e;
                while ((e = zis.getNextEntry()) != null) {
                    if (!"dump.jsonl".equals(e.getName())) continue;
                    zos.putNextEntry(new java.util.zip.ZipEntry(e.getName()));
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = zis.read(buf)) > 0) zos.write(buf, 0, n);
                    zos.closeEntry();
                }
            }
            APIException ex = assertThrows(APIException.class, () -> target.restore(
                    new ByteArrayInputStream(truncated.toByteArray()), RestoreMode.MERGE));
            assertTrue(ex.getMessage().contains("missing"), "must report the missing content entry: " + ex.getMessage());
        } finally {
            source.close();
            target.close();
        }
    }

    @Test
    public void testJsonlWithExternalEntriesRejectedWithoutZip() {
        H2PDataStore target = newStore(db("extref"));
        try {
            String jsonl = "{\"k\":\"file_version\",\"v\":{\"file_guid\":\"" + UUID.randomUUID()
                    + "\",\"version\":1,\"length\":7,\"created_ts\":1,\"entry\":\"files/x/1\"}}\n";
            APIException ex = assertThrows(APIException.class, () -> target.restore(
                    new ByteArrayInputStream(jsonl.getBytes(StandardCharsets.UTF_8)), RestoreMode.MERGE));
            assertTrue(ex.getMessage().contains("zip"),
                    "must point the caller at the zip archive: " + ex.getMessage());
        } finally {
            target.close();
        }
    }

    @Test
    public void testRestoreRejectsForeignFormat() {
        H2PDataStore target = newStore(db("badfmt"));
        try {
            byte[] bogus = "{\"k\":\"header\",\"v\":{\"format\":\"something-else\",\"version\":1}}\n"
                    .getBytes(StandardCharsets.UTF_8);
            assertThrows(APIException.class,
                    () -> target.restore(new ByteArrayInputStream(bogus), RestoreMode.MERGE));
        } finally {
            target.close();
        }
    }
}
