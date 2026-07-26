package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.SetNameDescriptionDAO;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVConfigEntityPortable;
import org.zoxweb.shared.util.NVConfigManager;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.SharedUtil;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for issues found in the h2p module analysis: cyclic entity graphs
 * (write + read), cross-thread sequence atomicity (incl. inside an ambient transaction),
 * {@code userSearchByID} subject scoping, {@code IS NULL} query criteria, and the
 * DynamicEnumMap upsert race.
 */
public class H2PRegressionTest {

    public static final String DB_URL = "jdbc:h2:mem:h2p_regression_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    private static H2PDataStore ds;

    @BeforeAll
    public static void setup() {
        ds = new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(DB_URL));
    }

    /** Self-referencing entity type: {@code peer} is a single reference to another CyclicDAO. */
    public static class CyclicDAO extends SetNameDescriptionDAO {
        public static final NVConfig NVC_PEER = NVConfigManager.createNVConfigEntity(
                "peer", "Peer reference", "Peer", false, true, CyclicDAO.class,
                NVConfigEntity.ArrayType.NOT_ARRAY);

        public static final NVConfigEntity NVC_CYCLIC_DAO = new NVConfigEntityPortable(
                "cyclic_dao", null, "CyclicDAO", true, false, false, false, CyclicDAO.class,
                SharedUtil.toNVConfigList(NVC_PEER), null, false,
                SetNameDescriptionDAO.NVC_NAME_DESCRIPTION_DAO);

        public CyclicDAO() {
            super(NVC_CYCLIC_DAO);
        }

        public CyclicDAO getPeer() {
            return lookupValue(NVC_PEER);
        }

        public void setPeer(CyclicDAO peer) {
            setValue(NVC_PEER, peer);
        }
    }

    @Test
    public void testCyclicPairInsertAndRead() {
        CyclicDAO a = new CyclicDAO();
        a.setName("cyclic-a-" + UUID.randomUUID());
        CyclicDAO b = new CyclicDAO();
        b.setName("cyclic-b-" + UUID.randomUUID());
        a.setPeer(b);
        b.setPeer(a);

        ds.insert(a); // must terminate (no StackOverflowError) and satisfy both FK columns

        CyclicDAO readA = (CyclicDAO) ds.searchByID(CyclicDAO.class.getName(), a.getGUID()).get(0);
        assertNotNull(readA.getPeer(), "a.peer must be resolved");
        assertEquals(b.getGUID(), readA.getPeer().getGUID());
        assertNotNull(readA.getPeer().getPeer(), "b.peer must be resolved (cycle back to a)");
        assertEquals(a.getGUID(), readA.getPeer().getPeer().getGUID());
        // The cycle must close on the same instance, not recurse into a fresh copy.
        assertTrue(readA == readA.getPeer().getPeer(), "cycle must resolve to the same instance");
    }

    @Test
    public void testSelfReferenceInsertAndRead() {
        CyclicDAO c = new CyclicDAO();
        c.setName("cyclic-self-" + UUID.randomUUID());
        c.setPeer(c);

        ds.insert(c);

        CyclicDAO read = (CyclicDAO) ds.searchByID(CyclicDAO.class.getName(), c.getGUID()).get(0);
        assertNotNull(read.getPeer(), "self reference must be resolved");
        assertEquals(c.getGUID(), read.getPeer().getGUID());
        assertTrue(read == read.getPeer(), "self reference must resolve to the same instance");
    }

    @Test
    public void testConcurrentSequenceUniqueness() throws Exception {
        final String seq = "reg_seq_" + Math.abs(UUID.randomUUID().hashCode());
        final int threads = 4, perThread = 50;
        final Set<Long> values = ConcurrentHashMap.newKeySet();
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        values.add(ds.nextSequenceValue(seq));
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers[t].start();
        }
        start.countDown();
        for (Thread w : workers) w.join(30_000);
        assertNull(failure.get(), "no worker may fail: " + failure.get());
        assertEquals(threads * perThread, values.size(), "every increment must yield a distinct value");
        assertEquals(threads * perThread, ds.currentSequenceValue(seq));
    }

    @Test
    public void testSequenceInsideTransactionDoesNotBlockOtherThreads() throws Exception {
        final String seq = "reg_txseq_" + Math.abs(UUID.randomUUID().hashCode());
        ds.beginTransaction();
        try {
            long first = ds.nextSequenceValue(seq);
            assertEquals(1, first);

            // Before the fix, the tx connection held the sequence row lock uncommitted and any
            // other caller blocked/deadlocked. Now sequences run out-of-band and commit at once.
            final AtomicReference<Long> other = new AtomicReference<>();
            Thread t = new Thread(() -> other.set(ds.nextSequenceValue(seq)));
            t.start();
            t.join(10_000);
            assertEquals(Long.valueOf(2), other.get(),
                    "a concurrent thread must not block on a sequence touched inside a transaction");

            assertEquals(3, ds.nextSequenceValue(seq));
        } finally {
            ds.abortTransaction();
        }
        // Sequence increments are non-transactional — the rollback must not undo them.
        assertEquals(3, ds.currentSequenceValue(seq));
    }

    @Test
    public void testUserSearchByIDScoping() {
        String owner = IDGs.UUIDV7.genID();
        String stranger = IDGs.UUIDV7.genID();

        PropertyDAO pd = new PropertyDAO();
        pd.setName("scoped-" + UUID.randomUUID());
        pd.setSubjectGUID(owner);
        ds.insert(pd);

        List<PropertyDAO> mine = ds.userSearchByID(owner, PropertyDAO.NVC_PROPERTY_DAO, pd.getGUID());
        assertEquals(1, mine.size(), "owner must see the entity");
        assertEquals(pd.getGUID(), mine.get(0).getGUID());

        List<PropertyDAO> theirs = ds.userSearchByID(stranger, PropertyDAO.NVC_PROPERTY_DAO, pd.getGUID());
        assertTrue(theirs.isEmpty(), "another subject must not see the entity by id");
    }

    @Test
    public void testNullCriteriaRendersIsNull() {
        PropertyDAO noDesc = new PropertyDAO();
        noDesc.setName("nulldesc-" + UUID.randomUUID());
        ds.insert(noDesc);

        PropertyDAO withDesc = new PropertyDAO();
        withDesc.setName("withdesc-" + UUID.randomUUID());
        withDesc.setDescription("present");
        ds.insert(withDesc);

        // description = null  ->  "description" IS NULL (a bound null parameter can never match)
        List<PropertyDAO> nullMatches = ds.search(PropertyDAO.NVC_PROPERTY_DAO, null,
                new QueryMatchString(RelationalOperator.EQUAL, null, MetaToken.DESCRIPTION));
        assertTrue(nullMatches.stream().anyMatch(p -> noDesc.getGUID().equals(p.getGUID())),
                "IS NULL must match the row with no description");
        assertTrue(nullMatches.stream().noneMatch(p -> withDesc.getGUID().equals(p.getGUID())),
                "IS NULL must not match a row with a description");

        // description != null  ->  "description" IS NOT NULL
        List<PropertyDAO> notNullMatches = ds.search(PropertyDAO.NVC_PROPERTY_DAO, null,
                new QueryMatchString(RelationalOperator.NOT_EQUAL, null, MetaToken.DESCRIPTION));
        assertTrue(notNullMatches.stream().anyMatch(p -> withDesc.getGUID().equals(p.getGUID())),
                "IS NOT NULL must match the row with a description");
        assertTrue(notNullMatches.stream().noneMatch(p -> noDesc.getGUID().equals(p.getGUID())),
                "IS NOT NULL must not match the row without a description");
    }

    /** Entity type whose name exceeds PostgreSQL's 63-byte identifier limit (hashed by sqlName). */
    public static class LongNameDAO extends SetNameDescriptionDAO {
        public static final NVConfigEntity NVC_LONG_NAME_DAO = new NVConfigEntityPortable(
                "regression_very_long_entity_type_name_that_exceeds_the_postgresql_identifier_limit",
                null, "LongNameDAO", true, false, false, false, LongNameDAO.class,
                SharedUtil.toNVConfigList(), null, false,
                SetNameDescriptionDAO.NVC_NAME_DESCRIPTION_DAO);

        public LongNameDAO() {
            super(NVC_LONG_NAME_DAO);
        }
    }

    @Test
    public void testSqlNameIdentifierLimit() {
        assertEquals("short_name", io.xlogistx.datastore.h2p.H2PUtil.sqlName("short_name"),
                "names within the limit pass through unchanged");
        String base = "x".repeat(70);
        String a = io.xlogistx.datastore.h2p.H2PUtil.sqlName(base + "_a");
        String b = io.xlogistx.datastore.h2p.H2PUtil.sqlName(base + "_b");
        assertEquals(63, a.length(), "hashed name must fit the 63-byte limit");
        assertEquals(a, io.xlogistx.datastore.h2p.H2PUtil.sqlName(base + "_a"), "must be deterministic");
        assertFalse(a.equals(b), "names differing only past the cut must not collide");
    }

    @Test
    public void testLongEntityTypeNameRoundTrip() {
        LongNameDAO ln = new LongNameDAO();
        ln.setName("long-" + UUID.randomUUID());
        ds.insert(ln); // table name is hashed to <=63 chars, consistently for DDL and DML
        LongNameDAO read = (LongNameDAO) ds.searchByID(LongNameDAO.class.getName(), ln.getGUID()).get(0);
        assertEquals(ln.getName(), read.getName());
    }

    @Test
    public void testMaxSelectResultsValve() {
        org.zoxweb.shared.api.APIConfigInfo cfg = H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:h2p_valve_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        cfg.getProperties().build(H2PDSCreator.H2PParam.MAX_SELECT_RESULTS.getName(), "2");
        H2PDataStore capped = new H2PDSCreator().createAPI(null, cfg);
        try {
            for (int i = 0; i < 5; i++) {
                PropertyDAO pd = new PropertyDAO();
                pd.setName("valve-" + i);
                capped.insert(pd);
            }
            List<PropertyDAO> found = capped.search(PropertyDAO.NVC_PROPERTY_DAO, null);
            assertEquals(2, found.size(), "MAX_SELECT_RESULTS must cap the SELECT");
        } finally {
            capped.close();
        }
    }

    @Test
    public void testProviderBaseLifecycle() {
        assertTrue(ds.lastTimeAccessed() > 0, "touch() must record access time");
        assertTrue(ds.inactivityDuration() >= 0);
        assertEquals(Boolean.TRUE,
                ds.lookupProperty(org.zoxweb.shared.data.DataConst.APIProperty.ASYNC_CREATE),
                "framework ASYNC_CREATE property must be answered by APIServiceProviderBase");
        assertFalse(ds.isBusy());
    }

    @Test
    public void testPatchIncludeMode() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("patch-inc-" + UUID.randomUUID());
        pd.setDescription("original");
        ds.insert(pd);

        String newName = "patched-" + UUID.randomUUID();
        pd.setName(newName);
        pd.setDescription("should-not-be-written");
        ds.patch(pd, false, false, false, true, "name"); // include mode: only "name" is written

        PropertyDAO read = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        assertEquals(newName, read.getName(), "included field must be updated");
        assertEquals("original", read.getDescription(), "non-included field must keep its stored value");
    }

    @Test
    public void testPatchExcludeMode() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("patch-exc-" + UUID.randomUUID());
        pd.setDescription("original");
        ds.insert(pd);

        String newName = "patched-" + UUID.randomUUID();
        pd.setName(newName);
        pd.setDescription("should-not-be-written");
        ds.patch(pd, false, false, false, false, "description"); // exclude mode: everything but "description"

        PropertyDAO read = (PropertyDAO) ds.searchByID(PropertyDAO.class.getName(), pd.getGUID()).get(0);
        assertEquals(newName, read.getName(), "non-excluded field must be updated");
        assertEquals("original", read.getDescription(), "excluded field must keep its stored value");
    }

    @Test
    public void testPatchMissingObjectFails() {
        PropertyDAO ghost = new PropertyDAO();
        ghost.setName("ghost-" + UUID.randomUUID());
        ghost.setGUID(IDGs.UUIDV7.genID()); // never inserted
        try {
            ds.patch(ghost, false, false, false, true, "name");
            throw new AssertionError("patch of a missing object must fail");
        } catch (org.zoxweb.shared.api.APIException expected) {
            // expected
        }
    }

    @Test
    public void testSearchProjection() {
        PropertyDAO pd = new PropertyDAO();
        pd.setName("proj-" + UUID.randomUUID());
        pd.setDescription("full description");
        pd.getProperties().build(new NVPair("k", "v"));
        ds.insert(pd);

        List<PropertyDAO> found = ds.search(PropertyDAO.NVC_PROPERTY_DAO,
                java.util.Arrays.asList("name"),
                new QueryMatchString(MetaToken.GUID, pd.getGUID(), RelationalOperator.EQUAL));
        assertEquals(1, found.size());
        PropertyDAO slim = found.get(0);
        assertEquals(pd.getGUID(), slim.getGUID(), "guid is always selected");
        assertEquals(pd.getName(), slim.getName(), "projected field must be populated");
        assertNull(slim.getDescription(), "non-projected field must stay unset");
        assertEquals(0, slim.getProperties().size(), "non-projected schemaless field must stay empty");

        // Null/empty fieldNames -> all fields (contract).
        PropertyDAO full = (PropertyDAO) ds.search(PropertyDAO.NVC_PROPERTY_DAO, null,
                new QueryMatchString(MetaToken.GUID, pd.getGUID(), RelationalOperator.EQUAL)).get(0);
        assertEquals("full description", full.getDescription());
        assertEquals("v", full.getProperties().getValue("k"));
    }

    @Test
    public void testDeleteWithReferenceCascadesFromDbState() {
        CyclicDAO a = new CyclicDAO();
        a.setName("del-parent-" + UUID.randomUUID());
        CyclicDAO b = new CyclicDAO();
        b.setName("del-child-" + UUID.randomUUID());
        a.setPeer(b);
        ds.insert(a);

        // Shell entity: only the GUID, children NOT loaded — the cascade must come from the DB.
        CyclicDAO shell = new CyclicDAO();
        shell.setGUID(a.getGUID());
        assertTrue(ds.delete(shell, true));

        assertTrue(ds.searchByID(CyclicDAO.class.getName(), a.getGUID()).isEmpty(), "parent row must be gone");
        assertTrue(ds.searchByID(CyclicDAO.class.getName(), b.getGUID()).isEmpty(),
                "stored child must cascade even when the passed entity is a shell");
    }

    @Test
    public void testDeleteWithReferenceKeepsSharedChild() {
        CyclicDAO shared = new CyclicDAO();
        shared.setName("shared-child-" + UUID.randomUUID());
        CyclicDAO p1 = new CyclicDAO();
        p1.setName("del-p1-" + UUID.randomUUID());
        p1.setPeer(shared);
        CyclicDAO p2 = new CyclicDAO();
        p2.setName("del-p2-" + UUID.randomUUID());
        p2.setPeer(shared);
        ds.insert(p1);
        ds.insert(p2);

        // Before the fix this blew up with a raw FK violation; now the shared child is kept.
        assertTrue(ds.delete(p1, true));

        assertTrue(ds.searchByID(CyclicDAO.class.getName(), p1.getGUID()).isEmpty(), "p1 must be deleted");
        assertFalse(ds.searchByID(CyclicDAO.class.getName(), shared.getGUID()).isEmpty(),
                "a child still referenced by another parent must be kept");
        CyclicDAO p2Read = (CyclicDAO) ds.searchByID(CyclicDAO.class.getName(), p2.getGUID()).get(0);
        assertEquals(shared.getGUID(), p2Read.getPeer().getGUID(), "the other parent must stay intact");
    }

    @Test
    public void testOrphanCleanupOnUpdateOptIn() {
        // Opt-in store: update() deletes detached children (unless shared).
        org.zoxweb.shared.api.APIConfigInfo cfg = H2PDSCreator.toAPIConfigInfo(
                "jdbc:h2:mem:h2p_orphan_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        cfg.getProperties().build(H2PDSCreator.H2PParam.ORPHAN_CLEANUP.getName(), "true");
        H2PDataStore cleaning = new H2PDSCreator().createAPI(null, cfg);
        try {
            CyclicDAO a = new CyclicDAO();
            a.setName("orphan-parent-" + UUID.randomUUID());
            CyclicDAO b = new CyclicDAO();
            b.setName("orphan-old-" + UUID.randomUUID());
            a.setPeer(b);
            cleaning.insert(a);

            CyclicDAO c = new CyclicDAO();
            c.setName("orphan-new-" + UUID.randomUUID());
            a.setPeer(c);
            cleaning.update(a);

            assertTrue(cleaning.searchByID(CyclicDAO.class.getName(), b.getGUID()).isEmpty(),
                    "detached child must be cleaned up when ORPHAN_CLEANUP is on");
            assertFalse(cleaning.searchByID(CyclicDAO.class.getName(), c.getGUID()).isEmpty(),
                    "the new child must exist");
        } finally {
            cleaning.close();
        }

        // Default store: detached children remain (no implicit deletes).
        CyclicDAO a2 = new CyclicDAO();
        a2.setName("orphan-def-parent-" + UUID.randomUUID());
        CyclicDAO b2 = new CyclicDAO();
        b2.setName("orphan-def-old-" + UUID.randomUUID());
        a2.setPeer(b2);
        ds.insert(a2);
        a2.setPeer(null);
        ds.update(a2);
        assertFalse(ds.searchByID(CyclicDAO.class.getName(), b2.getGUID()).isEmpty(),
                "without ORPHAN_CLEANUP a detached child must remain");
    }

    @Test
    public void testConcurrentDynamicEnumMapUpsert() throws Exception {
        final String name = "reg_dem_" + Math.abs(UUID.randomUUID().hashCode());
        final int threads = 4, perThread = 20;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        // Construct serially (the global DynamicEnumMap registry is not thread-safe); the race under
        // test is the datastore's UPDATE-then-INSERT upsert of the same name.
        final DynamicEnumMap[][] dems = new DynamicEnumMap[threads][perThread];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                DynamicEnumMap dem = new DynamicEnumMap(name);
                dem.addEnumValue(new NVPair("k" + t + "_" + i, "v"));
                dems[t][i] = dem;
            }
        }
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int id = t;
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        ds.insertDynamicEnumMap(dems[id][i]);
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            workers[t].start();
        }
        start.countDown();
        for (Thread w : workers) w.join(30_000);
        assertNull(failure.get(), "concurrent DEM upserts must not fail: " + failure.get());
        // DynamicEnumMap canonicalizes its name (DynamicEnumMap:<name>) — search by getName().
        assertNotNull(ds.searchDynamicEnumMapByName(dems[0][0].getName()));
    }
}
