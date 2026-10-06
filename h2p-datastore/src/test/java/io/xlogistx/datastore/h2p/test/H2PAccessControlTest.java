package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.test.H2PRegressionTest.CyclicDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.SetNameDescriptionDAO;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.util.ArrayValues;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVConfigEntityPortable;
import org.zoxweb.shared.util.NVConfigManager;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.SUS;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Datastore access control (user rule 2026-09-30, built 2026-10-02): with a
 * {@code SecurityController} configured a subject reaches only the entities it holds the proper
 * permission on — read, update, delete — <b>encrypted or not</b>. The store used here carries the
 * controller alone (no key maker): the check does not depend on encryption. Verdicts come from the
 * Shiro-free {@link TestSecurityController}: owner through the self permission, anyone else through
 * a grant on the row, {@code resource:*:*:<verb>} as the catalog wildcard, the system context for
 * the code that runs with nobody logged in.
 */
public class H2PAccessControlTest {

    private static H2PDataStore ds;

    /** A folder: a name and a list of documents ({@link CyclicDAO} rows), for the collection checks. */
    public static class AclFolder extends SetNameDescriptionDAO {
        public static final NVConfig NVC_ITEMS = NVConfigManager.createNVConfigEntity(
                "items", "Items", "Items", false, true, CyclicDAO.NVC_CYCLIC_DAO, NVConfigEntity.ArrayType.LIST);

        public static final NVConfigEntity NVC_ACL_FOLDER = new NVConfigEntityPortable(
                "acl_folder", null, "AclFolder", true, false, false, false, AclFolder.class,
                SUS.toNVConfigList(NVC_ITEMS), null, false,
                SetNameDescriptionDAO.NVC_NAME_DESCRIPTION_DAO);

        public AclFolder() {
            super(NVC_ACL_FOLDER);
        }

        @SuppressWarnings("unchecked")
        public ArrayValues<NVEntity> items() {
            return (ArrayValues<NVEntity>) lookup(NVC_ITEMS);
        }
    }

    @BeforeAll
    public static void setup() {
        ds = CryptoTestSupport.newStore("h2p_access_control", true, true);
        assertTrue(ds.isAccessControlActive());
        assertTrue(ds.isEncryptionActive(), "a store that connects always has both");
    }

    @AfterEach
    public void reset() {
        TestSecurityController.currentSubject = null;
        TestSecurityController.revokeAll();
    }

    private static String login() {
        String s = IDGs.UUIDV7.genID();
        TestSecurityController.currentSubject = s;
        return s;
    }

    private static void as(String subject) {
        TestSecurityController.currentSubject = subject;
    }

    private static PropertyDAO prop(String name) {
        PropertyDAO p = new PropertyDAO();
        p.setName(name);
        return p;
    }

    private static CyclicDAO doc(String name) {
        CyclicDAO d = new CyclicDAO();
        d.setName(name);
        return d;
    }

    private static String tag(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static QueryMatch<String> named(String name) {
        return new QueryMatch<>(RelationalOperator.EQUAL, name, "name");
    }

    private static <T extends NVEntity> T systemRead(NVConfigEntity nvce, String guid) {
        List<T> found = TestSecurityController.system(() -> ds.searchByID(nvce, guid));
        return found.isEmpty() ? null : found.get(0);
    }

    // ------------------------------------------------------------------ read

    @Test
    public void read_ownerSees_strangerSeesNothing_grantOpens_everyReadPath() {
        String name = tag("acl-read");
        String owner = login();
        PropertyDAO p = ds.insert(prop(name));
        assertEquals(owner, p.getSubjectGUID(), "a new row belongs to the bound subject");

        assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).size());
        assertEquals(1, ds.search(PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).size());
        assertEquals(1, ds.userSearch(owner, PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).size());
        assertEquals(1, ds.countMatch(PropertyDAO.NVC_PROPERTY_DAO, named(name)));
        APISearchResult<Object> report = ds.batchSearch(PropertyDAO.NVC_PROPERTY_DAO, named(name));
        assertEquals(1, report.size());
        assertEquals(1, ds.nextBatch(report, 0, 10).getBatch().size());
        assertNotNull(ds.lookupByReferenceID(PropertyDAO.NVC_PROPERTY_DAO.getName(), p.getGUID()));

        String stranger = login();
        assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty(), "plaintext row, no read: absent");
        assertTrue(ds.search(PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).isEmpty());
        assertTrue(ds.search(PropertyDAO.NVC_PROPERTY_DAO, List.of("name"), named(name)).isEmpty(), "a projection is checked too");
        assertTrue(ds.userSearch(owner, PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).isEmpty(), "naming the owner is not a permission");
        assertEquals(0, ds.countMatch(PropertyDAO.NVC_PROPERTY_DAO, named(name)), "not counted");
        assertEquals(0, ds.batchSearch(PropertyDAO.NVC_PROPERTY_DAO, named(name)).size(), "not in the report");
        assertTrue(ds.nextBatch(report, 0, 10).getBatch().isEmpty(), "a report made by the owner does not open the rows for someone else");
        assertNull(ds.lookupByReferenceID(PropertyDAO.NVC_PROPERTY_DAO.getName(), p.getGUID()));

        // a share: resource:<guid>:<stranger>:read
        TestSecurityController.grant(p.getGUID(), stranger, "read");
        assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).size());
        List<PropertyDAO> projected = ds.search(PropertyDAO.NVC_PROPERTY_DAO, List.of("name"), named(name));
        assertEquals(1, projected.size());
        assertEquals(name, projected.get(0).getName());
        assertEquals(1, ds.countMatch(PropertyDAO.NVC_PROPERTY_DAO, named(name)));
        assertEquals(1, ds.batchSearch(PropertyDAO.NVC_PROPERTY_DAO, named(name)).size());

        // nobody logged in: nothing, never an exception
        as(null);
        assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty());
        assertTrue(ds.search(PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).isEmpty());
        assertEquals(0, ds.countMatch(PropertyDAO.NVC_PROPERTY_DAO, named(name)));

        // the system context reads everything, with nobody bound
        assertNotNull(H2PAccessControlTest.<PropertyDAO>systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()));
    }

    @Test
    public void read_ownerlessRow_reachedByGrantOrWildcardOnly() {
        String name = tag("acl-ownerless");
        PropertyDAO p = TestSecurityController.system(() -> ds.insert(prop(name))); // nobody bound: no owner
        assertNull(p.getSubjectGUID());

        String anyone = login();
        assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty(), "no owner token to pass");
        TestSecurityController.grant(p.getGUID(), anyone, "read");
        assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).size(), "a grant on the row");

        TestSecurityController.revokeAll();
        TestSecurityController.grant("*", "*", "read");
        assertEquals(1, ds.search(PropertyDAO.NVC_PROPERTY_DAO, null, named(name)).size(), "resource:*:*:read");
    }

    @Test
    public void read_referencedEntityAndCollectionMembers_judgedOnTheirOwn() {
        String a = login();
        CyclicDAO foreign = ds.insert(doc(tag("acl-foreign")));

        String b = login();
        TestSecurityController.grant(foreign.getGUID(), b, "read");
        CyclicDAO mine = doc(tag("acl-mine"));
        CyclicDAO foreignView = (CyclicDAO) ds.searchByID(CyclicDAO.NVC_CYCLIC_DAO, foreign.getGUID()).get(0);
        foreignView.setName("renamed by a reader");
        mine.setPeer(foreignView);
        mine = ds.insert(mine);
        // a referenced row the caller may not update is linked, never rewritten
        assertEquals(foreign.getName(), H2PAccessControlTest.<CyclicDAO>systemRead(CyclicDAO.NVC_CYCLIC_DAO, foreign.getGUID()).getName());
        assertEquals(a, H2PAccessControlTest.<CyclicDAO>systemRead(CyclicDAO.NVC_CYCLIC_DAO, foreign.getGUID()).getSubjectGUID());

        AclFolder folder = new AclFolder();
        folder.setName(tag("acl-folder"));
        CyclicDAO own = doc(tag("acl-own-item"));
        folder.items().add(own);
        folder.items().add(foreignView);
        folder = ds.insert(folder);

        CyclicDAO back = (CyclicDAO) ds.searchByID(CyclicDAO.NVC_CYCLIC_DAO, mine.getGUID()).get(0);
        assertNotNull(back.getPeer(), "readable reference resolved");
        assertEquals(2, ((AclFolder) ds.searchByID(AclFolder.NVC_ACL_FOLDER, folder.getGUID()).get(0)).items().size());

        // the share is withdrawn: the parent rows stay b's, the foreign row disappears from them
        TestSecurityController.revokeAll();
        back = (CyclicDAO) ds.searchByID(CyclicDAO.NVC_CYCLIC_DAO, mine.getGUID()).get(0);
        assertNull(back.getPeer(), "a referenced row the caller may not read is absent");
        AclFolder folderBack = (AclFolder) ds.searchByID(AclFolder.NVC_ACL_FOLDER, folder.getGUID()).get(0);
        assertEquals(1, folderBack.items().size(), "only the member the caller may read");
        assertEquals(own.getGUID(), ((NVEntity) folderBack.items().values()[0]).getGUID());

        // referencing a row one may not even read is refused
        CyclicDAO another = doc(tag("acl-another"));
        another.setPeer(foreignView);
        assertThrows(AccessSecurityException.class, () -> ds.insert(another));
    }

    // ------------------------------------------------------------------ create

    @Test
    public void create_stampsTheCaller_refusesForeignOwnerAndNobody_wildcardCreatesForOthers() {
        String a = login();
        String b = login();

        PropertyDAO preset = prop(tag("acl-preset"));
        preset.setGUID(IDGs.UUIDV7.genID());
        assertEquals(b, ds.insert(preset).getSubjectGUID(), "a preset GUID does not leave the row without owner");

        PropertyDAO planted = prop(tag("acl-planted"));
        planted.setSubjectGUID(a);
        assertThrows(AccessSecurityException.class, () -> ds.insert(planted), "no rows in somebody else's name");
        assertNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, String.valueOf(planted.getGUID())));

        TestSecurityController.grant("*", "*", "create"); // an admin: resource:*:*:create
        PropertyDAO onBehalf = prop(tag("acl-on-behalf"));
        onBehalf.setSubjectGUID(a);
        assertEquals(a, ds.insert(onBehalf).getSubjectGUID());
        TestSecurityController.revokeAll();

        as(null);
        assertThrows(AccessSecurityException.class, () -> ds.insert(prop(tag("acl-anonymous"))), "nobody logged in");
        assertThrows(AccessSecurityException.class, () -> ds.update(prop(tag("acl-anonymous-update"))));
    }

    // ------------------------------------------------------------------ update / patch

    @Test
    public void update_judgedAgainstTheStoredOwner_neverTheCallersObject() {
        String name = tag("acl-update");
        String a = login();
        PropertyDAO p = ds.insert(prop(name));

        String b = login();
        PropertyDAO forged = prop("hacked");
        forged.setGUID(p.getGUID());
        forged.setSubjectGUID(b); // claims the row
        assertThrows(AccessSecurityException.class, () -> ds.update(forged));
        assertThrows(AccessSecurityException.class, () -> ds.insert(forged), "insert of an existing GUID is an update");
        assertThrows(AccessSecurityException.class, () -> ds.patch(forged, true, false, false, true, "name"));
        assertEquals(name, H2PAccessControlTest.<PropertyDAO>systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).getName());

        // an update grant lets b write, never take the row over
        TestSecurityController.grant(p.getGUID(), b, "update");
        assertThrows(AccessSecurityException.class, () -> ds.update(forged), "the owner is not changed by an update");
        PropertyDAO shell = prop("renamed by a grantee");
        shell.setGUID(p.getGUID());
        ds.update(shell);
        PropertyDAO stored = systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID());
        assertEquals("renamed by a grantee", stored.getName());
        assertEquals(a, stored.getSubjectGUID(), "the stored owner is kept");

        shell.setName("patched by a grantee");
        shell.setSubjectGUID(null);
        ds.patch(shell, true, false, false, true, "name");
        stored = systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID());
        assertEquals("patched by a grantee", stored.getName());
        assertEquals(a, stored.getSubjectGUID());

        // the owner itself
        as(a);
        PropertyDAO mine = (PropertyDAO) ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).get(0);
        mine.setName(name);
        ds.update(mine);
        assertEquals(name, ((PropertyDAO) ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).get(0)).getName());

        // only the system context moves a row to another owner
        mine.setSubjectGUID(b);
        assertThrows(AccessSecurityException.class, () -> ds.update(mine));
        TestSecurityController.system(() -> ds.update(mine));
        assertEquals(b, H2PAccessControlTest.<PropertyDAO>systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).getSubjectGUID());
    }

    // ------------------------------------------------------------------ delete

    @Test
    public void delete_needsThePermission_cascadeKeepsWhatTheCallerMayNotDelete() {
        String a = login();
        PropertyDAO p = ds.insert(prop(tag("acl-delete")));
        CyclicDAO foreignChild = ds.insert(doc(tag("acl-foreign-child")));

        String b = login();
        PropertyDAO shell = new PropertyDAO();
        shell.setGUID(p.getGUID());
        assertThrows(AccessSecurityException.class, () -> ds.delete(shell, false));
        TestSecurityController.grant(p.getGUID(), b, "read");
        assertThrows(AccessSecurityException.class, () -> ds.delete(shell, false), "read is not delete");
        TestSecurityController.grant(p.getGUID(), b, "delete");
        assertTrue(ds.delete(shell, false));
        assertNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()));

        // b's parent references a's row: deleting the parent with references keeps a's row
        TestSecurityController.grant(foreignChild.getGUID(), b, "read");
        CyclicDAO parent = doc(tag("acl-parent"));
        CyclicDAO ownChild = doc(tag("acl-own-child"));
        parent.setPeer((CyclicDAO) ds.searchByID(CyclicDAO.NVC_CYCLIC_DAO, foreignChild.getGUID()).get(0));
        parent = ds.insert(parent);
        CyclicDAO parent2 = doc(tag("acl-parent2"));
        parent2.setPeer(ownChild);
        parent2 = ds.insert(parent2);

        assertTrue(ds.delete(parent, true));
        assertNull(systemRead(CyclicDAO.NVC_CYCLIC_DAO, parent.getGUID()));
        assertNotNull(systemRead(CyclicDAO.NVC_CYCLIC_DAO, foreignChild.getGUID()), "a's row is not b's to delete: kept");
        assertTrue(ds.delete(parent2, true));
        assertNull(systemRead(CyclicDAO.NVC_CYCLIC_DAO, ownChild.getGUID()), "its own child goes with the parent");
    }

    @Test
    public void deleteByCriteria_skipsWhatTheCallerCannotSee_abortsOnWhatItMayNotDelete() {
        String name = tag("acl-criteria");
        String a = login();
        PropertyDAO ofA = ds.insert(prop(name));
        String b = login();
        PropertyDAO ofB = ds.insert(prop(name));
        PropertyDAO ofB2 = ds.insert(prop(name));

        // a's row is invisible to b: skipped, b's two rows go
        assertTrue(ds.delete(PropertyDAO.NVC_PROPERTY_DAO, named(name)));
        assertNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, ofB.getGUID()));
        assertNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, ofB2.getGUID()));
        assertNotNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, ofA.getGUID()), "not b's to see, not b's to delete");
        assertFalse(ds.delete(PropertyDAO.NVC_PROPERTY_DAO, named(name)), "nothing left that b may see");

        // a row b may read but not delete aborts the call before anything is deleted
        PropertyDAO again = ds.insert(prop(name));
        TestSecurityController.grant(ofA.getGUID(), b, "read");
        assertThrows(AccessSecurityException.class, () -> ds.delete(PropertyDAO.NVC_PROPERTY_DAO, named(name)));
        assertNotNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, again.getGUID()), "nothing was deleted");
        assertNotNull(systemRead(PropertyDAO.NVC_PROPERTY_DAO, ofA.getGUID()));
    }

    // ------------------------------------------------------------------ system context / no controller

    @Test
    public void systemContext_isPerThread_reentrant_andEndsOnFailure() throws Exception {
        String a = login();
        PropertyDAO p = ds.insert(prop(tag("acl-system")));
        login(); // a stranger from here on

        assertFalse(TestSecurityController.inSystem());
        TestSecurityController.systemRun(() -> {
            assertTrue(ds.getAPIConfigInfo().getSecurityController().isSystemContext());
            TestSecurityController.systemRun(() -> assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).size()));
            assertEquals(1, ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).size(), "still inside after the nested call");
        });
        assertFalse(TestSecurityController.inSystem());
        assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty());

        assertThrows(IllegalStateException.class, () -> TestSecurityController.system(() -> {
            throw new IllegalStateException("boom");
        }));
        assertFalse(TestSecurityController.inSystem(), "the context ends when the work fails");

        // another thread is not in the system context of this one
        boolean[] seen = new boolean[1];
        TestSecurityController.systemRun(() -> {
            Thread t = new Thread(() -> seen[0] = !ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty());
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertFalse(seen[0]);
        assertEquals(a, H2PAccessControlTest.<PropertyDAO>systemRead(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).getSubjectGUID());
    }

    /** There is no unchecked store any more: without controller and master key the database is refused. */
    @Test
    public void noController_noDatabase() {
        H2PDataStore open = CryptoTestSupport.newStore("h2p_access_control_open", false, false);
        assertFalse(open.isAccessControlActive());
        String name = tag("acl-open");
        login();
        assertThrows(AccessSecurityException.class, () -> open.insert(prop(name)));
        assertThrows(AccessSecurityException.class, () -> open.search(PropertyDAO.NVC_PROPERTY_DAO, null, named(name)));
        assertThrows(AccessSecurityException.class, () -> TestSecurityController.system(() -> open.dumpToJSON(PropertyDAO.NVC_PROPERTY_DAO)));
    }
}
