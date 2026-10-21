package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.test.H2PRegressionTest.CyclicDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.PropertyImpl;
import org.zoxweb.shared.data.SetNameDescriptionImpl;
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
    public static class AclFolder extends SetNameDescriptionImpl {
        public static final NVConfig NVC_ITEMS = NVConfigManager.createNVConfigEntity(
                "items", "Items", "Items", false, true, CyclicDAO.NVC_CYCLIC_DAO, NVConfigEntity.ArrayType.LIST);

        public static final NVConfigEntity NVC_ACL_FOLDER = new NVConfigEntityPortable(
                "acl_folder", null, "AclFolder", true, false, false, false, AclFolder.class,
                SUS.toNVConfigList(NVC_ITEMS), null, false,
                SetNameDescriptionImpl.NVC_NAME_DESCRIPTION_IMPL);

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
        TestSecurityController.currentApp = null;
        TestSecurityController.revokeAll();
    }

    /** An app record owned by {@code ownerSubject} (its registrar), a row of the store under test, inserted in the system context. */
    private static org.zoxweb.shared.app.AppIDDefault newApp(String ownerSubject) {
        org.zoxweb.shared.app.AppIDDefault app = new org.zoxweb.shared.app.AppIDDefault(
                "acl" + Long.toHexString(System.nanoTime()) + ".com", "app");
        app.setSubjectGUID(ownerSubject);
        return TestSecurityController.system(() -> ds.insert(app));
    }

    // ------------------------------------------------------------------ app ownership (dual ownership, 2026-10-20)

    @Test
    public void appOwnership_registeredClassStamped_judgedOnTheAppToken_immutable_everyReadPath() {
        String registrar = IDGs.UUIDV7.genID();
        org.zoxweb.shared.app.AppIDDefault app = newApp(registrar);
        String appGUID = app.getGUID();
        assertEquals(appGUID, app.getAppGUID(), "the app record is its own app");
        String fqcn = PropertyImpl.class.getName();
        String creator = login();
        TestSecurityController.currentApp = appGUID;
        TestSecurityController.register(appGUID, PropertyImpl.class);

        // a registered class created in a login into the app: the app owns it, the creator is recorded
        PropertyImpl owned = ds.insert(prop(tag("app-owned")));
        assertEquals(appGUID, owned.getAppGUID(), "stamped with the login's app");
        assertEquals(creator, owned.getSubjectGUID(), "the creator is recorded");
        PropertyImpl stored = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID());
        assertEquals(appGUID, stored.getAppGUID(), "app_guid is in the row");
        // an unregistered class in the same login stays the subject's
        CyclicDAO mine = ds.insert(doc(tag("subject-owned")));
        assertNull(mine.getAppGUID());
        assertEquals(1, ds.searchByID(CyclicDAO.NVC_CYCLIC_DAO, mine.getGUID()).size(), "the owner reads its own row");

        // the creator holds nothing by subject_guid: without an app right the row is not there for it
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID()).isEmpty(), "creator without app right sees nothing");
        assertEquals(0, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(owned.getName())));
        assertThrows(AccessSecurityException.class, () -> ds.update(owned));
        assertThrows(AccessSecurityException.class, () -> ds.delete(owned, false));

        // a reader with the app right on the class, in a login into the app: reads through every path, no update, no delete
        String reader = login();
        TestSecurityController.currentApp = appGUID;
        TestSecurityController.grantApp(appGUID, fqcn, reader, "read");
        List<PropertyImpl> seen = ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID());
        assertEquals(1, seen.size(), "read through the app token");
        assertEquals(appGUID, seen.get(0).getAppGUID());
        assertEquals(1, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(owned.getName())));
        assertEquals(1, ds.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(owned.getName())).size());
        APISearchResult<UUID> report = ds.batchSearch(PropertyImpl.NVC_PROPERTY_IMPL, named(owned.getName()));
        assertEquals(1, report.getMatchIDs().size(), "batch search reports it");
        seen.get(0).setName(tag("renamed"));
        assertThrows(AccessSecurityException.class, () -> ds.update(seen.get(0)), "read is not update");
        assertThrows(AccessSecurityException.class, () -> ds.delete(seen.get(0), false), "read is not delete");
        // a stranger in the app sees nothing; the reader in a login without app holds no app right either
        login();
        TestSecurityController.currentApp = appGUID;
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID()).isEmpty());
        assertEquals(0, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(owned.getName())));
        assertTrue(ds.batchSearch(PropertyImpl.NVC_PROPERTY_IMPL, named(owned.getName())).getMatchIDs().isEmpty());
        // a share of the row opens it like any row
        String grantee = login();
        TestSecurityController.grant(owned.getGUID(), grantee, "read");
        assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID()).size(), "a share reaches an app-owned row");

        // an editor: update works, the owners are immutable
        String editor = login();
        TestSecurityController.currentApp = appGUID;
        TestSecurityController.grantApp(appGUID, fqcn, editor, "read", "update");
        PropertyImpl edit = ds.<PropertyImpl>searchByID(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID()).get(0);
        edit.setName(tag("edited"));
        edit.setAppGUID(null);
        ds.update(edit);
        PropertyImpl after = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID());
        assertEquals(appGUID, after.getAppGUID(), "an empty app_guid keeps the stored app");
        assertEquals(creator, after.getSubjectGUID(), "and the stored creator");
        PropertyImpl reown = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID());
        reown.setAppGUID(IDGs.UUIDV7.genID());
        assertThrows(AccessSecurityException.class, () -> ds.update(reown), "the owning app never changes");
        PropertyImpl reclaim = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID());
        reclaim.setSubjectGUID(editor);
        assertThrows(AccessSecurityException.class, () -> ds.update(reclaim), "the creator never changes");
        assertThrows(AccessSecurityException.class, () -> ds.delete(edit, false), "update is not delete");
        TestSecurityController.grantApp(appGUID, fqcn, editor, "delete");
        assertTrue(ds.delete(edit, false), "delete through the app token");
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, owned.getGUID()));

        // creation rules: a login without app stamps nothing; a foreign app_guid needs the create wildcard
        as(creator);
        TestSecurityController.currentApp = null;
        PropertyImpl global = ds.insert(prop(tag("global")));
        assertNull(global.getAppGUID(), "no app in the login: the subject's row");
        PropertyImpl foreign = prop(tag("foreign"));
        foreign.setAppGUID(appGUID);
        assertThrows(AccessSecurityException.class, () -> ds.insert(foreign), "a row for an app the login is not in");
        TestSecurityController.grant("*", "*", "create");
        PropertyImpl onBehalf = ds.insert(foreign);
        assertEquals(appGUID, onBehalf.getAppGUID(), "the create wildcard creates for the app");
        assertEquals(creator, onBehalf.getSubjectGUID());
        // an entity arriving with a GUID or an owner is not app-stamped, even in the app
        TestSecurityController.currentApp = appGUID;
        PropertyImpl preset = prop(tag("preset-guid"));
        preset.setGUID(IDGs.UUIDV7.genID());
        assertNull(ds.insert(preset).getAppGUID(), "client-supplied GUID: not stamped");
        // a class no longer registered stops being stamped at once
        TestSecurityController.unregister(appGUID, PropertyImpl.class);
        assertNull(ds.insert(prop(tag("unregistered"))).getAppGUID());

        // delete by criteria: rows the caller cannot see are skipped, a visible undeletable one aborts
        TestSecurityController.register(appGUID, PropertyImpl.class);
        String victimName = tag("criteria");
        PropertyImpl victim = ds.insert(prop(victimName));
        assertEquals(appGUID, victim.getAppGUID());
        as(reader);
        TestSecurityController.currentApp = appGUID;
        assertThrows(AccessSecurityException.class, () -> ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(victimName)), "visible, not deletable");
        login();
        TestSecurityController.currentApp = appGUID;
        ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(victimName)); // invisible: skipped
        assertNotNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, victim.getGUID()), "still there");
        as(editor);
        ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(victimName));
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, victim.getGUID()), "deleted through the app token");

        // the app record: its own app, read by whoever holds the app right on its class (the app admin), not by a bystander
        TestSecurityController.grantApp(appGUID, "*", editor, "*");
        assertEquals(1, ds.searchByID(org.zoxweb.shared.app.AppIDDefault.NVC_APP_ID_DEFAULT, appGUID).size(), "app_resource:<app>:*:<S>:* opens every class");
        login();
        TestSecurityController.currentApp = appGUID;
        assertTrue(ds.searchByID(org.zoxweb.shared.app.AppIDDefault.NVC_APP_ID_DEFAULT, appGUID).isEmpty());
        TestSecurityController.systemRun(() -> ds.delete(app, false));
    }

    private static String login() {
        String s = IDGs.UUIDV7.genID();
        TestSecurityController.currentSubject = s;
        return s;
    }

    private static void as(String subject) {
        TestSecurityController.currentSubject = subject;
    }

    private static PropertyImpl prop(String name) {
        PropertyImpl p = new PropertyImpl();
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
        PropertyImpl p = ds.insert(prop(name));
        assertEquals(owner, p.getSubjectGUID(), "a new row belongs to the bound subject");

        assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).size());
        assertEquals(1, ds.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).size());
        assertEquals(1, ds.userSearch(owner, PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).size());
        assertEquals(1, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)));
        APISearchResult<Object> report = ds.batchSearch(PropertyImpl.NVC_PROPERTY_IMPL, named(name));
        assertEquals(1, report.size());
        assertEquals(1, ds.nextBatch(report, 0, 10).getBatch().size());
        assertNotNull(ds.lookupByReferenceID(PropertyImpl.NVC_PROPERTY_IMPL.getName(), p.getGUID()));

        String stranger = login();
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).isEmpty(), "plaintext row, no read: absent");
        assertTrue(ds.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).isEmpty());
        assertTrue(ds.search(PropertyImpl.NVC_PROPERTY_IMPL, List.of("name"), named(name)).isEmpty(), "a projection is checked too");
        assertTrue(ds.userSearch(owner, PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).isEmpty(), "naming the owner is not a permission");
        assertEquals(0, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)), "not counted");
        assertEquals(0, ds.batchSearch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)).size(), "not in the report");
        assertTrue(ds.nextBatch(report, 0, 10).getBatch().isEmpty(), "a report made by the owner does not open the rows for someone else");
        assertNull(ds.lookupByReferenceID(PropertyImpl.NVC_PROPERTY_IMPL.getName(), p.getGUID()));

        // a share: resource:<guid>:<stranger>:read
        TestSecurityController.grant(p.getGUID(), stranger, "read");
        assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).size());
        List<PropertyImpl> projected = ds.search(PropertyImpl.NVC_PROPERTY_IMPL, List.of("name"), named(name));
        assertEquals(1, projected.size());
        assertEquals(name, projected.get(0).getName());
        assertEquals(1, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)));
        assertEquals(1, ds.batchSearch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)).size());

        // nobody logged in: nothing, never an exception
        as(null);
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).isEmpty());
        assertTrue(ds.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).isEmpty());
        assertEquals(0, ds.countMatch(PropertyImpl.NVC_PROPERTY_IMPL, named(name)));

        // the system context reads everything, with nobody bound
        assertNotNull(H2PAccessControlTest.<PropertyImpl>systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()));
    }

    @Test
    public void read_ownerlessRow_reachedByGrantOrWildcardOnly() {
        String name = tag("acl-ownerless");
        PropertyImpl p = TestSecurityController.system(() -> ds.insert(prop(name))); // nobody bound: no owner
        assertNull(p.getSubjectGUID());

        String anyone = login();
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).isEmpty(), "no owner token to pass");
        TestSecurityController.grant(p.getGUID(), anyone, "read");
        assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).size(), "a grant on the row");

        TestSecurityController.revokeAll();
        TestSecurityController.grant("*", "*", "read");
        assertEquals(1, ds.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)).size(), "resource:*:*:read");
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

        PropertyImpl preset = prop(tag("acl-preset"));
        preset.setGUID(IDGs.UUIDV7.genID());
        assertEquals(b, ds.insert(preset).getSubjectGUID(), "a preset GUID does not leave the row without owner");

        PropertyImpl planted = prop(tag("acl-planted"));
        planted.setSubjectGUID(a);
        assertThrows(AccessSecurityException.class, () -> ds.insert(planted), "no rows in somebody else's name");
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, String.valueOf(planted.getGUID())));

        TestSecurityController.grant("*", "*", "create"); // an admin: resource:*:*:create
        PropertyImpl onBehalf = prop(tag("acl-on-behalf"));
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
        PropertyImpl p = ds.insert(prop(name));

        String b = login();
        PropertyImpl forged = prop("hacked");
        forged.setGUID(p.getGUID());
        forged.setSubjectGUID(b); // claims the row
        assertThrows(AccessSecurityException.class, () -> ds.update(forged));
        assertThrows(AccessSecurityException.class, () -> ds.insert(forged), "insert of an existing GUID is an update");
        assertThrows(AccessSecurityException.class, () -> ds.patch(forged, true, false, false, true, "name"));
        assertEquals(name, H2PAccessControlTest.<PropertyImpl>systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).getName());

        // an update grant lets b write, never take the row over
        TestSecurityController.grant(p.getGUID(), b, "update");
        assertThrows(AccessSecurityException.class, () -> ds.update(forged), "the owner is not changed by an update");
        PropertyImpl shell = prop("renamed by a grantee");
        shell.setGUID(p.getGUID());
        ds.update(shell);
        PropertyImpl stored = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID());
        assertEquals("renamed by a grantee", stored.getName());
        assertEquals(a, stored.getSubjectGUID(), "the stored owner is kept");

        shell.setName("patched by a grantee");
        shell.setSubjectGUID(null);
        ds.patch(shell, true, false, false, true, "name");
        stored = systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID());
        assertEquals("patched by a grantee", stored.getName());
        assertEquals(a, stored.getSubjectGUID());

        // the owner itself
        as(a);
        PropertyImpl mine = (PropertyImpl) ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).get(0);
        mine.setName(name);
        ds.update(mine);
        assertEquals(name, ((PropertyImpl) ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).get(0)).getName());

        // only the system context moves a row to another owner
        mine.setSubjectGUID(b);
        assertThrows(AccessSecurityException.class, () -> ds.update(mine));
        TestSecurityController.system(() -> ds.update(mine));
        assertEquals(b, H2PAccessControlTest.<PropertyImpl>systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).getSubjectGUID());
    }

    // ------------------------------------------------------------------ delete

    @Test
    public void delete_needsThePermission_cascadeKeepsWhatTheCallerMayNotDelete() {
        String a = login();
        PropertyImpl p = ds.insert(prop(tag("acl-delete")));
        CyclicDAO foreignChild = ds.insert(doc(tag("acl-foreign-child")));

        String b = login();
        PropertyImpl shell = new PropertyImpl();
        shell.setGUID(p.getGUID());
        assertThrows(AccessSecurityException.class, () -> ds.delete(shell, false));
        TestSecurityController.grant(p.getGUID(), b, "read");
        assertThrows(AccessSecurityException.class, () -> ds.delete(shell, false), "read is not delete");
        TestSecurityController.grant(p.getGUID(), b, "delete");
        assertTrue(ds.delete(shell, false));
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()));

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
        PropertyImpl ofA = ds.insert(prop(name));
        String b = login();
        PropertyImpl ofB = ds.insert(prop(name));
        PropertyImpl ofB2 = ds.insert(prop(name));

        // a's row is invisible to b: skipped, b's two rows go
        assertTrue(ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(name)));
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, ofB.getGUID()));
        assertNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, ofB2.getGUID()));
        assertNotNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, ofA.getGUID()), "not b's to see, not b's to delete");
        assertFalse(ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(name)), "nothing left that b may see");

        // a row b may read but not delete aborts the call before anything is deleted
        PropertyImpl again = ds.insert(prop(name));
        TestSecurityController.grant(ofA.getGUID(), b, "read");
        assertThrows(AccessSecurityException.class, () -> ds.delete(PropertyImpl.NVC_PROPERTY_IMPL, named(name)));
        assertNotNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, again.getGUID()), "nothing was deleted");
        assertNotNull(systemRead(PropertyImpl.NVC_PROPERTY_IMPL, ofA.getGUID()));
    }

    // ------------------------------------------------------------------ system context / no controller

    @Test
    public void systemContext_isPerThread_reentrant_andEndsOnFailure() throws Exception {
        String a = login();
        PropertyImpl p = ds.insert(prop(tag("acl-system")));
        login(); // a stranger from here on

        assertFalse(TestSecurityController.inSystem());
        TestSecurityController.systemRun(() -> {
            assertTrue(ds.getAPIConfigInfo().getSecurityController().isSystemContext());
            TestSecurityController.systemRun(() -> assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).size()));
            assertEquals(1, ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).size(), "still inside after the nested call");
        });
        assertFalse(TestSecurityController.inSystem());
        assertTrue(ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).isEmpty());

        assertThrows(IllegalStateException.class, () -> TestSecurityController.system(() -> {
            throw new IllegalStateException("boom");
        }));
        assertFalse(TestSecurityController.inSystem(), "the context ends when the work fails");

        // another thread is not in the system context of this one
        boolean[] seen = new boolean[1];
        TestSecurityController.systemRun(() -> {
            Thread t = new Thread(() -> seen[0] = !ds.searchByID(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).isEmpty());
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertFalse(seen[0]);
        assertEquals(a, H2PAccessControlTest.<PropertyImpl>systemRead(PropertyImpl.NVC_PROPERTY_IMPL, p.getGUID()).getSubjectGUID());
    }

    /** There is no unchecked store any more: without controller and master key the database is refused. */
    @Test
    public void noController_noDatabase() {
        H2PDataStore open = CryptoTestSupport.newStore("h2p_access_control_open", false, false);
        assertFalse(open.isAccessControlActive());
        String name = tag("acl-open");
        login();
        assertThrows(AccessSecurityException.class, () -> open.insert(prop(name)));
        assertThrows(AccessSecurityException.class, () -> open.search(PropertyImpl.NVC_PROPERTY_IMPL, null, named(name)));
        assertThrows(AccessSecurityException.class, () -> TestSecurityController.system(() -> open.dumpToJSON(PropertyImpl.NVC_PROPERTY_IMPL)));
    }
}
