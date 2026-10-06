package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PDataStore.RestoreMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIFileInfoMap;
import org.zoxweb.shared.crypto.EncapsulatedKey;
import org.zoxweb.shared.data.FileInfo;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.util.NVGenericMap;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * File content at rest (plan Stream B): with a controller + key maker every version is an AESCrypt
 * VX container under the file's entity key; reads are opened for the owner (self permission) or a
 * READ grantee and write nothing for anybody else; UPDATE/DELETE need the matching permission;
 * legacy plaintext versions stay readable; dumps carry the ciphertext verbatim.
 */
public class H2PSecureFileTest {

    private static H2PDataStore ds;
    private static final Random RAND = new Random(20260929);

    @BeforeAll
    public static void setup() {
        ds = CryptoTestSupport.newStore("h2p_secure_file", true, true);
    }

    @AfterEach
    public void reset() {
        TestSecurityController.currentSubject = null;
        TestSecurityController.revokeAll();
        KeyMakerProvider.SINGLETON.setMasterSecretKey(CryptoTestSupport.masterKey());
    }

    private static String login(H2PDataStore store) {
        String s = CryptoTestSupport.newSubjectWithKey(store);
        TestSecurityController.currentSubject = s;
        return s;
    }

    private static FileInfo newFileInfo(String name) {
        FileInfo fid = new FileInfo();
        fid.setFullPathName(name);
        fid.setFileType(FileInfo.FileType.FILE);
        fid.setCreationTime(System.currentTimeMillis());
        return fid;
    }

    private static byte[] randomBytes(int size) {
        byte[] b = new byte[size];
        RAND.nextBytes(b);
        return b;
    }

    /** @return the bytes read, or null when the store returned null (denied) */
    private static byte[] readBack(H2PDataStore store, FileInfo fid) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        APIFileInfoMap ret = store.readFile(fid, bos, true);
        return ret != null ? bos.toByteArray() : null;
    }

    private static Object[] rawVersion(H2PDataStore store, String guid, long version) throws Exception {
        try (Connection con = store.newConnection();
             PreparedStatement ps = con.prepareStatement("SELECT \"data\", \"enc\", \"length\" FROM \"sys_file_version\" WHERE \"file_guid\" = ? AND \"version\" = ?")) {
            ps.setObject(1, IDGs.UUIDV7.decode(guid));
            ps.setLong(2, version);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "version row exists");
                return new Object[]{rs.getBytes(1), rs.getInt(2), rs.getLong(3)};
            }
        }
    }

    private static void assertVX(byte[] stored, byte[] plain) {
        assertEquals('Z', stored[0]);
        assertEquals('A', stored[1]);
        assertEquals('E', stored[2]);
        assertEquals('S', stored[3]);
        assertTrue(stored.length > plain.length + 38, "header + tags");
        assertFalse(Arrays.equals(stored, plain));
    }

    @Test
    public void ownerRoundTrip_vxContainer_lengthIsPlaintext() throws Exception {
        String owner = login(ds);
        byte[] small = randomBytes(1024);
        FileInfo fid = newFileInfo("/secure/small.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(small), true);
        assertEquals(owner, fid.getSubjectGUID());
        assertEquals(1024, fid.getLength());

        Object[] raw = rawVersion(ds, fid.getGUID(), 1);
        assertVX((byte[]) raw[0], small);
        assertEquals(1, raw[1]);
        assertEquals(1024L, raw[2]);
        assertArrayEquals(small, readBack(ds, fid));
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, fid.getGUID()), "one entity key for the file");

        // ~3 MB spans many 64 KiB segments
        byte[] big = randomBytes(3 * 1024 * 1024 + 17);
        FileInfo bigFid = newFileInfo("/secure/big.bin");
        ds.createFile(null, bigFid, new ByteArrayInputStream(big), true);
        assertArrayEquals(big, readBack(ds, bigFid));
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, bigFid.getGUID()));
    }

    @Test
    public void update_versions_rollback_allEncrypted() throws Exception {
        login(ds);
        byte[] v1 = randomBytes(2048), v2 = randomBytes(4096);
        FileInfo fid = newFileInfo("/secure/versioned.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(v1), true);
        ds.updateFile(fid, new ByteArrayInputStream(v2), true);
        assertArrayEquals(v2, readBack(ds, fid));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        assertNotNull(ds.readFile(fid, 1, bos, true));
        assertArrayEquals(v1, bos.toByteArray());

        List<NVGenericMap> versions = ds.fileVersions(fid);
        assertEquals(2, versions.size());
        for (NVGenericMap v : versions) {
            assertTrue((Boolean) v.getValue("encrypted"), "every version is a VX container");
        }
        assertEquals(4096L, (long) versions.get(0).getValue("length"), "plaintext length");

        ds.rollbackFile(fid, 1);
        assertArrayEquals(v1, readBack(ds, fid));
        assertEquals(2048, fid.getLength());
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, fid.getGUID()), "still one key across versions");
    }

    @Test
    public void nonOwner_readWritesNothing_grantReads_updateDeleteNeedPermission() throws Exception {
        login(ds);
        byte[] content = randomBytes(777);
        FileInfo fid = newFileInfo("/secure/shared.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(content), true);

        String other = login(ds);
        FileInfo shell = new FileInfo();
        shell.setGUID(fid.getGUID()); // a shell: no subject_guid, the store resolves the owner itself
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        assertNull(ds.readFile(shell, bos, true), "denied read returns null");
        assertEquals(0, bos.size(), "and writes nothing");
        assertThrows(AccessSecurityException.class, () -> ds.updateFile(shell, new ByteArrayInputStream(randomBytes(10)), true));
        assertThrows(AccessSecurityException.class, () -> ds.deleteFile(shell));

        TestSecurityController.grant(fid.getGUID(), other, "read");
        assertArrayEquals(content, readBack(ds, shell), "READ grant opens it under the owner's chain");
        assertThrows(AccessSecurityException.class, () -> ds.updateFile(shell, new ByteArrayInputStream(randomBytes(10)), true), "read is not update");

        TestSecurityController.grant(fid.getGUID(), other, "update");
        byte[] v2 = randomBytes(99);
        ds.updateFile(shell, new ByteArrayInputStream(v2), true);
        assertArrayEquals(v2, readBack(ds, shell));

        // nobody bound: nothing
        TestSecurityController.currentSubject = null;
        assertNull(readBack(ds, shell));
        assertThrows(AccessSecurityException.class, () -> ds.deleteFile(shell));
    }

    @Test
    public void delete_removesVersionsHeadMetadataAndKey() throws Exception {
        login(ds);
        FileInfo fid = newFileInfo("/secure/gone.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(randomBytes(100)), true);
        ds.updateFile(fid, new ByteArrayInputStream(randomBytes(100)), true);
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, fid.getGUID()));
        ds.deleteFile(fid);
        assertTrue(ds.searchByID(FileInfo.NVC_FILE_INFO, fid.getGUID()).isEmpty());
        assertTrue(ds.fileVersions(fid).isEmpty());
        assertEquals(0, CryptoTestSupport.countKeyRows(ds, fid.getGUID()), "entity key removed with the file");
    }

    @Test
    public void wrongMasterKey_tamperedContent_failLoud() throws Exception {
        login(ds);
        byte[] content = randomBytes(5000);
        FileInfo fid = newFileInfo("/secure/tamper.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(content), true);

        // another master key: the subject key does not unwrap -> loud
        byte[] mk = new byte[32];
        CryptoTestSupport.RANDOM.nextBytes(mk);
        KeyMakerProvider.SINGLETON.setMasterSecretKey(new SecretKeySpec(mk, "AES"));
        assertThrows(AccessSecurityException.class, () -> readBack(ds, fid));
        KeyMakerProvider.SINGLETON.setMasterSecretKey(CryptoTestSupport.masterKey());
        assertArrayEquals(content, readBack(ds, fid));

        // flip a ciphertext byte
        try (Connection con = ds.newConnection();
             PreparedStatement sel = con.prepareStatement("SELECT \"data\" FROM \"sys_file_version\" WHERE \"file_guid\" = ? AND \"version\" = 1")) {
            sel.setObject(1, IDGs.UUIDV7.decode(fid.getGUID()));
            byte[] data;
            try (ResultSet rs = sel.executeQuery()) {
                rs.next();
                data = rs.getBytes(1);
            }
            data[data.length - 20] ^= 0x55;
            try (PreparedStatement upd = con.prepareStatement("UPDATE \"sys_file_version\" SET \"data\" = ? WHERE \"file_guid\" = ? AND \"version\" = 1")) {
                upd.setBytes(1, data);
                upd.setObject(2, IDGs.UUIDV7.decode(fid.getGUID()));
                upd.executeUpdate();
            }
        }
        APIException e = assertThrows(APIException.class, () -> readBack(ds, fid));
        assertTrue(e.getMessage().contains("tampered") || e.getMessage().contains("decryption"), e.getMessage());
    }

    @Test
    public void legacyPlaintextVersion_readableInActiveStore() throws Exception {
        login(ds);
        byte[] content = randomBytes(300);
        FileInfo fid = newFileInfo("/secure/legacy.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(content), true);
        // rewrite version 1 as a pre-encryption plaintext row
        try (Connection con = ds.newConnection();
             PreparedStatement upd = con.prepareStatement("UPDATE \"sys_file_version\" SET \"data\" = ?, \"enc\" = 0 WHERE \"file_guid\" = ? AND \"version\" = 1")) {
            upd.setBytes(1, content);
            upd.setObject(2, IDGs.UUIDV7.decode(fid.getGUID()));
            upd.executeUpdate();
        }
        assertArrayEquals(content, readBack(ds, fid), "the owner reads a plaintext version as it is");
        assertFalse((Boolean) ds.fileVersions(fid).get(0).getValue("encrypted"));

        // a plaintext version is access-checked like an encrypted one
        login(ds);
        FileInfo shell = new FileInfo();
        shell.setGUID(fid.getGUID());
        assertNull(readBack(ds, shell), "a stranger reads nothing");
        assertTrue(ds.fileVersions(shell).isEmpty(), "and sees no versions");
        TestSecurityController.currentSubject = null;
        assertNull(readBack(ds, shell), "nobody bound reads nothing");
    }

    /** No controller, no key maker or neither: the store refuses to connect, no file is written in the clear. */
    @Test
    public void incompleteConfiguration_databaseRefused() {
        TestSecurityController.currentSubject = CryptoTestSupport.newSubjectGUID();
        for (boolean[] flags : new boolean[][]{{true, false}, {false, true}, {false, false}}) {
            H2PDataStore incomplete = CryptoTestSupport.newStore("h2p_secure_file_incomplete_" + flags[0] + "_" + flags[1], flags[0], flags[1]);
            assertThrows(org.zoxweb.shared.security.AccessSecurityException.class,
                    () -> incomplete.createFile(null, newFileInfo("/x"), new ByteArrayInputStream(randomBytes(10)), true));
        }
    }

    @Test
    public void dumpRestore_jsonlAndZip_keepCiphertext_readableUnderSameMasterKey() throws Exception {
        String owner = login(ds);
        byte[] content = randomBytes(70_000); // two segments
        FileInfo fid = newFileInfo("/secure/dumped.bin");
        ds.createFile(null, fid, new ByteArrayInputStream(content), true);

        for (boolean zip : new boolean[]{false, true}) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (zip) TestSecurityController.system(() -> ds.dumpZip(bos, true, FileInfo.NVC_FILE_INFO, EncapsulatedKey.NVCE_ENCAPSULATED_KEY));
            else TestSecurityController.system(() -> ds.dump(bos, true, FileInfo.NVC_FILE_INFO, EncapsulatedKey.NVCE_ENCAPSULATED_KEY));
            if (!zip) {
                String text = bos.toString(java.nio.charset.StandardCharsets.ISO_8859_1);
                assertTrue(text.contains("\"enc\":1"), "file_version records carry enc");
            }
            H2PDataStore target = CryptoTestSupport.newStore("h2p_secure_file_restore_" + (zip ? "zip" : "jsonl") + "_"
                    + UUID.randomUUID().toString().replace("-", ""), true, true);
            TestSecurityController.system(() -> target.restore(new ByteArrayInputStream(bos.toByteArray()), RestoreMode.MERGE));
            Object[] raw = rawVersion(target, fid.getGUID(), 1);
            assertEquals(1, raw[1], "enc restored");
            assertVX((byte[]) raw[0], content);
            TestSecurityController.currentSubject = owner;
            assertArrayEquals(content, readBack(target, fid), "same master key + key rows -> readable");
        }
    }
}
