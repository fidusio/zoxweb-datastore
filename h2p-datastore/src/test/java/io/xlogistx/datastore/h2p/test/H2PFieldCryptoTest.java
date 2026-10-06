package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PDataStore.RestoreMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.security.CipherCodecs;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.crypto.EncapsulatedKey;
import org.zoxweb.shared.crypto.EncryptedData;
import org.zoxweb.shared.data.CreditCardDAO;
import org.zoxweb.shared.data.DetailedCreditCardDAO;
import org.zoxweb.shared.data.PropertyDAO;
import org.zoxweb.shared.data.SecureDocument;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.SharedBase64;
import org.zoxweb.shared.util.SharedBase64.Base64Type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Field encryption at rest (plan Stream A, packed form): ENCRYPT / ENCRYPT_MASK attributes live in a
 * {@code bytea} column holding the record's {@code CipherCodecs} packed bytes when the store
 * encrypts (UTF-8 clear text otherwise), are opened under the owner's key chain, keys live and die
 * with the entity, dumps carry the bytes beside the entity line, and an encrypting store refuses to
 * query encrypted columns by value. Since the store checks every read (2026-10-02) a subject without
 * {@code read} on a row does not get the row at all, so it never sees a mask either; the mask is
 * still what a masked value written back is recognized by.
 */
public class H2PFieldCryptoTest {

    private static final String CARD = "4111111111111111";
    private static final String CARD2 = "4012888888881881";
    private static H2PDataStore ds;

    @BeforeAll
    public static void setup() {
        ds = CryptoTestSupport.newStore("h2p_field_crypto", true, true);
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

    private static CreditCardDAO card(String number) {
        CreditCardDAO c = new CreditCardDAO();
        c.setCardNumber(number);
        c.setCardHolderName("Test Owner");
        return c;
    }

    private static <T extends NVEntity> T byId(H2PDataStore store, NVConfigEntity nvce, String guid) {
        List<T> found = store.searchByID(nvce, guid);
        assertFalse(found.isEmpty(), "row " + guid + " exists");
        return found.get(0);
    }

    private static CreditCardDAO cardById(H2PDataStore store, String guid) { return H2PFieldCryptoTest.<CreditCardDAO>byId(store, CreditCardDAO.NVC_CREDIT_CARD_DAO, guid); }
    private static DetailedCreditCardDAO detailedById(H2PDataStore store, String guid) { return H2PFieldCryptoTest.<DetailedCreditCardDAO>byId(store, DetailedCreditCardDAO.NVC_DETAILED_CREDIT_CARD_DAO, guid); }
    private static SecureDocument docById(H2PDataStore store, String guid) { return H2PFieldCryptoTest.<SecureDocument>byId(store, SecureDocument.NVC_SECURE_DOCUMENT, guid); }
    private static PropertyDAO propById(H2PDataStore store, String guid) { return H2PFieldCryptoTest.<PropertyDAO>byId(store, PropertyDAO.NVC_PROPERTY_DAO, guid); }

    /** The stored bytes of the card_number column. */
    private static byte[] rawCard(H2PDataStore store, String guid) throws Exception {
        Object v = CryptoTestSupport.rawColumn(store, CreditCardDAO.NVC_CREDIT_CARD_DAO.getName(), "card_number", guid);
        assertTrue(v == null || v instanceof byte[], "bytea column, got " + (v != null ? v.getClass() : null));
        return (byte[]) v;
    }

    private static EncryptedData assertPacked(byte[] stored, String... mustNotContain) {
        assertNotNull(stored);
        assertEquals(EncryptedData.VERSION, stored[0] & 0xFF, "packed record starts with the version byte");
        String asText = new String(stored, StandardCharsets.ISO_8859_1);
        for (String s : mustNotContain) assertFalse(asText.contains(s), "no plaintext in the column");
        EncryptedData record = CipherCodecs.EDDecoder.decode(stored);
        assertEquals("A256GCM", record.getAlgorithm());
        assertEquals(String.class.getName(), record.getDataType());
        return record;
    }

    @Test
    public void ownerRoundTrip_columnHoldsPackedRecordNotPlaintext() throws Exception {
        String owner = login(ds);
        CreditCardDAO c = ds.insert(card(CARD));
        assertEquals(owner, c.getSubjectGUID(), "associated with the bound subject");
        assertEquals(CARD, c.getCardNumber(), "the caller's object keeps its plaintext");

        EncryptedData record = assertPacked(rawCard(ds, c.getGUID()), CARD);
        assertEquals("****1111", record.getMask(), "mask authenticated in the record");

        assertEquals(CARD, cardById(ds, c.getGUID()).getCardNumber(), "owner reads the clear value");
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, c.getGUID()), "exactly one entity key row");
    }

    @Test
    public void nonOwner_rowAbsent_grantReadsClear_nobodyBoundSeesNothing() throws Exception {
        login(ds);
        CreditCardDAO masked = ds.insert(card(CARD));
        DetailedCreditCardDAO plain = new DetailedCreditCardDAO();
        plain.setCardNumber(CARD);
        plain.setCardHolderName("Test Owner");
        plain = ds.insert(plain);
        final String plainGUID = plain.getGUID();

        String stranger = login(ds);
        assertTrue(ds.searchByID(CreditCardDAO.NVC_CREDIT_CARD_DAO, masked.getGUID()).isEmpty(), "no read permission: the row is not returned");
        assertTrue(ds.searchByID(DetailedCreditCardDAO.NVC_DETAILED_CREDIT_CARD_DAO, plainGUID).isEmpty());

        // a share (resource:<guid>:<stranger>:read) returns the row and opens it under the OWNER's key chain
        TestSecurityController.grant(masked.getGUID(), stranger, "read");
        TestSecurityController.grant(plainGUID, stranger, "read");
        assertEquals(CARD, cardById(ds, masked.getGUID()).getCardNumber());
        DetailedCreditCardDAO p = detailedById(ds, plainGUID);
        assertEquals(CARD, p.getCardNumber());
        assertEquals("Test Owner", p.getCardHolderName());

        // nobody bound: nothing, never an exception
        TestSecurityController.currentSubject = null;
        assertTrue(ds.searchByID(CreditCardDAO.NVC_CREDIT_CARD_DAO, masked.getGUID()).isEmpty());
        assertTrue(ds.searchByID(DetailedCreditCardDAO.NVC_DETAILED_CREDIT_CARD_DAO, plainGUID).isEmpty());
    }

    @Test
    public void maskWriteBackKeepsSecret_newValueReseals_emptyClears() throws Exception {
        String owner = login(ds);
        CreditCardDAO c = ds.insert(card(CARD));
        byte[] stored = rawCard(ds, c.getGUID());

        // the mask written back (a client that only ever showed ****1111) keeps the stored ciphertext byte for byte
        assertEquals(owner, c.getSubjectGUID());
        CreditCardDAO maskedView = cardById(ds, c.getGUID());
        ((NVPair) maskedView.lookup("card_number")).setValueFilter(FilterType.ENCRYPT_MASK);
        ((NVPair) maskedView.lookup("card_number")).setValue("****1111");
        maskedView.setCardHolderName("Renamed Owner");
        ds.update(maskedView);
        assertArrayEquals(stored, rawCard(ds, c.getGUID()), "stored record kept");
        assertEquals(CARD, cardById(ds, c.getGUID()).getCardNumber());
        assertEquals("Renamed Owner", cardById(ds, c.getGUID()).getCardHolderName());

        // a new value re-seals with a fresh nonce
        CreditCardDAO fresh = cardById(ds, c.getGUID());
        fresh.setCardNumber(CARD2);
        ds.update(fresh);
        byte[] stored2 = rawCard(ds, c.getGUID());
        assertFalse(java.util.Arrays.equals(stored, stored2));
        assertEquals("****1881", assertPacked(stored2, CARD, CARD2).getMask());
        assertEquals(CARD2, cardById(ds, c.getGUID()).getCardNumber());

        // empty clears
        CreditCardDAO clear = cardById(ds, c.getGUID());
        ((NVPair) clear.lookup("card_number")).setValueFilter(FilterType.ENCRYPT_MASK);
        ((NVPair) clear.lookup("card_number")).setValue("");
        ds.update(clear);
        assertNull(rawCard(ds, c.getGUID()));
    }

    @Test
    public void secureDocument_encryptOnly_roundTrip_andPatch() throws Exception {
        login(ds);
        SecureDocument doc = new SecureDocument();
        doc.setName("doc");
        doc.setContent("top secret body");
        doc = ds.insert(doc);
        Object raw = CryptoTestSupport.rawColumn(ds, SecureDocument.NVC_SECURE_DOCUMENT.getName(), "content", doc.getGUID());
        assertPacked((byte[]) raw, "secret");
        assertEquals("top secret body", docById(ds, doc.getGUID()).getContent());

        doc.setContent("patched body");
        ds.patch(doc, true, false, false, true, "content");
        assertEquals("patched body", docById(ds, doc.getGUID()).getContent());
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, doc.getGUID()), "still one key row after update/patch");
    }

    @Test
    public void nonOwnerWrite_deniedByController_updateGrantAllows() {
        login(ds);
        CreditCardDAO c = ds.insert(card(CARD));
        String other = login(ds);
        TestSecurityController.grant(c.getGUID(), other, "read");
        CreditCardDAO view = cardById(ds, c.getGUID());
        view.setCardNumber(CARD2);
        assertThrows(AccessSecurityException.class, () -> ds.update(view), "read is not update");
        TestSecurityController.grant(c.getGUID(), other, "update");
        ds.update(view);
        assertEquals(CARD2, cardById(ds, c.getGUID()).getCardNumber());
    }

    @Test
    public void queryOnEncryptedColumnRejected_nullTestAllowed() {
        login(ds);
        ds.insert(card(CARD));
        assertThrows(IllegalArgumentException.class, () -> ds.search(CreditCardDAO.NVC_CREDIT_CARD_DAO, null,
                new QueryMatch<>(RelationalOperator.EQUAL, CARD, "card_number")));
        List<CreditCardDAO> withCard = ds.search(CreditCardDAO.NVC_CREDIT_CARD_DAO, null,
                new QueryMatch<>(RelationalOperator.NOT_EQUAL, null, "card_number"));
        assertFalse(withCard.isEmpty(), "IS NOT NULL still allowed");
    }

    @Test
    public void missingSubjectKey_missingSubject_missingMasterKey_failLoud() {
        // bound subject without a subject key
        TestSecurityController.currentSubject = CryptoTestSupport.newSubjectGUID();
        AccessSecurityException e = assertThrows(AccessSecurityException.class, () -> ds.insert(card(CARD)));
        assertTrue(e.getMessage().contains("No key"), e.getMessage());

        // nobody bound: no subject_guid -> refused before any statement
        TestSecurityController.currentSubject = null;
        assertThrows(AccessSecurityException.class, () -> ds.insert(card(CARD)));

        // master key unloaded
        login(ds);
        KeyMakerProvider.SINGLETON.setMasterSecretKey((javax.crypto.SecretKey) null);
        assertThrows(AccessSecurityException.class, () -> ds.insert(card(CARD)));
    }

    /** No controller, no key maker or neither: the store refuses to connect, so nothing — sealed or plain — reaches the database. */
    @Test
    public void incompleteConfiguration_databaseRefused() {
        TestSecurityController.currentSubject = CryptoTestSupport.newSubjectGUID();
        PropertyDAO p = new PropertyDAO();
        p.setName("plain");
        for (boolean[] flags : new boolean[][]{{true, false}, {false, true}, {false, false}}) {
            H2PDataStore incomplete = CryptoTestSupport.newStore("h2p_field_crypto_incomplete_" + flags[0] + "_" + flags[1], flags[0], flags[1]);
            assertThrows(AccessSecurityException.class, incomplete::newConnection);
            assertThrows(AccessSecurityException.class, () -> incomplete.insert(card(CARD)));
            assertThrows(AccessSecurityException.class, () -> incomplete.insert(p), "a plain entity is refused too");
            assertThrows(AccessSecurityException.class, () -> incomplete.search(CreditCardDAO.NVC_CREDIT_CARD_DAO, null));
        }
        assertTrue(ds.isEncryptionActive());
    }

    @Test
    public void clearTextRow_readableInActiveStore_deleteRemovesKeys() throws Exception {
        String owner = login(ds);
        CreditCardDAO c = ds.insert(card(CARD));
        // a row written by a non-encrypting store: UTF-8 clear text in the bytea column
        try (Connection con = ds.newConnection();
             PreparedStatement ps = con.prepareStatement("UPDATE \"credit_card_dao\" SET \"card_number\" = ? WHERE \"guid\" = ?")) {
            ps.setBytes(1, CARD2.getBytes(StandardCharsets.UTF_8));
            ps.setObject(2, IDGs.UUIDV7.decode(c.getGUID()));
            ps.executeUpdate();
        }
        assertEquals(CARD2, cardById(ds, c.getGUID()).getCardNumber(), "clear text is handed back as it is");
        assertEquals(owner, cardById(ds, c.getGUID()).getSubjectGUID());

        // delete by entity and by criteria both remove the entity's key row
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, c.getGUID()));
        assertTrue(ds.delete(c, false));
        assertEquals(0, CryptoTestSupport.countKeyRows(ds, c.getGUID()));

        CreditCardDAO c2 = ds.insert(card(CARD));
        assertEquals(1, CryptoTestSupport.countKeyRows(ds, c2.getGUID()));
        assertTrue(ds.delete(CreditCardDAO.NVC_CREDIT_CARD_DAO, new QueryMatch<>(RelationalOperator.EQUAL, c2.getGUID(), "guid")));
        assertEquals(0, CryptoTestSupport.countKeyRows(ds, c2.getGUID()));
    }

    @Test
    public void schemalessPair_encryptMask_inProperties() throws Exception {
        login(ds);
        String secret = "s3cr3t-value-12345678";
        PropertyDAO p = new PropertyDAO();
        p.setName("with-secret");
        p.getProperties().build(new NVPair("api_secret", secret, FilterType.ENCRYPT_MASK));
        p.getProperties().build(new NVPair("plain", "visible"));
        p = ds.insert(p);
        assertEquals(secret, p.getProperties().getValue("api_secret"), "caller's object untouched");

        Object raw = CryptoTestSupport.rawColumn(ds, PropertyDAO.NVC_PROPERTY_DAO.getName(), "properties", p.getGUID());
        String json = raw instanceof org.postgresql.util.PGobject ? ((org.postgresql.util.PGobject) raw).getValue() : raw.toString();
        assertFalse(json.contains("s3cr3t"), json);
        assertFalse(json.contains("A256GCM"), "no canonical text anywhere: " + json);
        assertTrue(json.contains("visible"));
        // the pair carries base64url of the packed record
        NVGenericMap storedProps = org.zoxweb.server.util.GSONUtil.fromJSONDefault(json, NVGenericMap.class);
        String carrier = storedProps.getValue("api_secret");
        EncryptedData record = CipherCodecs.EDDecoder.decode(SharedBase64.decode(Base64Type.URL, carrier));
        assertEquals("****5678", record.getMask());

        PropertyDAO back = propById(ds, p.getGUID());
        assertEquals(secret, back.getProperties().getValue("api_secret"));
        assertEquals("visible", back.getProperties().getValue("plain"));

        TestSecurityController.currentSubject = CryptoTestSupport.newSubjectWithKey(ds);
        assertTrue(ds.searchByID(PropertyDAO.NVC_PROPERTY_DAO, p.getGUID()).isEmpty(), "no read permission: no row, so no pair either");
    }

    @Test
    public void dumpCarriesBytesBesideEntity_restoreReadableUnderSameMasterKey() throws Exception {
        String owner = login(ds);
        CreditCardDAO c = ds.insert(card(CARD));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // a dump moves every row: refused for a logged-in subject, it runs in the system context only
        assertThrows(AccessSecurityException.class, () -> ds.dump(new ByteArrayOutputStream(), false, CreditCardDAO.NVC_CREDIT_CARD_DAO));
        TestSecurityController.system(() -> ds.dump(bos, false, CreditCardDAO.NVC_CREDIT_CARD_DAO, EncapsulatedKey.NVCE_ENCAPSULATED_KEY));
        String dump = bos.toString(StandardCharsets.UTF_8);
        assertFalse(dump.contains(CARD), "dump carries the record bytes, never the plaintext");
        // the EncapsulatedKey entities legitimately carry alg=A256GCM as a field; the |-joined canonical
        // text form of a record must not appear anywhere
        assertFalse(dump.contains("|A256GCM|"), "no canonical text form in the dump");
        assertFalse(dump.contains("\"card_number\":\"2|"), "no canonical text in the entity line");
        assertTrue(dump.contains("\"enc\":{\"card_number\":\""), "encrypted column rides beside the entity line");

        H2PDataStore target = CryptoTestSupport.newStore("h2p_field_crypto_restore_" + UUID.randomUUID().toString().replace("-", ""), true, true);
        assertThrows(AccessSecurityException.class, () -> target.restore(new ByteArrayInputStream(bos.toByteArray()), RestoreMode.MERGE));
        NVGenericMap stats = TestSecurityController.system(() -> target.restore(new ByteArrayInputStream(bos.toByteArray()), RestoreMode.MERGE));
        assertNotNull(stats);
        // same master key + the key rows travelled with the dump: the owner reads the clear value
        TestSecurityController.currentSubject = owner;
        assertEquals(CARD, cardById(target, c.getGUID()).getCardNumber());
        assertArrayEquals(rawCard(ds, c.getGUID()), rawCard(target, c.getGUID()), "restore writes the bytes verbatim");
    }
}
