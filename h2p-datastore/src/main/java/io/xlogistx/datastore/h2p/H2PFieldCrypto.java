/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.xlogistx.datastore.h2p;

import org.zoxweb.server.security.AESCrypt;
import org.zoxweb.server.security.CipherCodecs;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.crypto.EncryptedData;
import org.zoxweb.shared.filters.ChainedFilter;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.filters.ValueFilter;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.security.KeyMaker;
import org.zoxweb.shared.security.SecurityController;
import org.zoxweb.shared.util.ArrayValues;
import org.zoxweb.shared.util.CRUD;
import org.zoxweb.shared.util.NVBase;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVGenericMapList;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.NamedValue;
import org.zoxweb.shared.util.SUS;
import org.zoxweb.shared.util.SharedBase64;
import org.zoxweb.shared.util.SharedBase64.Base64Type;

import javax.crypto.AEADBadTagException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Encryption at rest for {@link H2PDataStore} (user decisions 2026-09-28/29; the record format is
 * zoxweb-core's {@code META-ENCRYPTED-DATA.md}). Two mechanisms, one key chain:
 * <ul>
 * <li><b>Field values</b> ({@link FilterType#ENCRYPT} / {@link FilterType#ENCRYPT_MASK} attributes,
 * and such {@link NVPair}s inside schemaless containers) are sealed by the
 * {@link SecurityController} into an {@link EncryptedData} record and stored in its <b>packed
 * binary form</b> ({@link CipherCodecs#EDEncoder}) in a {@code bytea} column — the storage form the
 * record design names for a datastore column. The record's canonical text form is never used.</li>
 * <li><b>File content</b> ({@code sys_file_version.data}) is an {@link AESCrypt} VX container
 * ({@code ZAES}, self-describing header, segmented AES-256-GCM) under the file's entity key.</li>
 * </ul>
 * Key chain = {@link KeyMaker}: master key → subject key (created with the subject, never here) →
 * entity key (minted here on the first encrypted write, one per entity) → records / containers.
 * <p>
 * <b>Activation:</b> both a {@link SecurityController} and a {@link KeyMaker} on the store's
 * {@link APIConfigInfo}. Neither ⇒ the {@code bytea} column holds the clear text as UTF-8 bytes
 * (dev/tests; the schema never depends on a store instance's configuration). Exactly one ⇒
 * {@link APIException} on the first encrypted write ({@link #requireConsistent}) — never silently
 * plaintext. Access is decided by the controller alone (ownership is a permission, see
 * {@code SecurityModel.RESOURCE}); this class never compares {@code subject_guid} itself.
 * <p>
 * <b>Denied read</b> ⇒ the attribute stays null, an {@code ENCRYPT_MASK} attribute shows the
 * record's mask; a file read writes nothing. Denied write/delete ⇒ {@link AccessSecurityException}
 * from the controller.
 * <p>
 * A packed record is recognized by its layout ({@link #isPackedRecord}): version byte
 * {@code EncryptedData.VERSION} first, then a decodable layout. Clear text stored by a
 * non-encrypting store never starts with that control byte, so the two never collide.
 */
final class H2PFieldCrypto {

    /** Table of the {@code EncapsulatedKey} rows ({@code EncapsulatedKey.NVCE_ENCAPSULATED_KEY.getName()}). */
    static final String KEY_TABLE = "encapsulated_key";
    static final String KEY_REFERENCE_COLUMN = "reference_guid";
    static final String DEFAULT_MASK = "****";

    private final H2PDataStore ds;
    private final Set<String> warnedTypes = ConcurrentHashMap.newKeySet();
    /**
     * Raw mode (per thread): reads leave encrypted attributes untouched (null) — the dump moves the
     * stored bytes beside the entity line itself, so a backup carries ciphertext, never the
     * plaintext of whoever runs it, and nothing ever goes through the entity's filters.
     */
    private final ThreadLocal<Boolean> raw = new ThreadLocal<>();

    H2PFieldCrypto(H2PDataStore ds) {
        this.ds = ds;
    }

    /** Switches raw mode for the calling thread; callers pair it with {@code false} in a finally block. */
    void setRaw(boolean on) {
        if (on) raw.set(Boolean.TRUE);
        else raw.remove();
    }

    boolean isRaw() {
        return Boolean.TRUE.equals(raw.get());
    }

    // ------------------------------------------------------------------ activation

    SecurityController controller() {
        APIConfigInfo cfg = ds.getAPIConfigInfo();
        return cfg != null ? cfg.getSecurityController() : null;
    }

    KeyMaker keyMaker() {
        APIConfigInfo cfg = ds.getAPIConfigInfo();
        return cfg != null ? cfg.getKeyMaker() : null;
    }

    /** True when both the controller and the key maker are configured: encryption is on. */
    boolean active() {
        return controller() != null && keyMaker() != null;
    }

    /**
     * Fail loudly on a half configuration (one of controller / key maker) when something that
     * would have to be encrypted is about to be written.
     *
     * @param what the subject of the write, for the message
     * @throws APIException when exactly one of the two is configured
     */
    void requireConsistent(String what) {
        boolean sc = controller() != null, km = keyMaker() != null;
        if (sc != km) {
            throw new APIException("encryption misconfigured for " + what + ": "
                    + (sc ? "SecurityController without KeyMaker" : "KeyMaker without SecurityController")
                    + " — configure both on the APIConfigInfo or neither");
        }
    }

    // ------------------------------------------------------------------ declaration + record form

    static boolean isEncrypted(ValueFilter<?, ?> filter) {
        return ChainedFilter.isFilterSupported(filter, FilterType.ENCRYPT)
                || ChainedFilter.isFilterSupported(filter, FilterType.ENCRYPT_MASK);
    }

    static boolean isMasked(ValueFilter<?, ?> filter) {
        return ChainedFilter.isFilterSupported(filter, FilterType.ENCRYPT_MASK);
    }

    static boolean isEncrypted(NVConfig nvc) {
        return nvc != null && isEncrypted(nvc.getValueFilter());
    }

    /** The packed storage form of a sealed record ({@code CipherCodecs.EDEncoder}). */
    static byte[] pack(EncryptedData record) {
        return CipherCodecs.EDEncoder.encode(record);
    }

    /** The record behind its packed storage form; {@code IllegalArgumentException} on a bad layout. */
    static EncryptedData unpack(byte[] packed) {
        return CipherCodecs.EDDecoder.decode(packed);
    }

    /**
     * True when the bytes are a packed record: the version byte first, then a decodable layout.
     * Clear text stored by a non-encrypting store never starts with {@code 0x02}.
     */
    static boolean isPackedRecord(byte[] bytes) {
        if (bytes == null || bytes.length < 2 || (bytes[0] & 0xFF) != EncryptedData.VERSION) {
            return false;
        }
        try {
            unpack(bytes);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Text carrier of a packed record for places that can only hold text (a pair inside a JSON
     * column, a dump line): base64url of the packed bytes. Not a record grammar of its own.
     */
    static String packedText(byte[] packed) {
        return SharedBase64.encodeAsString(Base64Type.URL, packed);
    }

    /** Decodes {@link #packedText}; null when the text is not base64 of a packed record. */
    static byte[] fromPackedText(String text) {
        if (SUS.isEmpty(text)) {
            return null;
        }
        try {
            byte[] bytes = SharedBase64.decode(Base64Type.URL, text);
            return isPackedRecord(bytes) ? bytes : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** UTF-8 bytes of a clear value for the {@code bytea} column of a non-encrypting store. */
    static byte[] plaintextBytes(Object value) {
        if (value == null) {
            return null;
        }
        String s = value.toString();
        return s.isEmpty() ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ key chain

    /**
     * Makes sure the entity has its {@code EncapsulatedKey} (one per entity, minted once): looked up
     * by reference GUID, else wrapped under the owner's subject key and inserted through this store
     * by the key maker. The subject key must already exist (created with the subject).
     *
     * @throws AccessSecurityException if the entity has no {@code subject_guid}, the owner has no
     *                                 subject key, or the master key is not loaded
     */
    void ensureEntityKey(NVEntity nve) {
        KeyMaker km = keyMaker();
        SUS.checkIfNulls("entity guid required for its key", nve.getGUID());
        // The key rows belong to the entity's owner; a grantee writing the first sealed value must
        // still be able to mint and read them, so the key chain is walked in the system context —
        // access to the entity itself was decided before this point.
        controller().runAsSystem(() -> {
            if (km.lookupEncapsulatedKey(ds, nve.getGUID()) != null) {
                return null;
            }
            if (SUS.isEmpty(nve.getSubjectGUID())) {
                throw new AccessSecurityException("encrypted attribute on an entity without subject_guid: "
                        + nve.getNVConfig().getName() + " " + nve.getGUID());
            }
            byte[] subjectKey = km.getKey(ds, null, nve.getSubjectGUID()); // "No key for <subject>" when missing
            try {
                km.createNVEntityKey(ds, nve, subjectKey);
            } finally {
                Arrays.fill(subjectKey, (byte) 0);
            }
            return null;
        });
    }

    /**
     * The material of the entity's own key: master → owner subject key → entity key, read in the
     * system context (the key rows are the owner's; see {@link #ensureEntityKey}). Zero it after use.
     */
    byte[] entityKey(NVEntity nve) {
        SUS.checkIfNulls("guid and subject_guid required", nve.getGUID(), nve.getSubjectGUID());
        return controller().runAsSystem(() -> keyMaker().getKey(ds, null, nve.getSubjectGUID(), nve.getGUID()));
    }

    // ------------------------------------------------------------------ fields: write

    /** True when the value of an encrypted attribute needs a key to be written: a non-empty clear value. */
    static boolean needsSealing(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String) {
            String s = (String) value;
            return !s.isEmpty() && fromPackedText(s) == null;
        }
        return true;
    }

    /**
     * The bytes to bind for an encrypted scalar attribute of an <b>encrypting</b> store.
     * <ul>
     * <li>null / empty ⇒ null (clears)</li>
     * <li>masked attribute whose value equals the stored record's mask ⇒ the stored record (a
     * masked read written back keeps the secret)</li>
     * <li>else the controller seals it (access check + key chain inside) ⇒ the packed record</li>
     * </ul>
     *
     * @param stored the currently stored column bytes on update/patch, null on insert
     */
    byte[] encryptScalar(NVEntity nve, NVConfig nvc, boolean masked, NVBase<?> nvb, byte[] stored) {
        Object value = nvb != null ? nvb.getValue() : null;
        if (value == null) {
            return null;
        }
        String s = value.toString();
        if (s.isEmpty()) {
            return null;
        }
        if (masked && stored != null && isPackedRecord(stored)) {
            String storedMask = unpack(stored).getMask();
            if (s.equals(storedMask != null ? storedMask : DEFAULT_MASK)) {
                return stored;
            }
        }
        Object sealed = controller().encryptValue(ds, nve, nvc, nvb, null);
        if (sealed instanceof EncryptedData) {
            return pack((EncryptedData) sealed);
        }
        return plaintextBytes(sealed); // the controller declined to encrypt this value
    }

    // ------------------------------------------------------------------ fields: read

    /**
     * Restores an encrypted scalar from its {@code bytea} column. Bytes that are not a packed record
     * are the clear text of a non-encrypting store (UTF-8) and are set as-is. A record is opened by
     * the controller (READ access = owner-or-grant, owner's key chain); when denied the attribute
     * stays null, or shows the record's mask for {@code ENCRYPT_MASK}. Without an active
     * configuration a record is never handed to the entity — the attribute stays null and a
     * warning is logged once per type. In raw mode nothing is set (the dump moves the bytes itself).
     */
    void decryptScalar(NVEntity nve, NVConfig nvc, boolean masked, byte[] col) {
        if (col == null || isRaw()) {
            return;
        }
        NVBase<?> nvb = nve.lookup(nvc.getName());
        if (nvb == null) {
            return;
        }
        if (!isPackedRecord(col)) {
            setString(nvb, new String(col, StandardCharsets.UTF_8)); // non-encrypting store
            return;
        }
        if (!active()) {
            warnInactive(nve, nvc.getName());
            return;
        }
        EncryptedData record = unpack(col);
        try {
            controller().decryptValue(ds, nve, nvb, record, null);
        } catch (AccessSecurityException denied) {
            if (masked) {
                setMasked(nvb, record.getMask());
            }
        }
    }

    /** A schemaless container needs the crypto pass when it holds an ENCRYPT* pair with a clear value. */
    static boolean hasEncryptedPairs(NVBase<?> nvb) {
        boolean[] found = {false};
        walkPairs(nvb, p -> {
            if (isEncrypted(p.getValueFilter()) && needsSealing(p.getValue())) found[0] = true;
        });
        return found[0];
    }

    /** A schemaless container holds sealed pairs (packed records in their text carrier). */
    static boolean hasSealedPairs(NVBase<?> nvb) {
        boolean[] found = {false};
        walkPairs(nvb, p -> {
            if (isEncrypted(p.getValueFilter()) && fromPackedText(p.getValue()) != null) found[0] = true;
        });
        return found[0];
    }

    /**
     * JSON for a schemaless container whose ENCRYPT* {@link NVPair}s are sealed: the container is
     * copied through JSON (the caller's object keeps its plaintext), the copy's pairs are sealed by
     * the controller and re-serialized carrying {@link #packedText} under the bare marker filter, so
     * the reader recognizes them. Returns {@code plainJson} unchanged when nothing needs sealing.
     */
    String encodeSchemaless(NVEntity nve, NVBase<?> nvb, String plainJson) {
        if (plainJson == null || !active() || !hasEncryptedPairs(nvb)) {
            return plainJson;
        }
        NVBase<?> copy = GSONUtil.fromJSONDefault(plainJson, nvb.getClass());
        copy.setName(nvb.getName());
        walkPairs(copy, p -> {
            if (!isEncrypted(p.getValueFilter()) || !needsSealing(p.getValue())) {
                return;
            }
            Object sealed = controller().encryptValue(ds, nve, null, p, null);
            if (sealed instanceof EncryptedData) {
                // the bare marker accepts the carrier text; a chained business filter would not
                p.setValueFilter(isMasked(p.getValueFilter()) ? FilterType.ENCRYPT_MASK : FilterType.ENCRYPT);
                p.setValue(packedText(pack((EncryptedData) sealed)));
            }
        });
        return GSONUtil.toJSONDefault(copy);
    }

    /** Opens the sealed ENCRYPT* pairs of a decoded schemaless container in place (denied ⇒ null / mask). */
    void decryptSchemaless(NVEntity nve, NVBase<?> parsed) {
        if (isRaw()) {
            return; // dump: the pairs travel with their carrier text
        }
        walkPairs(parsed, p -> {
            if (!isEncrypted(p.getValueFilter())) {
                return;
            }
            byte[] packed = fromPackedText(p.getValue());
            if (packed == null) {
                return; // clear text (non-encrypting store) or empty
            }
            if (!active()) {
                warnInactive(nve, p.getName());
                p.setValue(null);
                return;
            }
            EncryptedData record = unpack(packed);
            try {
                controller().decryptValue(ds, nve, p, record, null);
            } catch (AccessSecurityException denied) {
                p.setValue(isMasked(p.getValueFilter()) ? maskOf(record) : null);
            }
        });
    }

    /** Depth-first over the {@link NVPair}s of a schemaless container (maps, pair lists, named values). */
    static void walkPairs(NVBase<?> nvb, Consumer<NVPair> visitor) {
        if (nvb == null) {
            return;
        }
        if (nvb instanceof NVPair) {
            visitor.accept((NVPair) nvb);
        } else if (nvb instanceof NamedValue) {
            walkPairs(((NamedValue<?>) nvb).getProperties(), visitor);
        } else if (nvb instanceof NVGenericMapList) {
            for (NVGenericMap m : ((NVGenericMapList) nvb).getValue()) walkPairs(m, visitor);
        } else if (nvb instanceof ArrayValues) {
            for (Object v : ((ArrayValues<?>) nvb).values()) {
                if (v instanceof NVBase) walkPairs((NVBase<?>) v, visitor);
            }
        }
    }

    // ------------------------------------------------------------------ access + files

    /**
     * The controller's verdict on a resource for the bound subject — owner (self permission) or grant;
     * a null {@code ownerGUID} is a resource without owner, reachable by a grant only. Any exception
     * counts as denied. Never compares {@code subject_guid} here.
     */
    boolean accessAllowed(String resourceGUID, String ownerGUID, CRUD crud) {
        SecurityController sc = controller();
        if (sc == null || resourceGUID == null) {
            return false;
        }
        try {
            return sc.isNVEntityAccessible(resourceGUID, ownerGUID, crud);
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** Seals file content into a VX container under the file's entity key (raw 32-byte key ⇒ HKDF). */
    byte[] encryptFile(NVEntity fileInfo, byte[] plain) {
        byte[] key = entityKey(fileInfo);
        try {
            return AESCrypt.encryptBuffer(key, plain).toByteArray();
        } catch (GeneralSecurityException | IOException e) {
            throw new APIException("file encryption failed for " + fileInfo.getGUID() + ": " + e.getMessage());
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    /**
     * Opens a VX container to {@code os} under the file's entity key; every segment is written only
     * after its tag verifies. Streams are left open (the caller owns them).
     *
     * @throws APIException on a tag failure (tampering or wrong key) or a malformed container
     */
    void decryptFile(NVEntity fileInfo, byte[] cipher, OutputStream os) throws IOException {
        byte[] key = entityKey(fileInfo);
        try {
            new AESCrypt(key).decrypt(cipher.length, new ByteArrayInputStream(cipher), os, false, false);
        } catch (AEADBadTagException e) {
            throw new APIException("file content tampered with or key mismatch: " + fileInfo.getGUID());
        } catch (GeneralSecurityException e) {
            throw new APIException("file decryption failed for " + fileInfo.getGUID() + ": " + e.getMessage());
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    // ------------------------------------------------------------------ helpers

    static String maskOf(EncryptedData record) {
        String m = record != null ? record.getMask() : null;
        return m != null ? m : DEFAULT_MASK;
    }

    @SuppressWarnings("unchecked")
    private static void setString(NVBase<?> nvb, String value) {
        ((NVBase<Object>) nvb).setValue(value);
    }

    /**
     * Shows the mask on a denied masked read. An {@link NVPair}'s instance filter is swapped to the
     * bare marker first: a chained business filter (e.g. a credit-card number filter) would reject
     * {@code ****1234}. Per instance only; a later write of that value keeps the stored record
     * ({@link #encryptScalar}).
     */
    @SuppressWarnings("unchecked")
    static void setMasked(NVBase<?> nvb, String mask) {
        String m = mask != null ? mask : DEFAULT_MASK;
        if (nvb instanceof NVPair) {
            ((NVPair) nvb).setValueFilter(FilterType.ENCRYPT_MASK);
            ((NVPair) nvb).setValue(m);
        } else {
            ((NVBase<Object>) nvb).setValue(m);
        }
    }

    private void warnInactive(NVEntity nve, String attribute) {
        String type = nve.getNVConfig().getName();
        if (warnedTypes.add(type)) {
            H2PDataStore.log.getLogger().warning("encrypted attribute " + type + "." + attribute
                    + " read without a SecurityController + KeyMaker: values left null (configure both to decrypt)");
        }
    }
}
