package io.xlogistx.datastore.h2p.test;

import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.EncryptedData;
import org.zoxweb.shared.filters.BytesValueFilter;
import org.zoxweb.shared.filters.ChainedFilter;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.security.AccessSecurityException;
import org.zoxweb.shared.security.CredentialInfo;
import org.zoxweb.shared.security.SecurityController;
import org.zoxweb.shared.util.CRUD;
import org.zoxweb.shared.util.NVBase;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.filters.ValueFilter;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Shiro-free {@link SecurityController} for the h2p tests, modelling the standardized resource
 * permission grammar (user decision 2026-09-29) without a realm:
 * <ul>
 * <li>the acting subject is {@link #currentSubject} (null = nobody bound);</li>
 * <li>every subject implicitly holds {@code resource:S:S:create,read,update,delete,share} (the
 * self permission) — so the owner of a resource passes {@code resource:<owner>:<S>:<verb>};</li>
 * <li>a share is a token {@code resource:<resource guid>:<grantee>:<verb>} in {@link #permissions};
 * {@code resource:*:*:<verb>} there is the catalog wildcard (an admin);</li>
 * <li>a resource without owner is reachable by a grant only;</li>
 * <li>{@link #runAsSystem} lifts every check for the calling thread (the system context).</li>
 * </ul>
 * Encryption follows the {@code ShiroSecurityController} contract: {@code encryptValue} returns an
 * {@link EncryptedData} (data_type + mask set before sealing) under the chain
 * master → owner subject key → entity key via {@link KeyMakerProvider#SINGLETON}; the decrypt
 * overloads open a record after a READ check and write the plaintext into the NV pair.
 */
public class TestSecurityController implements SecurityController {

    public static volatile String currentSubject;
    public static final Set<String> permissions = ConcurrentHashMap.newKeySet();
    private static final Set<String> SELF_VERBS = Set.of("create", "read", "update", "delete", "share");
    private static final ThreadLocal<int[]> SYSTEM_DEPTH = new ThreadLocal<>();

    public static <V> V system(Supplier<V> work) {
        int[] depth = SYSTEM_DEPTH.get();
        if (depth == null) {
            depth = new int[1];
            SYSTEM_DEPTH.set(depth);
        }
        depth[0]++;
        try {
            return work.get();
        } finally {
            if (--depth[0] == 0) SYSTEM_DEPTH.remove();
        }
    }

    public static void systemRun(Runnable work) {
        system(() -> {
            work.run();
            return null;
        });
    }

    public static boolean inSystem() {
        int[] depth = SYSTEM_DEPTH.get();
        return depth != null && depth[0] > 0;
    }

    public static String token(String resourceGUID, String subjectGUID, String verb) {
        return ("resource:" + resourceGUID + ":" + subjectGUID + ":" + verb).toLowerCase();
    }

    public static void grant(String resourceGUID, String subjectGUID, String... verbs) {
        for (String v : verbs) permissions.add(token(resourceGUID, subjectGUID, v));
    }

    public static void revokeAll() {
        permissions.clear();
    }

    /** owner token (self permission) first, then the grant token — never a bare equality on its own. */
    public static boolean permitted(String resourceGUID, String ownerGUID, String verb) {
        if (inSystem()) return true;
        String s = currentSubject;
        if (s == null || resourceGUID == null) return false;
        // resource:<owner>:<S>:<verb> implied by resource:S:S:create,read,update,delete,share
        if (ownerGUID != null && s.equalsIgnoreCase(ownerGUID) && SELF_VERBS.contains(verb)) return true;
        if (ownerGUID != null && permissions.contains(token(ownerGUID, s, verb))) return true;
        return permissions.contains(token(resourceGUID, s, verb)) || permissions.contains(token("*", "*", verb));
    }

    /** @return the owner's GUID (key-chain root) when any of the verbs is held */
    private static String checkAccess(NVEntity nve, String... anyVerb) {
        if (nve.getGUID() == null || nve.getSubjectGUID() == null) {
            throw new AccessSecurityException("resource without guid/subject_guid");
        }
        for (String v : anyVerb) {
            if (permitted(nve.getGUID(), nve.getSubjectGUID(), v)) return nve.getSubjectGUID();
        }
        throw new AccessSecurityException("Access denied " + token(nve.getGUID(), String.valueOf(currentSubject), anyVerb[0]));
    }

    private static boolean encrypted(ValueFilter<?, ?> f) {
        return ChainedFilter.isFilterSupported(f, FilterType.ENCRYPT) || ChainedFilter.isFilterSupported(f, FilterType.ENCRYPT_MASK);
    }

    private static boolean masked(ValueFilter<?, ?> f) {
        return ChainedFilter.isFilterSupported(f, FilterType.ENCRYPT_MASK);
    }

    @Override
    public void validateCredential(CredentialInfo ci, String input) {
    }

    @Override
    public void validateCredential(CredentialInfo ci, byte[] input) {
    }

    @Override
    public Object encryptValue(APIDataStore<?, ?> dataStore, NVEntity container, NVConfig nvc, NVBase<?> nvb, byte[] msKey) {
        boolean enc, mask;
        if (nvb instanceof NVPair && encrypted(((NVPair) nvb).getValueFilter())) {
            enc = true;
            mask = masked(((NVPair) nvb).getValueFilter());
        } else if (nvc != null && encrypted(nvc.getValueFilter())) {
            enc = true;
            mask = masked(nvc.getValueFilter());
        } else {
            return nvb.getValue();
        }
        if (!enc || nvb.getValue() == null) return nvb.getValue();
        String owner = checkAccess(container, "update", "create");
        // the key rows are the owner's: walked in the system context, after the check on the entity
        byte[] key = system(() -> KeyMakerProvider.SINGLETON.getKey(dataStore, msKey, owner, container.getGUID()));
        try {
            EncryptedData record = new EncryptedData();
            record.setDataType(String.class.getName());
            if (mask) {
                String clear = String.valueOf(nvb.getValue());
                record.setMask(clear.length() >= 8 ? "****" + clear.substring(clear.length() - 4) : "****");
            }
            return CryptoUtil.encryptData(record, key, BytesValueFilter.SINGLETON.validate(nvb));
        } catch (GeneralSecurityException e) {
            throw new AccessSecurityException(e.getMessage());
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static byte[] open(APIDataStore<?, ?> ds, NVEntity container, EncryptedData record, byte[] msKey, String userID) {
        String owner = userID != null ? userID : checkAccess(container, "read");
        byte[] key = system(() -> KeyMakerProvider.SINGLETON.getKey(ds, msKey, owner, container.getGUID()));
        try {
            return CryptoUtil.decryptEncryptedData(record, key);
        } catch (GeneralSecurityException e) {
            throw new AccessSecurityException(e.getMessage());
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    @Override
    public Object decryptValue(APIDataStore<?, ?> dataStore, NVEntity container, NVBase<?> nvb, Object value, byte[] msKey) {
        if (!(value instanceof EncryptedData)) return value;
        byte[] plain = open(dataStore, container, (EncryptedData) value, msKey, null);
        String s = new String(plain, StandardCharsets.UTF_8);
        if (nvb instanceof NVPair) ((NVPair) nvb).setValue(s);
        else ((NVBase<Object>) nvb).setValue(s);
        return s;
    }

    @Override
    public String decryptValue(APIDataStore<?, ?> dataStore, NVEntity container, byte[] value, byte[] msKey) {
        if (value == null) return null;
        // the storage form: the packed record (CipherCodecs), never canonical text
        EncryptedData record = org.zoxweb.server.security.CipherCodecs.EDDecoder.decode(value);
        return new String(open(dataStore, container, record, msKey, null), StandardCharsets.UTF_8);
    }

    @Override
    public Object decryptValue(String userID, APIDataStore<?, ?> dataStore, NVEntity container, Object value, byte[] msKey) {
        if (!(value instanceof EncryptedData)) return value;
        return new String(open(dataStore, container, (EncryptedData) value, msKey, userID), StandardCharsets.UTF_8);
    }

    @Override
    public NVEntity decryptValues(APIDataStore<?, ?> dataStore, NVEntity container, byte[] msKey) {
        return container; // the store opens sealed values on read; a pair cannot hold a packed record
    }

    @Override
    public void associateNVEntityToSubjectGUID(NVEntity nve, String subjectGUID) {
        if (nve.getSubjectGUID() == null) {
            String s = subjectGUID != null ? subjectGUID : currentSubject;
            if (s != null) nve.setSubjectGUID(s);
        }
    }

    @Override
    public String currentSubjectID() {
        return currentSubject;
    }

    @Override
    public String currentSubjectGUID() {
        return currentSubject;
    }

    @Override
    public <V> V runAsSystem(Supplier<V> work) {
        return system(work);
    }

    @Override
    public boolean isSystemContext() {
        return inSystem();
    }

    @Override
    public boolean isNVEntityAccessible(String nveRefID, String nveUserID, CRUD... crud) {
        if (crud == null || crud.length == 0) return false;
        for (CRUD c : crud) {
            if (!permitted(nveRefID, nveUserID, c.name().toLowerCase())) return false;
        }
        return true;
    }
}
