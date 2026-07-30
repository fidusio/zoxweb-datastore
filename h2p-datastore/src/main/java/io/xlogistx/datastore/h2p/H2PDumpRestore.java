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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.FileInfoDAO;
import org.zoxweb.shared.io.SharedIOUtil;
import org.zoxweb.shared.util.ArrayValues;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.NVBase;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.SharedBase64;
import org.zoxweb.shared.util.SharedBase64.Base64Type;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * JSON dump/restore engine behind {@link H2PDataStore#dump(NVConfigEntity, java.io.OutputStream)},
 * {@link H2PDataStore#dump(java.io.OutputStream, boolean, NVConfigEntity...)} and
 * {@link H2PDataStore#restore}. Engine-portable (the same stream moves data H2 ↔ PostgreSQL, or
 * into any store that can consume the entity JSON) — not a same-engine backup tool.
 *
 * <p><b>Format — JSONL</b> (one JSON envelope per line, streamed both ways so memory stays bounded
 * by one batch / one entity graph):
 * <pre>
 * {"k":"header","v":{"format":"h2p-json-dump","version":1,"ts":...,"ds_type":"H2"}}
 * {"k":"entity","v":{...GSONUtil.toJSON(nve, printClassType=true)...}}
 * {"k":"dem","v":{...GSONUtil.toJSONDynamicEnumMap...}}
 * {"k":"seq","v":{"name":...,"value":...,"increment":...}}
 * {"k":"file_version","v":{"file_guid":...,"version":...,"length":...,"created_ts":...,"content":"&lt;b64&gt;"}}
 * {"k":"file_head","v":{"file_guid":...,"current_version":...}}
 * </pre>
 *
 * <p><b>Entity semantics.</b> {@code GSONUtil} inlines referenced entities, so every entity line
 * carries its full subtree and restore has no cross-line ordering constraints; shared children
 * appear redundantly (in each parent and as their own table's line) and converge on restore because
 * {@code insert} upserts by GUID. The flip side: a <b>cyclic</b> reference graph cannot be
 * represented in this JSON at all — cyclic entities are detected up front ({@link #hasCycle}) and
 * skipped, counted in the stats as {@code cycles_skipped}.
 */
final class H2PDumpRestore {

    static final String FORMAT = "h2p-json-dump";
    static final int FORMAT_VERSION = 1;
    /** Entities per read page on dump and per transaction on restore. */
    static final int DEFAULT_BATCH_SIZE = 256;

    // envelope fields and record kinds
    static final String K = "k";
    static final String V = "v";
    static final String KIND_HEADER = "header";
    static final String KIND_ENTITY = "entity";
    static final String KIND_DEM = "dem";
    static final String KIND_SEQ = "seq";
    static final String KIND_FILE_VERSION = "file_version";
    static final String KIND_FILE_HEAD = "file_head";

    // zip container layout
    static final String JSONL_ENTRY = "dump.jsonl";
    static final String FILES_PREFIX = "files/";

    private final H2PDataStore ds;

    H2PDumpRestore(H2PDataStore ds) {
        this.ds = ds;
    }

    // ---------- Dump ----------

    long dumpType(NVConfigEntity nvce, OutputStream out) {
        Writer w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        try {
            long[] r = writeEntities(nvce, w);
            w.flush();
            return r[0];
        } catch (IOException e) {
            throw wrap("dump failed for " + nvce.getName(), e);
        }
    }

    String dumpTypeToJSONArray(NVConfigEntity nvce) {
        List<NVEntity> all = new ArrayList<>();
        forEachStored(nvce, all::add, null);
        try {
            return GSONUtil.toJSONArray(all, false, false, null);
        } catch (IOException e) {
            throw wrap("dump failed for " + nvce.getName(), e);
        }
    }

    NVGenericMap dumpStore(OutputStream out, boolean includeFiles, NVConfigEntity... types) {
        Writer w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        try {
            NVGenericMap stats = writeStore(w, includeFiles, true, types);
            w.flush();
            return stats;
        } catch (IOException e) {
            throw wrap("dump failed", e);
        }
    }

    /**
     * Zip-container dump: entry {@value #JSONL_ENTRY} holds the JSONL stream (identical to
     * {@link #dumpStore} except {@code file_version} records carry an {@code entry} name instead of
     * inline base64 content), followed by one raw {@code files/<file_guid>/<version>} entry per
     * stored file version — content deflated by the zip layer, no base64 inflation. The zip is
     * {@code finish()}ed but the underlying stream is not closed.
     */
    NVGenericMap dumpZip(OutputStream out, boolean includeFiles, NVConfigEntity... types) {
        try {
            ZipOutputStream zos = new ZipOutputStream(out, StandardCharsets.UTF_8);
            zos.putNextEntry(new ZipEntry(JSONL_ENTRY));
            Writer w = new BufferedWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8));
            NVGenericMap stats = writeStore(w, includeFiles, false, types);
            w.flush(); // flush the writer, never close it — that would close the zip stream
            zos.closeEntry();
            if (includeFiles) {
                writeZipContentEntries(zos);
            }
            zos.finish(); // finalize the central directory without closing the caller's stream
            return stats;
        } catch (IOException e) {
            throw wrap("zip dump failed", e);
        }
    }

    /**
     * Writes the whole store as JSONL to {@code w}. {@code inlineContent} selects how file versions
     * carry their bytes: base64 {@code content} inline (self-contained stream) or an {@code entry}
     * pointer into the surrounding zip container.
     */
    private NVGenericMap writeStore(Writer w, boolean includeFiles, boolean inlineContent,
                                    NVConfigEntity... types) throws IOException {
        NVGenericMap stats = new NVGenericMap("dump_stats");
        {
            JsonObject header = new JsonObject();
            header.addProperty("format", FORMAT);
            header.addProperty("version", FORMAT_VERSION);
            header.addProperty("ts", System.currentTimeMillis());
            header.addProperty("ds_type", String.valueOf(ds.getDSType()));
            writeLine(w, KIND_HEADER, header);

            Map<String, NVConfigEntity> typeSet = new LinkedHashMap<>();
            if (types != null && types.length > 0) {
                for (NVConfigEntity t : types) {
                    if (t != null) typeSet.putIfAbsent(t.getName().toLowerCase(), t);
                }
            } else {
                for (NVConfigEntity t : ds.discoverStoreTypes()) {
                    typeSet.putIfAbsent(t.getName().toLowerCase(), t);
                }
            }
            // File content without its FileInfoDAO metadata rows would restore incoherently.
            if (includeFiles) {
                typeSet.putIfAbsent(FileInfoDAO.NVC_FILE_INFO_DAO.getName().toLowerCase(),
                        FileInfoDAO.NVC_FILE_INFO_DAO);
            }

            long cyclesSkipped = 0;
            NVGenericMap typeStats = new NVGenericMap("types");
            for (NVConfigEntity nvce : typeSet.values()) {
                long[] r = writeEntities(nvce, w);
                typeStats.build(new NVLong(nvce.getName(), r[0]));
                cyclesSkipped += r[1];
            }
            stats.build(typeStats);

            long dems = 0;
            for (DynamicEnumMap dem : ds.getAllDynamicEnumMap(null, null)) {
                writeLine(w, KIND_DEM, JsonParser.parseString(GSONUtil.toJSONDynamicEnumMap(dem)));
                dems++;
            }
            stats.build(new NVLong("dem", dems));
            stats.build(new NVLong("sequences", dumpSequences(w)));
            if (includeFiles) {
                long[] f = dumpFiles(w, inlineContent);
                stats.build(new NVLong("file_versions", f[0]));
                stats.build(new NVLong("file_heads", f[1]));
            }
            stats.build(new NVLong("cycles_skipped", cyclesSkipped));
            return stats;
        }
    }

    /** Streams every stored entity of the type as envelope lines. @return {written, cyclesSkipped} */
    private long[] writeEntities(NVConfigEntity nvce, Writer w) throws IOException {
        long[] counts = new long[2];
        try {
            forEachStored(nvce, nve ->
                            writeLine(w, KIND_ENTITY,
                                    JsonParser.parseString(GSONUtil.toJSON(nve, false, false, true, null))),
                    counts);
        } catch (UncheckedIOException e) {
            throw e.cause;
        }
        return counts;
    }

    private static final class UncheckedIOException extends RuntimeException {
        final IOException cause;

        UncheckedIOException(IOException cause) {
            this.cause = cause;
        }
    }

    @FunctionalInterface
    private interface EntitySink {
        void accept(NVEntity nve) throws IOException;
    }

    /**
     * Pages through every stored row of the type ({@code batchSearch} guid report + {@code nextBatch}
     * — deterministic order, memory bounded by one batch) and feeds acyclic entities to {@code sink}.
     * The page size is clamped to {@code MAX_SELECT_RESULTS} when that valve is set, otherwise the
     * id-list fetch behind {@code nextBatch} would be silently LIMIT-truncated.
     */
    private void forEachStored(NVConfigEntity nvce, EntitySink sink, long[] counts) {
        int batch = DEFAULT_BATCH_SIZE;
        int maxResults = ds.intParam(H2PDSCreator.H2PParam.MAX_SELECT_RESULTS, 0);
        if (maxResults > 0) batch = Math.min(batch, maxResults);
        APISearchResult<Object> report = ds.batchSearch(nvce);
        for (int i = 0; i < report.size(); i += batch) {
            APIBatchResult<NVEntity> page = ds.nextBatch(report, i, batch);
            if (page == null) break;
            for (NVEntity nve : page.getBatch()) {
                if (hasCycle(nve)) {
                    if (counts != null) counts[1]++;
                    if (H2PDataStore.log.isEnabled()) {
                        H2PDataStore.log.getLogger().warning("cyclic entity not JSON-representable, skipped: "
                                + nvce.getName() + " " + nve.getGUID());
                    }
                    continue;
                }
                try {
                    sink.accept(nve);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (counts != null) counts[0]++;
            }
        }
    }

    private long dumpSequences(Writer w) throws IOException {
        long n = 0;
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = ds.newConnection();
            if (!ds.rawTableExists(con, H2PDataStore.SEQ_TABLE)) return 0;
            st = con.createStatement();
            rs = st.executeQuery("SELECT " + H2PUtil.q("name") + ", " + H2PUtil.q("seq_value") + ", "
                    + H2PUtil.q("increment_value") + " FROM " + H2PUtil.q(H2PDataStore.SEQ_TABLE));
            while (rs.next()) {
                JsonObject o = new JsonObject();
                o.addProperty("name", rs.getString(1));
                o.addProperty("value", rs.getLong(2));
                o.addProperty("increment", rs.getLong(3));
                writeLine(w, KIND_SEQ, o);
                n++;
            }
        } catch (SQLException e) {
            throw wrap("sequence dump failed", e);
        } finally {
            SharedIOUtil.close(rs, st, con);
        }
        return n;
    }

    /** Zip entry name for one stored file version: {@code files/<file_guid>/<version>}. */
    private static String contentEntryName(String fileGuid, long version) {
        return FILES_PREFIX + fileGuid + "/" + version;
    }

    /**
     * Streams the versioned file content records: one line per stored version, then the head
     * pointers. {@code inline} = base64 {@code content} in the line (one version's bytes in memory
     * at a time — same bound as the file API itself); otherwise the line only carries the zip
     * {@code entry} name and the bytes follow as their own zip entries
     * ({@link #writeZipContentEntries}).
     * @return {versions, heads}
     */
    private long[] dumpFiles(Writer w, boolean inline) throws IOException {
        long[] counts = new long[2];
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = ds.newConnection();
            if (!ds.rawTableExists(con, H2PDataStore.FILE_VERSION_TABLE)) return counts;
            st = con.createStatement();
            rs = st.executeQuery("SELECT " + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version") + ", "
                    + H2PUtil.q("length") + ", " + H2PUtil.q("created_ts")
                    + (inline ? ", " + H2PUtil.q("data") : "")
                    + " FROM " + H2PUtil.q(H2PDataStore.FILE_VERSION_TABLE)
                    + " ORDER BY " + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version"));
            while (rs.next()) {
                String fileGuid = String.valueOf(rs.getObject(1, UUID.class));
                long version = rs.getLong(2);
                JsonObject o = new JsonObject();
                o.addProperty("file_guid", fileGuid);
                o.addProperty("version", version);
                o.addProperty("length", rs.getLong(3));
                o.addProperty("created_ts", rs.getLong(4));
                if (inline) {
                    o.addProperty("content", SharedBase64.encodeAsString(Base64Type.DEFAULT, rs.getBytes(5)));
                } else {
                    o.addProperty("entry", contentEntryName(fileGuid, version));
                }
                writeLine(w, KIND_FILE_VERSION, o);
                counts[0]++;
            }
            SharedIOUtil.close(rs, st);
            rs = null;
            st = con.createStatement();
            rs = st.executeQuery("SELECT " + H2PUtil.q("file_guid") + ", " + H2PUtil.q("current_version")
                    + " FROM " + H2PUtil.q(H2PDataStore.FILE_HEAD_TABLE));
            while (rs.next()) {
                JsonObject o = new JsonObject();
                o.addProperty("file_guid", String.valueOf(rs.getObject(1, UUID.class)));
                o.addProperty("current_version", rs.getLong(2));
                writeLine(w, KIND_FILE_HEAD, o);
                counts[1]++;
            }
        } catch (SQLException e) {
            throw wrap("file dump failed", e);
        } finally {
            SharedIOUtil.close(rs, st, con);
        }
        return counts;
    }

    /** Second pass of the zip dump: one raw content entry per stored version, one row in memory at a time. */
    private void writeZipContentEntries(ZipOutputStream zos) throws IOException {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = ds.newConnection();
            if (!ds.rawTableExists(con, H2PDataStore.FILE_VERSION_TABLE)) return;
            st = con.createStatement();
            rs = st.executeQuery("SELECT " + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version") + ", "
                    + H2PUtil.q("data") + " FROM " + H2PUtil.q(H2PDataStore.FILE_VERSION_TABLE)
                    + " ORDER BY " + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version"));
            while (rs.next()) {
                zos.putNextEntry(new ZipEntry(
                        contentEntryName(String.valueOf(rs.getObject(1, UUID.class)), rs.getLong(2))));
                zos.write(rs.getBytes(3));
                zos.closeEntry();
            }
        } catch (SQLException e) {
            throw wrap("zip content dump failed", e);
        } finally {
            SharedIOUtil.close(rs, st, con);
        }
    }

    private static void writeLine(Writer w, String kind, JsonElement payload) throws IOException {
        JsonObject env = new JsonObject();
        env.addProperty(K, kind);
        env.add(V, payload);
        w.write(env.toString()); // JsonElement.toString() is compact — one line per envelope
        w.write('\n');
    }

    // ---------- Cycle detection ----------

    /**
     * True when the entity's reference graph (single refs + collections) reaches itself. The read
     * path materializes a cycle as the <b>same instance</b> (per-call GUID cache), so identity-based
     * DFS is exact; {@code done} keeps shared-child diamonds from re-walking.
     */
    static boolean hasCycle(NVEntity root) {
        return root != null && hasCycle(root,
                Collections.newSetFromMap(new IdentityHashMap<>()),
                Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    @SuppressWarnings("unchecked")
    private static boolean hasCycle(NVEntity nve, Set<NVEntity> path, Set<NVEntity> done) {
        if (done.contains(nve)) return false;
        if (!path.add(nve)) return true;
        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        for (NVConfig nvc : nvce.getAttributes()) {
            H2PUtil.AttrKind kind = H2PUtil.classify(nvc);
            if (kind == H2PUtil.AttrKind.ENTITY_REF) {
                NVBase<?> nvb = nve.lookup(nvc.getName());
                Object child = nvb != null ? nvb.getValue() : null;
                if (child instanceof NVEntity && hasCycle((NVEntity) child, path, done)) return true;
            } else if (kind == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                ArrayValues<NVEntity> av = (ArrayValues<NVEntity>) nve.lookup(nvc.getName());
                if (av != null) {
                    for (NVEntity child : av.values()) {
                        if (child != null && hasCycle(child, path, done)) return true;
                    }
                }
            }
        }
        path.remove(nve);
        done.add(nve);
        return false;
    }

    // ---------- Restore ----------

    /**
     * Restores a dump, auto-detecting the container: a stream starting with the zip magic
     * ({@code PK}) is a {@link #dumpZip} archive ({@value #JSONL_ENTRY} + raw content entries);
     * anything else is a plain JSONL stream with inline base64 file content.
     */
    NVGenericMap restore(InputStream in, H2PDataStore.RestoreMode mode) {
        BufferedInputStream bin = in instanceof BufferedInputStream
                ? (BufferedInputStream) in : new BufferedInputStream(in);
        try {
            bin.mark(2);
            int b1 = bin.read();
            int b2 = bin.read();
            bin.reset();
            if (b1 == 'P' && b2 == 'K') {
                return restoreZip(bin, mode);
            }
        } catch (IOException e) {
            throw wrap("restore failed probing the stream", e);
        }
        return restoreJsonl(bin, mode);
    }

    /**
     * Zip-container restore. The zip is consumed sequentially: the leading {@value #JSONL_ENTRY}
     * entry loads exactly like a plain JSONL restore, except {@code file_version} records that
     * reference a content {@code entry} are held as pending metadata; the following raw content
     * entries then complete them one at a time (one version's bytes in memory). Versions still
     * pending when the archive ends mean a truncated/foreign zip — the restore fails rather than
     * leave files without content (already-committed batches keep MERGE re-runs convergent).
     */
    private NVGenericMap restoreZip(InputStream in, H2PDataStore.RestoreMode mode) {
        try {
            ZipInputStream zis = new ZipInputStream(in, StandardCharsets.UTF_8);
            ZipEntry first = zis.getNextEntry();
            if (first == null || !JSONL_ENTRY.equals(first.getName())) {
                throw new APIException("not a " + FORMAT + " zip: first entry must be " + JSONL_ENTRY
                        + " (found " + (first != null ? first.getName() : "empty archive") + ")");
            }
            Map<String, JsonObject> pending = new HashMap<>();
            // The reader stops at the entry boundary (ZipInputStream reports EOF per entry) and is
            // intentionally never closed — closing it would close the whole zip stream.
            NVGenericMap stats = restoreJsonl(
                    new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8)), mode, pending);
            long versions = 0;
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                JsonObject meta = pending.remove(entry.getName());
                if (meta == null) {
                    if (H2PDataStore.log.isEnabled()) {
                        H2PDataStore.log.getLogger().warning(
                                "zip entry not referenced by the dump — skipped: " + entry.getName());
                    }
                    continue;
                }
                ByteArrayOutputStream content = new ByteArrayOutputStream();
                byte[] buf = new byte[64 * 1024];
                int n;
                while ((n = zis.read(buf)) > 0) content.write(buf, 0, n);
                restoreFileVersion(meta, content.toByteArray());
                versions++;
            }
            if (!pending.isEmpty()) {
                throw new APIException("zip archive is missing " + pending.size()
                        + " file content entr" + (pending.size() == 1 ? "y" : "ies")
                        + " referenced by the dump, e.g. " + pending.keySet().iterator().next());
            }
            long inline = stats.getValue("file_versions", 0L); // records that carried base64 inline
            stats.build(new NVLong("file_versions", inline + versions));
            return stats;
        } catch (IOException e) {
            throw wrap("zip restore failed", e);
        }
    }

    private NVGenericMap restoreJsonl(InputStream in, H2PDataStore.RestoreMode mode) {
        return restoreJsonl(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)), mode, null);
    }

    /**
     * Core JSONL restore loop. {@code pendingVersions} non-null = zip mode: {@code file_version}
     * records carrying an {@code entry} pointer are parked there (keyed by entry name) for the
     * content pass instead of being applied; inline-content records apply immediately either way.
     */
    private NVGenericMap restoreJsonl(BufferedReader r, H2PDataStore.RestoreMode mode,
                                      Map<String, JsonObject> pendingVersions) {
        long[] counts = new long[5]; // entities, dem, sequences, file_versions, file_heads
        boolean wipe = mode == H2PDataStore.RestoreMode.WIPE_AND_LOAD;
        long lineNo = 0;
        boolean txOpen = false;
        int txCount = 0;
        boolean wiped = false;
        try {
            String line;
            while ((line = r.readLine()) != null) {
                lineNo++;
                if (line.trim().isEmpty()) continue;
                JsonObject env = JsonParser.parseString(line).getAsJsonObject();
                String kind = env.get(K).getAsString();
                JsonElement v = env.get(V);
                if (KIND_HEADER.equals(kind)) {
                    JsonObject h = v.getAsJsonObject();
                    String format = h.has("format") ? h.get("format").getAsString() : null;
                    if (!FORMAT.equals(format)) {
                        throw new APIException("not a " + FORMAT + " stream: " + format);
                    }
                    int version = h.has("version") ? h.get("version").getAsInt() : -1;
                    if (version > FORMAT_VERSION) {
                        throw new APIException("unsupported dump version " + version
                                + " (this store reads up to " + FORMAT_VERSION + ")");
                    }
                    continue;
                }
                // Wipe once, before the first data record (a header-less per-type dump wipes too —
                // WIPE_AND_LOAD always means "clear the discoverable store first").
                if (wipe && !wiped) {
                    wipeStore();
                    wiped = true;
                }
                if (KIND_ENTITY.equals(kind)) {
                    if (!txOpen) {
                        ds.beginTransaction();
                        txOpen = true;
                        txCount = 0;
                    }
                    ds.insert(GSONUtil.fromJSON(v.toString()));
                    counts[0]++;
                    if (++txCount >= DEFAULT_BATCH_SIZE) {
                        ds.endTransaction();
                        txOpen = false;
                    }
                } else {
                    // Non-entity records run outside the batch transaction: sequences are
                    // contractually non-transactional, and the file-content SQL runs on its own
                    // connection, which must see the already-committed FileInfoDAO rows.
                    if (txOpen) {
                        ds.endTransaction();
                        txOpen = false;
                    }
                    switch (kind) {
                        case KIND_DEM:
                            ds.insertDynamicEnumMap(GSONUtil.fromJSONDynamicEnumMap(v.toString()));
                            counts[1]++;
                            break;
                        case KIND_SEQ:
                            restoreSequence(v.getAsJsonObject(), wipe);
                            counts[2]++;
                            break;
                        case KIND_FILE_VERSION: {
                            JsonObject fv = v.getAsJsonObject();
                            if (fv.has("content")) {
                                restoreFileVersion(fv, SharedBase64.decode(
                                        Base64Type.DEFAULT, fv.get("content").getAsString()));
                                counts[3]++;
                            } else if (fv.has("entry") && pendingVersions != null) {
                                // zip mode: the raw bytes follow as their own archive entry
                                pendingVersions.put(fv.get("entry").getAsString(), fv);
                            } else {
                                throw new APIException("file_version record without inline content"
                                        + " at line " + lineNo
                                        + " — this dump references external content entries;"
                                        + " restore it from its zip archive");
                            }
                            break;
                        }
                        case KIND_FILE_HEAD:
                            restoreFileHead(v.getAsJsonObject());
                            counts[4]++;
                            break;
                        default:
                            if (H2PDataStore.log.isEnabled()) {
                                H2PDataStore.log.getLogger().warning("unknown dump record kind '"
                                        + kind + "' at line " + lineNo + " — skipped");
                            }
                            break;
                    }
                }
            }
            if (txOpen) {
                ds.endTransaction();
                txOpen = false;
            }
        } catch (Exception e) {
            if (txOpen) {
                try {
                    ds.abortTransaction();
                } catch (RuntimeException ignore) {
                    // surface the original failure
                }
            }
            if (e instanceof APIException) throw (APIException) e;
            throw wrap("restore failed at line " + lineNo, e);
        }
        return new NVGenericMap("restore_stats")
                .build(new NVLong("entities", counts[0]))
                .build(new NVLong("dem", counts[1]))
                .build(new NVLong("sequences", counts[2]))
                .build(new NVLong("file_versions", counts[3]))
                .build(new NVLong("file_heads", counts[4]));
    }

    /**
     * WIPE_AND_LOAD ground clearing over every discoverable type: collection join tables first,
     * then file content, then every ENTITY_REF column nulled (breaks FK reference chains and cycles
     * portably — no H2-only {@code SET REFERENTIAL_INTEGRITY}), then the entity rows, DEM and
     * sequences. Types not discoverable (pre-catalog databases) are not touched.
     */
    private void wipeStore() {
        List<NVConfigEntity> types = ds.discoverStoreTypes();
        Connection con = null;
        Statement st = null;
        try {
            con = ds.newConnection();
            st = con.createStatement();
            for (NVConfigEntity nvce : types) {
                for (NVConfig nvc : nvce.getAttributes()) {
                    if (H2PUtil.classify(nvc) == H2PUtil.AttrKind.ENTITY_COLLECTION) {
                        String jt = H2PUtil.sqlName(nvce.getName() + "__" + nvc.getName());
                        if (ds.rawTableExists(con, jt)) st.executeUpdate("DELETE FROM " + H2PUtil.q(jt));
                    }
                }
            }
            // file content references file_info_dao rows — clear before the entity rows go
            if (ds.rawTableExists(con, H2PDataStore.FILE_VERSION_TABLE)) {
                st.executeUpdate("DELETE FROM " + H2PUtil.q(H2PDataStore.FILE_VERSION_TABLE));
            }
            if (ds.rawTableExists(con, H2PDataStore.FILE_HEAD_TABLE)) {
                st.executeUpdate("DELETE FROM " + H2PUtil.q(H2PDataStore.FILE_HEAD_TABLE));
            }
            for (NVConfigEntity nvce : types) {
                String table = H2PDataStore.tableName(nvce);
                if (!ds.rawTableExists(con, table)) continue;
                for (NVConfig nvc : nvce.getAttributes()) {
                    if (H2PUtil.classify(nvc) == H2PUtil.AttrKind.ENTITY_REF) {
                        st.executeUpdate("UPDATE " + H2PUtil.q(table)
                                + " SET " + H2PUtil.q(nvc.getName()) + " = NULL");
                    }
                }
            }
            for (NVConfigEntity nvce : types) {
                String table = H2PDataStore.tableName(nvce);
                if (ds.rawTableExists(con, table)) st.executeUpdate("DELETE FROM " + H2PUtil.q(table));
            }
            if (ds.rawTableExists(con, H2PDataStore.DEM_TABLE)) {
                st.executeUpdate("DELETE FROM " + H2PUtil.q(H2PDataStore.DEM_TABLE));
            }
            if (ds.rawTableExists(con, H2PDataStore.SEQ_TABLE)) {
                st.executeUpdate("DELETE FROM " + H2PUtil.q(H2PDataStore.SEQ_TABLE));
            }
        } catch (SQLException e) {
            throw wrap("wipe failed", e);
        } finally {
            SharedIOUtil.close(st, con);
        }
    }

    /**
     * Seeds the sequence when absent, then applies the dumped value: unconditionally after a wipe,
     * raise-only on MERGE ({@code seq_value < dumped}) — a merge must never lower a live sequence,
     * or previously issued values would be re-issued. Dedicated auto-commit connection, matching the
     * store's non-transactional sequence contract.
     */
    private void restoreSequence(JsonObject o, boolean wipe) {
        String name = o.get("name").getAsString();
        long value = o.get("value").getAsLong();
        long increment = o.get("increment").getAsLong();
        ds.createSequence(name, value, increment);
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ds.newConnection();
            String sql = "UPDATE " + H2PUtil.q(H2PDataStore.SEQ_TABLE) + " SET "
                    + H2PUtil.q("seq_value") + " = ?, " + H2PUtil.q("increment_value") + " = ?"
                    + " WHERE " + H2PUtil.q("name") + " = ?"
                    + (wipe ? "" : " AND " + H2PUtil.q("seq_value") + " < ?");
            ps = con.prepareStatement(sql);
            ps.setLong(1, value);
            ps.setLong(2, increment);
            ps.setString(3, name.toLowerCase());
            if (!wipe) ps.setLong(4, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw wrap("sequence restore failed: " + name, e);
        } finally {
            SharedIOUtil.close(ps, con);
        }
    }

    /**
     * Upserts one stored file version preserving its number ({@code createFile} would renumber).
     * The FileInfoDAO metadata row already exists — entity lines precede file lines in the dump and
     * their batch transaction is committed before any file record is processed.
     */
    private void restoreFileVersion(JsonObject o, byte[] data) {
        ds.ensureFileTables();
        UUID guid = UUID.fromString(o.get("file_guid").getAsString());
        long version = o.get("version").getAsLong();
        long length = o.get("length").getAsLong();
        long createdTs = o.get("created_ts").getAsLong();
        Connection con = null;
        PreparedStatement upd = null;
        PreparedStatement ins = null;
        try {
            con = ds.newConnection();
            upd = con.prepareStatement("UPDATE " + H2PUtil.q(H2PDataStore.FILE_VERSION_TABLE) + " SET "
                    + H2PUtil.q("length") + " = ?, " + H2PUtil.q("created_ts") + " = ?, "
                    + H2PUtil.q("data") + " = ? WHERE " + H2PUtil.q("file_guid") + " = ? AND "
                    + H2PUtil.q("version") + " = ?");
            upd.setLong(1, length);
            upd.setLong(2, createdTs);
            upd.setBytes(3, data);
            upd.setObject(4, guid);
            upd.setLong(5, version);
            if (upd.executeUpdate() == 0) {
                try {
                    ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(H2PDataStore.FILE_VERSION_TABLE) + " ("
                            + H2PUtil.q("file_guid") + ", " + H2PUtil.q("version") + ", " + H2PUtil.q("length") + ", "
                            + H2PUtil.q("created_ts") + ", " + H2PUtil.q("data") + ") VALUES (?, ?, ?, ?, ?)");
                    ins.setObject(1, guid);
                    ins.setLong(2, version);
                    ins.setLong(3, length);
                    ins.setLong(4, createdTs);
                    ins.setBytes(5, data);
                    ins.executeUpdate();
                } catch (SQLException e) {
                    // Concurrent writer took the slot — the row exists now, apply the dump's content.
                    if (!"23505".equals(e.getSQLState())) throw e;
                    upd.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw wrap("file version restore failed: " + guid + " v" + version, e);
        } finally {
            SharedIOUtil.close(ins, upd, con);
        }
    }

    /** Repoints (or creates) a file's head at the dumped current version — DEM-style portable upsert. */
    private void restoreFileHead(JsonObject o) {
        ds.ensureFileTables();
        UUID guid = UUID.fromString(o.get("file_guid").getAsString());
        long version = o.get("current_version").getAsLong();
        Connection con = null;
        PreparedStatement upd = null;
        PreparedStatement ins = null;
        try {
            con = ds.newConnection();
            upd = con.prepareStatement("UPDATE " + H2PUtil.q(H2PDataStore.FILE_HEAD_TABLE) + " SET "
                    + H2PUtil.q("current_version") + " = ? WHERE " + H2PUtil.q("file_guid") + " = ?");
            upd.setLong(1, version);
            upd.setObject(2, guid);
            if (upd.executeUpdate() == 0) {
                try {
                    ins = con.prepareStatement("INSERT INTO " + H2PUtil.q(H2PDataStore.FILE_HEAD_TABLE) + " ("
                            + H2PUtil.q("file_guid") + ", " + H2PUtil.q("current_version") + ") VALUES (?, ?)");
                    ins.setObject(1, guid);
                    ins.setLong(2, version);
                    ins.executeUpdate();
                } catch (SQLException e) {
                    if (!"23505".equals(e.getSQLState())) throw e;
                    upd.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw wrap("file head restore failed: " + guid, e);
        } finally {
            SharedIOUtil.close(ins, upd, con);
        }
    }

    private static APIException wrap(String message, Exception e) {
        APIException ret = new APIException(message + ": " + e.getMessage());
        ret.initCause(e);
        return ret;
    }

    // ---------- Command line ----------

    private static final String USAGE =
            "Usage:\n"
            + "  H2PDumpRestore dump    --url <jdbc-url> --out <file> [options]\n"
            + "  H2PDumpRestore restore --url <jdbc-url> --in <file> [--mode merge|wipe] [options]\n"
            + "\n"
            + "Connection (H2 or PostgreSQL — engine resolved from the URL):\n"
            + "  --url <jdbc-url>          e.g. jdbc:h2:file:./data/db;CIPHER=AES\n"
            + "                                 jdbc:postgresql://host:5432/db\n"
            + "  --user <user>             database user\n"
            + "  --password <password>     database password\n"
            + "  --file-password <pwd>     encrypted-H2 file password\n"
            + "\n"
            + "dump options:\n"
            + "  --out <file>              output file; a .zip extension selects the zip container\n"
            + "                            (dump.jsonl + raw files/* content), anything else JSONL\n"
            + "                            with inline base64 content\n"
            + "  --format zip|jsonl        override the container inferred from the extension\n"
            + "  --types <c1,c2,...>       explicit entity types (Java class names or registered\n"
            + "                            meta-type names); default: discover via sys_meta_catalog\n"
            + "  --no-files                skip versioned file content\n"
            + "\n"
            + "restore options:\n"
            + "  --in <file>               dump file — container (zip or JSONL) is auto-detected\n"
            + "  --mode merge|wipe         merge (default): guid-keyed upsert, nothing deleted;\n"
            + "                            wipe: clear the discoverable store first (WIPE_AND_LOAD)\n"
            + "\n"
            + "Exit codes: 0 success, 1 usage error, 2 operation failed.";

    /**
     * Command-line dump/restore, e.g.
     * <pre>
     *   java -cp ... io.xlogistx.datastore.h2p.H2PDumpRestore dump \
     *       --url jdbc:postgresql://host:5432/db --user u --password p --out store.zip
     *   java -cp ... io.xlogistx.datastore.h2p.H2PDumpRestore restore \
     *       --url jdbc:h2:file:./data/db --in store.zip --mode wipe
     * </pre>
     * Prints the operation's stats as JSON on success.
     */
    public static void main(String[] args) {
        String command = args.length > 0 ? args[0] : null;
        if (!"dump".equals(command) && !"restore".equals(command)) {
            System.err.println(USAGE);
            System.exit(1);
        }
        Map<String, String> opts = new HashMap<>();
        Set<String> flagsOnly = Collections.singleton("--no-files");
        for (int i = 1; i < args.length; i++) {
            String opt = args[i];
            if (!opt.startsWith("--")) {
                System.err.println("unexpected argument: " + opt + "\n\n" + USAGE);
                System.exit(1);
            }
            if (flagsOnly.contains(opt)) {
                opts.put(opt, "true");
            } else if (i + 1 < args.length) {
                opts.put(opt, args[++i]);
            } else {
                System.err.println("missing value for " + opt + "\n\n" + USAGE);
                System.exit(1);
            }
        }
        String url = opts.get("--url");
        String io = "dump".equals(command) ? opts.get("--out") : opts.get("--in");
        if (url == null || io == null) {
            System.err.println("--url and --" + ("dump".equals(command) ? "out" : "in")
                    + " are required\n\n" + USAGE);
            System.exit(1);
        }

        H2PDataStore ds = null;
        try {
            ds = new H2PDSCreator().createAPI(null, H2PDSCreator.toAPIConfigInfo(
                    url, opts.get("--user"), opts.get("--password"), opts.get("--file-password")));
            NVGenericMap stats;
            if ("dump".equals(command)) {
                NVConfigEntity[] types = resolveCliTypes(ds, opts.get("--types"));
                boolean includeFiles = !opts.containsKey("--no-files");
                String format = opts.getOrDefault("--format",
                        io.toLowerCase().endsWith(".zip") ? "zip" : "jsonl");
                try (OutputStream out = java.nio.file.Files.newOutputStream(java.nio.file.Paths.get(io))) {
                    stats = "zip".equals(format)
                            ? ds.dumpZip(out, includeFiles, types)
                            : ds.dump(out, includeFiles, types);
                }
                System.out.println("dumped to " + io);
            } else {
                String mode = opts.getOrDefault("--mode", "merge");
                H2PDataStore.RestoreMode rm;
                if ("merge".equalsIgnoreCase(mode)) rm = H2PDataStore.RestoreMode.MERGE;
                else if ("wipe".equalsIgnoreCase(mode)) rm = H2PDataStore.RestoreMode.WIPE_AND_LOAD;
                else {
                    System.err.println("invalid --mode " + mode + " (merge|wipe)\n\n" + USAGE);
                    System.exit(1);
                    return;
                }
                try (InputStream in = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(io))) {
                    stats = ds.restore(in, rm);
                }
                System.out.println("restored from " + io);
            }
            System.out.println(GSONUtil.toJSONDefault(stats, true));
        } catch (Exception e) {
            System.err.println(command + " failed: " + e.getMessage());
            System.exit(2);
        } finally {
            if (ds != null) {
                try {
                    ds.close();
                } catch (RuntimeException ignore) {
                    // already reporting the operation's outcome
                }
            }
        }
    }

    /** Comma-separated type names → NVConfigEntities via the store's resolver; null spec = discover. */
    private static NVConfigEntity[] resolveCliTypes(H2PDataStore ds, String spec) {
        if (spec == null || spec.trim().isEmpty()) return new NVConfigEntity[0];
        List<NVConfigEntity> ret = new ArrayList<>();
        for (String name : spec.split(",")) {
            name = name.trim();
            if (name.isEmpty()) continue;
            NVConfigEntity nvce = ds.resolveNVCE(name);
            if (nvce == null) {
                throw new APIException("unknown entity type (not a Java class name or registered"
                        + " meta-type name): " + name);
            }
            ret.add(nvce);
        }
        return ret.toArray(new NVConfigEntity[0]);
    }
}
