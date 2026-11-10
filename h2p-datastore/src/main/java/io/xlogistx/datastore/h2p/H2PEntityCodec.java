/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.xlogistx.datastore.h2p;

import com.google.gson.Gson;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.util.EntityCodec;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.SUS;

import java.util.List;
import java.util.logging.Level;

/**
 * The datastore's {@link EntityCodec}: an {@link NVEntity} embedded in an {@code NVGenericMap} is
 * represented by its identity ({@link EntityID}: class + guid) instead of an inline copy, and resolved
 * back from the entity's own table.
 * <ul>
 *   <li>{@link #encode(NVEntity)} — stores the entity (insert, or update of its existing row) and returns
 *       its identity, so the map never holds a copy the table does not have.</li>
 *   <li>{@link #decode(EntityID)} — the row read through the normal read path (access control applies):
 *       a missing, denied, unknown-type or malformed reference yields {@code null}, never an exception.</li>
 *   <li>{@link #gson()} — the Gson whose {@code NVGenericMap} adapter carries this codec
 *       ({@link GSONUtil#createDefaultGson(EntityCodec)}).</li>
 * </ul>
 * Built 2026-11-10 (user dictation). Not yet used by the schemaless column path.
 */
public final class H2PEntityCodec implements EntityCodec {
    public static final LogWrapper log = new LogWrapper(H2PEntityCodec.class).setEnabled(true);

    private final H2PDataStore ds;
    private volatile Gson gson;

    H2PEntityCodec(H2PDataStore ds) {
        this.ds = ds;
    }

    /**
     * Stores the entity and returns its identity, so the reference written into the map always names a row
     * of the entity's table: an entity without a guid is inserted (guid minted by the store), an entity with
     * a guid is written through the store's upsert (its row updated), both under the caller's access rights.
     *
     * @return the identity of the stored entity, or {@code null} for a {@code null} entity
     */
    @Override
    public EntityID encode(NVEntity nve) {
        if (nve == null) return null;
        NVEntity stored = ds.insert(nve);
        return new EntityID(stored != null ? stored : nve);
    }

    /**
     * @return the stored entity for {@code id}, or {@code null} when the id is incomplete, its type
     * cannot be resolved, its guid is malformed, or the row is missing or not readable by the caller
     */
    @Override
    public NVEntity decode(EntityID id) {
        if (id == null || id.getMetaType() == null || SUS.isEmpty(id.getGUID())) return null;
        String typeName = id.getMetaType().getName();
        NVConfigEntity nvce = ds.resolveNVCE(typeName);
        if (nvce == null) {
            if (log.isEnabled()) log.getLogger().warning("unresolvable entity type in reference: " + typeName);
            return null;
        }
        try {
            List<NVEntity> found = ds.searchByID(nvce, id.getGUID());
            return found.isEmpty() ? null : found.get(0);
        } catch (IllegalArgumentException e) {
            if (log.isEnabled()) log.getLogger().log(Level.WARNING, "malformed guid in reference: "
                    + typeName + "/" + id.getGUID(), e);
            return null;
        }
    }

    /** The Gson whose {@code NVGenericMap} adapter encodes/decodes embedded entities through this codec. */
    public Gson gson() {
        Gson g = gson;
        if (g == null) {
            synchronized (this) {
                g = gson;
                if (g == null) gson = g = GSONUtil.createDefaultGson(this);
            }
        }
        return g;
    }
}
