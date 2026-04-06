/*
 * Copyright (c) 2012-2017 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.xlogistx.datastore;


import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.zoxweb.shared.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * Per-datastore metadata / index manager.
 * <p>
 * Previously a JVM-wide singleton, which meant only one datastore per JVM could track
 * metadata correctly. Now each {@link XlogistxMongoDataStore} owns its own instance.
 * <p>
 * Cache state is stored in {@link ConcurrentHashMap}s so that reads are lock-free.
 * The only synchronized sections are the short critical regions around first-time
 * index creation, which call into the Mongo driver — the class-wide lock is no
 * longer held during that I/O.
 */
public class XlogistxMongoMetaManager {
    private static final Logger log = Logger.getLogger(XlogistxMongoMetaManager.class.getName());

    /** Full-collection-name → NVConfigEntity (or DynamicEnumMap.class marker). */
    private final ConcurrentMap<String, Object> indexedCollections = new ConcurrentHashMap<>();

    /** canonicalID → NVConfigEntity with reference_id populated — avoids re-querying Mongo. */
    private final ConcurrentMap<String, NVConfigEntity> nvConfigEntityCache = new ConcurrentHashMap<>();

    private volatile MongoCollection<Document> nvConfigEntities = null;
    private volatile MongoDatabase mongoDatabase = null;

    public enum MetaCollections
            implements GetName {
        NV_CONFIG_ENTITIES("nv_config_entities"),
        DOMAIN_INFOS("domain_infos");

        private final String name;

        MetaCollections(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }


    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    protected void setMongoDatabase(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    /**
     * Public constructor — each datastore creates its own manager.
     */
    public XlogistxMongoMetaManager() {
    }

    /**
     * @param collection to check
     * @return true if the collection's unique indexes have already been ensured
     */
    public boolean isIndexed(MongoCollection<Document> collection) {
        SUS.checkIfNulls("Null value DBCollection", collection);
        return indexedCollections.containsKey(collection.getNamespace().getFullName());
    }

    /**
     * Register a collection's NVConfigEntity and create its unique indexes if not done already.
     * <p>
     * The Mongo I/O for {@code createIndex} is NOT performed under a wide lock — only the
     * cache entry is written via {@link ConcurrentMap#putIfAbsent(Object, Object)}, so
     * concurrent callers may race to call {@code createIndex} once each, but Mongo treats
     * repeated {@code createIndex} as idempotent.
     */
    public void addCollectionInfo(MongoCollection<Document> collection, NVConfigEntity nvce) {
        SUS.checkIfNulls("Null value", collection, nvce);

        String fullName = collection.getNamespace().getFullName();
        if (indexedCollections.containsKey(fullName)) {
            return;
        }

        addUniqueIndexes(collection, nvce);
        indexedCollections.putIfAbsent(fullName, nvce);
    }

    /**
     * Delete a cached collection entry.
     */
    public void removeCollectionInfo(String collectionFullName) {
        SUS.checkIfNulls("Null value", collectionFullName);
        indexedCollections.remove(collectionFullName);
    }

    /**
     * Cached version — reuses already-resolved NVConfigEntities by canonical id
     * instead of re-querying Mongo on every call.
     */
    public NVConfigEntity addNVConfigEntity(MongoDatabase mongo, NVConfigEntity nvce) {
        if (nvce.getReferenceID() != null) {
            return nvce;
        }

        String canonicalId = nvce.toCanonicalID();
        NVConfigEntity cached = nvConfigEntityCache.get(canonicalId);
        if (cached != null && cached.getReferenceID() != null) {
            nvce.setReferenceID(cached.getReferenceID());
            return nvce;
        }

        if (nvConfigEntities == null) {
            synchronized (this) {
                if (nvConfigEntities == null) {
                    MongoCollection<Document> c = mongo.getCollection(MetaCollections.NV_CONFIG_ENTITIES.getName());
                    createUniqueIndex(c, MetaToken.CANONICAL_ID.getName());
                    nvConfigEntities = c;
                }
            }
        }

        Document search = new Document(MetaToken.CANONICAL_ID.getName(), canonicalId);
        Document nvceDBO = nvConfigEntities.find(search).first();
        if (nvceDBO == null) {
            nvceDBO = dbMapNVConfigEntity(nvce);
            nvConfigEntities.insertOne(nvceDBO);
        }

        nvce.setReferenceID(MongoUtil.SINGLETON.getRefIDAsUUID(nvceDBO).toString());
        nvConfigEntityCache.putIfAbsent(canonicalId, nvce);
        return nvce;
    }

    public void removeCollectionInfo(MongoCollection<Document> collection) {
        SUS.checkIfNulls("Null value", collection);
        removeCollectionInfo(collection.getNamespace().getFullName());
    }

    public XlogistxMongoDBObjectMeta lookupCollectionName(XlogistxMongoDataStore mds, UUID collectionID) {
        Document nvceDB = mds.lookupByReferenceID(MetaCollections.NV_CONFIG_ENTITIES.getName(), collectionID);

        NVConfigEntity nvce;

        try {
            nvce = fromBasicDBObject(nvceDB);
        } catch (Exception e) {
            if (log.isLoggable(java.util.logging.Level.WARNING)) {
                log.log(java.util.logging.Level.WARNING, "lookupCollectionName failed for " + collectionID, e);
            }
            return null;
        }

        return new XlogistxMongoDBObjectMeta(nvce);
    }

    public static Document dbMapNVConfigEntity(NVConfigEntity nvce) {
        Document entryElement = new Document();
        entryElement.put(MongoUtil.ReservedID.REFERENCE_ID.getValue(), UUID.randomUUID());
        entryElement.put(MetaToken.NAME.getName(), nvce.getName());
        entryElement.put(MetaToken.DESCRIPTION.getName(), nvce.getDescription());
        entryElement.put(MetaToken.DOMAIN_ID.getName(), nvce.getDomainID());
        entryElement.put(MetaToken.CLASS_TYPE.getName(), nvce.getMetaTypeBase().getName());
        entryElement.put(MetaToken.IS_ARRAY.getName(), nvce.isArray());
        entryElement.put(MetaToken.CANONICAL_ID.getName(), nvce.toCanonicalID());
        return entryElement;
    }

    public static NVConfigEntity fromBasicDBObject(Document dbo) throws ClassNotFoundException {
        Class<?> clazz = Class.forName(dbo.getString(MetaToken.CLASS_TYPE.getName()));
        NVConfigEntity ret = new NVConfigEntityPortable();
        ret.setName(dbo.getString(MetaToken.NAME.getName()));
        ret.setDescription(dbo.getString(MetaToken.DESCRIPTION.getName()));
        ret.setDomainID(dbo.getString(MetaToken.DOMAIN_ID.getName()));
        ret.setMetaType(clazz);
        ret.setArray(dbo.getBoolean(MetaToken.IS_ARRAY.getName()));
        ret.setReferenceID(MongoUtil.ReservedID.REFERENCE_ID.decode(dbo));
        return ret;
    }

    /**
     * @param collection to be indexed
     * @param nvce       the meta config info
     */
    private void addUniqueIndexes(MongoCollection<Document> collection, NVConfigEntity nvce) {
        addNVConfigEntity(getMongoDatabase(), nvce);
        for (NVConfig nvc : nvce.getAttributes()) {
            if (!nvc.getName().equals(MetaToken.REFERENCE_ID.getName()) && nvc.isUnique() && !nvc.isArray()) {
                createUniqueIndex(collection, nvc.getName());
            }
        }
    }


    private List<String> createUniqueIndex(MongoCollection<Document> collection, String... uniqueIndexNames) {
        ArrayList<String> ret = new ArrayList<String>();

        ListIndexesIterable<Document> indexes = collection.listIndexes();
        if (log.isLoggable(java.util.logging.Level.FINE)) {
            log.fine("List of DBObject Indexes: " + indexes);
        }

        for (String indexToAdd : uniqueIndexNames) {
            if (log.isLoggable(java.util.logging.Level.FINE)) {
                log.fine("Index to be added:" + indexToAdd + " to collection:" + collection.getNamespace().getFullName());
            }
            if (SUS.isNotEmpty(indexToAdd)) {
                for (Document dbIndex : indexes) {
                    if ((dbIndex.get("key") != null && ((Document) dbIndex.get("key")).get(indexToAdd) != null)) {
                        indexToAdd = null;
                        break;
                    }
                }

                if (indexToAdd != null) {
                    ret.add(indexToAdd);
                    collection.createIndex(new Document(indexToAdd, 1), new IndexOptions().unique(true));
                    if (log.isLoggable(java.util.logging.Level.FINE)) {
                        log.fine("Index added:" + indexToAdd + " to collection:" + collection.getNamespace().getFullName());
                    }
                }
            }
        }

        return ret;
    }

    protected void addUniqueIndexesForDynamicEnumMap(MongoCollection<Document> collection) {
        String fullName = collection.getNamespace().getFullName();
        if (indexedCollections.containsKey(fullName)) {
            return;
        }
        createUniqueIndex(collection, MetaToken.NAME.getName());
        indexedCollections.putIfAbsent(fullName, DynamicEnumMap.class);
    }
}
