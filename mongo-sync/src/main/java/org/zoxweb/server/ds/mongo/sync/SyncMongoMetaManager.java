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
package org.zoxweb.server.ds.mongo.sync;


import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.zoxweb.shared.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class SyncMongoMetaManager {
    private static final Logger log = Logger.getLogger(SyncMongoMetaManager.class.getName());

    /**
     * Set hash map to NVConfigEntity mapped values.
     */
    private final HashMap<String, Object> map = new HashMap<>();
    private volatile MongoCollection nvConfigEntities = null;


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
     * This variable declares that only one instance of this class can be created.
     */
    public static final SyncMongoMetaManager SINGLETON = new SyncMongoMetaManager();

    /**
     * The default constructor is declared private to prevent
     * outside instantiation of this class.
     */
    private SyncMongoMetaManager() {

    }

    /**
     * @param collection to check
     * @return true if indexed
     */
    public boolean isIndexed(MongoCollection<Document> collection) {
        SUS.checkIfNulls("Null value DBCollection", collection);

        return lookupCollectionByFullName(collection.getNamespace().getFullName()) != null;
    }

    /**
     * @param collectionFullName to look for
     * @return the mapped value
     */
    private synchronized Object lookupCollectionByFullName(String collectionFullName) {
        SUS.checkIfNulls("Null value", collectionFullName);

        return map.get(collectionFullName);
    }

    /**
     * @param collection to be added to
     * @param nvce       to be added
     */
    public synchronized void addCollectionInfo(MongoCollection<Document> collection, NVConfigEntity nvce) {
        SUS.checkIfNulls("Null value", collection, nvce);

        if (!isIndexed(collection)) {
            addUniqueIndexes(collection, nvce);

            map.put(collection.getNamespace().getFullName(), nvce);
        }
    }

    /**
     * Delete a collection
     *
     * @param collectionFullName to be delete
     */
    public synchronized void removeCollectionInfo(String collectionFullName) {
        SUS.checkIfNulls("Null value", collectionFullName);

        map.remove(collectionFullName);
    }

    public synchronized NVConfigEntity addNVConfigEntity(MongoDatabase mongo, NVConfigEntity nvce) {
        if (nvce.getReferenceID() != null)
            return nvce;

        if (nvConfigEntities == null) {
            nvConfigEntities = mongo.getCollection(MetaCollections.NV_CONFIG_ENTITIES.getName());
            createUniqueIndex(nvConfigEntities, MetaToken.CANONICAL_ID.getName());
        }
        Document search = new Document();
        search.put(MetaToken.CANONICAL_ID.getName(), nvce.toCanonicalID());
        Document nvceDBO = (Document) nvConfigEntities.find(search).first();
        if (nvceDBO == null) {
            nvceDBO = dbMapNVConfigEntity(nvce);

            nvConfigEntities.insertOne(nvceDBO);
        }

//        nvce.setReferenceID(nvceDBO.getObjectId(MongoUtil.ReservedID.REFERENCE_ID.getValue()).toHexString());
        nvce.setReferenceID(MongoUtil.SINGLETON.getRefIDAsUUID(nvceDBO).toString());
        return nvce;
    }


    /**
     * Delete a collection
     *
     * @param collection to be deleted
     */
    public synchronized void removeCollectionInfo(MongoCollection collection) {
        SUS.checkIfNulls("Null value", collection);

        removeCollectionInfo(collection.getNamespace().getFullName());
    }

    public synchronized SyncMongoDBObjectMeta lookupCollectionName(SyncMongoDS mds, UUID collectionID) {
        Document nvceDB = mds.lookupByReferenceID(MetaCollections.NV_CONFIG_ENTITIES.getName(), collectionID);

        NVConfigEntity nvce;

        try {
            nvce = fromBasicDBObject(nvceDB);//(Class<? extends NVEntity>) Class.forName((String)nvceDB.getString(MetaToken.CLASS_TYPE.getName()));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return new SyncMongoDBObjectMeta(nvce);
    }

    public static Document dbMapNVConfigEntity(NVConfigEntity nvce) {
        Document entryElement = new Document();
//        entryElement.put(MetaToken.GUID.getName(), UUID.randomUUID());
        entryElement.put(MongoUtil.ReservedID.REFERENCE_ID.getValue(), UUID.randomUUID());
        entryElement.put(MetaToken.NAME.getName(), nvce.getName());
        entryElement.put(MetaToken.DESCRIPTION.getName(), nvce.getDescription());
        entryElement.put(MetaToken.DOMAIN_ID.getName(), nvce.getDomainID());
        entryElement.put(MetaToken.CLASS_TYPE.getName(), nvce.getMetaTypeBase().getName());
        entryElement.put(MetaToken.IS_ARRAY.getName(), nvce.isArray());
        entryElement.put(MetaToken.CANONICAL_ID.getName(), nvce.toCanonicalID());
        return entryElement;
    }

//    public static NVConfigEntity fromBasicDBObject(BasicDBObject dbo) throws ClassNotFoundException {
//        Class<?> clazz = Class.forName(dbo.getString(MetaToken.CLASS_TYPE.getName()));
//        NVConfigEntity ret = new NVConfigEntityLocal();
//        ret.setName(dbo.getString(MetaToken.NAME.getName()));
//        ret.setDescription(dbo.getString(MetaToken.DESCRIPTION.getName()));
//        ret.setDomainID(dbo.getString(MetaToken.DOMAIN_ID.getName()));
//        ret.setMetaType(clazz);
//        ret.setArray(dbo.getBoolean(MetaToken.IS_ARRAY.getName()));
//        ret.setReferenceID(dbo.getObjectId(MongoUtil.ReservedID.REFERENCE_ID.getValue()).toHexString());
//        return ret;
//    }

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
     * @param nvce       the met config info
     */
    private synchronized void addUniqueIndexes(MongoCollection<Document> collection, NVConfigEntity nvce) {
        if (!isIndexed(collection)) {

            addNVConfigEntity(getMongoDatabase(), nvce);
            for (NVConfig nvc : nvce.getAttributes()) {

                if (!nvc.getName().equals(MetaToken.REFERENCE_ID.getName()) && nvc.isUnique() && !nvc.isArray()) {
                    createUniqueIndex(collection, nvc.getName());

                }
            }

        }
    }


    private List<String> createUniqueIndex(MongoCollection<Document> collection, String... uniqueIndexNames) {
        ArrayList<String> ret = new ArrayList<String>();

        ListIndexesIterable<Document> indexes = collection.listIndexes();
        log.info("List of DBObject Indexes: " + indexes);

        for (String indexToAdd : uniqueIndexNames) {
            log.info("Index to be added:" + indexToAdd + " to collection:" + collection.getNamespace().getFullName());
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
                    log.info("Index added:" + indexToAdd + " to collection:" + collection.getNamespace().getFullName());
                }
            }
        }

        return ret;
    }

    protected synchronized void addUniqueIndexesForDynamicEnumMap(MongoCollection<Document> collection) {
        if (!isIndexed(collection)) {
            createUniqueIndex(collection, MetaToken.NAME.getName());

            //collection.createIndex(new BasicDBObject(MetaToken.NAME.getName(), 1), new BasicDBObject("unique", true));
            map.put(collection.getNamespace().getCollectionName(), DynamicEnumMap.class);
        }
    }

}
