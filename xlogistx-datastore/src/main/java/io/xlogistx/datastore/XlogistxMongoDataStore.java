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

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.zoxweb.server.api.APIServiceProviderBase;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.util.*;
import org.zoxweb.shared.api.*;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.EncapsulatedKey;
import org.zoxweb.shared.crypto.EncryptedData;
import org.zoxweb.shared.data.CRUDNVEntityDAO;
import org.zoxweb.shared.data.CRUDNVEntityListDAO;
import org.zoxweb.shared.data.DataConst.APIProperty;
import org.zoxweb.shared.data.DataConst.DataParam;
import org.zoxweb.shared.data.LongSequence;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.filters.ChainedFilter;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.filters.LowerCaseFilter;
import org.zoxweb.shared.filters.ValueFilter;
import org.zoxweb.shared.io.SharedIOUtil;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.SecurityController;
import org.zoxweb.shared.util.*;
import org.zoxweb.shared.util.Const.RelationalOperator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class is used to define the MongoDB object for data storage. This object primarily contains methods
 * used to store, retrieve, update, and delete documents in the database.
 *
 * @author javaconsigliere@gmail.com
 */
@SuppressWarnings("serial")
public class XlogistxMongoDataStore
        extends APIServiceProviderBase<MongoClient, MongoDatabase>
        implements APIDataStore<MongoClient, MongoDatabase>, APIDocumentStore<MongoClient, MongoDatabase> {
    public static final LogWrapper log = new LogWrapper(XlogistxMongoDataStore.class);


    private volatile MongoClient mongoClient = null;
    //private DBAddress dbAddress;
    private volatile MongoDatabase mongoDB = null;
    private volatile MongoDatabase gridFSDB = null;
    private APIConfigInfo configInfo;
    private String name;
    private String description;
    //private APIExceptionHandler exceptionHandler;
    private NVECRUDMonitor dataCacheMonitor = null;

    private final Lock updateLock = new ReentrantLock();

    /** Per-instance metadata manager (replaces the JVM-wide singleton). */
    private final XlogistxMongoMetaManager metaManager = new XlogistxMongoMetaManager();

    /** @return this datastore's metadata manager — useful for tests and diagnostics. */
    public XlogistxMongoMetaManager getMetaManager() {
        return metaManager;
    }

    /** Default max execution time for find/cursor operations, in milliseconds. 0 = no limit. */
    private volatile long operationTimeoutMS = 30_000L;

    /** Hard safety cap on rows materialized by {@link #search}/{@link #userSearch}. */
    private volatile int maxSearchResults = 10_000;

    public long getOperationTimeoutMS() { return operationTimeoutMS; }
    public void setOperationTimeoutMS(long ms) { this.operationTimeoutMS = Math.max(0L, ms); }

    public int getMaxSearchResults() { return maxSearchResults; }
    public void setMaxSearchResults(int n) { this.maxSearchResults = Math.max(1, n); }

    /** Apply the configured {@link #operationTimeoutMS} to a FindIterable if > 0. */
    private <D> com.mongodb.client.FindIterable<D> withTimeout(com.mongodb.client.FindIterable<D> it) {
        long t = operationTimeoutMS;
        if (t > 0) {
            it.maxTime(t, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        return it;
    }

    //private Set<String> sequenceSet = new HashSet<String>();


    //private LockQueue updateLock = new LockQueue(5);


    /**
     * The default constructor.
     */
    public XlogistxMongoDataStore() {
    }


    /**
     * Returns the MongoDB client.
     *
     * @return mongoClient
     */
    public MongoClient getMongoClient() {
        return newConnection();
    }

    /**
     * Connects to the database.
     *
     * @return singleton mongo data store
     */

    public MongoDatabase connect()
            throws APIException {
        if (mongoDB == null) {
            synchronized (this) {
                if (mongoDB == null) {
                    if (getAPIConfigInfo() == null) {
                        throw new APIException("Missing configuration information");
                    }

                    mongoDB = newConnection().getDatabase(XlogistxMongoDSCreator.MongoParam.dataStoreName(getAPIConfigInfo()));
                    metaManager.setMongoDatabase(mongoDB);

                    // Dynamic enum maps are now loaded lazily on first use, not eagerly on connect,
                    // to avoid paying that cost during first-touch connection establishment.
                }
            }
        }
        return mongoDB;

    }

    /**
     * Closes all connections to the database.
     */
    public synchronized void close() {
        SharedIOUtil.close(mongoClient);
        mongoClient = null;
        mongoDB = null;
        gridFSDB = null;
    }

    private Document serNamedValue(NamedValue<?> namedValue) {
        Document bsonDoc = new Document();
        bsonDoc.append(MetaToken.VALUE.getName(), namedValue.getValue())
                .append("properties", serNVGenericMap(namedValue.getProperties()));
        return bsonDoc;
    }


    public NVGenericMap ping(boolean detailed)
            throws APIException {
        Document status = null;
        Document buildInfo = null;
        Document ping = null;
        try {
            MongoDatabase admin = newConnection().getDatabase("admin");

            ping = admin.runCommand(new Document("ping", 1));
            status = admin.runCommand(new Document("serverStatus", 1));
            buildInfo = admin.runCommand(new Document("buildInfo", 1));

        } catch (Exception e) {
            if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING, "ping failed", e);
            APIException apiEx = new APIException(e.getMessage());
            apiEx.initCause(e);
            throw apiEx;
        }

        NVGenericMap ret = new NVGenericMap();
        ret.build("time_stamp", DateUtil.DEFAULT_DATE_FORMAT_TZ.format(new Date()));

        if (detailed) {
            NVGenericMap tmp = GSONUtil.fromJSONDefault(buildInfo.toJson(), NVGenericMap.class);
            tmp.setName("build-info");
            ret.build(tmp);

            tmp = GSONUtil.fromJSONDefault(ping.toJson(), NVGenericMap.class);
            tmp.setName("ping");
            ret.build(tmp);

            tmp = GSONUtil.fromJSONDefault(status.toJson(), NVGenericMap.class);
            tmp.setName("status");
            ret.build(tmp);
        } else {
            Document summary = new Document("status", ping.get("ok").equals(1.0) ? "UP" : "DOWN")
                    .append("version", status.get("version"))
                    .append("uptime", Const.TimeInMillis.toString(status.get("uptimeMillis", Number.class).longValue()))
                    .append("connections", status.get("connections"))
                    .append("mem", status.get("mem"))
//                .append("network", status.get("network"))
                    ;
            NVGenericMap tmp = GSONUtil.fromJSONDefault(summary.toJson(), NVGenericMap.class);
            NVGenericMap.copy(tmp, ret, false);


        }

        return ret;

    }

    private Document serNVPair(NVEntity container, NVPair nvp, boolean sync) {
        Document bsonDoc = new Document();

        Object value = nvp.getValue();

        if (container != null && (ChainedFilter.isFilterSupported(nvp.getValueFilter(), FilterType.ENCRYPT) || ChainedFilter.isFilterSupported(nvp.getValueFilter(), FilterType.ENCRYPT_MASK))) {
            getAPIConfigInfo().getKeyMaker().createNVEntityKey(this, container, getAPIConfigInfo().getKeyMaker().getKey(this, getAPIConfigInfo().getKeyMaker().getMasterKey(), container.getSubjectGUID()));
            value = getAPIConfigInfo().getSecurityController().encryptValue(this, container, null, nvp, null);
        }

        bsonDoc.append(MetaToken.NAME.getName(), nvp.getName());
        bsonDoc.append(MetaToken.VALUE.getName(), value instanceof EncryptedData ? serNVEntity((EncryptedData) value, true, sync, false) : value);

        if (nvp.getValueFilter() != null) {
            if (nvp.getValueFilter() != FilterType.CLEAR && !(nvp.getValueFilter() instanceof DynamicEnumMap)) {
                bsonDoc.append(MetaToken.VALUE_FILTER.getName(), nvp.getValueFilter().toCanonicalID());
            }

            if (nvp.getValueFilter() instanceof DynamicEnumMap) {
                DynamicEnumMap dem = (DynamicEnumMap) nvp.getValueFilter();

                if (dem.getReferenceID() == null) {
                    dem = searchDynamicEnumMapByName(dem.getName(), dem.getClass());

                    if (dem == null) {
                        dem = insertDynamicEnumMap(dem);
                        nvp.setValueFilter(dem);
                    }
                }

                Document dbObject = new Document();
                dbObject.append(MetaToken.REFERENCE_ID.getName(), IDGs.UUIDV4.decode(dem.getReferenceID()));
                dbObject.append(MetaToken.COLLECTION_NAME.getName(), dem.getClass().getName());

                bsonDoc.append(MetaToken.VALUE_FILTER.getName(), dbObject);

            }

        }

        return bsonDoc;
    }


    private Document serNVGenericMap(NVGenericMap nvgm) {
        Document ret = new Document();
        ret.put(MetaToken.CLASS_TYPE.getName(), NVGenericMap.class.getName());

        for (GetNameValue<?> gnv : nvgm.values()) {

            Object value = gnv.getValue();
            if (value instanceof String ||
                    value instanceof Boolean ||
                    value instanceof Integer ||
                    value instanceof Long ||
                    value instanceof Float ||
                    value instanceof Double ||
                    value instanceof byte[] ||
                    gnv instanceof NVStringList ||
                    gnv instanceof NVStringSet ||
                    gnv instanceof NVIntList ||
                    gnv instanceof NVLongList ||
                    gnv instanceof NVFloatList ||
                    gnv instanceof NVDoubleList)
//				value instanceof BigDecimal ||
//				value instanceof Enum)
            {
                ret.put(gnv.getName(), value);
            } else if (value instanceof Enum) {
                Document toAdd = new Document();
                toAdd.put(MetaToken.CLASS_TYPE.getName(), value.getClass().getName());
                toAdd.put(MetaToken.VALUE.getName(), ((Enum<?>) value).name());
                ret.put(gnv.getName(), toAdd);
            } else if (value instanceof BigDecimal) {
                Document toAdd = new Document();
                toAdd.put(MetaToken.CLASS_TYPE.getName(), value.getClass().getName());
                toAdd.put(MetaToken.VALUE.getName(), "" + value);
                ret.put(gnv.getName(), toAdd);
            } else if (value instanceof NVEntity) {
                NVEntity nve = (NVEntity) value;
                if (nve.getReferenceID() == null || nve.getGUID() == null)
                    insert(nve);
                ret.put(gnv.getName(), serNVEntityReference(connect(), nve));
            } else if (gnv instanceof NVGenericMap) {
                ret.put(gnv.getName(), serNVGenericMap((NVGenericMap) gnv));
            }
        }


        return ret;
    }

    private ArrayList<Document> serArrayValuesNVPair(NVEntity container, ArrayValues<NVPair> listOfNVPair, boolean sync) {
        ArrayList<Document> listOfDBObject = new ArrayList<Document>();

        for (NVPair nvp : listOfNVPair.values()) {
            listOfDBObject.add(serNVPair(container, nvp, sync));
        }

        return listOfDBObject;
    }

    private ArrayList<Document> serArrayValuesNVGetNameValueString(NVEntity container, ArrayValues<GetNameValue<String>> listOfNVPair, boolean sync) {
        ArrayList<Document> listOfDBObject = new ArrayList<Document>();

        for (GetNameValue<String> nvp : listOfNVPair.values()) {
            listOfDBObject.add(serNVPair(container, new NVPair(nvp.getName(), nvp.getValue()), sync));
        }

        return listOfDBObject;
    }

    /**
     * Maps an enum list to an array list of strings.
     *
     * @param enumList
     * @return enumNames
     */
    private List<String> mapEnumListToStringList(NVEnumList enumList) {
        ArrayList<String> enumNames = new ArrayList<String>();

        for (Enum<?> e : enumList.getValue()) {
            enumNames.add(e.name());
        }

        return enumNames;
    }


    private List<Document> serArrayValuesNVEntity(MongoDatabase db,
                                                  NVEntity container,
                                                  ArrayValues<NVEntity> refList,
                                                  boolean embed,
                                                  boolean sync,
                                                  boolean updateReferenceOnly) {

        ArrayList<Document> list = new ArrayList<Document>();

        for (NVEntity nve : refList.values()) {
            if (nve != null)
                list.add(serComplexNVEntityReference(db, container, nve, embed, sync, updateReferenceOnly));

        }

        return list;
    }

    private Document serComplexNVEntityReference(MongoDatabase db,
                                                 NVEntity container,
                                                 NVEntity nve,
                                                 boolean embed,
                                                 boolean sync,
                                                 boolean updateReferenceOnly) {
        if (nve != null) {
            if (nve instanceof CIPassword) {
                return serNVEntity(nve, true, sync, updateReferenceOnly);
            } else if (!(nve instanceof EncapsulatedKey) && nve instanceof EncryptedData) {
                return serNVEntity(nve, true, sync, updateReferenceOnly);
            } else if (embed)
                return serNVEntity(nve, embed, sync, updateReferenceOnly);

            else {
                if (nve.getReferenceID() == null || nve.getGUID() == null) {
                    //if(log.isEnabled()) log.getLogger().info("NVE do not exist we need to create it");
                    insert(nve);
                } else {

                    // sync was added to avoid FolderInfo Content deep update
                    // it will lock the update and only update references
                    if (!updateReferenceOnly && UpdateFilterClass.SINGLETON.isValid(nve)) {
                        patch(nve, true, sync, updateReferenceOnly, true);
                    } else {
                        if (log.isEnabled()) log.getLogger().info("Not updated:" + nve.getClass().getName());
                    }
                }
                return serNVEntityReference(db, nve);
            }
        }
        return null;
    }


    private Document serNVEntityReference(MongoDatabase db, NVEntity nve) {
        Document entryElement = new Document();
        NVConfigEntity nvce = metaManager.addNVConfigEntity(db, ((NVConfigEntity) nve.getNVConfig()));
        entryElement.put(MetaToken.CANONICAL_ID.getName(), IDGs.UUIDV4.decode(nvce.getGUID()));
        entryElement.put(MetaToken.GUID.getName(), IDGs.UUIDV4.decode(nve.getGUID()));
        return entryElement;
    }

    /**
     * Maps an object from the database to NVEntity type.
     *
     * @param db
     * @param dbObject
     * @param clazz
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @SuppressWarnings("unchecked")
    public <V extends NVEntity> V fromDB(String subjectGUID, MongoDatabase db, Document dbObject, Class<? extends NVEntity> clazz)
            throws InstantiationException, IllegalAccessException {
        try {
            NVEntity nve = clazz.getConstructor().newInstance();
            NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();

            for (NVConfig nvc : nvce.getAttributes()) {
                //if (ReservedID.lookupByName(nvc) == null)
                updateMappedValue(subjectGUID, db, dbObject, nve, nvc, nve.lookup(nvc.getName()));
            }


            return (V) nve;
        } catch (ReflectiveOperationException e) {
            throw new InstantiationException(e.getMessage());
        }

    }

    /**
     * @param db
     * @param doc
     * @param nvc
     * @param nvb
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @SuppressWarnings("unchecked")
    private void updateMappedValue(String subjectGUID, MongoDatabase db, Document doc, NVEntity container, NVConfig nvc, NVBase<?> nvb)
            throws InstantiationException, IllegalAccessException, APIException {

        Class<?> clazz = nvc.getMetaType();

        MongoUtil.ReservedID resID = MongoUtil.ReservedID.lookupByName(nvc);
        if (resID != null && clazz == String.class) {
            if (container.lookupValue(nvc) != null)
                return;

            Object value = doc.get(resID.getValue());
            if (value instanceof UUID)
                ((NVPair) nvb).setValue(IDGs.UUIDV4.encode((UUID) value));
            else if (value instanceof String)
                ((NVPair) nvb).setValue((String) value);

            return;
        }


        if (doc.get(nvc.getName()) == null)
            return;


        MongoUtil.DataDeserializer updater = MongoUtil.SINGLETON.lookupDataDeserializer(clazz);
        if (updater != null) {
            updater.deserialize(this, subjectGUID, db, doc, container, nvc, nvb);
            return;
        }


        if (nvc.isArray()) {
            Object value = doc.get(nvc.getName());
            if (value instanceof List) {
                if (((List) value).size() == 0) {
                    return;
                }
            }


            // Adding a list or set of NVEntities an array in this case
            if (NVEntity.class.isAssignableFrom(nvc.getMetaTypeBase())) {

                // Use the batched lookup to avoid an N+1 query pattern.
                List<XlogistxMongoDBObjectMeta> listOfDBObject = lookupByReferenceIDsMaybe((List<Document>) doc.get(nvc.getName()));
                if (listOfDBObject != null) {
                    //List<NVEntity> ret = new ArrayList<NVEntity>();
                    for (XlogistxMongoDBObjectMeta tempDBObject : listOfDBObject) {
                        // check if the content is null
                        // if the actual object was delete from the collection
                        // by still referenced by a list or set
                        if (tempDBObject.getContent() != null)
                            ((ArrayValues<NVEntity>) nvb).add(fromDB(subjectGUID, db, tempDBObject.getContent(), (Class<? extends NVEntity>) tempDBObject.getNVConfigEntity().getMetaTypeBase()));
                    }


                    //((ArrayValues<NVEntity>) nvb).add(ret.toArray(new NVEntity[0]), true);
                }


                return;
            }
        }

        if (nvc instanceof NVConfigEntity) {
            //String nveRefID = dbObject.getString(nvc.getName());
            if (container instanceof EncapsulatedKey) {
                return;
            }
            Document obj = (Document) doc.get(nvc.getName());

            Object className = obj.get(MetaToken.CLASS_TYPE.getName());
            if (className != null) {
                try {
                    ((NVEntityReference) nvb).setValue(fromDB(subjectGUID, db, obj, (Class<? extends NVEntity>) Class.forName((String) className)));
                } catch (ClassNotFoundException e) {
                    if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING,
                            "Class not found for embedded entity: " + className, e);
                }
                return;
            }
            XlogistxMongoDBObjectMeta mdbom = lookupByReferenceID(obj);
            if (mdbom != null && mdbom.getContent() != null) {
                ((NVEntityReference) nvb).setValue(fromDB(subjectGUID, db, mdbom.getContent(), (Class<? extends NVEntity>) mdbom.getNVConfigEntity().getMetaTypeBase()));
            }

            return;
        }

        throw new IllegalArgumentException("Unsupported type: " + nvc + " " + clazz);
    }


    /**
     * Converts a database object to NVPair.
     *
     * @param dbObject
     * @return
     */
    protected NVPair toNVPair(String userID, NVEntity container, Document dbObject) {
        NVPair nvp = new NVPair();

        Object value = dbObject.get(MetaToken.VALUE.getName());
        if (container != null) {

            if (value instanceof Document) {
                //if(log.isEnabled()) log.getLogger().info("userID:" + userID);
                try {
                    value = getAPIConfigInfo().getSecurityController().decryptValue(userID, this, container, fromDB(userID, connect(), (Document) value, EncryptedData.class), null);
                } catch (InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        nvp.setName(dbObject.getString(MetaToken.NAME.getName()));
        nvp.setValue((String) value);
        nvp.setValueFilter(getValueFilter(dbObject));

        return nvp;
    }


    @SuppressWarnings("unchecked")
    private ValueFilter<String, String> getValueFilter(Document dbObject) {
        //String valueFilterName = dbObject.getString(MetaToken.VALUE_FILTER.getName());
        Object valueFilterValue = dbObject.get(MetaToken.VALUE_FILTER.getName());

        if (valueFilterValue != null) {
            if (valueFilterValue instanceof String) {
                String filterTypeName = (String) valueFilterValue;

                if (!SUS.isEmpty(filterTypeName)) {
                    return ((ValueFilter<String, String>) SharedUtil.lookupEnum(filterTypeName, FilterType.values()));
                }
            }

            if (valueFilterValue instanceof Document) {
                Document demRefObject = (Document) valueFilterValue;


                UUID demRefID = MongoUtil.SINGLETON.getRefIDAsUUID(demRefObject);
                String collectionName = demRefObject.getString(MetaToken.COLLECTION_NAME.getName());

                return searchDynamicEnumMapByReferenceID(demRefID, collectionName);
            }

        }

        return null;
    }


    @SuppressWarnings("unchecked")
    NVGenericMap fromNVGenericMap(String userID, NVGenericMap nvgm, Document dbNVGM) throws InstantiationException, IllegalAccessException, APIException {
        if (nvgm == null) {
            nvgm = new NVGenericMap();
        }

        for (String key : dbNVGM.keySet()) {
            Object value = dbNVGM.get(key);
            NVBase<?> possibleNVB = SharedUtil.toNVBasePrimitive(key, value);
            if (possibleNVB != null) {
                if (possibleNVB.getValue() instanceof String && MetaToken.CLASS_TYPE.getName().equalsIgnoreCase(key)) {
                    try {
                        Class.forName((String) possibleNVB.getValue());
                        continue;
                    } catch (Exception e) {

                    }
                }
                nvgm.add(possibleNVB);
            } else if (value instanceof Document) {
                String classType = (String) ((Document) value).get(MetaToken.CLASS_TYPE.getName());
                Document subDBObject = (Document) value;
                if (classType != null) {
                    Class<?> subClass = null;
                    try {
                        subClass = Class.forName(classType);
                    } catch (ClassNotFoundException e) {

                        e.printStackTrace();
                    }
                    if (subClass.isEnum()) {
                        NVEnum nvEnum = new NVEnum(key, SharedUtil.enumValue(subClass, (String) ((Document) value).get(MetaToken.VALUE.getName())));
                        nvgm.add(nvEnum);
                        continue;
                    }
                    if (subClass == BigDecimal.class) {
                        NVBigDecimal nvBD = new NVBigDecimal(key, new BigDecimal((String) ((Document) value).get(MetaToken.VALUE.getName())));
                        nvgm.add(nvBD);
                        continue;
                    }
                    if (subClass == NVGenericMap.class) {
                        NVGenericMap nvgmToAdd = fromNVGenericMap(userID, null, subDBObject);
                        nvgmToAdd.setName(key);
                        nvgm.add(nvgmToAdd);
                        continue;
                    }
                } else if (subDBObject.containsKey(MetaToken.CANONICAL_ID.getName()) && subDBObject.containsKey(MetaToken.REFERENCE_ID.getName())) {
                    XlogistxMongoDBObjectMeta mdbom = lookupByReferenceID(subDBObject);
                    NVEntity nveToAdd = fromDB(userID, connect(), mdbom.getContent(), (Class<? extends NVEntity>) mdbom.getNVConfigEntity().getMetaTypeBase());
                    nvgm.add(key, nveToAdd);
                }
            }
        }

        return nvgm;
    }

    /**
     * Returns all the documents in the collection. After using this function,
     * the cursor must be closed.
     *
     * @param db
     * @param nve
     * @return
     */
    public MongoCursor<Document> getAllDocuments(MongoDatabase db, NVEntity nve) {
        SUS.checkIfNulls("Null value", nve);

        MongoCursor<Document> cur = null;
        MongoCollection<Document> collection = db.getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());

//        try {
        if (collection != null) {
            cur = collection.find().iterator();
        }


        return cur;
    }

    /**
     * Returns the database name.
     *
     * @return
     */
    @Override
    public String getStoreName() {
        if (connect().getName() != null)
            return connect().getName();

        return null;
    }

    /**
     * This method returns the names of collections in the database.
     *
     * @return
     */
    @Override
    public Set<String> getStoreTables() {
        Set<String> ret = new LinkedHashSet<>();
        for (String tableName : connect().listCollectionNames())
            ret.add(tableName);

        return ret;
    }




    @Override
    public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {
        return lookupByReferenceID(metaTypeName, objectId, null);
    }

    /**
     *
     * @param metaTypeName
     * @param refID
     * @param projection
     * @param <NT>
     * @param <RT>
     * @param <NIT>
     * @return Document not entity
     */

    @SuppressWarnings("unchecked")
    @Override
    public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT refID, NIT projection) {
        SUS.checkIfNulls("NULL refID or metaTypeName", refID, metaTypeName);

        Document query = new Document();

        if (refID instanceof UUID)
            query.put(MongoUtil.ReservedID.REFERENCE_ID.getValue(), refID);
        else if (refID instanceof String)
            query.put(MongoUtil.ReservedID.REFERENCE_ID.getValue(), IDGs.UUIDV4.decode((String) refID));
        else
            throw new IllegalArgumentException("Invalid refID: " + refID);

        MongoCollection<Document> collection = lookupCollection(metaTypeName);

        Document dbObj = null;
        try {
            dbObj = withTimeout(collection.find(query).projection((Bson) projection)).first();
        } catch (MongoException e) {
            getAPIExceptionHandler().throwException(e);
        }

        return (NT) dbObj;
    }




    /**
     * Looks up and returns a list of database objects based on a list of
     * reference id's.
     *
     * @param collectionName
     * @param listOfObjectId
     * @return
     */
    public List<Document> lookupByReferenceIDs(String collectionName, List<?> listOfObjectId) {
        if (listOfObjectId == null || listOfObjectId.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        MongoCollection<Document> collection = lookupCollection(collectionName);

        Bson inQuery = Filters.in(MongoUtil.ReservedID.REFERENCE_ID.getValue(), listOfObjectId);

        List<Document> listOfDocuments = new ArrayList<>();
        try (MongoCursor<Document> cur = withTimeout(collection.find(inQuery)).iterator()) {
            while (cur.hasNext()) {
                listOfDocuments.add(cur.next());
            }
        } catch (MongoException e) {
            getAPIExceptionHandler().throwException(e);
        }

        return listOfDocuments;
    }


    public List<XlogistxMongoDBObjectMeta> lookupByReferenceIDsMaybe(List<Document> listOfObjectRefID) {
        if (listOfObjectRefID == null || listOfObjectRefID.isEmpty()) {
            return null;
        }

        List<XlogistxMongoDBObjectMeta> listOfDBObjects = new ArrayList<>();

        // Step 1: Group references by collection (canonical_id)
        Map<UUID, List<UUID>> refsByCollection = new HashMap<>();
        Map<UUID, Document> refDocMap = new HashMap<>();  // To retrieve original doc info

        for (Document toFind : listOfObjectRefID) {
            UUID canonicalId = (UUID) toFind.get(MetaToken.CANONICAL_ID.getName());
            UUID refId = (UUID) toFind.get(MongoUtil.ReservedID.REFERENCE_ID.getValue());

            if (canonicalId != null && refId != null) {
                refsByCollection.computeIfAbsent(canonicalId, k -> new ArrayList<>()).add(refId);
                refDocMap.put(refId, toFind);
            }
        }

        // Step 2: Execute one batch query per collection
        for (Map.Entry<UUID, List<UUID>> entry : refsByCollection.entrySet()) {
            UUID canonicalId = entry.getKey();
            List<UUID> refIds = entry.getValue();

            // Lookup collection metadata
            XlogistxMongoDBObjectMeta meta = metaManager.lookupCollectionName(this, canonicalId);
            if (meta == null || meta.getNVConfigEntity() == null) {
                continue;
            }

            String collectionName = meta.getNVConfigEntity().toCanonicalID();
            MongoCollection<Document> collection = connect().getCollection(collectionName);

            // Single query with $in operator for all refs in this collection
            Document query = new Document(MongoUtil.ReservedID.REFERENCE_ID.getValue(), new Document("$in", refIds));

            try (MongoCursor<Document> cursor = collection.find(query).iterator()) {
                while (cursor.hasNext()) {
                    Document dbObject = cursor.next();
                    XlogistxMongoDBObjectMeta toAdd = new XlogistxMongoDBObjectMeta(meta.getNVConfigEntity());
                    toAdd.setContent(dbObject);
                    listOfDBObjects.add(toAdd);
                }
            }
        }

        return listOfDBObjects;
    }



    public XlogistxMongoDBObjectMeta lookupByReferenceID(Document toFind) {
        if (toFind != null) {
            XlogistxMongoDBObjectMeta ret = metaManager.lookupCollectionName(this, (UUID) toFind.get(MetaToken.CANONICAL_ID.getName()));
            if (ret != null) {
                UUID refID = (UUID) toFind.get(MongoUtil.ReservedID.REFERENCE_ID.getValue());
                Document dbObject = lookupByReferenceID(ret.getNVConfigEntity().toCanonicalID(), refID);
                if (dbObject != null) {
                    ret.setContent(dbObject);
                }

                return ret;
            }
        }
        return null;
    }


    /**
     * Formats a list of field names used for search criteria.
     *
     * @param fieldNames
     * @return
     */
    private static Document formatSearchFields(List<String> fieldNames) {
        if (fieldNames != null && !fieldNames.isEmpty()) {
            Document dbObject = new Document();

            for (String str : fieldNames) {
                dbObject.append(str, true);
            }

            return dbObject;
        }

        return null;
    }


    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames, QueryMarker... queryCriteria) {
        return userSearch(null, className, fieldNames, queryCriteria);
    }

    /**
     * Used to search and read a document from the collection based on certain
     * criteria and specified fields.
     *
     * @param nvce
     * @param fieldNames
     * @param queryCriteria
     * @return
     */
    @Override
    public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria) {
        return userSearch(null, nvce, fieldNames, queryCriteria);
    }

    /**
     * Inserts a document into the database.
     *
     * @param nve to be inserted
     * @return the inserted nve
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V extends NVEntity> V insert(V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nve);

        MongoCollection<Document> collection = lookupCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());

        Document doc = new Document();

        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        // TODO need to revisit
        SecurityController securityController = getAPIConfigInfo().getSecurityController();

        if (securityController != null)
            securityController.associateNVEntityToSubjectGUID(nve, null);

        if (SUS.isEmpty(nve.getGUID())) {
            nve.setGUID(IDGs.UUIDV4.generateID());
            nve.setReferenceID(nve.getGUID());
        }

        if (nve instanceof TimeStampInterface) {
            SharedUtil.touch((TimeStampInterface) nve, CRUD.CREATE, CRUD.UPDATE);
        }

        for (NVConfig nvc : nvce.getAttributes()) {
            if (securityController != null) {
                if (ChainedFilter.isFilterSupported(nvc.getValueFilter(), FilterType.ENCRYPT) || ChainedFilter.isFilterSupported(nvc.getValueFilter(), FilterType.ENCRYPT_MASK)) {
                    getAPIConfigInfo().
                            getKeyMaker().
                            createNVEntityKey(this,
                                    nve,
                                    getAPIConfigInfo().getKeyMaker().
                                            getKey(this,
                                                    getAPIConfigInfo().getKeyMaker()
                                                            .getMasterKey(),
                                                    nve.getSubjectGUID()));
                }
            }

            NVBase<?> nvb = nve.lookup(nvc.getName());

            if (nvb instanceof NVPairList) {
                if (((NVPairList) nvb).isFixed())
                    doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
                doc.append(nvc.getName(), serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false));
            } else if (nvb instanceof NVGetNameValueList) {
                if (((NVGetNameValueList) nvb).isFixed())
                    doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVGetNameValueList) nvb).isFixed());
                doc.append(nvc.getName(), serArrayValuesNVGetNameValueString(nve, (ArrayValues<GetNameValue<String>>) nvb, false));
            } else if (nvb instanceof NVPairGetNameMap) {
                List<Document> vals = serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false);
                doc.append(nvc.getName(), vals);
                //if(log.isEnabled()) log.getLogger().info("vals:" + vals);
            } else if (nvb instanceof NVEnum) {
                doc.append(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
            } else if (nvb instanceof NVEnumList) {
                doc.append(nvc.getName(), mapEnumListToStringList((NVEnumList) nvb));
            } else if (nvb instanceof NVStringList) {
                doc.append(nvc.getName(), ((NVStringList) nvb).getValue());
            } else if (nvb instanceof NVStringSet) {
                doc.append(nvc.getName(), ((NVStringSet) nvb).getValue());
            } else if (nvb instanceof NVGenericMap) {
                doc.append(nvc.getName(), serNVGenericMap((NVGenericMap) nvb));
            } else if (nvb instanceof NVGenericMapList) {

            } else if (nvb instanceof NVEntityReference) {
                NVEntity temp = (NVEntity) nvb.getValue();

                NVEntityReference nver = (NVEntityReference) nvb;
                if (nver.getNVConfig().isEmbedded()) {
                    log.getLogger().info("We Have an embedded Object: " + nver);
                }
                doc.append(nvc.getName(), serComplexNVEntityReference(connect(), nve, temp, nver.getNVConfig().isEmbedded(), false, false));
            } else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap) {
                doc.append(nvc.getName(), serArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, false, false, false));
            } else if (nvb instanceof NVBigDecimal) {
                doc.append(nvc.getName(), nvb.getValue().toString());
            } else if (nvb instanceof NVBigDecimalList) {
                List<String> values = new ArrayList<>();
                List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();

                for (BigDecimal decimal : valuesToConvert) {
                    values.add(decimal.toString());
                }

                doc.append(nvc.getName(), values);
            } else if (nvb instanceof NVBlob) {
                doc.append(nvc.getName(), nvb.getValue());
            } else if (nvb instanceof NamedValue) {

                doc.append(nvb.getName(), serNamedValue((NamedValue<?>) nvb));
            }


            else if (nvc.isArray()) {
                doc.append(nvc.getName(), nvb.getValue());
            } else if (MetaToken.GUID.getName().equals(nvc.getName())) {
                // set the GUID ass uuid in the database
                doc.append(MongoUtil.ReservedID.GUID.getValue(), IDGs.UUIDV4.decode((String) nvb.getValue()));
            } else if (nvc.isTypeReferenceID() && !MongoUtil.ReservedID.REFERENCE_ID.getName().equals(nvc.getName())) {
                String value = (String) nvb.getValue();
                if (value != null) {
                    doc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), IDGs.UUIDV4.decode(value));
                } else {
                    doc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), null);
                }
            }



            else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName())) {
                Object tempValue = securityController != null ? securityController.encryptValue(this, nve, nvc, nvb, null) : nvb.getValue();
                if (tempValue instanceof EncryptedData) {
                    doc.append(nvc.getName(), serNVEntity((EncryptedData) tempValue, true, false, false));
                } else {
                    doc.append(nvc.getName(), tempValue);
                }
            }
        } // end of for loop of the attributes

        //////We might need to put before the insert, need to test to conclude.
        if (!metaManager.isIndexed(collection)) {
            metaManager.addCollectionInfo(collection, nvce);
        }

        if (!SUS.isEmpty(nve.getReferenceID())) {
            doc.append(MongoUtil.ReservedID.REFERENCE_ID.getValue(), IDGs.UUIDV4.decode(nve.getGUID()));
        }


        try {
            collection.insertOne(doc);
        } catch (MongoException e) {
            e.printStackTrace();
            getAPIExceptionHandler().throwException(e);
        }

        if (dataCacheMonitor != null) {
            dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.CREATE, nve));
        }
        return nve;
    }


    @SuppressWarnings("unchecked")
    public Document serNVEntity(NVEntity nve, boolean embed, boolean sync, boolean updateReferenceOnly)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nve);


        Document doc = new Document();

        NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
        if (nve instanceof TimeStampInterface) {
            SharedUtil.touch((TimeStampInterface) nve, CRUD.UPDATE);
        }
        if (embed) {
            doc.put(MetaToken.CLASS_TYPE.getName(), nve.getClass().getName());
        }
        for (NVConfig nvc : nvce.getAttributes()) {
            NVBase<?> nvb = nve.lookup(nvc.getName());

            if (nvb instanceof NVPairList) {
                if (((NVPairList) nvb).isFixed())
                    doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
                doc.append(nvc.getName(), serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
            } else if (nvb instanceof NVGetNameValueList) {
                if (((NVGetNameValueList) nvb).isFixed())
                    doc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVGetNameValueList) nvb).isFixed());
                doc.append(nvc.getName(), serArrayValuesNVGetNameValueString(nve, (ArrayValues<GetNameValue<String>>) nvb, false));
            } else if (nvb instanceof NVPairGetNameMap) {
                ArrayList<Document> vals = serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync);
                doc.append(nvc.getName(), vals);
                //if(log.isEnabled()) log.getLogger().info("vals:" + vals);
            } else if (nvb instanceof NVEnum) {
                doc.append(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
            } else if (nvb instanceof NVEnumList) {
                doc.append(nvc.getName(), mapEnumListToStringList((NVEnumList) nvb));
            } else if (nvb instanceof NVEntityReference) {
                NVEntity temp = (NVEntity) nvb.getValue();


                doc.append(nvc.getName(), serComplexNVEntityReference(connect(), nve, temp, embed, sync, updateReferenceOnly));
            } else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap) {
                doc.append(nvc.getName(), serArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, embed, sync, updateReferenceOnly));
            } else if (nvb instanceof NVBigDecimal) {
                doc.append(nvc.getName(), nvb.getValue().toString());
            } else if (nvb instanceof NamedValue) {
                doc.append(nvc.getName(), serNamedValue((NamedValue<?>) nvb));
            } else if (nvb instanceof NVBigDecimalList) {
                List<String> values = new ArrayList<>();
                List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();

                for (BigDecimal decimal : valuesToConvert) {
                    values.add(decimal.toString());
                }

                doc.append(nvc.getName(), values);
            } else if (nvb instanceof NVBlob) {
                doc.append(nvc.getName(), nvb.getValue());
            }


            else if (nvc.isArray()) {
                doc.append(nvc.getName(), nvb.getValue());
            } else if (nvc.isTypeReferenceID() && !MongoUtil.ReservedID.REFERENCE_ID.getName().equals(nvc.getName())) {
                String value = (String) nvb.getValue();

                if (value != null) {
                    doc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), IDGs.UUIDV4.decode(value));
                } else {
                    doc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), null);
                }
            }


            else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName())) {
                doc.append(nvc.getName(), nvb.getValue());
            }
        }


        if (!SUS.isEmpty(nve.getReferenceID())) {
            doc.append(MongoUtil.ReservedID.REFERENCE_ID.getValue(), IDGs.UUIDV4.decode(nve.getReferenceID()));
        }


        if (!embed) {
            MongoCollection<Document> collection = lookupCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
            //////We might need to put before the insert, need to test to conclude.
            if (!metaManager.isIndexed(collection)) {
                metaManager.addCollectionInfo(collection, nvce);
            }
        }

        return doc;
    }


    public <V extends NVEntity> V update(V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        return patch(nve, true, false, false, false);
    }



    /**
     * @param nve                 to be update
     * @param updateTS            if the NVE instanceof TimeStampInterface will update the timestamp value
     * @param sync                if true synchronized access
     * @param updateReferenceOnly update only reference
     * @param includeParam
     * @param nvConfigNames
     * @param <V>
     * @return
     * @throws NullPointerException
     * @throws IllegalArgumentException
     * @throws APIException
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateReferenceOnly, boolean includeParam, String... nvConfigNames)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nve);

        try {
            if (sync) {
                updateLock.lock();
            }
            // TODO revisit logic
            if (getAPIConfigInfo().getSecurityController() != null)
                getAPIConfigInfo()
                        .getSecurityController()
                        .associateNVEntityToSubjectGUID(nve, null);

            if (nve.lookupValue(MetaToken.GUID) == null) {
                return insert(nve);
            }

            NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
            MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());

            UUID refIdUUID = IDGs.UUIDV4.decode(nve.getReferenceID());
            Document originalDoc = lookupByReferenceID(nvce.toCanonicalID(), refIdUUID);
            if (originalDoc == null) {
                throw new APIException("Can not update a missing object " + nve.getReferenceID());
            }
            Document updatedDoc = new Document();

            if (updateTS && nve instanceof TimeStampInterface) {
                SharedUtil.touch((TimeStampInterface) nve, CRUD.UPDATE);
            }

            boolean patchMode = true;
            List<NVConfig> paramsToUpdate = null;

            if (nvConfigNames != null && nvConfigNames.length != 0) {
                paramsToUpdate = new ArrayList<NVConfig>();

                if (includeParam) {
                    // the list of parameters to be updated
                    for (String name : nvConfigNames) {
                        name = SUS.trimOrNull(name);
                        if (name != null) {
                            NVConfig nvc = nvce.lookup(name);

                            if (nvc != null) {
                                paramsToUpdate.add(nvc);
                            }
                        }
                    }
                } else {
                    // the list of parameters to be excluded from the update
                    Set<String> exclusionSet = new HashSet<String>();
                    for (String name : nvConfigNames) {
                        name = SUS.trimOrNull(name);
                        if (name != null)
                            exclusionSet.add(name);
                    }

                    if (exclusionSet.size() > 0) {
                        for (NVConfig nvc : nvce.getAttributes()) {
                            if (!exclusionSet.contains(nvc.getName())) {
                                paramsToUpdate.add(nvc);
                            }
                        }
                    }
                }
            }

            if (paramsToUpdate == null || paramsToUpdate.size() == 0) {
                paramsToUpdate = nvce.getAttributes();
                patchMode = false;
            }

            for (NVConfig nvc : paramsToUpdate) {
                NVBase<?> nvb = nve.lookup(nvc.getName());

                if (nvb instanceof NVPairList) {
                    if (((NVPairList) nvb).isFixed())
                        updatedDoc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());

                    updatedDoc.put(nvc.getName(), serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
                } else if (nvb instanceof NVGetNameValueList) {
                    if (((NVGetNameValueList) nvb).isFixed())
                        updatedDoc.append(SharedUtil.toCanonicalID('_', nvc.getName(), MetaToken.IS_FIXED.getName()), ((NVGetNameValueList) nvb).isFixed());
                    updatedDoc.append(nvc.getName(), serArrayValuesNVGetNameValueString(nve, (ArrayValues<GetNameValue<String>>) nvb, false));
                } else if (nvb instanceof NVPairGetNameMap) {
                    updatedDoc.put(nvc.getName(), serArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
                } else if (nvb instanceof NVEnum) {
                    updatedDoc.put(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
                } else if (nvb instanceof NVEnumList) {
                    updatedDoc.put(nvc.getName(), mapEnumListToStringList((NVEnumList) nvb));
                } else if (nvb instanceof NVStringList) {
                    updatedDoc.put(nvc.getName(), ((NVStringList) nvb).getValue());
                } else if (nvb instanceof NVStringSet) {
                    updatedDoc.put(nvc.getName(), ((NVStringSet) nvb).getValue());
                } else if (nvb instanceof NVGenericMap) {
                    updatedDoc.put(nvc.getName(), serNVGenericMap((NVGenericMap) nvb));
                } else if (nvb instanceof NamedValue) {
                    updatedDoc.put(nvc.getName(), serNamedValue((NamedValue<?>) nvb));
                } else if (nvb instanceof NVGenericMapList) {

                } else if (nvb instanceof NVEntityReference) {
                    NVEntity temp = (NVEntity) nvb.getValue();

                    if (temp != null) {


                        //updatedDoc.put(nvc.getName(), temp.getReferenceID());
                        updatedDoc.put(nvc.getName(), serComplexNVEntityReference(connect(), nve, temp, nvc.isEmbedded(), sync, updateReferenceOnly));// new ObjectId(temp.getReferenceID()));
                    } else {
                        updatedDoc.put(nvc.getName(), null);
                    }

                } else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap) {
                    updatedDoc.put(nvc.getName(), serArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, false, sync, updateReferenceOnly));
                } else if (nvb instanceof NVBigDecimal) {
                    updatedDoc.put(nvc.getName(), nvb.getValue().toString());
                } else if (nvb instanceof NVBigDecimalList) {
                    List<String> values = new ArrayList<>();
                    List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();

                    for (BigDecimal decimal : valuesToConvert) {
                        values.add(decimal.toString());
                    }

                    updatedDoc.put(nvc.getName(), values);
                } else if (nvb instanceof NVBlob) {
                    updatedDoc.put(nvc.getName(), nvb.getValue());
                } else if (nvc.isArray()) {
                    updatedDoc.put(nvc.getName(), nvb.getValue());
                } else if (nvc.isTypeReferenceID() && !MongoUtil.ReservedID.REFERENCE_ID.getName().equals(nvc.getName())) {
                    String value = (String) nvb.getValue();

                    if (value != null) {
                        updatedDoc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), IDGs.UUIDV4.decode(value));
                    } else {
                        updatedDoc.append(MongoUtil.ReservedID.map(nvc, nvc.getName()), null);
                    }
                } else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName())) {
                    Object tempValue = getAPIConfigInfo().getSecurityController() != null ? getAPIConfigInfo().getSecurityController().encryptValue(this, nve, nvc, nvb, null) : nvb.getValue();
                    if (tempValue instanceof EncryptedData) {
                        updatedDoc.put(nvc.getName(), serNVEntity((EncryptedData) tempValue, true, sync, updateReferenceOnly));
                    } else {
                        updatedDoc.put(nvc.getName(), tempValue);
                    }

                }
            }

            Document updatedObj = new Document();
            updatedObj.put("$set", updatedDoc);

            try {
                if (collection != null) {
                    collection.updateOne(Filters.eq(MongoUtil.ReservedID.REFERENCE_ID.getValue(), refIdUUID), updatedObj);
                }
            } catch (MongoException e) {
                getAPIExceptionHandler().throwException(e);
            }

            if (patchMode) {
                nve = (V) searchByID((NVConfigEntity) nve.getNVConfig(), nve.getReferenceID()).get(0);
            }


            if (dataCacheMonitor != null) {
                dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.UPDATE, nve));
            }
        } finally {
            if (sync) {
                updateLock.unlock();
            }
        }

        return nve;
    }

    /**
     * Deletes a document from the database.
     *
     * @param nve
     * @param withReference
     * @return
     */
    @Override
    public <V extends NVEntity> boolean delete(V nve, boolean withReference)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nve);
        boolean ret = false;

        if (nve.getReferenceID() != null) {
            UUID deleteKey = IDGs.UUIDV4.decode(nve.getReferenceID());
            Document filter = new Document(MongoUtil.ReservedID.REFERENCE_ID.getValue(), deleteKey);
            MongoCollection<Document> collection = lookupCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());


            try {
                ret = collection.deleteOne(filter).wasAcknowledged();
                if (ret && dataCacheMonitor != null) {
                    dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, nve));
                }
            } catch (MongoException e) {
                getAPIExceptionHandler().throwException(e);
            }

            if (ret && withReference) {

                // the associated encryption key dao
                MongoCollection<Document> ekdCollection = lookupCollection(EncapsulatedKey.NVCE_ENCAPSULATED_KEY.getName());
                //if(log.isEnabled()) log.getLogger().info("EncryptedKeyDAO:" + ekdCollection);
                if (ekdCollection != null)
                    ekdCollection.deleteOne(filter);
                // end

                NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();

                for (NVConfig tempNVC : nvce.getAttributes()) {
                    if (tempNVC instanceof NVConfigEntity) {
                        if (!tempNVC.isArray()) {
                            NVEntity toRemove = nve.lookupValue(tempNVC);
                            if (toRemove != null) {
                                delete(toRemove, withReference);
                                if (dataCacheMonitor != null) {
                                    dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, toRemove));
                                }
                            }
                        } else {
                            NVBase<?> val = nve.lookup(tempNVC);
                            if (val instanceof ArrayValues) {
                                @SuppressWarnings("unchecked")
                                NVEntity[] toRemoves = ((ArrayValues<NVEntity>) val).values();
                                for (NVEntity toRemove : toRemoves) {
                                    if (toRemove != null) {
                                        delete(toRemove, withReference);
                                        if (dataCacheMonitor != null) {
                                            dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, toRemove));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return ret;
    }

    public boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, APIException, AccessException {
        SUS.checkIfNulls("Null value", nvce);
        if (queryCriteria == null || queryCriteria.length == 0) {
            throw new IllegalArgumentException("delete(NVConfigEntity, QueryMarker...) refuses empty criteria; use deleteAll for that.");
        }
        Document query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
        if (query.isEmpty()) {
            throw new IllegalArgumentException("delete(NVConfigEntity, QueryMarker...) refuses empty filter; use deleteAll for that.");
        }
        MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());
        return collection.deleteMany(query).wasAcknowledged();
    }

    /**
     * Deletes all the documents in the collection. Use with caution.
     *
     * @param nvce
     * @return
     * @throws NullPointerException
     * @throws IllegalArgumentException
     * @throws APIException
     */
    public boolean deleteAll(NVConfigEntity nvce)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Null value", nvce);

        MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());

        try {
            collection.deleteMany(new Document());
        } catch (MongoException e) {
            getAPIExceptionHandler().throwException(e);
        }
        return true;
    }


    private MongoCollection<Document> lookupCollection(String name) {
        SUS.checkIfNulls("Null collection name ", name);
        return connect().getCollection(name);
    }

    /**
     * Returns the API service information.
     *
     * @return The configuration object for the data store
     */
    @Override
    public APIConfigInfo getAPIConfigInfo() {
        return configInfo;
    }

    /**
     * Sets the API service information.
     *
     * @param ci
     */
    @Override
    public synchronized void setAPIConfigInfo(APIConfigInfo ci) {
        this.configInfo = ci;
    }

    /**
     * Returns the number of matches found based on query criteria.
     *
     * @param nvce
     * @param queryCriteria
     * @return
     */
    @Override
    public long countMatch(NVConfigEntity nvce, QueryMarker... queryCriteria) {
        Document query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
        MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());

        return collection.countDocuments(query);
    }

    /**
     * @return GridFSBucket associated with GridFS data store
     */
    private GridFSBucket createBucket() {
        if (gridFSDB == null)
            synchronized (this) {
                if (gridFSDB == null) {
                    gridFSDB = newConnection().getDatabase(XlogistxMongoDSCreator.MongoParam.gridFSDataStoreName(getAPIConfigInfo()));
                }
            }

        return GridFSBuckets.create(gridFSDB);
    }

    /**
     * Creates and stores a file into the database.
     *
     * @param file
     * @param is
     * @return
     */
    @Override
    public APIFileInfoMap createFile(String folder, APIFileInfoMap file, InputStream is, boolean closeStream)
            throws IllegalArgumentException, IOException, NullPointerException {
        SUS.checkIfNulls("Null value", file, is);
        if (log.isEnabled()) log.getLogger().info(file.getOriginalFileInfo().getName());


        GridFSBucket gridFSBucket = createBucket();
        try {

            gridFSBucket.uploadFromStream(MongoUtil.SINGLETON.bsonNVEGUID(file.getOriginalFileInfo()), file.getOriginalFileInfo().getName(), is, new GridFSUploadOptions());
            GridFSFile gridFSFile = gridFSBucket.find(MongoUtil.SINGLETON.idAsGUID(file.getOriginalFileInfo())).first();
            if (gridFSFile != null) {
                file.getOriginalFileInfo().setLength(gridFSFile.getLength());
            }

            try {
                update(file.getOriginalFileInfo());
            } catch (RuntimeException metadataEx) {
                // Metadata write failed — roll back the uploaded chunks to avoid orphans.
                try {
                    gridFSBucket.delete(MongoUtil.SINGLETON.bsonNVEGUID(file.getOriginalFileInfo()));
                } catch (Exception rollbackEx) {
                    if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING,
                            "GridFS rollback failed for " + file.getOriginalFileInfo().getName(), rollbackEx);
                }
                throw metadataEx;
            }

        } finally {

            if (closeStream)
                SharedIOUtil.close(is);
        }

        return file;

    }

    /**
     * Reads a file from the database.
     *
     * @param file
     * @param os
     * @return
     */
    @Override
    public APIFileInfoMap readFile(APIFileInfoMap file, OutputStream os, boolean closeStream)
            throws IllegalArgumentException, IOException, NullPointerException {
        SUS.checkIfNulls("Null value", file, os);

        try {

            GridFSBucket gridFSBucket = createBucket();
            GridFSFile gridFSFile = gridFSBucket.find(MongoUtil.SINGLETON.idAsGUID(file.getOriginalFileInfo())).first();


            if (gridFSFile != null) {
                gridFSBucket.downloadToStream(MongoUtil.SINGLETON.bsonNVEGUID(file.getOriginalFileInfo()), os);
                //out.writeTo(os);
            }
        } finally {
            if (closeStream)
                SharedIOUtil.close(os);
        }
        if (log.isEnabled()) log.getLogger().info(file.getOriginalFileInfo().getName());
        return file;
    }


    /**
     * Updates and overwrites an existing file in the database.
     *
     * @param file
     * @param is
     * @return
     */
    @Override
    public APIFileInfoMap updateFile(APIFileInfoMap file, InputStream is, boolean closeStream)
            throws IllegalArgumentException, IOException, NullPointerException {

        deleteFile(file);
        if (log.isEnabled()) log.getLogger().info(file.getOriginalFileInfo().getName());
        return createFile(null, file, is, closeStream);



    }

    /**
     * Deletes a file in the database.
     *
     * @param file
     */
    @Override
    public void deleteFile(APIFileInfoMap file)
            throws IllegalArgumentException, IOException, NullPointerException {
        SUS.checkIfNulls("Null value", file);
        if (log.isEnabled()) log.getLogger().info(file.getOriginalFileInfo().getName());


        GridFSBucket gridFSBucket = createBucket();
        gridFSBucket.delete(MongoUtil.SINGLETON.bsonNVEGUID(file.getOriginalFileInfo()));


    }

    /**
     * Searches and returns a list of files in the database.
     *
     * @param args
     */
    @Override
    public List<APIFileInfoMap> search(String... args)
            throws IllegalArgumentException, IOException, NullPointerException {
        return null;
    }

    @Override
    public boolean isProviderActive() {
        return mongoClient != null && mongoDB != null;
    }

    /**
     * Sets the description.
     *
     * @param str
     */
    @Override
    public void setDescription(String str) {
        description = str;
    }

    /**
     * Returns the description.
     */
    @Override
    public String getDescription() {

        return description;
    }

    /**
     * Sets the name.
     *
     * @param name
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the name.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns a string representation of the class.
     */
    @Override
    public String toCanonicalID() {
        return XlogistxMongoDSCreator.API_NAME + ":" + (name != null ? name : "unnamed");
    }

    /**
     * Searches by ID.
     *
     * @param nvce
     * @param ids
     */
    @Override
    public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, APIException {
        return userSearchByID(null, nvce, ids);
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(String className, String... ids)
            throws NullPointerException, IllegalArgumentException, APIException {
        NVEntity tempNVE;

        try {
            tempNVE = (NVEntity) Class.forName(className).getConstructor().newInstance();
            return userSearchByID(null, (NVConfigEntity) tempNVE.getNVConfig(), ids);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException |
                 NoSuchMethodException | InvocationTargetException e) {
            throw new APIException("Invalid class name " + className);
        }
    }

    public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        DynamicEnumMapManager.validateDynamicEnumMap(dynamicEnumMap);

        MongoCollection<Document> collection = lookupCollection(dynamicEnumMap.getClass().getName());
        metaManager.addUniqueIndexesForDynamicEnumMap(collection);

        Document doc = new Document();
        doc.append(MetaToken.NAME.getName(), dynamicEnumMap.getName());
        doc.append(MetaToken.VALUE.getName(), serArrayValuesNVPair(null, dynamicEnumMap, false));
        doc.append(MetaToken.DESCRIPTION.getName(), dynamicEnumMap.getDescription());

        if (!SUS.isEmpty(dynamicEnumMap.getReferenceID())) {
            // Since we are referencing the object, we will use the reference_id NOT _id.
            doc.append(MetaToken.REFERENCE_ID.getName(), IDGs.UUIDV4.decode(dynamicEnumMap.getReferenceID()));
        }

        try {
            if (collection != null)
                collection.insertOne(doc);
        } catch (MongoException e) {

            getAPIExceptionHandler().throwException(e);
        }

        dynamicEnumMap.setReferenceID(IDGs.UUIDV4.encode(MongoUtil.SINGLETON.getRefIDAsUUID(doc)));

        return dynamicEnumMap;
    }

    public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        DynamicEnumMapManager.validateDynamicEnumMap(dynamicEnumMap);

        MongoCollection<Document> collection = lookupCollection(dynamicEnumMap.getClass().getName());

        //BasicDBObject originalDoc = lookupByReferenceID(collection.getName(), new ObjectId(dynamicEnumMap.getReferenceID()));
        Document originalDoc = lookupByName(collection.getNamespace().getCollectionName(), dynamicEnumMap.getName());

        if (originalDoc == null) {
            return insertDynamicEnumMap(dynamicEnumMap);
        } else {
            Document updatedDoc = new Document();

            updatedDoc.put(MetaToken.VALUE.getName(), serArrayValuesNVPair(null, dynamicEnumMap, false));
            updatedDoc.put(MetaToken.DESCRIPTION.getName(), dynamicEnumMap.getDescription());

            Document updatedObj = new Document();
            updatedObj.put("$set", updatedDoc);

            try {
                if (collection != null)
                    collection.updateOne(originalDoc, updatedObj);
            } catch (MongoException e) {
                getAPIExceptionHandler().throwException(e);
            }

            return DynamicEnumMapManager.SINGLETON.addDynamicEnumMap(dynamicEnumMap);
        }
    }


    /**
     * Looks up the database object in the collection by name.
     *
     * @param collectionName
     * @param name
     * @return
     */
    private Document lookupByName(String collectionName, String name) {
        MongoCollection<Document> collection = lookupCollection(collectionName);
        Document query = new Document();
        query.put(MetaToken.NAME.getName(), name);

        Document dbObj = null;

        try {
            dbObj = collection.find(query).first();
        } catch (MongoException e) {
            getAPIExceptionHandler().throwException(e);
        }

        return dbObj;
    }


    /**
     * Converts the database object to dynamic enum type.
     *
     * @param obj
     * @return
     */
    private DynamicEnumMap fromDBtoDynamicEnumMap(Document obj) {
        List<Document> list = (List<Document>) obj.get(MetaToken.VALUE.getName());
        UUID objectID = MongoUtil.SINGLETON.getRefIDAsUUID(obj);

        List<NVPair> nvpl = new ArrayList<NVPair>();

        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                nvpl.add(toNVPair(null, null, list.get(i)));
            }
        }


        String demName = (String) obj.get(MetaToken.NAME.getName());
        //if(log.isEnabled()) log.getLogger().info("DynamicEnumMap Name : " + demName);
        DynamicEnumMap dem = new DynamicEnumMap(demName, nvpl);
        dem.setReferenceID(IDGs.UUIDV4.encode(objectID));
        //if(log.isEnabled()) log.getLogger().info("values " + dem.getValue());
        dem = DynamicEnumMapManager.SINGLETON.addDynamicEnumMap(dem);
        //if(log.isEnabled()) log.getLogger().info(dem.getName() + ":" + dem.getValue());



        return dem;
    }


    /**
     * Searches for the dynamic enum by reference ID.
     *
     * @param refID
     * @return
     */
    public DynamicEnumMap searchDynamicEnumMapByReferenceID(String refID)
            throws NullPointerException, IllegalArgumentException, APIException {
        return searchDynamicEnumMapByReferenceID(refID, null);
    }


    /**
     * Searches for the dynamic enum by reference ID.
     *
     * @param refID
     * @param clazz
     * @return
     */
    public DynamicEnumMap searchDynamicEnumMapByReferenceID(String refID, Class<? extends DynamicEnumMap> clazz)
            throws NullPointerException, IllegalArgumentException, APIException {
        MongoCollection<Document> collection = null;

        if (clazz != null) {
            collection = lookupCollection(clazz.getName());
        } else {
            collection = lookupCollection(DynamicEnumMap.class.getName());
        }


        return searchDynamicEnumMapByReferenceID(IDGs.UUIDV4.decode(refID), collection.getNamespace().getCollectionName());
    }


    private DynamicEnumMap searchDynamicEnumMapByReferenceID(UUID objectID, String collectionName)
            throws NullPointerException, IllegalArgumentException, APIException {
        MongoCollection<Document> collection = lookupCollection(collectionName);

        Document doc = null;

        if (collection != null)
            doc = lookupByReferenceID(collection.getNamespace().getCollectionName(), objectID);


        return fromDBtoDynamicEnumMap(doc);
    }


    /**
     * Searches for the dynamic enum by name.
     *
     * @param name
     * @return
     */
    public DynamicEnumMap searchDynamicEnumMapByName(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
        return searchDynamicEnumMapByName(name, null);
    }


    /**
     * Searches for the dynamic enum by name.
     *
     * @param name
     * @param clazz
     * @return
     */
    public DynamicEnumMap searchDynamicEnumMapByName(String name, Class<? extends DynamicEnumMap> clazz)
            throws NullPointerException, IllegalArgumentException, APIException {
        if (!name.startsWith(DynamicEnumMap.NAME_PREFIX + ":")) {
            name = SharedUtil.toCanonicalID(':', DynamicEnumMap.NAME_PREFIX, name);
        }

        Document doc = null;

        MongoCollection<Document> collection = null;

        if (clazz != null) {
            collection = lookupCollection(clazz.getName());
        } else {
            collection = lookupCollection(DynamicEnumMap.class.getName());
        }


        if (collection != null)
            doc = lookupByName(collection.getNamespace().getCollectionName(), name);

        return fromDBtoDynamicEnumMap(doc);
    }


    /**
     * Deletes a dynamic enum based on name.
     *
     * @param name
     */
    public void deleteDynamicEnumMap(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
        deleteDynamicEnumMap(name, null);
    }


    /**
     * Deletes a dynamic enum based on name.
     *
     * @param name
     * @param clazz
     */
    public void deleteDynamicEnumMap(String name, Class<? extends DynamicEnumMap> clazz)
            throws NullPointerException, IllegalArgumentException, APIException {
        if (!name.startsWith(DynamicEnumMap.NAME_PREFIX + ":")) {
            name = SharedUtil.toCanonicalID(':', DynamicEnumMap.NAME_PREFIX, name);
        }

        MongoCollection<Document> collection = null;

        if (clazz != null) {
            collection = lookupCollection(clazz.getName());
        } else {
            collection = lookupCollection(DynamicEnumMap.class.getName());
        }

        Document doc = lookupByName(collection.getNamespace().getCollectionName(), name);

        if (doc != null) {
            if (collection != null)
                collection.deleteOne(doc);
        }

        DynamicEnumMapManager.SINGLETON.deleteDynamicEnumMap(name);
    }

    /**
     * Returns a list of dynamic enum map in the dynamic enum map collection.
     *
     * @param domainID
     * @param userID
     * @return
     */
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
            throws NullPointerException, IllegalArgumentException, APIException {
        return getAllDynamicEnumMap(domainID, userID, null);
    }

    /**
     * Returns a list of dynamic enum map in the dynamic enum map collection.
     *
     * @param domainID
     * @param userID
     * @param clazz
     * @return
     */
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID, Class<? extends DynamicEnumMap> clazz)
            throws NullPointerException, IllegalArgumentException, APIException {
        List<DynamicEnumMap> list = new ArrayList<DynamicEnumMap>();

        MongoCollection<Document> collection;
        if (clazz != null) {
            collection = lookupCollection(clazz.getName());
        } else {
            collection = lookupCollection(DynamicEnumMap.class.getName());
        }

        Document filter = new Document();
        if (SUS.isNotEmpty(domainID)) {
            filter.append(MetaToken.DOMAIN_ID.getName(), domainID);
        }
        if (SUS.isNotEmpty(userID)) {
            filter.append(MetaToken.SUBJECT_GUID.getName(), userID);
        }

        try (MongoCursor<Document> cur = withTimeout(collection.find(filter)).cursor()) {
            while (cur.hasNext()) {
                list.add(fromDBtoDynamicEnumMap(cur.next()));
            }
        }

        return list;
    }

    @Override
    public Map<String, APIFileInfoMap> discover() {
        return null;
    }

    @Override
    public MongoClient newConnection() throws APIException {
        if (mongoClient == null)
            synchronized (this) {
                try {
                    if (mongoClient == null)
                        mongoClient = MongoClients.create(XlogistxMongoDSCreator.MongoParam.dataStoreURI(getAPIConfigInfo()));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new APIException(e.getMessage());
                }
            }

        return mongoClient;

    }

    private static ServerAddress getDBAddress(APIConfigInfo aci) throws NumberFormatException, UnknownHostException {
        return new ServerAddress((String) aci.getProperties().getValue(XlogistxMongoDSCreator.MongoParam.HOST),
                aci.getProperties().getValue(XlogistxMongoDSCreator.MongoParam.PORT));
        //SharedUtil.lookupValue(aci.getConfigParameters().get(MongoDataStoreCreator.MongoParam.DB_NAME.getName())));
    }

    @Override
    public APIFileInfoMap createFolder(String folderFullPath)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID,
                                                       NVConfigEntity nvce,
                                                       String... ids)
            throws NullPointerException,
            IllegalArgumentException,
            AccessException,
            APIException {
        List<V> retNVEs = new ArrayList<>();
        List<UUID> refIdsToLookFor = new ArrayList<>();

        for (String id : ids) {
            refIdsToLookFor.add(IDGs.UUIDV4.decode(id));
        }

        List<Document> listOfDBObject = lookupByReferenceIDs(nvce.getName(), refIdsToLookFor);

        for (Document dbObject : listOfDBObject) {
            try {
                retNVEs.add(fromDB(userID, connect(), dbObject, (Class<? extends NVEntity>) nvce.getMetaType()));
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        if (userID == null && dataCacheMonitor != null) {
            dataCacheMonitor.monitorNVEntity(new CRUDNVEntityListDAO(CRUD.READ, (List<NVEntity>) retNVEs));
        }
        return retNVEs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends NVEntity> List<V> userSearch(String userID,
                                                   NVConfigEntity nvce, List<String> fieldNames,
                                                   QueryMarker... queryCriteria) throws NullPointerException,
            IllegalArgumentException, AccessException, APIException {
        List<V> list = new ArrayList<V>();


        Document query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
        MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());
        MongoCursor<Document> cur = null;

        try {
            if (collection != null) {
                cur = withTimeout(collection.find(query).projection(formatSearchFields(fieldNames))).limit(maxSearchResults).cursor();

                while (cur.hasNext()) {
                    try {
                        V nve = fromDB(userID, connect(), cur.next(), (Class<? extends NVEntity>) nvce.getMetaType());
                        list.add(nve);
                    } catch (InstantiationException | IllegalAccessException e) {
                        if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING, "fromDB failed", e);
                    }
                }
                if (list.size() == maxSearchResults && log.isEnabled()) {
                    log.getLogger().warning("userSearch hit maxSearchResults cap (" + maxSearchResults + ") for " + nvce.toCanonicalID());
                }
            }
        } catch (MongoException e) {
            if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING, "userSearch failed", e);
            getAPIExceptionHandler().throwException(e);
        } finally {
            SharedIOUtil.close(cur);
        }

        if (userID == null && dataCacheMonitor != null) {
            dataCacheMonitor.monitorNVEntity(new CRUDNVEntityListDAO(CRUD.READ, (List<NVEntity>) list));
        }

        return list;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID,
                                                   String className, List<String> fieldNames,
                                                   QueryMarker... queryCriteria) throws NullPointerException,
            IllegalArgumentException, AccessException, APIException {
        NVConfigEntity nvce = null;
        SUS.checkIfNulls("null class name", className);
        try {
            nvce = MetaUtil.fromClass(className);
        } catch (Exception e) {
            e.printStackTrace();
            throw new APIException("Class name not found:" + className);

        }

        return userSearch(userID, nvce, fieldNames, queryCriteria);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T lookupProperty(GetName propertyName) {
        if (propertyName != null && propertyName instanceof APIProperty) {
            APIProperty apiProperty = (APIProperty) propertyName;
            switch (apiProperty) {
                case ASYNC_CREATE:
                    return (T) Boolean.TRUE;
                case ASYNC_DELETE:
                    break;
                case ASYNC_READ:
                    break;
                case ASYNC_UPDATE:
                    break;
                case RETRY_DELAY:
                    return (T) Long.valueOf(Const.TimeInMillis.SECOND.MILLIS * ServerUtil.RNG.nextInt(4) + Const.TimeInMillis.SECOND.MILLIS * 2);

                default:
                    break;

            }
        }

        return null;
    }

    @Override
    public boolean isBusy() {
        // Busy whenever another thread is holding the patch/update lock.
        return ((ReentrantLock) updateLock).isLocked();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("NVConfigEntity is null.", nvce);
        List<T> list = new ArrayList<T>();

        Document query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
        MongoCollection<Document> collection = lookupCollection(nvce.toCanonicalID());
        MongoCursor<Document> cur = null;

        try {
            if (collection != null) {
                List<String> fieldNames = new ArrayList<String>();
                fieldNames.add(MongoUtil.ReservedID.REFERENCE_ID.getValue());
                fieldNames.add(MetaToken.GUID.getName());
                fieldNames.add(MetaToken.SUBJECT_GUID.getName());
                cur = withTimeout(collection.find(query).projection(formatSearchFields(fieldNames))).limit(maxSearchResults).cursor();

                SecurityController sc = getAPIConfigInfo() != null ? getAPIConfigInfo().getSecurityController() : null;

                while (cur.hasNext()) {
                    Document dbObject = cur.next();
                    UUID refID = (UUID) dbObject.get(MongoUtil.ReservedID.REFERENCE_ID.getValue());
                    if (refID == null) continue;

                    String guid = MongoUtil.ReservedID.GUID.decode(dbObject);
                    String subjectGUID = MongoUtil.ReservedID.SUBJECT_GUID.decode(dbObject);

                    // Access control: if a security controller is present, enforce it.
                    // Otherwise allow the match.
                    boolean allowed;
                    if (sc != null) {
                        allowed = guid != null && subjectGUID != null && sc.isNVEntityAccessible(guid, subjectGUID, CRUD.READ);
                    } else {
                        allowed = true;
                    }

                    if (allowed) {
                        list.add((T) refID);
                    }
                }
            }
        } catch (MongoException e) {
            if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING, "batchSearch failed", e);
            getAPIExceptionHandler().throwException(e);
        } finally {
            SharedIOUtil.close(cur);
        }

        APISearchResult<T> results = new APISearchResult<T>();
        results.setNVConfigEntity(nvce);
        results.setReportID(UUID.randomUUID().toString());
        results.setMatchIDs(list);
        results.setCreationTime(System.currentTimeMillis());
        results.setLastTimeUpdated(System.currentTimeMillis());
        results.setLastTimeRead(System.currentTimeMillis());

        return results;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(String className, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        NVEntity nve = null;

        try {
            Class<?> clazz = Class.forName(className);
            nve = (NVEntity) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Class " + className + " not supported.");
        }

        return batchSearch((NVConfigEntity) nve.getNVConfig(), queryCriteria);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> reportResults, int startIndex, int batchSize)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        APIBatchResult<V> batch = new APIBatchResult<V>();
        batch.setReportID(reportResults.getReportID());
        batch.setTotalMatches(reportResults.size());

        if (startIndex >= reportResults.size()) {
            return null;
        }

        int endIndex = 0;

        //	If batch size is -1, retrieve all remaining.
        if (batchSize == -1 || (startIndex + batchSize >= reportResults.size())) {
            endIndex = reportResults.size();
        } else {
            endIndex = startIndex + batchSize;
        }

        batch.setRange(startIndex, endIndex);

        List<UUID> objectIDsList = (List<UUID>) reportResults.getMatchIDs().subList(startIndex, endIndex);

        List<Document> dbObjectsList = lookupByReferenceIDs(reportResults.getNVConfigEntity().toCanonicalID(), objectIDsList);
        List<NVEntity> nveList = new ArrayList<NVEntity>();

        if (dbObjectsList != null) {
            for (Document obj : dbObjectsList) {
                try {
                    nveList.add((V) fromDB(null, connect(), obj, (Class<? extends NVEntity>) reportResults.getNVConfigEntity().getMetaType()));
                } catch (InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        batch.setBatch(nveList);

        return batch;
    }


    @Override
    public LongSequence createSequence(String sequenceName, long startValue, long defaultIncrement)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        sequenceName = LowerCaseFilter.SINGLETON.validate(sequenceName);
        SUS.checkIfNulls("Null sequence name", sequenceName);
        if (startValue < 0)
            throw new IllegalArgumentException("Sequence start value can't be negative:" + startValue);

        if (defaultIncrement < 1)
            throw new IllegalArgumentException("Sequence default increment can't < 1:" + startValue);


        List<LongSequence> result = search(LongSequence.NVC_LONG_SEQUENCE, null, new QueryMatchString(DataParam.NAME.getNVConfig(),
                LowerCaseFilter.SINGLETON.validate(sequenceName), RelationalOperator.EQUAL));
        if (result == null || result.size() == 0) {
            LongSequence ls = new LongSequence();
            ls.setName(sequenceName);
            ls.setSequenceValue(startValue);
            ls.setDefaultIncrement(defaultIncrement);
            insert(ls);

            return ls;
        }
        return result.get(0);


    }

    @Override
    public long currentSequenceValue(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        List<LongSequence> result = search(LongSequence.NVC_LONG_SEQUENCE, null, new QueryMatchString(DataParam.NAME.getNVConfig(),
                LowerCaseFilter.SINGLETON.validate(sequenceName), RelationalOperator.EQUAL));
        if (result == null || result.size() != 1) {
            throw new APIException(sequenceName + " not found");
        }
        return result.get(0).getSequenceValue();
    }

    @Override
    public long nextSequenceValue(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return localNextSequenceValue(sequenceName, 0, true);
    }

    @Override
    public long nextSequenceValue(String sequenceName, long increment)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return localNextSequenceValue(sequenceName, increment, false);
    }

    /**
     * Atomic sequence increment backed by Mongo's {@code findOneAndUpdate} + {@code $inc}.
     * Safe across multiple JVMs / replicas.
     */
    private long localNextSequenceValue(String sequenceName, long increment, boolean defaultIncrement)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        if (!defaultIncrement && increment < 1)
            throw new IllegalArgumentException("wrong increment");

        String normalizedName = LowerCaseFilter.SINGLETON.validate(sequenceName);

        // When the caller asks for the "default" increment, we need to read the sequence's
        // configured default first — unavoidable extra round trip, but still atomic on the
        // $inc below.
        if (defaultIncrement) {
            List<LongSequence> result = search(LongSequence.NVC_LONG_SEQUENCE, null,
                    new QueryMatchString(DataParam.NAME.getNVConfig(), normalizedName, RelationalOperator.EQUAL));
            if (result == null || result.size() != 1) {
                throw new APIException(sequenceName + " not found");
            }
            increment = result.get(0).getDefaultIncrement();
        }

        MongoCollection<Document> collection = lookupCollection(LongSequence.NVC_LONG_SEQUENCE.toCanonicalID());
        Document updated = collection.findOneAndUpdate(
                new Document(DataParam.NAME.getNVConfig().getName(), normalizedName),
                new Document("$inc", new Document(LongSequence.Param.SEQUENCE.getNVConfig().getName(), increment)),
                new com.mongodb.client.model.FindOneAndUpdateOptions()
                        .returnDocument(com.mongodb.client.model.ReturnDocument.AFTER));

        if (updated == null) {
            throw new APIException(sequenceName + " not found");
        }

        Long next = updated.getLong(LongSequence.Param.SEQUENCE.getNVConfig().getName());
        if (next == null) {
            throw new APIException(sequenceName + " sequence value missing");
        }
        return next;
    }

    @Override
    public LongSequence createSequence(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return createSequence(sequenceName, 0, 1);
    }

    @Override
    public synchronized void deleteSequence(String sequenceName)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        delete(LongSequence.NVC_LONG_SEQUENCE, new QueryMatchString(DataParam.NAME.getNVConfig(),
                LowerCaseFilter.SINGLETON.validate(sequenceName), RelationalOperator.EQUAL));
    }


    @SuppressWarnings("unchecked")
    public IDGenerator<String, UUID> getIDGenerator() {
        return IDGs.UUIDV4;
    }


    protected void setDataCacheMonitor(NVECRUDMonitor dcm) {
        dataCacheMonitor = dcm;
    }


    public boolean isValidReferenceID(String refID) {
        return IDGs.UUIDV4.isValid(refID);
    }
}
