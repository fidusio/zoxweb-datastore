package org.zoxweb.server.ds.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.zoxweb.server.util.ServerUtil;
import org.zoxweb.server.util.MetaUtil;
import org.zoxweb.shared.filters.ChainedFilter;
import org.zoxweb.shared.filters.FilterType;
import org.zoxweb.shared.filters.ValueFilter;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.KeyMaker;
import org.zoxweb.shared.util.CRUD;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.DynamicEnumMapManager;
import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.GetNameValue;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.NVBase;
import org.zoxweb.shared.util.NVBigDecimal;
import org.zoxweb.shared.util.NVBigDecimalList;
import org.zoxweb.shared.util.NVBlob;
import org.zoxweb.shared.util.NVBoolean;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVDouble;
import org.zoxweb.shared.util.NVDoubleList;
import org.zoxweb.shared.util.NVECRUDMonitor;
import org.zoxweb.shared.util.NVEntity;
import org.zoxweb.shared.util.NVEntityReferenceIDMap;
import org.zoxweb.shared.util.NVEntityGetNameMap;
import org.zoxweb.shared.util.NVEntityReference;
import org.zoxweb.shared.util.NVEntityReferenceList;
import org.zoxweb.shared.util.NVEnum;
import org.zoxweb.shared.util.NVEnumList;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVFloatList;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVIntList;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.NVLongList;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.NVPairList;
import org.zoxweb.shared.util.NVPairGetNameMap;
import org.zoxweb.shared.util.ArrayValues;
import org.zoxweb.shared.util.SharedStringUtil;
import org.zoxweb.shared.util.SharedUtil;
import org.zoxweb.shared.util.TimeStampInterface;


import org.bson.types.ObjectId;


import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
//import com.zoxweb.fidusstore.server.data.DataCacheMonitor;




import org.zoxweb.server.api.APIDocumentStore;
import org.zoxweb.server.api.APIServiceProviderBase;
import org.zoxweb.server.ds.mongo.MongoDataStoreCreator.MongoParam;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.shiro.SecurityManagerAPI;
import org.zoxweb.shared.api.APIBatchResult;

import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIFileInfoMap;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.crypto.EncryptedDAO;
import org.zoxweb.shared.crypto.EncryptedKeyDAO;
import org.zoxweb.shared.crypto.PasswordDAO;
import org.zoxweb.shared.data.CRUDNVEntityDAO;
import org.zoxweb.shared.data.CRUDNVEntityListDAO;
import org.zoxweb.shared.data.DataConst.APIProperty;
import org.zoxweb.shared.db.QueryMarker;

/**
 * This class is used to define the MongoDB object for data storage. This object primarily contains methods 
 * used to store, retrieve, update, and delete documents in the database.
 * @author mzebib
 *
 */
@SuppressWarnings("serial")
public class MongoDataStore
extends APIServiceProviderBase<DB>
	implements APIDataStore<DB>, APIDocumentStore<DB>
{
	private MongoClient mongoClient;
	//private DBAddress dbAddress;
	private DB mongoDB;
	private APIConfigInfo configInfo;
	private String name;
	private String description;
	//private APIExceptionHandler exceptionHandler;
	private NVECRUDMonitor dataCacheMonitor = null;
	
	private Lock updateLock = new ReentrantLock();
	
	private KeyMaker keyMaker;
	private SecurityManagerAPI securityManagerAPI;
	
	//private LockQueue updateLock = new LockQueue(5);
	
	



	private static final transient Logger log = Logger.getLogger("MongoDataStore");
	
	/**
	 * This enum contains reference ID, account ID, and user ID.
	 * @author mzebib
	 *
	 */
	public enum ReservedID 
		implements GetNameValue<String>
	{
		REFERENCE_ID(MetaToken.REFERENCE_ID .getName(), "_id"),
//		ACCOUNT_ID("account_id", "_account_id"),
//		USER_ID("user_id", "_user_id")
		;

		private String name;
		private String value;
		
		private ReservedID(String name, String value)
		{
			this.name = name;
			this.value = value;
		}
		
		@Override
		public String getName() 
		{
			return name;
		}

		@Override
		public String getValue() 
		{
			return value;
		}
		
		public static ReservedID lookupByName(String name)
		{
			for (ReservedID ri : ReservedID.values())
			{
				if (ri.getName().equals(name))
					return ri;
			}
			
			return null;
		}
		
		
		
		public static String map(NVConfig nvc, String name)
		{
			ReservedID resID = lookupByName(name);
			if (resID != null)
				return resID.getValue();
			
			if (name != null && nvc != null && nvc.isTypeReferenceID())
			{
				return "_" + nvc.getName();
			}
			
			return name;
		}
		
	}
	
	
	
	/**
	 * The default constructor.
	 */
	public MongoDataStore()
	{
		
	}
	
	
	
	/**
	 * This method returns the MongoDB client.
	 * @return mongoClient
	 */
	public MongoClient getMongoClient()
	{
		return mongoClient;
	}
		
	



	/**
	 * This method connects to the database.
	 * @return mongoDB
	 * @throws UnknownHostException
	 */
	public DB connect() 
			throws APIException
	{
		if(mongoDB == null)
		{
			synchronized(this)
			{
				if(mongoDB == null)
				{
					if (configInfo == null)
					{
						throw new APIException("Missing configuration information");
					}
					
			//		try 
			//		{
			//			setDBAddress(new DBAddress(SharedUtil.lookupValue(configInfo.getConfigParameters().get(MongoDataStoreCreator.MongoParam.HOST.getName())), 
			//					Integer.valueOf(SharedUtil.lookupValue(configInfo.getConfigParameters().get(MongoDataStoreCreator.MongoParam.PORT.getName()))), 
			//							SharedUtil.lookupValue(configInfo.getConfigParameters().get(MongoDataStoreCreator.MongoParam.DB_NAME.getName()))));
			//		} 
			//		catch (NumberFormatException | UnknownHostException e) 
			//		{
			//			throw new APIException("Cannot set DB address.");
			//		}
								
			
					try 
					{
						mongoDB = newConnection();
						mongoClient = (MongoClient) mongoDB.getMongo();
					}
					catch (MongoException e)
					{
						getAPIExceptionHandler().throwException(e);			
					}
					NVPair dcParam = getAPIConfigInfo().getConfigParameters().get(MongoParam.DATA_CACHE.getName());
					
					if (dcParam != null && dcParam.getValue() != null && Boolean.parseBoolean(dcParam.getValue()))
					{
						NVPair dcClassNameParam = getAPIConfigInfo().getConfigParameters().get(MongoParam.DATA_CACHE_CLASS_NAME.getName());
						try
						{
							dataCacheMonitor = (NVECRUDMonitor) Class.forName(dcClassNameParam.getValue()).newInstance();
							log.info("Data Cache monitor created " + dcClassNameParam);
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
					log.info("Connect finished");
					
					getAllDynamicEnumMap(null, null);
				}
			}
		}
		return mongoDB;
		
	}
	

	
	/**
	 * This method closes all connections to the database.
	 */
	public synchronized void close()
	{
		IOUtil.close(mongoClient);
		mongoClient = null;
		
	}
	

	

	private DBObject mapNVPair(NVEntity container, NVPair nvp, boolean sync)
	{
		BasicDBObject db = new BasicDBObject();
		
		Object value = nvp.getValue();
		
		if (container != null && (ChainedFilter.isFilterSupported(nvp.getValueFilter(), FilterType.ENCRYPT) || ChainedFilter.isFilterSupported(nvp.getValueFilter(), FilterType.ENCRYPT_MASK)))
		{
			keyMaker.createNVEntityKey(this, container, keyMaker.getKey(this, keyMaker.getMasterKey(), container.getUserID()));
			value = securityManagerAPI.encryptValue(this, container, null, nvp, null);
		}
		
		db.append(MetaToken.NAME.getName(), nvp.getName());
		db.append(MetaToken.VALUE.getName(), value instanceof EncryptedDAO ? toDBObject((EncryptedDAO)value, true, sync, false) : value);
		
		if (nvp.getValueFilter() != null)
		{ 
			if (nvp.getValueFilter() != FilterType.CLEAR && !(nvp.getValueFilter() instanceof DynamicEnumMap))
			{
				db.append(MetaToken.VALUE_FILTER.getName(), nvp.getValueFilter().toCanonicalID());
			}
		
			if (nvp.getValueFilter() instanceof DynamicEnumMap)
			{
				DynamicEnumMap dem = (DynamicEnumMap) nvp.getValueFilter();
			
				if (dem.getReferenceID() == null)
				{
					dem = searchDynamicEnumMapByName(dem.getName(), dem.getClass());
					
					if (dem == null)
					{
						dem = insertDynamicEnumMap(dem);
						nvp.setValueFilter(dem);
					}
				}
				
				BasicDBObject dbObject = new BasicDBObject();
				dbObject.append(MetaToken.REFERENCE_ID.getName(), new ObjectId(dem.getReferenceID()));
				dbObject.append(MetaToken.COLLECTION_NAME.getName(), dem.getClass().getName());
				
				db.append(MetaToken.VALUE_FILTER.getName(), dbObject);
				
			}
			
		}
		
		return db;
	}
	
	private ArrayList<DBObject> mapArrayValuesNVPair(NVEntity container, ArrayValues<NVPair> listOfNVPair, boolean sync)
	{
		ArrayList<DBObject> listOfDBObject = new ArrayList<DBObject>();
		
		for (NVPair nvp : listOfNVPair.values())
		{
			listOfDBObject.add(mapNVPair(container, nvp, sync));
		}
		
		return listOfDBObject;		
	}

	
	
	
	
	
	/**
	 * This method maps an enum list to an array list of strings.
	 * @param enumList
	 * @return enumNames
	 */
	private ArrayList<String> mapEnumList(NVEnumList enumList)
	{
		ArrayList<String> enumNames = new ArrayList<String>();
		
		for (Enum<?> e : enumList.getValue())
		{
			enumNames.add(e.name());
		}
		
		return enumNames;		
	}


	private ArrayList<DBObject> mapArrayValuesNVEntity(DB db,
													   NVEntity container,
													   ArrayValues<NVEntity> refList,
													   boolean embed,
													   boolean sync,
													   boolean updateReferenceOnly)
	{
		
		ArrayList<DBObject> list = new ArrayList<DBObject>();
		
		
		//System.out.println(container.getName() +":Array Values Type " + refList.getClass().getName() + " NVBase.getName():" + ((NVBase<?>)refList).getName());
		//System.out.println(container.getName() +":Array content:" + refList);
		for (NVEntity nve : refList.values())
		{
			if (nve != null)
				list.add(mapComplexNVEntityReference(db, container, nve, embed, sync, updateReferenceOnly));
			
		}
		
		return list;
	}
	
	private DBObject mapComplexNVEntityReference(DB db,
												 NVEntity container,
												 NVEntity nve,
												 boolean embed,
												 boolean sync,
												 boolean updateReferenceOnly)
	{
		if (nve != null)
		{
			if (nve instanceof PasswordDAO)
			{
				return toDBObject(nve, true, sync, updateReferenceOnly);
			}
			else if (!(nve instanceof EncryptedKeyDAO) && nve instanceof EncryptedDAO)
			{
				return toDBObject(nve, true, sync, updateReferenceOnly);
			}
			
			if (!embed)
			{
				if (nve.getReferenceID() == null)
				{
					//log.info("NVE do not exist we need to create it");
					insert(nve);
				}
				else
				{
					
					// sync was added to avoid FolderInfo Content deep update
					// it will lock the update and only update references
					if (!updateReferenceOnly && UpdateFilterClass.SINGLETON.isValid(nve))
					{
						patch(nve, true, sync, updateReferenceOnly, true);
					}
					else
					{
						log.info("Not updated:" + nve.getClass().getName());
					}
				}
				return mapNVEntityReference(db, nve);
			}
		}
		return null;
	}
	
	private DBObject mapNVEntityReference(DB db, NVEntity nve)
	{
		BasicDBObject entryElement = new BasicDBObject();
		NVConfigEntity nvce = MongoMetaManager.SINGLETON.addNVConfigEntity(db, ((NVConfigEntity)nve.getNVConfig()));
		entryElement.put(MetaToken.CANONICAL_ID.getName(), new ObjectId(nvce.getReferenceID()));
		entryElement.put(MetaToken.REFERENCE_ID.getName(), new ObjectId(nve.getReferenceID()));
		return entryElement;
	}
	
	
	/**
	 * This method maps an object from the database to NVEntity type.
	 * @param db
	 * @param dbObject
	 * @param clazz
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("unchecked")
	public <V extends NVEntity> V fromDB(String userID, DB db, BasicDBObject dbObject, Class<? extends NVEntity> clazz) 
			throws InstantiationException, IllegalAccessException
	{
		NVEntity nve = clazz.newInstance();
		NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
		
		
		// for encryption support we must set the referenced id and user id prehand
		updateMappedValue(userID, db, dbObject, nve, nvce.lookup(MetaToken.REFERENCE_ID.getName()), nve.lookup(MetaToken.REFERENCE_ID.getName()));
		updateMappedValue(userID, db, dbObject, nve, nvce.lookup(MetaToken.USER_ID.getName()), nve.lookup(MetaToken.USER_ID.getName()));
		//updateMappedValue(userID, db, dbObject, nve, nvce.lookup(MetaToken.DOMAIN_ID.getName()), nve.lookup(MetaToken.DOMAIN_ID.getName()));
		
		for (NVConfig nvc : nvce.getAttributes())
		{
			updateMappedValue(userID, db, dbObject, nve, nvc, nve.lookup(nvc.getName()));
		}
		
		
		return (V)nve;
	}
	


	/**
	 * 
	 * @param db
	 * @param dbObject
	 * @param nvc
	 * @param nvb
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("unchecked")
	private void updateMappedValue(String userID, DB db, BasicDBObject dbObject, NVEntity container, NVConfig nvc, NVBase<?> nvb) 
			throws InstantiationException, IllegalAccessException
	{
		// This issue must be investigated further, seems to be object reference is null.
//		if (nvc == null)
//		{
//			return;
//		}
		
		// get the meta type step one
		Class<?> clazz = nvc.getMetaType();
		// step 2 check if the parameter name is reference_id
		// NB: this procedure must be set up at the start of the function
		if (nvc.isTypeReferenceID() && clazz == String.class) 
		{
			//ReservedID ri = ReservedID.lookupByName(nvc.getName());
			
			String mappedName = ReservedID.map(nvc, nvc.getName());
			if (mappedName != null)
			{
				ObjectId oi = dbObject.getObjectId(mappedName);
				if (oi != null)
				{
					((NVPair) nvb).setValue(oi.toHexString());
					return;
				}
			}
			
//			if (ri != null)
//			{
//				ObjectId oi = null;
//				switch (ri)
//				{
//					case REFERENCE_ID:
//								
//						oi = dbObject.getObjectId(
//								ri.getValue());
//						if (oi != null)
//						{
//							((NVPair) nvb).setValue(oi.toHexString()); 
//						}
//					
//						return;
//					case USER_ID:
//					case ACCOUNT_ID:
//						 // this case we user the name of the object
//						oi = dbObject.getObjectId(ri.getValue());
//						if (oi != null)
//						{
//							((NVPair) nvb).setValue(oi.toHexString()); 
//						}
//						return;
//				}	
//			}			
		}
		
		
		if (dbObject.get(nvc.getName()) == null)
			return;
		
			
	
		if (nvc.isArray())
		{
			Object value = dbObject.get(nvc.getName());
			if (value instanceof BasicDBList)
			{
				if (((BasicDBList) value).size() == 0)
				{
					return;
				}
			}
			
			if (nvc.isEnum())
			{		
				List<String> listOfEnumNames = (List<String>) dbObject.get(nvc.getName());
				List<Enum<?>> listOfEnums = new ArrayList<Enum<?>>();
				
				for (String enumName : listOfEnumNames)
				{
					listOfEnums.add(SharedUtil.enumValue(clazz, enumName));
				}
				
				((NVEnumList) nvb).setValue(listOfEnums);
				
				return;
			}
			
			if (clazz == long[].class || clazz == Long[].class || clazz == Date[].class)
			{
				
				List<Long> values = new ArrayList<Long>();
				BasicDBList dbValues = (BasicDBList) dbObject.get(nvc.getName());
				
				for (Object val : dbValues)
				{
					values.add((Long) val);
				}
				((NVLongList) nvb).setValue(values);
				return;
			}
			
			if (clazz == boolean[].class || clazz == Boolean[].class)
			{
				//((NVBooleanList) nvb).setValue((List<Boolean>) dbObject.get(nvc.getName()));
				return;
			}
			
			if (clazz == BigDecimal[].class)
			{
				List<BigDecimal> ret = new ArrayList<BigDecimal>();
				List<String> values = (List<String>) dbObject.get(nvc.getName());
				
				for(String val : values)
				{
					ret.add(new BigDecimal(val));
				}
				
				((NVBigDecimalList) nvb).setValue(ret);
				
				return;
			}
			
			if (clazz == double[].class || clazz == Double[].class)
			{
				
				List<Double> values = new ArrayList<Double>();
				BasicDBList dbValues = (BasicDBList) dbObject.get(nvc.getName());
				
				for (Object val : dbValues)
				{
					values.add((Double) val);
				}
				((NVDoubleList) nvb).setValue(values);
				return;
			}
			
			if (clazz == float[].class || clazz == Float[].class)
			{
				List<Float> values = new ArrayList<Float>();
				BasicDBList dbValues = (BasicDBList) dbObject.get(nvc.getName());
				
				for (Object val : dbValues)
				{
					if (val instanceof Double)
					{
						val = ((Double) val).floatValue();
					}
					
					values.add((Float) val);
				}
				((NVFloatList) nvb).setValue(values);
				
				return;
			}

			if (clazz == int[].class || clazz == Integer[].class)
			{
				List<Integer> values = new ArrayList<Integer>();
				BasicDBList dbValues = (BasicDBList) dbObject.get(nvc.getName());
				
				for (Object val : dbValues)
				{
					values.add((Integer) val);
				}
				((NVIntList) nvb).setValue(values);
				return;
			}

			if (clazz == String[].class)
			{
				//log.info("nvc:" + nvc.getName());
				boolean isFixed = dbObject.getBoolean(SharedUtil.toCanonicalID('_', nvc.getName(),MetaToken.IS_FIXED.getName()));
				
				ArrayList<BasicDBObject> list = (ArrayList<BasicDBObject>) dbObject.get(nvc.getName());
				
				//List<NVPair> nvpl = new ArrayList<NVPair>();
				
				ArrayValues<NVPair> arrayValues = (ArrayValues<NVPair>) nvb;
				
				if (list != null)
				{
					for (int i = 0; i < list.size(); i++)
					{
						arrayValues.add(toNVPair(userID, container, list.get(i)));
					}
				}
				
				if (nvb instanceof NVPairList)
					((NVPairList) nvb).setFixed(isFixed);
				//((NVPairList) nvb).setValue(nvpl);
				
				return;
			}
			// Adding a list or set of NVEntities an array in this case
			if (NVEntity.class.isAssignableFrom(nvc.getMetaTypeBase()))
			{

				List<MongoDBObjectMeta> listOfDBObject = lookupByReferenceIDs( (List<BasicDBObject>) dbObject.get(nvc.getName()));
				if (listOfDBObject != null)
				{
					//List<NVEntity> ret = new ArrayList<NVEntity>();
					for (MongoDBObjectMeta tempDBObject : listOfDBObject)
					{
						// check if the content is null
						// if the actual object was delete from the collection
						// by still referenced by a list or set
						if (tempDBObject.getContent() != null)
						((ArrayValues<NVEntity>) nvb).add(fromDB(userID, db, tempDBObject.getContent(), (Class<? extends NVEntity>) tempDBObject.getNVConfigEntity().getMetaTypeBase()));
					}
					
					
					//((ArrayValues<NVEntity>) nvb).add(ret.toArray(new NVEntity[0]), true);					
				}

				
				return;
			}
		}
		
		if (nvc instanceof NVConfigEntity)
		{
			//String nveRefID = dbObject.getString(nvc.getName());
			if (container instanceof EncryptedKeyDAO)
			{
				return;
			}
			BasicDBObject obj = (BasicDBObject) dbObject.get(nvc.getName());
			
			Object className = obj.get(MetaToken.CLASS_TYPE.getName());
			if (className != null)
			{
				try 
				{
					((NVEntityReference) nvb).setValue(fromDB(userID, db, obj, (Class<? extends NVEntity>) Class.forName((String)className)));
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return;
			}
			MongoDBObjectMeta mdbom = lookupByReferenceID(obj);
			if (mdbom != null && mdbom.getContent() != null)
			{
				((NVEntityReference) nvb).setValue(fromDB(userID, db, mdbom.getContent(), (Class<? extends NVEntity>) mdbom.getNVConfigEntity().getMetaTypeBase()));
			}
//			if (!SharedStringUtil.isEmpty(dbObject.getObjectId(nvc.getName()).toHexString()))
//			{
//				NVConfigEntity nvce = (NVConfigEntity) nvc;
//				//BasicDBObject dbObj = lookupByReferenceID(db, nvce.getReferencedNVConfigEntity().getName(), nveRefID);
//				BasicDBObject dbObj = lookupByReferenceID(nvce.getReferencedNVConfigEntity().getName(), dbObject.getObjectId(nvc.getName()));
//								
//				if (dbObj != null)
//				{
//					((NVEntityReference) nvb).setValue(fromDB(db, dbObj, (Class<? extends NVEntity>) nvce.getMetaType()));
//				}
//			}
			
			return;
		}
		
		if (clazz == String.class)
		{
			
			Object tempValue = dbObject.get(nvc.getName());
			if (tempValue instanceof DBObject)
			{
				tempValue = fromDB(userID, db, (BasicDBObject) tempValue, EncryptedDAO.class);	
			}
		
			((NVPair)nvb).setValue((String)securityManagerAPI.decryptValue(this, container, nvb, tempValue, null));
			
			return;
		}
		
		if (clazz.isEnum())
		{
			((NVEnum) nvb).setValue(SharedUtil.enumValue(clazz, dbObject.getString(nvc.getName())));
			return;
		}
		
		if (clazz == long.class || clazz == Long.class)
		{
			((NVLong) nvb).setValue(dbObject.getLong(nvc.getName()));
			return;
		}
		
		if (clazz == boolean.class || clazz == Boolean.class)
		{
			((NVBoolean) nvb).setValue(dbObject.getBoolean(nvc.getName()));
			return;
		}

		if (clazz == byte[].class)
		{
			((NVBlob) nvb).setValue((byte[]) dbObject.get(nvc.getName()));
			return;
		}
		
		if (clazz == BigDecimal.class)
		{
			((NVBigDecimal) nvb).setValue(new BigDecimal(dbObject.getString(nvc.getName())));
			return;
		}
		
		if (clazz == double.class || clazz == Double.class)
		{
			((NVDouble) nvb).setValue(dbObject.getDouble(nvc.getName()));
			return;
		}
		
		if (clazz == float.class || clazz == Float.class)
		{
			((NVFloat) nvb).setValue((float) dbObject.getDouble(nvc.getName()));
			return;
		}

		if (clazz == int.class || clazz == Integer.class)
		{
			((NVInt) nvb).setValue(dbObject.getInt(nvc.getName()));
			return;
		}
		
		if (clazz == Date.class)
		{	
			((NVLong) nvb).setValue(dbObject.getLong(nvc.getName()));
			return;
		}

		throw new IllegalArgumentException("Unsupported type: " + nvc);
	}
	
	/**
	 * This method converts a database object to NVPair.
	 * @param dbObject
	 * @return
	 */
	private NVPair toNVPair(String userID, NVEntity container, BasicDBObject dbObject)
	{
		NVPair nvp = new NVPair();
		
		Object value = dbObject.get(MetaToken.VALUE.getName());
		if (container != null)
		{
			
			if (value instanceof DBObject)
			{
				//log.info("userID:" + userID);
				try 
				{
					value = securityManagerAPI.decryptValue(userID, this, container, fromDB(userID, connect(), (BasicDBObject)value, EncryptedDAO.class), null);
				} catch ( InstantiationException
						| IllegalAccessException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
		nvp.setName(dbObject.getString(MetaToken.NAME.getName()));
		nvp.setValue((String) value);
		nvp.setValueFilter(getValueFilter(dbObject));
		
//		try
//		{
//			nvp.setValueFilter(getValueFilter(dbObject));
//		}
//		catch(NullPointerException e )
//		{
//			System.out.println("nvp:"+nvp + " " + dbObject);
//			
//		}
		
		return nvp;
	}
	

	
	
	@SuppressWarnings("unchecked")
	private ValueFilter<String, String> getValueFilter(BasicDBObject dbObject)
	{
		//String valueFilterName = dbObject.getString(MetaToken.VALUE_FILTER.getName());
		Object valueFilterValue = dbObject.get(MetaToken.VALUE_FILTER.getName());
		
		if (valueFilterValue != null)
		{
			
			if (valueFilterValue instanceof String)
			{
				String filterTypeName = (String) valueFilterValue;
				
				if (!SharedStringUtil.isEmpty(filterTypeName))
				{
					 return ((ValueFilter<String, String>) SharedUtil.lookupEnum(FilterType.values(), filterTypeName));
				}
			}
			
			if (valueFilterValue instanceof BasicDBObject)
			{
				BasicDBObject demRefObject = (BasicDBObject) valueFilterValue;
				
				// Dynamic enums are stored based on reference ID and collection name.
				// {
				//   "reference_id" : { _id: CollectionName#IDvalue},
				//	 "collection_name" : "Class name which is the collection name"
				// }
				
				ObjectId demRefID = demRefObject.getObjectId(MetaToken.REFERENCE_ID.getName());
				String collectionName = demRefObject.getString(MetaToken.COLLECTION_NAME.getName());
				
				return searchDynamicEnumMapByReferenceID(demRefID, collectionName);			
			}
			
		}
		
		return null;
	}
	
	
	
	
	
	
	
	
	/**
	 * This method returns all the documents in the collection. After using this function,
	 * the cursor must be closed.
	 * @param db
	 * @param nve
	 * @return
	 */
	public DBCursor getAllDocuments(DB db, NVEntity nve)
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		DBCursor cur = null;
		DBCollection collection = db.getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
		
		try
		{
			if (collection != null)
			{
				cur = collection.find();
			}
		}
		finally
		{
			IOUtil.close(cur);
		}
		
		
		return cur;
	}
	
	/**
	 * This method returns the database name.
	 * @return
	 */
	@Override
	public String getStoreName() 
	{
		if (connect().getName() != null)
			return connect().getName();
		
		return null;
	}

	/**
	 * This method returns the names of collections in the database.
	 * @return
	 */
	@Override
	public Set<String> getStoreTables()
	{
		return connect().getCollectionNames();
	}
	
	/**
	 * This method looks up and returns the database object based on the reference id.
	 * @param db
	 * @param collectionName
	 * @param refID
	 * @return
	 */

//	public BasicDBObject lookupByReferenceID(String collectionName, ObjectId objectId)
//	{
//		return lookupByReferenceID(collectionName, objectId, null);
//	}
	
//	public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId)
//	{
//		return lookupByReferenceID(metaTypeName, objectId, null);
//	}
	
	
	
	@Override
	public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {
		// TODO Auto-generated method stub
		return lookupByReferenceID(metaTypeName, objectId, null);
	}



	@SuppressWarnings("unchecked")
	@Override
	public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection) {
		// TODO Auto-generated method stubmetaTypeName
		DBCollection collection = connect().getCollection(metaTypeName);
		BasicDBObject query = new BasicDBObject(); 
		//query.put("_id", new ObjectId(refID)); 
		query.put("_id", objectId);
		
		DBObject dbObj = null;
		
		try
		{
			if (collection != null)
				dbObj = collection.findOne(query, (DBObject)projection); 
		}
		catch (MongoException e)
		{
			getAPIExceptionHandler().throwException(e);			
		}
		
		return (NT) dbObj;	
	}
	
//	public  BasicDBObject lookupByReferenceID(String collectionName, ObjectId objectId, BasicDBObject projection)
//	{
//		DBCollection collection = connect().getCollection(collectionName);
//		BasicDBObject query = new BasicDBObject(); 
//		//query.put("_id", new ObjectId(refID)); 
//		query.put("_id", objectId);
//		
//		DBObject dbObj = null;
//		
//		try
//		{
//			if (collection != null)
//				dbObj = collection.findOne(query, projection); 
//		}
//		catch (MongoException e)
//		{
//			getAPIExceptionHandler().throwException(e);			
//		}
//		
//		return (BasicDBObject) dbObj;	
//	}
	
	
	
	
	
	/**
	 * This method looks up and returns a list of database objects based on a list of 
	 * reference id's.
	 * @param db
	 * @param collectionName
	 * @param listOfRefID
	 * @return
	 */
	public List<BasicDBObject> lookupByReferenceIDs(String collectionName, List<ObjectId> listOfObjectId)
	{
		if (listOfObjectId != null && !listOfObjectId.isEmpty())
		{	
			DBCollection collection = connect().getCollection(collectionName);
			
			BasicDBObject inQuery = new BasicDBObject();
			List<BasicDBObject> listOfDBObjects = new ArrayList<BasicDBObject>();
			List<ObjectId> listOfObjectID = new ArrayList<>();
			
			for (ObjectId objectId : listOfObjectId)
			{
				listOfObjectID.add(objectId);
			}
			
			inQuery.put("_id", new BasicDBObject("$in", listOfObjectID));
			
			DBCursor cur = null;
			
			try
			{
				cur = collection.find(inQuery);
				
				while (cur.hasNext())
				{
					listOfDBObjects.add((BasicDBObject) cur.next());
				}
			}
			catch (MongoException e)
			{
				getAPIExceptionHandler().throwException(e);			
			}
			finally
			{
				IOUtil.close(cur);
			}
	
			return listOfDBObjects;
		}
		
		return null;
	}
	
	
	
	
	public List<MongoDBObjectMeta> lookupByReferenceIDs(List<BasicDBObject> listOfObjectRefID)
	{
		if (listOfObjectRefID != null && !listOfObjectRefID.isEmpty())
		{	
			List<MongoDBObjectMeta> listOfDBObjects = new ArrayList<MongoDBObjectMeta>();
			for(BasicDBObject  toFind:  listOfObjectRefID)
			{
//				MongoDBObjectMeta toAdd = MongoMetaManager.SINGLETON.lookupCollectionName( this, toFind.getObjectId(MetaToken.CANONICAL_ID.getName()));
//				if ( toAdd != null)
//				{
//					BasicDBObject dbObject = lookupByReferenceID(toAdd.getNVConfigEntity().toCanonicalID(), toFind.getObjectId(MetaToken.REFERENCE_ID.getName()));
//					if ( dbObject != null)
//					{
//						toAdd.setContent(dbObject);
//						listOfDBObjects.add( toAdd);
//					}
//				}
				MongoDBObjectMeta toAdd = lookupByReferenceID(toFind);
				if (toAdd != null)
				{
					listOfDBObjects.add(toAdd);
				}
				
			}
			return listOfDBObjects;
		}
		return null;
	}
	
	
	public MongoDBObjectMeta lookupByReferenceID(BasicDBObject toFind)
	{
		if (toFind != null )
		{	
			MongoDBObjectMeta ret = MongoMetaManager.SINGLETON.lookupCollectionName( this, toFind.getObjectId(MetaToken.CANONICAL_ID.getName()));
			if ( ret != null)
			{
				BasicDBObject dbObject = lookupByReferenceID(ret.getNVConfigEntity().toCanonicalID(), toFind.getObjectId(MetaToken.REFERENCE_ID.getName()));
				if ( dbObject != null)
				{
					ret.setContent(dbObject);
				}
				
				return ret;
			}			
		}
		return null;
	}
	

	/**
	 * This method formats a list of field names used for search criteria.
	 * @param fieldNames
	 * @return
	 */
	private static DBObject formatSearchFields(List<String> fieldNames)
	{
		if (fieldNames != null && !fieldNames.isEmpty())
		{
			BasicDBObject dbObject = new BasicDBObject();
			
			for (String str : fieldNames)
			{
				dbObject.append(str, true);
			}
			
			return dbObject;
		}
		
		return null;
	}
	
	
	
	public <V extends NVEntity> List<V> search(String className, List<String> fieldNames, QueryMarker... queryCriteria)
	{
		return userSearch(null, className, fieldNames, queryCriteria);
	}
	/**
	 * This method is used to search and read a document from the collection based on certain 
	 * criteria and specified fields.
	 * @param nvce
	 * @param fieldNames
	 * @param queryCriteria
	 * @return
	 */
	@Override
	public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria)
	{
		return userSearch(null, nvce, fieldNames, queryCriteria);
	}
	
	
	/**
	 * This method inserts a document into the database.
	 * @param nve
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <V extends NVEntity> V insert(V nve)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		DBCollection collection = connect().getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
		
		BasicDBObject doc = new BasicDBObject();
		
		NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
		securityManagerAPI.associateNVEntityToSubjectUserID(nve, null);
		if (nve.getReferenceID() == null)
		{
			nve.setReferenceID(ObjectId.get().toHexString());
		}
		if(nve.getGlobalID() == null)
		{
			nve.setGlobalID(UUID.randomUUID().toString());
		}
		if (nve instanceof TimeStampInterface)
		{
			SharedUtil.touch((TimeStampInterface) nve, CRUD.CREATE, CRUD.UPDATE);
		}
		for (NVConfig nvc : nvce.getAttributes())
		{
			
			
			if (ChainedFilter.isFilterSupported(nvc.getValueFilter(), FilterType.ENCRYPT) || ChainedFilter.isFilterSupported(nvc.getValueFilter(), FilterType.ENCRYPT_MASK))
			{
				keyMaker.createNVEntityKey(this, nve, keyMaker.getKey(this, keyMaker.getMasterKey(), nve.getUserID()));
			}
			
			NVBase<?> nvb = nve.lookup(nvc.getName());
			
			if (nvb instanceof NVPairList)
			{	
				if ( ((NVPairList) nvb).isFixed())
					doc.append(SharedUtil.toCanonicalID('_', nvc.getName(),MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
				doc.append(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false));
			}
			else if (nvb instanceof NVPairGetNameMap)
			{	
				//log.info("WE have NVPairGetNameMap:" + nvb.getName() + ":" +nvc);
				//doc.append(MetaToken.IS_FIXED.getName(), ((NVPairList) nvb).isFixed());
				ArrayList<DBObject> vals =  mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, false);
				doc.append(nvc.getName(), vals);
				//log.info("vals:" + vals);
			}
			else if (nvb instanceof NVEnum)
			{
				doc.append(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
			}
			
			else if (nvb instanceof NVEnumList)
			{
				doc.append(nvc.getName(), mapEnumList((NVEnumList) nvb));
			}
			
			else if (nvb instanceof NVEntityReference)
			{
				NVEntity temp = (NVEntity) nvb.getValue();
				
//				if (temp.getReferenceID() == null)
//					insert(temp);	
//				doc.append(nvc.getName(), new ObjectId(temp.getReferenceID()));
				
				doc.append(nvc.getName(), mapComplexNVEntityReference(connect(), nve, temp, false, false, false));
			}
			
			else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap)
			{
				doc.append(nvc.getName(), mapArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, false, false, false));
			}
			
			else if (nvb instanceof NVBigDecimal)
			{
				doc.append(nvc.getName(), nvb.getValue().toString());	
			}
			
			else if (nvb instanceof NVBigDecimalList)
			{
				List<String> values = new ArrayList<>();
				List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();
				
				for(BigDecimal decimal : valuesToConvert)
				{
					values.add(decimal.toString());
				}
				
				doc.append(nvc.getName(), values);
			}
			
			else if (nvb instanceof NVBlob)
			{
				doc.append(nvc.getName(), nvb.getValue());
			}
			
//			else if (nvc.getMetaTypeBase() == Date.class)
//			{
//				doc.append(nvc.getName(), nvb.getValue());
//			}
			
			else if (nvc.isArray())
			{
				doc.append(nvc.getName(), nvb.getValue());
			}
			else if (nvc.isTypeReferenceID() && !ReservedID.REFERENCE_ID.getName().equals(nvc.getName()))
			{
				String value = (String) nvb.getValue();
				
				if (value != null)
				{
					doc.append(ReservedID.map(nvc, nvc.getName()), new ObjectId(value));
				}
				
				else
				{
					doc.append(ReservedID.map(nvc, nvc.getName()), null);
				}
			}
			
			
//			else if (ReservedID.USER_ID.getName().equals(nvc.getName()))
//			{
//				String userId = (String) nvb.getValue();
//				
//				if (userId != null)
//				{
//					doc.append(ReservedID.USER_ID.getValue(), new ObjectId(userId));
//				}
//				
//				else
//				{
//					doc.append(ReservedID.USER_ID.getValue(), null);
//				}
//			}
//			
//			else if (ReservedID.ACCOUNT_ID.getName().equals(nvc.getName()))
//			{
//				String accountID = (String) nvb.getValue();
//				
//				if (accountID != null)
//				{
//					doc.append(ReservedID.ACCOUNT_ID.getValue(), new ObjectId(accountID));
//				}
//				
//				else
//				{
//					doc.append(ReservedID.ACCOUNT_ID.getValue(), null);
//				}
//				
//			
//			}
			
			else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName()))
			{	
				//if (nvc.getMetaTypeBase() == String.class)
				{
					Object tempValue = securityManagerAPI.encryptValue(this, nve, nvc, nvb, null);
					if (tempValue instanceof EncryptedDAO)
					{
						doc.append(nvc.getName(), toDBObject((EncryptedDAO)tempValue, true, false, false));
					}
					else
					{
						doc.append(nvc.getName(), tempValue);
					}
				}
//				else
//				{
//					doc.append(nvc.getName(), nvb.getValue());
//				}
			}
		}
		
		//////We might need to put before the insert, need to test to conclude.
		if (!MongoMetaManager.SINGLETON.isIndexed(collection))
		{
			MongoMetaManager.SINGLETON.addCollectionInfo(collection, nvce);
		}		
		
		if (!SharedStringUtil.isEmpty(nve.getReferenceID()))
		{
			doc.append(ReservedID.REFERENCE_ID.getValue(), new ObjectId(nve.getReferenceID()));
		}

		
		try
		{
			if (collection != null)
				collection.insert(doc);
		}
		catch (MongoException e)
		{
			e.printStackTrace();
			getAPIExceptionHandler().throwException(e);			
		}
		//nve.setReferenceID(doc.getString(nve.getReferenceID()));
		//nve.setReferenceID(doc.getObjectId(ReservedID.REFERENCE_ID.getValue()).toHexString());
		
		if (dataCacheMonitor != null)
		{
			dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.CREATE, nve));
		}
		return nve;
	}
	
	
	
	
	@SuppressWarnings("unchecked")
	public DBObject toDBObject(NVEntity nve, boolean embed, boolean sync, boolean updateReferenceOnly)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		
		
		BasicDBObject doc = new BasicDBObject();
		
		NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
		if (nve instanceof TimeStampInterface)
		{
			SharedUtil.touch((TimeStampInterface) nve, CRUD.UPDATE);
		}
		if (embed)
		{
			doc.put(MetaToken.CLASS_TYPE.getName(), nve.getClass().getName());
		}
		for (NVConfig nvc : nvce.getAttributes())
		{
			NVBase<?> nvb = nve.lookup(nvc.getName());
			
			if (nvb instanceof NVPairList)
			{	
				if ( ((NVPairList) nvb).isFixed())
					doc.append(SharedUtil.toCanonicalID('_', nvc.getName(),MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
				doc.append(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
			}
			else if (nvb instanceof NVPairGetNameMap)
			{	
				//log.info("WE have NVPairGetNameMap:" + nvb.getName() + ":" +nvc);
				//doc.append(MetaToken.IS_FIXED.getName(), ((NVPairList) nvb).isFixed());
				ArrayList<DBObject> vals =  mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync);
				doc.append(nvc.getName(), vals);
				//log.info("vals:" + vals);
			}
			else if (nvb instanceof NVEnum)
			{
				doc.append(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
			}
			
			else if (nvb instanceof NVEnumList)
			{
				doc.append(nvc.getName(), mapEnumList((NVEnumList) nvb));
			}
			
			else if (nvb instanceof NVEntityReference)
			{
				NVEntity temp = (NVEntity) nvb.getValue();
				
//				if (temp.getReferenceID() == null)
//					insert(temp);	
//				doc.append(nvc.getName(), new ObjectId(temp.getReferenceID()));
				
				doc.append(nvc.getName(), mapComplexNVEntityReference(connect(), nve, temp, embed, sync, updateReferenceOnly));
			}
			
			else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap)
			{
				doc.append(nvc.getName(), mapArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, embed, sync, updateReferenceOnly));
			}
			
			else if (nvb instanceof NVBigDecimal)
			{
				doc.append(nvc.getName(), nvb.getValue().toString());	
			}
			
			else if (nvb instanceof NVBigDecimalList)
			{
				List<String> values = new ArrayList<>();
				List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();
				
				for(BigDecimal decimal : valuesToConvert)
				{
					values.add(decimal.toString());
				}
				
				doc.append(nvc.getName(), values);
			}
			
			else if (nvb instanceof NVBlob)
			{
				doc.append(nvc.getName(), nvb.getValue());
			}
			
//			else if (nvc.getMetaTypeBase() == Date.class)
//			{
//				doc.append(nvc.getName(), nvb.getValue());
//			}
			
			else if (nvc.isArray())
			{
				doc.append(nvc.getName(), nvb.getValue());
			}
			
			else if (nvc.isTypeReferenceID() && !ReservedID.REFERENCE_ID.getName().equals(nvc.getName()))
			{
				String value = (String) nvb.getValue();
				
				if (value != null)
				{
					doc.append(ReservedID.map(nvc, nvc.getName()), new ObjectId(value));
				}
				
				else
				{
					doc.append(ReservedID.map(nvc, nvc.getName()), null);
				}
			}
			
//			else if (ReservedID.USER_ID.getName().equals(nvc.getName()))
//			{
//				String userId = (String) nvb.getValue();
//				
//				if (userId != null)
//				{
//					doc.append(ReservedID.USER_ID.getValue(), new ObjectId(userId));
//				}
//				
//				else
//				{
//					doc.append(ReservedID.USER_ID.getValue(), null);
//				}
//			}
//			
//			else if (ReservedID.ACCOUNT_ID.getName().equals(nvc.getName()))
//			{
//				String accountID = (String) nvb.getValue();
//				
//				if (accountID != null)
//				{
//					doc.append(ReservedID.ACCOUNT_ID.getValue(), new ObjectId(accountID));
//				}
//				
//				else
//				{
//					doc.append(ReservedID.ACCOUNT_ID.getValue(), null);
//				}
//				
//			
//			}
			
			else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName()))
			{				
				doc.append(nvc.getName(), nvb.getValue());
			}
		}
			
		
		if (!SharedStringUtil.isEmpty(nve.getReferenceID()))
		{
			doc.append(ReservedID.REFERENCE_ID.getValue(), new ObjectId(nve.getReferenceID()));
		}
		
		
		if (!embed)
		{
			DBCollection collection = connect().getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
			//////We might need to put before the insert, need to test to conclude.
			if (!MongoMetaManager.SINGLETON.isIndexed(collection))
			{
				MongoMetaManager.SINGLETON.addCollectionInfo(collection, nvce);
			}
		}
		
		return doc;
	}
	
	
	
	
	
	
	public  <V extends NVEntity> V  update(V nve) 
			throws NullPointerException, IllegalArgumentException, APIException
	{
		return patch(nve, true, false, false, false);
	}
	
	
	
	/**
	 * This method updates an existing document in the database.
	 * @param nve
	 * @return
	 */
//	@Override
//	public  <V extends NVEntity> V  update(V nve, boolean sync) 
//			throws NullPointerException, IllegalArgumentException, APIException
//	{
//		
//		
//		return patch(nve, sync);
//		SharedUtil.checkIfNulls("Null value", nve);
//		
//		try
//		{
//			if (sync)
//			{
//				updateLock.lock();
//			}
//			FidusStoreSecurityUtil.associateNVEntityToSubjectUserID(nve, null);
//			
//			if (nve.lookupValue(MetaToken.REFERENCE_ID) == null)
//			{
//				return insert(nve);
//			}
//			
//			DBCollection collection = mongoDB.getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
//			
//			BasicDBObject originalDoc = lookupByReferenceID(nve.getNVConfig().getName(), new ObjectId(nve.getReferenceID()));
//			if (originalDoc == null)
//			{
//				throw new APIException("Can not update a missing object " + nve.getReferenceID());
//			}
//			//BasicDBObject originalDoc = new BasicDBObject().append(ReservedID.REFERENCE_ID.getValue(), new ObjectId(nve.getReferenceID()));
//			BasicDBObject updatedDoc  = new BasicDBObject();   
//			
//			NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
//			if (nve instanceof TimeStampInterface)
//			{
//				SharedUtil.touch((TimeStampInterface) nve, CRUD.UPDATE);
//			}
//			
//			for (NVConfig nvc : nvce.getAttributes())
//			{
//				NVBase<?> nvb = nve.lookup(nvc.getName());
//				
//				if (nvb instanceof NVPairList)
//				{
//					if ( ((NVPairList) nvb).isFixed())
//						updatedDoc.append(SharedUtil.toCanonicalID('_', nvc.getName(),MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
//					
//					updatedDoc.put(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
//				}
//				else if (nvb instanceof NVPairGetNameMap)
//				{
//					//log.info("Unpdating NVPairGetNameMap:"+nvc.getName()+", isarray:"+nvc.isArray() + ":"+nvb);
//					//updatedDoc.put(MetaToken.IS_FIXED.getName(), ((NVPairList) nvb).isFixed());
//					updatedDoc.put(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
//				}
//				
//				else if (nvb instanceof NVEnum)
//				{
//					updatedDoc.put(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
//				}
//					
//				else if (nvb instanceof NVEnumList)
//				{
//					updatedDoc.put(nvc.getName(), mapEnumList((NVEnumList) nvb));
//				}
//				
//				else if (nvb instanceof NVEntityReference)
//				{
//					NVEntity temp = (NVEntity) nvb.getValue();
//					
//					if (temp != null)
//					{
//	//					if (temp.getReferenceID() == null)
//	//						insert(temp);
//	//					else
//	//						update(temp);
//					
//					
//						//updatedDoc.put(nvc.getName(), temp.getReferenceID());
//						updatedDoc.put(nvc.getName(), mapComplexNVEntityReference(mongoDB, nve, temp, false, sync));// new ObjectId(temp.getReferenceID()));
//					}
//					else
//					{
//						updatedDoc.put(nvc.getName(), null);
//					}
//				
//				}
//				
//				else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap)
//				{
//					updatedDoc.put(nvc.getName(), mapArrayValuesNVEntity(mongoDB, nve, (ArrayValues<NVEntity>) nvb, false, sync));
//				}
//				
//				else if (nvb instanceof NVBigDecimal)
//				{
//					updatedDoc.put(nvc.getName(), nvb.getValue().toString());	
//				}
//				
//				else if (nvb instanceof NVBigDecimalList)
//				{
//					List<String> values = new ArrayList<>();
//					List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();
//					
//					for(BigDecimal decimal : valuesToConvert)
//					{
//						values.add(decimal.toString());
//					}
//					
//					updatedDoc.put(nvc.getName(), values);
//				}
//				
//				else if (nvb instanceof NVBlob)
//				{
//					updatedDoc.put(nvc.getName(), nvb.getValue());
//				}
//				
//				else if (nvc.isArray())
//				{
//					updatedDoc.put(nvc.getName(), nvb.getValue());
//				}
//				
//				else if (nvc.isTypeReferenceID() && !ReservedID.REFERENCE_ID.getName().equals(nvc.getName()))
//				{
//					String value = (String) nvb.getValue();
//					
//					if (value != null)
//					{
//						updatedDoc.append(ReservedID.map(nvc, nvc.getName()), new ObjectId(value));
//					}
//					
//					else
//					{
//						updatedDoc.append(ReservedID.map(nvc, nvc.getName()), null);
//					}
//				}
//	
//				
//				else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName()))
//				{
//					Object tempValue = FidusStoreSecurityUtil.encryptValue(this, nve, nvc, nvb, null);
//					if (tempValue instanceof EncryptedDAO)
//					{
//						updatedDoc.put(nvc.getName(), toDBObject((EncryptedDAO)tempValue, true, sync));
//					}
//					else
//					{
//						updatedDoc.put(nvc.getName(), tempValue);
//					}
//					
//				}
//			}
//			
//		    BasicDBObject updatedObj = new BasicDBObject();
//		    updatedObj.put("$set", updatedDoc);
//		  
//		    try
//		    {
//		    	if (collection != null)
//		    	{
//		    		collection.update(originalDoc, updatedObj);
//		    		//System.out.println("MONGODB:updated in db:" + lookupByReferenceID(nve.getNVConfig().getName(), new ObjectId(nve.getReferenceID())));
//		    		//System.out.println("MONGODB:toupdate  obj:" + updatedObj);
//		    	}
//		    }
//			catch (MongoException e)
//			{
//				getAPIExceptionHandler().throwException(e);			
//			}
//		    
//		    
//		    if (dataCacheMonitor != null)
//			{
//				dataCacheMonitor.monitorDataCache(CRUD.UPDATE, nve);
//			}
//		}
//		finally
//		{
//			if (sync)
//			{
//				updateLock.unlock();
//			}
//		}
//		
//	    
//	    return nve;
//	}

	
	@SuppressWarnings("unchecked")
	@Override
	public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateReferenceOnly, boolean includeParam, String... nvConfigNames)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		try
		{
			if (sync)
			{
				updateLock.lock();
			}
			securityManagerAPI.associateNVEntityToSubjectUserID(nve, null);
			
			if (nve.lookupValue(MetaToken.REFERENCE_ID) == null)
			{
				return insert(nve);
			}
			
			if(nve.getGlobalID() == null)
			{
				nve.setGlobalID(UUID.randomUUID().toString());
			}
			
			DBCollection collection = connect().getCollection(((NVConfigEntity) nve.getNVConfig()).toCanonicalID());
			
			BasicDBObject originalDoc = lookupByReferenceID(nve.getNVConfig().getName(), new ObjectId(nve.getReferenceID()));
			if (originalDoc == null)
			{
				throw new APIException("Can not update a missing object " + nve.getReferenceID());
			}
			//BasicDBObject originalDoc = new BasicDBObject().append(ReservedID.REFERENCE_ID.getValue(), new ObjectId(nve.getReferenceID()));
			BasicDBObject updatedDoc  = new BasicDBObject();   
			
			NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
			if (updateTS && nve instanceof TimeStampInterface)
			{
				SharedUtil.touch((TimeStampInterface) nve, CRUD.UPDATE);
			}
			
			boolean patchMode = true;
			List<NVConfig> paramsToUpdate = null;
			
			if (nvConfigNames != null && nvConfigNames.length != 0)
			{
				paramsToUpdate = new ArrayList<NVConfig>();
				
				if (includeParam)
				{
					// the list of parameters to be updated
					for (String name : nvConfigNames)
					{
						name = SharedStringUtil.trimOrNull(name);
						if (name != null)
						{
							NVConfig nvc = nvce.lookup(name);
							
							if (nvc != null)
							{
								paramsToUpdate.add(nvc);
							}
						}
					}
				}
				else
				{
					// the list of parameters to be excluded from the update
					Set<String> exclusionSet = new HashSet<String>();
					for (String name : nvConfigNames)
					{
						name = SharedStringUtil.trimOrNull(name);
						if (name != null)
							exclusionSet.add(name);
					}
					
					if (exclusionSet.size() > 0)
					{
					for (NVConfig nvc : nvce.getAttributes())
					{
						if (!exclusionSet.contains(nvc.getName()))
						{
							paramsToUpdate.add(nvc);
						}
					}
					}
				}
			}
			
			if (paramsToUpdate == null || paramsToUpdate.size() == 0)
			{
				paramsToUpdate = nvce.getAttributes();
				patchMode = false;
			}
			
			for (NVConfig nvc : paramsToUpdate)
			{
				NVBase<?> nvb = nve.lookup(nvc.getName());
				
				if (nvb instanceof NVPairList)
				{
					if ( ((NVPairList) nvb).isFixed())
						updatedDoc.append(SharedUtil.toCanonicalID('_', nvc.getName(),MetaToken.IS_FIXED.getName()), ((NVPairList) nvb).isFixed());
					
					updatedDoc.put(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
				}
				else if (nvb instanceof NVPairGetNameMap)
				{
					updatedDoc.put(nvc.getName(), mapArrayValuesNVPair(nve, (ArrayValues<NVPair>) nvb, sync));
				}
				else if (nvb instanceof NVEnum)
				{
					updatedDoc.put(nvc.getName(), nvb.getValue() != null ? ((Enum<?>) nvb.getValue()).name() : null);
				}	
				else if (nvb instanceof NVEnumList)
				{
					updatedDoc.put(nvc.getName(), mapEnumList((NVEnumList) nvb));
				}
				else if (nvb instanceof NVEntityReference)
				{
					NVEntity temp = (NVEntity) nvb.getValue();
					
					if (temp != null)
					{
	//					if (temp.getReferenceID() == null)
	//						insert(temp);
	//					else
	//						update(temp);
					
					
						//updatedDoc.put(nvc.getName(), temp.getReferenceID());
						updatedDoc.put(nvc.getName(), mapComplexNVEntityReference(connect(), nve, temp, false, sync, updateReferenceOnly));// new ObjectId(temp.getReferenceID()));
					}
					else
					{
						updatedDoc.put(nvc.getName(), null);
					}
				
				}
				else if (nvb instanceof NVEntityReferenceList || nvb instanceof NVEntityGetNameMap || nvb instanceof NVEntityReferenceIDMap)
				{
					updatedDoc.put(nvc.getName(), mapArrayValuesNVEntity(connect(), nve, (ArrayValues<NVEntity>) nvb, false, sync, updateReferenceOnly));
				}
				else if (nvb instanceof NVBigDecimal)
				{
					updatedDoc.put(nvc.getName(), nvb.getValue().toString());	
				}
				else if (nvb instanceof NVBigDecimalList)
				{
					List<String> values = new ArrayList<>();
					List<BigDecimal> valuesToConvert = (List<BigDecimal>) nvb.getValue();
					
					for(BigDecimal decimal : valuesToConvert)
					{
						values.add(decimal.toString());
					}
					
					updatedDoc.put(nvc.getName(), values);
				}
				
				else if (nvb instanceof NVBlob)
				{
					updatedDoc.put(nvc.getName(), nvb.getValue());
				}
				else if (nvc.isArray())
				{
					updatedDoc.put(nvc.getName(), nvb.getValue());
				}
				else if (nvc.isTypeReferenceID() && !ReservedID.REFERENCE_ID.getName().equals(nvc.getName()))
				{
					String value = (String) nvb.getValue();
					
					if (value != null)
					{
						updatedDoc.append(ReservedID.map(nvc, nvc.getName()), new ObjectId(value));
					}
					
					else
					{
						updatedDoc.append(ReservedID.map(nvc, nvc.getName()), null);
					}
				}
				else if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName()))
				{
					Object tempValue = securityManagerAPI.encryptValue(this, nve, nvc, nvb, null);
					if (tempValue instanceof EncryptedDAO)
					{
						updatedDoc.put(nvc.getName(), toDBObject((EncryptedDAO)tempValue, true, sync, updateReferenceOnly));
					}
					else
					{
						updatedDoc.put(nvc.getName(), tempValue);
					}
					
				}
			}
			
		    BasicDBObject updatedObj = new BasicDBObject();
		    updatedObj.put("$set", updatedDoc);
		  
		    try
		    {
		    	if (collection != null)
		    	{
		    		collection.update(originalDoc, updatedObj);
		    	}
		    }
			catch (MongoException e)
			{
				getAPIExceptionHandler().throwException(e);		
			}
		    
		    if (patchMode)
		    {
		    	nve = (V) searchByID((NVConfigEntity)nve.getNVConfig(), nve.getReferenceID()).get(0);
		    }
		    
		    
		    if (dataCacheMonitor != null)
			{
				dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.UPDATE, nve));
			}
		}
		finally
		{
			if (sync)
			{
				updateLock.unlock();
			}
		}
	    
	    return nve;
	}
	
	/**
	 * This method deletes a document from the database.
	 * @param nve
	 * @param withReference
	 * @return
	 */
	@Override
	public <V extends NVEntity> boolean delete(V nve, boolean withReference)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		if (nve.getReferenceID() != null)
		{
			BasicDBObject doc = new BasicDBObject();
			doc.put(ReservedID.REFERENCE_ID.getValue(), new ObjectId(nve.getReferenceID()));
			DBCollection collection = connect().getCollection(nve.getNVConfig().getName());
			
			
			try
			{
				if (collection != null)
				{
					collection.remove(doc);
					if (dataCacheMonitor != null)
					{
						dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, nve));
					}
					
				}
			}
			catch (MongoException e)
			{
				getAPIExceptionHandler().throwException(e);			
			}
			
			if (withReference)
			{
				NVConfigEntity nvce = (NVConfigEntity) nve.getNVConfig();
				for (NVConfig tempNVC : nvce.getAttributes())
				{
					if (tempNVC instanceof NVConfigEntity)
					{
						if (!tempNVC.isArray())
						{
							NVEntity toRemove = nve.lookupValue(tempNVC);
							if (toRemove != null)
							{
								delete(toRemove, withReference);
								if (dataCacheMonitor != null)
								{
									dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, toRemove));
								}
							}
						}
						else 
						{
							NVBase<?> val = nve.lookup(tempNVC);
							if (val instanceof ArrayValues)
							{
								@SuppressWarnings("unchecked")
								NVEntity[] toRemoves = ((ArrayValues<NVEntity>) val).values();
								for (NVEntity toRemove : toRemoves)
								{
									if (toRemove != null)
									{
										delete(toRemove, withReference);
										if (dataCacheMonitor != null)
										{
											dataCacheMonitor.monitorNVEntity(new CRUDNVEntityDAO(CRUD.DELETE, toRemove));
										}
									}
								}
							}
						}
					}
				}
			}
			
			return true;
		}

		return false;
	}
	
	
	
	
	public boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
		throws NullPointerException, IllegalArgumentException, APIException, AccessException
	{
		DBObject query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
		DBCollection collection = connect().getCollection(nvce.toCanonicalID());
		collection.remove(query);
		return false;
	}

	/**
	 * This method deletes all the documents in the collection. Use with caution.
	 * @param nve
	 * @return
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 * @throws APIException
	 */
	public <V extends NVEntity> boolean deleteAll(V nve)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		SharedUtil.checkIfNulls("Null value", nve);
		
		DBCollection collection = connect().getCollection(nve.getNVConfig().getName());
		
		if (nve.getReferenceID() != null)
		{
			nve.getReferenceID();
			
			try
			{
				if (collection != null)
					collection.remove(new BasicDBObject());
			}
			catch (MongoException e)
			{
				getAPIExceptionHandler().throwException(e);			
			}
			return true;
		}

		return false;
	}
	
	/**
	 * This method returns the API service information.
	 * @return
	 */
	@Override
	public APIConfigInfo getAPIConfigInfo() 
	{
		return configInfo;
	}

	/**
	 * This method sets the API service information.
	 * @param service
	 */
	@Override
	public void setAPIConfigInfo(APIConfigInfo configInfo) 
	{
		this.configInfo = configInfo;
	}

	/**
	 * This method returns the number of matches found based on query criteria.
	 * @param nvce
	 * @param queryCriteria
	 * @return
	 */
	@Override
	public long countMatch(NVConfigEntity nvce, QueryMarker ... queryCriteria) 
	{
		DBObject query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
		DBCollection collection = connect().getCollection(nvce.toCanonicalID());
		
		return collection.count(query);
	}

	/**
	 * This method creates and stores a file into the database.
	 * @param file
	 * @param is
	 * @return
	 */
	@Override
	public APIFileInfoMap createFile(String folder, APIFileInfoMap file, InputStream is, boolean closeStream)
			throws IllegalArgumentException, IOException, NullPointerException 
	{
		SharedUtil.checkIfNulls("Null value", file, is);
		log.info(file.getOriginalFileInfo().getName());
		DB temp = null;
		OutputStream os = null;
		try
		{
			temp = newConnection();
			GridFS fs = new GridFS(temp);
			GridFSInputFile gfsFile = fs.createFile(is);
			gfsFile.setFilename(file.getOriginalFileInfo().getName());
			
			if (file.getOriginalFileInfo().getReferenceID() == null)
				insert(file.getOriginalFileInfo());
				
			gfsFile.setId(new ObjectId(file.getOriginalFileInfo().getReferenceID()));
			
			os = gfsFile.getOutputStream();
			IOUtil.relayStreams(is, os, false);
			
	
			//gfsFile.save();
			
		}
		finally
		{
			IOUtil.close(os);
			
			
			if (closeStream)
				IOUtil.close(is);
			if (temp != null)
			{
				try
				{
					temp.getMongo().close();
				}
				catch( Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		return file;
		
	}

	/**
	 * This method reads a file from the database.
	 * @param file
	 * @param os
	 * @return
	 */
	@Override
	public APIFileInfoMap readFile(APIFileInfoMap file, OutputStream os, boolean closeStream)
			throws IllegalArgumentException, IOException, NullPointerException 
	{
		SharedUtil.checkIfNulls("Null value", file, os);
		
		DB temp = null;
		try
		{
			temp = newConnection();
			//db.setWriteConcern(WriteConcern.SAFE);
			//;
			//db.setReadPreference(ReadPreference.primary());
			GridFS fs = new GridFS(temp);
		
			GridFSDBFile gridFSFile = fs.find(new ObjectId(file.getOriginalFileInfo().getReferenceID()));
		
		
			if (gridFSFile != null)
			{
				IOUtil.relayStreams(gridFSFile.getInputStream(), os, closeStream);
				//out.writeTo(os);
			}	
		}
		finally
		{
			if (closeStream)
				IOUtil.close(os);
			if (temp != null)
			{
				try
				{
					temp.getMongo().close();
				}
				catch( Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		log.info(file.getOriginalFileInfo().getName());
		return file;
	}

	/**
	 * This method updates and overwrites an existing file in the database.
	 * @param file
	 * @param is
	 * @return
	 */
	@Override
	public APIFileInfoMap updateFile(APIFileInfoMap file, InputStream is, boolean closeStream)
			throws IllegalArgumentException, IOException, NullPointerException 
	{
		
		deleteFile(file);
		log.info(file.getOriginalFileInfo().getName());
		return createFile(null,  file, is, closeStream);
		
		
//		SharedUtil.checkIfNulls("Null value", file, is);
//		
//		GridFS fs = new GridFS(mongoDB);
//		
//		deleteFile(file);
//		
//		GridFSInputFile gfsFile = fs.createFile(is);
//		gfsFile.setFilename(file.getOriginalFileInfo().getName());
//
//		if (file.getOriginalFileInfo().getReferenceID() == null)
//			insert(file.getOriginalFileInfo());
//			
//		gfsFile.setId(new ObjectId(file.getOriginalFileInfo().getReferenceID()));
//		try
//		{
//			gfsFile.save();
//		}
//		finally
//		{
//			if (closeStream)
//				IOUtil.close(is);
//		}
//		
//		return file;

	}

	/**
	 * This method deletes a file in the database.
	 * @param file
	 */
	@Override
	public void deleteFile(APIFileInfoMap file) 
			throws IllegalArgumentException, IOException, NullPointerException 
	{
		SharedUtil.checkIfNulls("Null value", file);
		log.info(file.getOriginalFileInfo().getName());
		GridFS fs = new GridFS(connect());
		
		fs.remove(new ObjectId(file.getOriginalFileInfo().getReferenceID()));
		
	}

	/**
	 * This method searches and returns a list of files in the database.
	 * @param args
	 */
	@Override
	public List<APIFileInfoMap> search(String... args)
			throws IllegalArgumentException, IOException, NullPointerException 
	{
		return null;
	}

	@Override
	public boolean isProviderActive() 
	{
		return false;
	}

	/**
	 * This method sets the description.
	 * @param str
	 */
	@Override
	public void setDescription(String str) 
	{
		description = str;
	}

	/**
	 * This method returns the description.
	 */
	@Override
	public String getDescription()
	{

		return description;
	}

	/**
	 * This method sets the name.
	 * @param name
	 */
	@Override
	public void setName(String name) 
	{
		this.name = name;
	}

	/**
	 * This method returns the name.
	 */
	@Override
	public String getName() 
	{
		return name;
	}

	/**
	 * This method returns a string representation of the class.
	 */
	@Override
	public String toCanonicalID() 
	{

		return null;
	}

	/**
	 * This method searches by ID.
	 * @param nvce
	 * @param ids
	 */
	@Override
	public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids) 
			throws NullPointerException, IllegalArgumentException, APIException 
	{
		return userSearchByID(null, nvce, ids);
	}
	
	@Override
	public <V extends NVEntity> List<V> searchByID(String className, String... ids) 
			throws NullPointerException, IllegalArgumentException, APIException 
	{

		
		NVEntity tempNVE;
		try 
		{
			tempNVE = (NVEntity) Class.forName(className).newInstance();
			return userSearchByID(null, (NVConfigEntity)tempNVE.getNVConfig(), ids);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			throw new APIException("Invalid class name " + className);
		}
			
		

	}



	

	public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		DynamicEnumMapManager.validateDynamicEnumMap(dynamicEnumMap);
		
		DBCollection collection = connect().getCollection(dynamicEnumMap.getClass().getName());
		MongoMetaManager.SINGLETON.addUniqueIndexesForDynamicEnumMap(collection);
		
		BasicDBObject doc = new BasicDBObject();
		doc.append(MetaToken.NAME.getName(), dynamicEnumMap.getName());
		doc.append(MetaToken.VALUE.getName(), mapArrayValuesNVPair(null, dynamicEnumMap, false));
		doc.append(MetaToken.DESCRIPTION.getName(), dynamicEnumMap.getDescription());
		
		if (!SharedStringUtil.isEmpty(dynamicEnumMap.getReferenceID()))
		{
			// Since we are referencing the object, we will use the reference_id NOT _id.
			doc.append(MetaToken.REFERENCE_ID.getName(), new ObjectId(dynamicEnumMap.getReferenceID()));
		}
		
		try
		{
			if (collection != null)
				collection.insert(doc);
		}
		catch (MongoException e)
		{
			
			getAPIExceptionHandler().throwException(e);
		} 

		dynamicEnumMap.setReferenceID(doc.getObjectId(ReservedID.REFERENCE_ID.getValue()).toHexString());
		
		return dynamicEnumMap;
	}
	
	public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		DynamicEnumMapManager.validateDynamicEnumMap(dynamicEnumMap);
		
		DBCollection collection = connect().getCollection(dynamicEnumMap.getClass().getName());

		//BasicDBObject originalDoc = lookupByReferenceID(collection.getName(), new ObjectId(dynamicEnumMap.getReferenceID()));		
		BasicDBObject originalDoc = lookupByName(collection.getName(), dynamicEnumMap.getName());

		if (originalDoc == null)
		{
			return insertDynamicEnumMap(dynamicEnumMap);
		}
		else
		{
			BasicDBObject updatedDoc = new BasicDBObject(); 
			
			updatedDoc.put(MetaToken.VALUE.getName(), mapArrayValuesNVPair(null, dynamicEnumMap, false));
			updatedDoc.put(MetaToken.DESCRIPTION.getName(), dynamicEnumMap.getDescription());
			
		    BasicDBObject updatedObj = new BasicDBObject();
		    updatedObj.put("$set", updatedDoc);
		  
		    try
		    {
		    	if (collection != null)
		    		collection.update(originalDoc, updatedObj);
		    }
			catch (MongoException e)
			{
				getAPIExceptionHandler().throwException(e);			
			}
		    
		    return DynamicEnumMapManager.SINGLETON.addDynamicEnumMap(dynamicEnumMap);
		}
	}
		
	
	
	/**
	 * This method looks up the database object in the collection by name.
	 * @param collectionName
	 * @param name
	 * @return
	 */
	private BasicDBObject lookupByName(String collectionName, String name)
	{
		DBCollection collection = connect().getCollection(collectionName);
		BasicDBObject query = new BasicDBObject(); 
		query.put(MetaToken.NAME.getName(), name);
		
		DBObject dbObj = null;
		
		try
		{
			dbObj = collection.findOne(query); 
		}
		catch (MongoException e)
		{
			getAPIExceptionHandler().throwException(e);			
		}
		
		return (BasicDBObject) dbObj;	
	}
	
	
	
	/**
	 * This method converts the database object to dynamic enum type.
	 * @param obj
	 * @return
	 */
	private DynamicEnumMap fromDBtoDynamicEnumMap(BasicDBObject obj)
	{
		BasicDBList list = (BasicDBList) obj.get(MetaToken.VALUE.getName());
		ObjectId objectID = obj.getObjectId(ReservedID.REFERENCE_ID.getValue());
		
		List<NVPair> nvpl = new ArrayList<NVPair>();
		
		if (list != null)
		{
			for (int i = 0; i < list.size(); i++)
			{
				nvpl.add(toNVPair(null, null, (BasicDBObject) list.get(i)));
			}
		}
		
		
		String demName = (String) obj.get(MetaToken.NAME.getName());
		//log.info("DynamicEnumMap Name : " + demName);
		DynamicEnumMap dem = new DynamicEnumMap(demName, nvpl);
		dem.setReferenceID(objectID.toHexString());
		//log.info("values " + dem.getValue());
		dem = DynamicEnumMapManager.SINGLETON.addDynamicEnumMap(dem);
		//log.info(dem.getName() + ":" + dem.getValue());
		
		
		
//		for ( DynamicEnumMap dems : DynamicEnumMapManager.SINGLETON.getAll())
//		{
//			log.info("dem name: " + dems.getName() + ": " +  dems.getValue());
//		}
		
		return dem;
	}
	
	
	/**
	 * This method searches for the dynamic enum by reference ID.
	 * @param refID
	 * @return
	 */
	public DynamicEnumMap searchDynamicEnumMapByReferenceID(String refID)
		throws NullPointerException, IllegalArgumentException, APIException
	{
		return searchDynamicEnumMapByReferenceID(refID, null);
	}
	
	
	/**
	 * This method searches for the dynamic enum by reference ID.
	 * @param refID
	 * @param clazz
	 * @return
	 */
	public DynamicEnumMap searchDynamicEnumMapByReferenceID(String refID, Class<? extends DynamicEnumMap> clazz)
		throws NullPointerException, IllegalArgumentException, APIException
	{
		DBCollection collection = null;
		
		if (clazz != null)
		{
			collection = connect().getCollection(clazz.getName());
		}
		else
		{
			collection = connect().getCollection(DynamicEnumMap.class.getName());
		}
		
		
		return searchDynamicEnumMapByReferenceID(new ObjectId(refID), collection.getName());
	}
	
	
	private DynamicEnumMap searchDynamicEnumMapByReferenceID(ObjectId objectID, String collectionName)
			throws NullPointerException, IllegalArgumentException, APIException
	{
			DBCollection collection = connect().getCollection(collectionName);
			
			BasicDBObject doc = null;
			
			if (collection != null)
				doc = lookupByReferenceID(collection.getName(), objectID);
			
			
			return fromDBtoDynamicEnumMap(doc);
	}
	
	
	/**
	 * This method searches for the dynamic enum by name.
	 * @param name
	 * @return
	 */
	public DynamicEnumMap searchDynamicEnumMapByName(String name)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		return searchDynamicEnumMapByName(name, null);
	}
	

	/**
	 * This method searches for the dynamic enum by name.
	 * @param name
	 * @param clazz
	 * @return
	 */
	public DynamicEnumMap searchDynamicEnumMapByName(String name, Class<? extends DynamicEnumMap> clazz)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		if (!name.startsWith(DynamicEnumMap.NAME_PREFIX + ":"))
		{
			name = SharedUtil.toCanonicalID(':', DynamicEnumMap.NAME_PREFIX, name);
		}
		
		BasicDBObject doc = null;
		
		DBCollection collection = null;
		
		if (clazz != null)
		{
			collection = connect().getCollection(clazz.getName());
		}
		else
		{
			collection = connect().getCollection(DynamicEnumMap.class.getName());
		}
		
		
		
		if (collection != null)
			doc = lookupByName(collection.getName(), name);
		
		return fromDBtoDynamicEnumMap(doc);
	}
	
	
	/**
	 * This method deletes a dynamic enum based on name.
	 * @param name
	 */
	public void deleteDynamicEnumMap(String name)
		throws NullPointerException, IllegalArgumentException, APIException
	{
		deleteDynamicEnumMap(name, null);
	}
	
	
	/**
	 * This method deletes a dynamic enum based on name.
	 * @param name
	 * @param clazz
	 */
	public void deleteDynamicEnumMap(String name, Class<? extends DynamicEnumMap> clazz)
		throws NullPointerException, IllegalArgumentException, APIException
	{
		if (!name.startsWith(DynamicEnumMap.NAME_PREFIX + ":"))
		{
			name = SharedUtil.toCanonicalID(':', DynamicEnumMap.NAME_PREFIX, name);
		}
		
		DBCollection collection = null;
		
		if (clazz != null)
		{
			 collection = connect().getCollection(clazz.getName());
		}
		else
		{
			 collection = connect().getCollection(DynamicEnumMap.class.getName());
		}
		
		BasicDBObject doc = lookupByName(collection.getName(), name);
		
		if (doc != null)
		{
			if (collection != null)
				collection.remove(doc);
		}
		
		DynamicEnumMapManager.SINGLETON.deleteDynamicEnumMap(name);
	}
	
	/**
	 * This method returns a list of dynamic enum map in the dynamic enum map collection.
	 * @param domainID
	 * @param userID
	 * @return
	 */
	public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
			throws NullPointerException, IllegalArgumentException, APIException
	{
		return getAllDynamicEnumMap(domainID, userID, null);
	}
	
	/**
	 * This method returns a list of dynamic enum map in the dynamic enum map collection.
	 * @param domainID
	 * @param userID
	 * @param clazz
	 * @return
	 */
	public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID, Class<? extends DynamicEnumMap> clazz)
		throws NullPointerException, IllegalArgumentException, APIException
	{
		List<DynamicEnumMap> list = new ArrayList<DynamicEnumMap>();
		
		DBCollection collection = null;
		
		if (clazz != null)
		{
			 collection = connect().getCollection(clazz.getName());
		}
		else
		{
			 collection = connect().getCollection(DynamicEnumMap.class.getName());
		}
		
		DBCursor cur = null;
		
		try
		{
			cur = collection.find();
			
			while (cur.hasNext())
			{
				list.add(fromDBtoDynamicEnumMap((BasicDBObject) cur.next()));
			}
		}
		finally
		{
			IOUtil.close(cur);
		}		
		
		return list;
	}

	@Override
	public Map<String, APIFileInfoMap> discover() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings({ "deprecation", "resource" })
	@Override
	public DB newConnection() throws APIException
	{
		try
		{	
		    return (DB) new MongoClient(getDBAddress(configInfo)).getDB(SharedUtil.lookupValue(getAPIConfigInfo().getConfigParameters().get(MongoDataStoreCreator.MongoParam.DB_NAME.getName())));
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new APIException(e.getMessage());
		}
		
	}
	
	
	
	private static ServerAddress getDBAddress(APIConfigInfo aci) throws NumberFormatException, UnknownHostException
	{
		return new ServerAddress(SharedUtil.lookupValue(aci.getConfigParameters().get(MongoDataStoreCreator.MongoParam.HOST.getName())), 
				Integer.valueOf(SharedUtil.lookupValue(aci.getConfigParameters().get(MongoDataStoreCreator.MongoParam.PORT.getName())))); 
						//SharedUtil.lookupValue(aci.getConfigParameters().get(MongoDataStoreCreator.MongoParam.DB_NAME.getName())));
	}

	@Override
	public APIFileInfoMap createFolder(String folderFullPath)
			throws NullPointerException, IllegalArgumentException, IOException,
			AccessException {
		// TODO Auto-generated method stub
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
				   APIException			   
	{
		List<V> list = new ArrayList<V>();
		List<ObjectId> listOfObjectIds = new ArrayList<ObjectId>();
		
		for (String id : ids)
		{
			listOfObjectIds.add(new ObjectId(id));
		}
		
		List<BasicDBObject> listOfDBObject = lookupByReferenceIDs(nvce.getName(), listOfObjectIds);
		
		for (BasicDBObject dbObject : listOfDBObject)
		{
			try 
			{
				list.add((V) fromDB(userID, connect(), dbObject, (Class<? extends NVEntity>) nvce.getMetaType()));
			} 
			
			catch (InstantiationException | IllegalAccessException e) 
			{
				e.printStackTrace();
			}
		}
		
		if (userID == null && dataCacheMonitor != null)
		{
			dataCacheMonitor.monitorNVEntity(new CRUDNVEntityListDAO(CRUD.READ, (List<NVEntity>) list));
		}
		return list;
	}



	@SuppressWarnings("unchecked")
	@Override
	public <V extends NVEntity> List<V> userSearch(String userID,
			NVConfigEntity nvce, List<String> fieldNames,
			QueryMarker... queryCriteria) throws NullPointerException,
			IllegalArgumentException, AccessException, APIException
	{
		List<V> list = new ArrayList<V>();
		
		
		DBObject query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
		DBCollection collection = connect().getCollection(nvce.toCanonicalID());
		DBCursor cur = null;
		
		try
		{
			if (collection != null)
			{	
				cur = collection.find(query, formatSearchFields(fieldNames));
	
				while (cur.hasNext())
				{
					try 
					{
						V nve = fromDB(userID, connect(), (BasicDBObject) cur.next(), (Class<? extends NVEntity>) nvce.getMetaType());
						list.add(nve);
					} 
					
					catch (InstantiationException | IllegalAccessException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		catch (MongoException e)
		{
			e.printStackTrace();
			getAPIExceptionHandler().throwException(e);
		}
		finally
		{
			IOUtil.close(cur);
		}
		
		if (userID == null && dataCacheMonitor != null)
		{
			dataCacheMonitor.monitorNVEntity(new CRUDNVEntityListDAO(CRUD.READ, (List<NVEntity>) list));
		}
		
		return list;
	}



	@Override
	public <V extends NVEntity> List<V> userSearch(String userID,
			String className, List<String> fieldNames,
			QueryMarker... queryCriteria) throws NullPointerException,
			IllegalArgumentException, AccessException, APIException
	{
		NVConfigEntity nvce = null;
		try 
		{
			nvce = MetaUtil.SINGLETON.fromClass( className);
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) 
		{
			e.printStackTrace();
			throw new APIException("Class name not found:" + className);
			
		}
		
		return userSearch(userID, nvce, fieldNames, queryCriteria);
	}



	@SuppressWarnings("unchecked")
	@Override
	public <T> T lookupProperty(GetName propertyName) 
	{
		if (propertyName != null && propertyName instanceof APIProperty)
		{
			APIProperty apiProperty = (APIProperty) propertyName;
			switch(apiProperty)
			{
			case ASYNC_CREATE:
				return (T) Boolean.TRUE;
			case ASYNC_DELETE:
				break;
			case ASYNC_READ:
				break;
			case ASYNC_UPDATE:
				break;
			case RETRY_DELAY:
				return (T) new Long(Const.TimeInMillis.SECOND.MILLIS*ServerUtil.RNG.nextInt(4) + Const.TimeInMillis.SECOND.MILLIS*2);
				
			default:
				break;
			
			}
		}
		
		return null;
	}



	@Override
	public boolean isBusy() 
	{
		return true;
	}



	@SuppressWarnings("unchecked")
	@Override
	public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria) 
			throws NullPointerException, IllegalArgumentException, AccessException, APIException 
	{
		SharedUtil.checkIfNulls("NVConfigEntity is null.", nvce);
		List<T> list = new ArrayList<T>();
		
		DBObject query = MongoQueryFormatter.formatQuery(nvce, queryCriteria);
		DBCollection collection = connect().getCollection(nvce.toCanonicalID());
		DBCursor cur = null;
		
		try
		{
			if (collection != null)
			{	
				List<String> fieldNames = new ArrayList<String>();
				fieldNames.add(ReservedID.REFERENCE_ID.getValue());
				fieldNames.add("_user_id");
				
				cur = collection.find(query, formatSearchFields(fieldNames));
	
				while (cur.hasNext())
				{
					DBObject dbObject = cur.next();
					ObjectId refID = (ObjectId) dbObject.get(ReservedID.REFERENCE_ID.getValue());
					ObjectId userID = (ObjectId) dbObject.get("_user_id");
					
					if (refID != null && userID != null && securityManagerAPI.isNVEntityAccessible(refID.toHexString(), userID.toHexString(), CRUD.READ))
					{
						list.add((T) refID);
					}
				}
			}
		}
		catch (MongoException e)
		{
			e.printStackTrace();
			getAPIExceptionHandler().throwException(e);			
		}
		finally
		{
			IOUtil.close(cur);
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
			throws NullPointerException, IllegalArgumentException, AccessException, APIException 
	{
		NVEntity nve = null;
		
		try
		{
			Class<?> clazz = Class.forName(className);
			nve = (NVEntity) clazz.newInstance();
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException("Class " + className + " not supported.");
		}
		
		return batchSearch((NVConfigEntity) nve.getNVConfig(), queryCriteria);
	}



	@SuppressWarnings("unchecked")
	@Override
	public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> reportResults, int startIndex, int batchSize)
			throws NullPointerException, IllegalArgumentException, AccessException, APIException 
	{
		APIBatchResult<V> batch = new APIBatchResult<V>();
		batch.setReportID(reportResults.getReportID());
		batch.setTotalMatches(reportResults.size());
		
		if (startIndex >= reportResults.size())
		{
			return null;
		}
		
		int endIndex = 0;
		
		//	If batch size is -1, retrieve all remaining.
		if (batchSize == -1 || (startIndex + batchSize >= reportResults.size()))
		{
			endIndex = reportResults.size();
		}
		else
		{
			endIndex = startIndex + batchSize;
		}		
		
		batch.setRange(startIndex, endIndex);
		
		List<ObjectId> objectIDsList = (List<ObjectId>) reportResults.getMatchIDs().subList(startIndex, endIndex);
		
		List<BasicDBObject> dbObjectsList = lookupByReferenceIDs(reportResults.getNVConfigEntity().toCanonicalID(), objectIDsList);
		List<NVEntity> nveList = new ArrayList<NVEntity>();
		
		if (dbObjectsList != null)
		{
			for (BasicDBObject obj : dbObjectsList)
			{
				try 
				{
					nveList.add((V) fromDB(null, connect(), obj, (Class<? extends NVEntity>) reportResults.getNVConfigEntity().getMetaType()));
				} 
				catch (InstantiationException | IllegalAccessException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		batch.setBatch(nveList);
		
		return batch;
	}


	public SecurityManagerAPI getSecurityManagerAPI()
	{
		return securityManagerAPI;
	}



	public void setSecurityManagerAPI(SecurityManagerAPI securityManagerAPI)
	{
		this.securityManagerAPI = securityManagerAPI;
	}



	public KeyMaker getKeyMaker()
	{
		return keyMaker;
	}



	public void setKeyMaker(KeyMaker keyMaker) 
	{
		this.keyMaker = keyMaker;
	}


	
}