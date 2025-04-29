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
package org.zoxweb.server.ds.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.zoxweb.server.ds.mongo.MongoDataStore.ReservedID;
import org.zoxweb.shared.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class MongoMetaManager 
{
	private static final Logger log = Logger.getLogger(MongoMetaManager.class.getName());

	/**
	 * Set hash map to NVConfigEntity mapped values.
	 */
	private final HashMap<String, Object> map = new HashMap<String, Object>();
	private DBCollection nvConfigEntities = null;

	public enum MetaCollections
		implements GetName
	{
		NV_CONVIG_ENTITIES("nv_config_entities"),
		DOMAIN_INFOS("domain_infos")
		;
		
		private final String name;
		MetaCollections( String name)
		{
			this.name = name;
		}
		@Override
		public String getName()
		{
			return name;
		}
	}
	
	/**
	 * This variable declares that only one instance of this class can be created.
	 */
	public static final MongoMetaManager SINGLETON = new MongoMetaManager();
	
	/**
	 * The default constructor is declared private to prevent
	 * outside instantiation of this class.
	 */
	private MongoMetaManager()
	{

	}
	
	/**
	 * 
	 * @param collection to check
	 * @return true if indexed
	 */
	public boolean isIndexed(DBCollection collection)
	{
		SUS.checkIfNulls("Null value DBCollection", collection);
		
		return lookupCollectionByFullName(collection.getFullName()) != null;
	}
	
	/**
	 * 
	 * @param collectionFullName to look for
	 * @return the mapped value
	 */
	private synchronized Object lookupCollectionByFullName(String collectionFullName)
	{
		SUS.checkIfNulls("Null value", collectionFullName);
		
		return map.get(collectionFullName);
	}
	
	/**
	 * 
	 * @param collection to be added to
	 * @param nvce to be added
	 */
	public synchronized void addCollectionInfo(DBCollection collection, NVConfigEntity nvce)
	{
		SUS.checkIfNulls("Null value", collection, nvce);
		
		if (!isIndexed(collection))
		{
			addUniqueIndexes(collection, nvce);
			
			map.put(collection.getFullName(), nvce);
		}
	}
	
	/**
	 * Delete a collection
	 * @param collectionFullName to be delete
	 */
	public synchronized void removeCollectionInfo(String collectionFullName)
	{
		SUS.checkIfNulls("Null value", collectionFullName);
		
		map.remove(collectionFullName);
	}

	public synchronized NVConfigEntity addNVConfigEntity(DB mongo, NVConfigEntity nvce)
	{
		if (nvce.getReferenceID() != null)
			return nvce;
		
		if (nvConfigEntities == null)
		{
			nvConfigEntities = mongo.getCollection(MetaCollections.NV_CONVIG_ENTITIES.getName());
			createUniqueIndex(nvConfigEntities, MetaToken.CANONICAL_ID.getName());
		}
		BasicDBObject search = new BasicDBObject();
		search.put(MetaToken.CANONICAL_ID.getName(), nvce.toCanonicalID());
		BasicDBObject nvceDBO = (BasicDBObject) nvConfigEntities.findOne(search);
		if ( nvceDBO == null)
		{
			nvceDBO = dbMapNVConfigEntity(nvce);
			
			nvConfigEntities.insert(nvceDBO);
		}
		
		nvce.setReferenceID(nvceDBO.getObjectId(ReservedID.REFERENCE_ID.getValue()).toHexString());
		return nvce;
	}
	
	
	/**
	 * Delete a collection
	 * @param collection to be deleted
	 */
	public synchronized void removeCollectionInfo(DBCollection collection)
	{
		SUS.checkIfNulls("Null value", collection);
		
		removeCollectionInfo(collection.getFullName());
	}

	public synchronized MongoDBObjectMeta lookupCollectionName(MongoDataStore mds, ObjectId collectionID)
	{
		BasicDBObject nvceDB = mds.lookupByReferenceID(MetaCollections.NV_CONVIG_ENTITIES.getName(), collectionID);
		
		NVConfigEntity nvce = null;
		
		try {
			nvce  = fromBasicDBObject( nvceDB);//(Class<? extends NVEntity>) Class.forName((String)nvceDB.getString(MetaToken.CLASS_TYPE.getName()));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	 	return new MongoDBObjectMeta(nvce);
	}

	public static BasicDBObject dbMapNVConfigEntity(NVConfigEntity nvce)
	{
		BasicDBObject entryElement = new BasicDBObject();
		entryElement.put(MetaToken.NAME.getName(), nvce.getName());
		entryElement.put(MetaToken.DESCRIPTION.getName(), nvce.getDescription());
		entryElement.put(MetaToken.DOMAIN_ID.getName(), nvce.getDomainID());
		entryElement.put(MetaToken.CLASS_TYPE.getName(), nvce.getMetaTypeBase().getName());
		entryElement.put(MetaToken.IS_ARRAY.getName(), nvce.isArray());
		entryElement.put(MetaToken.CANONICAL_ID.getName(), nvce.toCanonicalID());
		return entryElement;
	}

	public static NVConfigEntity fromBasicDBObject(BasicDBObject dbo) throws ClassNotFoundException
	{
		Class <?> clazz = Class.forName(dbo.getString(MetaToken.CLASS_TYPE.getName()));
		NVConfigEntity ret = new NVConfigEntityLocal();
		ret.setName(dbo.getString(MetaToken.NAME.getName()));
		ret.setDescription(dbo.getString(MetaToken.DESCRIPTION.getName()));
		ret.setDomainID(dbo.getString(MetaToken.DOMAIN_ID.getName()));
		ret.setMetaType(clazz);
		ret.setArray(dbo.getBoolean(MetaToken.IS_ARRAY.getName()));
		ret.setReferenceID(dbo.getObjectId(ReservedID.REFERENCE_ID.getValue()).toHexString());
		return ret;
	}

	/**
	 * 
	 * @param collection to be indexed
	 * @param nvce the met config info
	 */
	private synchronized void addUniqueIndexes(DBCollection collection, NVConfigEntity nvce)
	{
		if (!isIndexed(collection))
		{
			//List<DBObject> list = collection.getIndexInfo();
			//log.info("List of DBObject Indexes: " + list);
			//List<String> toIndex = new ArrayList<String>();
			addNVConfigEntity(collection.getDB(), nvce);
			for (NVConfig nvc : nvce.getAttributes())
			{
				
				if (!nvc.getName().equals(MetaToken.REFERENCE_ID.getName()) && nvc.isUnique() && !nvc.isArray())
				{
					createUniqueIndex(collection, nvc.getName());
//					toIndex.add( nvc.getName());
//					
//					boolean indexToAdd = true;
//					
//					for (DBObject dbIndex : list)
//					{
//						log.info(dbIndex.toString());
//						
//						if (dbIndex.get("key") != null && ((DBObject)dbIndex.get("key")).get(nvc.getName()) != null)
//						{
//							indexToAdd = false;
//							break;
//						}
//						
//					}
//					log.info("Index to Add: " + indexToAdd + " for " + nvc.getName());
//					if (indexToAdd)
//					{
//						//collection.ensureIndex(new BasicDBObject(nvc.getName(), 1), "unique", true);
//						collection.createIndex(new BasicDBObject(nvc.getName(), 1), new BasicDBObject("unique", true));
//					}
				}	
			}
			
		}
	}
	
	
//	private synchronized void addUniqueIndexes(DBCollection collection, NVConfigEntity nvce)
//	{
//		if (!isIndexed(collection))
//		{
//			List<DBObject> list = collection.getIndexInfo();
//			log.info("List of DBObject Indexes: " + list);
//			
//			for (NVConfig nvc : nvce.getAttributes())
//			{
//				
//				if (!nvc.getName().equals(MetaToken.REFERENCE_ID.getName()) && nvc.isUnique())
//				{
//					boolean indexToAdd = true;
//					
//					for (DBObject dbIndex : list)
//					{
//						log.info(dbIndex.toString());
//						
//						if (dbIndex.get("key") != null && ((DBObject)dbIndex.get("key")).get(nvc.getName()) != null)
//						{
//							indexToAdd = false;
//							break;
//						}
//						
//					}
//					log.info("Index to Add: " + indexToAdd + " for " + nvc.getName());
//					if (indexToAdd)
//					{
//						//collection.ensureIndex(new BasicDBObject(nvc.getName(), 1), "unique", true);
//						collection.createIndex(new BasicDBObject(nvc.getName(), 1), new BasicDBObject("unique", true));
//					}
//				}	
//			}
//			
//		}
//	}
	
	private List<String> createUniqueIndex(DBCollection collection, String ...uniqueIndexNames)
	{
		ArrayList<String> ret = new ArrayList<String>();
		
		List<DBObject> indexes = collection.getIndexInfo();
		log.info("List of DBObject Indexes: " + indexes);

		for (String indexToAdd : uniqueIndexNames)
		{
			log.info("Index to be added:" + indexToAdd + " to collection:" + collection.getName());
			if (SUS.isNotEmpty(indexToAdd))
			{
				for (DBObject dbIndex : indexes)
				{
					if ((dbIndex.get("key") != null && ((DBObject)dbIndex.get("key")).get(indexToAdd) != null))
					{
						indexToAdd = null;
						break;
					}
				}
				
				if (indexToAdd != null)
				{
					ret.add(indexToAdd);
					collection.createIndex(new BasicDBObject(indexToAdd, 1), new BasicDBObject("unique", true));
					log.info("Index added:" + indexToAdd + " to collection:" + collection.getName());
				}
			}
		}
		
		return ret;
	}
	
	protected synchronized void addUniqueIndexesForDynamicEnumMap(DBCollection collection)
	{
		if (!isIndexed(collection))
		{
			createUniqueIndex(collection, MetaToken.NAME.getName());
			
			//collection.createIndex(new BasicDBObject(MetaToken.NAME.getName(), 1), new BasicDBObject("unique", true));
			map.put(collection.getFullName(), DynamicEnumMap.class);
		}
	}
	
}
