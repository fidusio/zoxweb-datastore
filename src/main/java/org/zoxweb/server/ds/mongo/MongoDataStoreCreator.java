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

import java.util.List;

import org.zoxweb.shared.util.GetNameValue;
import org.zoxweb.shared.util.NVECRUDMonitor;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.SharedUtil;

import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;

import org.zoxweb.shared.api.APIServiceProviderCreator;
import org.zoxweb.shared.api.APIServiceType;
import org.zoxweb.shared.api.APITokenManager;

public class MongoDataStoreCreator 
	implements APIServiceProviderCreator
{
	
	public final static String API_NAME = "MongoDB";

    /**
     * Contains parameters needed to create the Mongo database.
     */
	public enum MongoParam 
		implements GetNameValue<String>
	{
		DB_NAME("db_name", "fidus_store"),
		HOST("host", "localhost"),
		PORT("port", "27017"),
		DATA_CACHE("data_cache", "false"),
		DATA_CACHE_CLASS_NAME("data_cache_class_name", null),
		
		;
		
		private final String name;
		private final String value;
		
		MongoParam(String name, String value)
		{
			this.name = name;
			this.value = value;
		}
		
		@Override
		public String getName() 
		{
			return name;
		}
		
		public String getValue()
		{
			return value;
		}
		
	}

	@Override
	public APIConfigInfo createEmptyConfigInfo() 
	{
		APIConfigInfo configInfo = new APIConfigInfoDAO();
		
		@SuppressWarnings("unchecked")
		List<NVPair> list = (List<NVPair>) SharedUtil.toNVPairs(MongoParam.values());
		configInfo.setConfigParameters(list);
		
		APIServiceType[] types = {APIServiceType.DATA_STORAGE, APIServiceType.DOCUMENT_STORAGE};
		configInfo.setServiceTypes(types);
		configInfo.setAPITypeName(API_NAME);
		configInfo.setDescription("MongoDB Configuration Info.");
		configInfo.setVersion("3.4.7");
			
		return configInfo;
	}
	
	
	@Override
	public MongoDataStore createAPI(APIDataStore<?> ds, APIConfigInfo apiConfig) 
			throws APIException
	{
		MongoDataStore mongoDS = new MongoDataStore();
		
		mongoDS.setAPIConfigInfo(apiConfig);
		mongoDS.setAPIExceptionHandler(MongoExceptionHandler.SINGLETON);
		
		NVPair dcParam = mongoDS.getAPIConfigInfo().getConfigParameters().get(MongoParam.DATA_CACHE.getName());
		
		if (dcParam != null && dcParam.getValue() != null && Boolean.parseBoolean(dcParam.getValue()))
		{
			NVPair dcClassNameParam = mongoDS.getAPIConfigInfo().getConfigParameters().get(MongoParam.DATA_CACHE_CLASS_NAME.getName());
			try
			{
				mongoDS.setDataCacheMonitor((NVECRUDMonitor) Class.forName(dcClassNameParam.getValue()).newInstance());
				//log.info("Data Cache monitor created " + dcClassNameParam);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		//log.info("Connect finished");
		// set the key
//		mongoDS.setKeyMaker(apiConfig.getKeyMaker());
//		mongoDS.setAPISecurityManager((APISecurityManager<Subject>) apiConfig.getAPISecurityManager());
		
		
		return mongoDS;
	}

	
	@Override
	public APIExceptionHandler getExceptionHandler() 
	{
		return MongoExceptionHandler.SINGLETON;
	}

	@Override
	public String getName()
    {
		return API_NAME;
	}

	@Override
	public APITokenManager getAPITokenManager()
    {
		return null;
	}

}