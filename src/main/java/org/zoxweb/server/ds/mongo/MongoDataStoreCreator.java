package org.zoxweb.server.ds.mongo;

import java.util.List;

import org.zoxweb.shared.util.GetNameValue;
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

/**
 * 
 * @author mzebib
 *
 */
public class MongoDataStoreCreator 
	implements APIServiceProviderCreator
{
	
	public final static String API_NAME = "MongoDB";
	
	/**
	 * This enum contains parameters needed to create the Mongo database.
	 * @author mzebib
	 *
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
		configInfo.setVersion("3.4.2");
			
		return configInfo;
	}

	
	@Override
	public MongoDataStore createAPI(APIDataStore<?> ds, APIConfigInfo apiConfig) 
			throws APIException
	{
		MongoDataStore mongoDS = new MongoDataStore();
		
		mongoDS.setAPIConfigInfo(apiConfig);
		mongoDS.setAPIExceptionHandler(MongoExceptionHandler.SINGLETON);
		
		return mongoDS;
	}

	
	@Override
	public APIExceptionHandler getExceptionHandler() 
	{
		return MongoExceptionHandler.SINGLETON;
	}

	@Override
	public String getName() {
		
		return API_NAME;
	}

	@Override
	public APITokenManager getAPITokenManager() {
	
		return null;
	}
	
	
	

}
