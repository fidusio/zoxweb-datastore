package org.zoxweb.server.ds.proxy;

import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APIServiceProviderCreator;
import org.zoxweb.shared.api.APITokenManager;
import org.zoxweb.shared.util.GetName;


public class ProxyDataStoreCreator implements APIServiceProviderCreator {
	
	
	public enum ProxyParam 
	implements GetName
	{
		URL("url"),
		URI("uri"),
		API_KEY("api_key"),
		HTTP_MESSAGE_CONFIG("data_cache"),	
		;
		
		private final String name;
	
		
		ProxyParam(String name)
		{
			this.name = name;
			
		}
		
		@Override
		public String getName() 
		{
			return name;
		}
	}

	
	

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public APIConfigInfo createEmptyConfigInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public APIExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataStoreProxy createAPI(APIDataStore<?> dataStore, APIConfigInfo apiConfig) throws APIException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public APITokenManager getAPITokenManager() {
		// TODO Auto-generated method stub
		return null;
	}

}
