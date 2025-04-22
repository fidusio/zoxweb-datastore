/*
 * Copyright (c) 2012-2019 ZoxWeb.com LLC.
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
package org.zoxweb.server.ds.derby;



import org.zoxweb.shared.util.GetNameValue;


import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;

import org.zoxweb.shared.api.APIServiceProviderCreator;
import org.zoxweb.shared.api.APIServiceType;
import org.zoxweb.shared.api.APITokenManager;

public class DerbyDataStoreCreator 
	implements APIServiceProviderCreator
{
	
	public final static String API_NAME = "DerbyDB";

    /**
     * Contains parameters needed to create the Mongo database.
     */
	public enum DerbyParam 
		implements GetNameValue<String>
	{
		DRIVER("driver", null),
		URL("url", null),
		USER("user", null),
		PASSWORD("password",null),
		
		
		;
		
		private final String name;
		private final String value;
		
		DerbyParam(String name, String value)
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
		
		for (DerbyParam dp : DerbyParam.values())
  		configInfo.getProperties().add(dp.getName(), dp.getValue());
		
		APIServiceType[] types = {APIServiceType.DATA_STORAGE};
		configInfo.setServiceTypes(types);
		configInfo.setAPITypeName(API_NAME);
		configInfo.setDescription("Derby DB Configuration Info.");
		configInfo.setVersion("1.0.0");
			
		return configInfo;
	}
	
	
	@Override
	public DerbyDataStore createAPI(APIDataStore<?, ?> ds, APIConfigInfo apiConfig)
			throws APIException
	{
	  DerbyDataStore derbyDS = new DerbyDataStore();
		
	  derbyDS.setAPIConfigInfo(apiConfig);
	  return derbyDS;
	}

	
	@Override
	public APIExceptionHandler getExceptionHandler() 
	{
		return null;
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