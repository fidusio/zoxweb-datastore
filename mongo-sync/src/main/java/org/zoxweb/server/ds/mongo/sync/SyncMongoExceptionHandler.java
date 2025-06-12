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

import com.mongodb.MongoException;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIException.Category;
import org.zoxweb.shared.api.APIException.Code;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.util.GetValue;

/**
 * This class handles exceptions in MongoDB.
 */
public class SyncMongoExceptionHandler
	implements APIExceptionHandler
{

    /**
     * Contains Monogo error codes.
     */
	public enum MongoError 
		implements GetValue<Integer>
	{
		DUPLICATE_KEY("Already exists.", 11000, Category.OPERATION, Code.DUPLICATE_ENTRY_NOT_ALLOWED),
		INVALID_FIELD_NAME("Invalid field name.", 10333, Category.OPERATION, Code.MISSING_PARAMETERS),
		CONNECTION_FAILED("Failed to connect.", 13328, Category.CONNECTION, Code.CONNECTION_FAILED),
		
		;
		
		private final String message;
		private final Integer value;
		private final Category category;
		private final Code code;
		
		MongoError(String message, Integer value, Category category, Code code)
		{
			this.message = message;
			this.value = value;
			this.category = category;
			this.code = code;
		}

		public String getMessage() 
		{
			return message;
		}

		@Override
		public Integer getValue() 
		{
			return value;
		}

		public Category getCategory()
		{
			return category;
		}
		
		public Code getCode()
		{
			return code;
		}
	}
	
	
	/**
	 * This variable declares that only one instance of this class can be created.
	 */
	public static final SyncMongoExceptionHandler SINGLETON = new SyncMongoExceptionHandler();
	
	/**
	 * The default constructor is declared private to prevent
	 * outside instantiation of this class.
	 */
	private SyncMongoExceptionHandler()
	{
		
	}

	/**
	 * Throws an API exception.
	 * @param e to be thrown
	 */
	@Override
	public void throwException(Exception e) 
			throws APIException 
	{
		APIException apiException = mapException(e);	
		
		if (apiException != null)
			throw apiException;
	}

	/**
	 * Maps an exception to an API exception.
	 * @param e to be mapped 
	 */
	@Override
	public APIException mapException(Exception e) 
	{
		APIException apiException = null;
	
		if (e instanceof MongoException)
		{
			MongoException me = (MongoException) e;	
			
			int code = me.getCode();
			
			for (MongoError mError : MongoError.values())
			{
				if (mError.getValue() == code)
				{
					apiException = new APIException(mError.getMessage(), mError.getCategory(), mError.getCode());
					break;
				}
			}
			
			if (apiException == null)
			{
				apiException = new APIException("" + e);
			}
		}
		
		if (apiException == null)
		{
			apiException = new APIException(e.getMessage());
		}
		
		return apiException;
	}

}
