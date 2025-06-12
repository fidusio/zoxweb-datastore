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

import org.bson.types.ObjectId;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.GetNVConfig;
import org.zoxweb.shared.util.GetName;

@SuppressWarnings("serial")
public class QueryMatchObjectId
	extends QueryMatch<ObjectId>
{
	
	/**
	 * This constructor instantiates QueryMatch object based on name, value, and operator
	 * parameters.
	 * @param operator to be used
	 * @param id reference id
	 * @param names to be collected
	 */
	public QueryMatchObjectId(RelationalOperator operator, String id, String... names)
	{
		super(operator, new ObjectId(id), names);
	}
	
	/**
	 * This constructor instantiates QueryMatch object based on name, value, and operator
	 * parameters.
	 * @param operator to be used
	 * @param id reference id
	 * @param names to be collected
	 */
	public QueryMatchObjectId(RelationalOperator operator, String id, GetName... names)
	{
		super(operator, new ObjectId(id), names);
	}
	
	/**
	 * This constructor instantiates QueryMatch object based on name, GetNVConfig value, and operator
	 * parameters.
	 * @param operator to be used
	 * @param id reference id
	 * @param gnvc to be collected
	 */
	public QueryMatchObjectId(RelationalOperator operator, String id, GetNVConfig... gnvc)
	{
		super(operator, new ObjectId(id), gnvc);
	}
	
}