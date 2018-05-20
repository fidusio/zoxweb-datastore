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
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.bson.types.ObjectId;
import org.zoxweb.server.ds.mongo.MongoDataStore.ReservedID;
import org.zoxweb.server.filters.TimestampFilter;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const.LogicalOperator;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.SharedStringUtil;

import java.util.Date;

/**
 * Includes utility methods to format Mongo query requests.
 */
public class MongoQueryFormatter 
{

	private static QueryBuilder formatQueryMatch(QueryBuilder qb, NVConfigEntity nvce, QueryMarker  qm)
	{
		if (qm != null && qm instanceof QueryMatch)
		{
			QueryMatch<?> queryMatch = (QueryMatch<?>) qm; 
			NVConfig nvc = nvce.lookup(queryMatch.getName());
			//log.info(queryMatch.getName());
			//ReservedID resID = ReservedID.lookupByName(queryMatch.getName());
			String queryKey =  ReservedID.map(nvc, queryMatch.getName());//(resID != null ? resID.getValue() : queryMatch.getName());
			
			if (qb == null)
			{
				qb = new QueryBuilder();
			}

			qb = qb.put(queryKey);

			if (nvc == null && !queryMatch.isCanonicalName())
			{
				throw new IllegalArgumentException("Parameter name does not exist in table: " + queryMatch.getName());
			}
			else
			{
                switch(queryMatch.getOperator())
                {
                case EQUAL:
                	//Object val = map(nvc, queryMatch);
                    qb.is(map(nvc, queryMatch));
                    break;
                case GT:
                    qb.greaterThan(map(nvc, queryMatch));
                    break;
                case GTE:
                    qb.greaterThanEquals(map(nvc, queryMatch));
                    break;
                case LT:
                    qb.lessThan(map(nvc, queryMatch));
                    break;
                case LTE:
                    qb.lessThanEquals(map(nvc, queryMatch));
                    break;
                case NOT_EQUAL:
                    qb.notEquals(map(nvc, queryMatch));
                    break;
                }
			}
		}

		return qb;
	}

	/**
	 * Formats a query request for MongoDB based on NVConfigEntity and specified
	 * query criteria.
	 * @param nvce the meta class descriptor
	 * @param queryCriteria list 
	 * @return DBOject formated based on the criteria
	 */
	public static DBObject formatQuery(NVConfigEntity nvce, QueryMarker ... queryCriteria)
	{
		QueryBuilder qb = null;

		if (queryCriteria != null)
		{
			for (int i = 0; i < queryCriteria.length; i++)
			{
				if (queryCriteria[i] instanceof QueryMatch)
				{
					qb = formatQueryMatch(qb, nvce, (QueryMatch<?>) queryCriteria[i]);
//					QueryMatch<?> queryMatch = (QueryMatch<?>) queryCriteria[i];
//					
//					NVConfig nvc = nvce.lookup(queryMatch.getName());
//					//log.info(queryMatch.getName());
//					//ReservedID resID = ReservedID.lookupByName(queryMatch.getName());
//					String queryKey =  ReservedID.map(nvc, queryMatch.getName());//(resID != null ? resID.getValue() : queryMatch.getName());
//					
//					if ( qb == null)
//					{
//						qb = new QueryBuilder();
//					}
//					qb = qb.put(queryKey);
//				
//					//System.out.println(querykey+":" + resID + ":" + queryMatch.getValue());
//					
//					if (nvc == null && !queryMatch.isCanonicalName())
//					{
//						throw new IllegalArgumentException("Parameter name does not exist in table: " + queryMatch.getName());
//					}
//					else
//					{
//							switch(queryMatch.getOperator())
//							{
//							case EQUAL:
//								//System.out.println(querykey + ":" + map(nvc, resID, queryMatch).getClass().getName());
//								qb.is(map(nvc, queryMatch));
//								break;
//							case GT:
//								qb.greaterThan(map(nvc, queryMatch));
//								break;
//							case GTE:
//								qb.greaterThanEquals(map(nvc, queryMatch));
//								break;
//							case LT:
//								qb.lessThan(map(nvc, queryMatch));
//								break;
//							case LTE:
//								qb.lessThanEquals(map(nvc, queryMatch));
//								break;
//							case NOT_EQUAL:
//								qb.notEquals(map(nvc, queryMatch));
//								break;
//							
//							}
//					}
				}
				else if (queryCriteria[i] instanceof LogicalOperator)
				{
					LogicalOperator logical = (LogicalOperator) queryCriteria[i];
					
					if ((i + 1) < queryCriteria.length)
					{	
						switch(logical)
						{
						case AND:
							
							// This condition is true
							// since the refid is unique if you and it with another one
							// the query will never match
							ReservedID resID = ReservedID.lookupByName(((QueryMatch<?>) queryCriteria[i+1]).getName());
							if (resID == null)
								qb.and(((QueryMatch<?>) queryCriteria[i+1]).getName());
							break;
						case OR:
							DBObject temp = qb.get();
							QueryBuilder tempQB = formatQueryMatch(null, nvce, queryCriteria[++i]);
							if (tempQB != null)
							{
								qb = new QueryBuilder();
								qb.or(temp, tempQB.get());
							}
						
//							String n = ((QueryMatch<?>) queryCriteria[i+1]).getName();
//							qb.or(n);
//							break;
							
							//throw new IllegalArgumentException("OR feature not supported yet.");
						}
					}
				}
			}
		}
		
		if (qb == null)
			return new BasicDBObject();
	
		return qb.get();
	}

	private static Object map(NVConfig nvc, QueryMatch<?> queryMatch)
	{
		if (nvc != null && nvc.getMetaTypeBase() == Date.class && queryMatch.getValue() instanceof String)
		{
			return TimestampFilter.SINGLETON.validate((String) queryMatch.getValue());
		}
		
		if (nvc != null && nvc.isTypeReferenceID() && queryMatch.getValue() instanceof String)
		{
			ObjectId toRet = new ObjectId((String)queryMatch.getValue());
			return toRet;
		}
		
		if (nvc == null && ReservedID.lookupByName(SharedStringUtil.valueAfterRightToken(queryMatch.getName(), ".")) == ReservedID.REFERENCE_ID 
			&& queryMatch.getValue() instanceof String)
		{
			ObjectId toRet = new ObjectId((String)queryMatch.getValue());
			return toRet;
		}
		
		return queryMatch.getValue();
	}

}