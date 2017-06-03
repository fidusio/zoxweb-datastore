package org.zoxweb.server.ds.mongo;

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
	public QueryMatchObjectId(RelationalOperator operator, String id, String ...names)
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
	public QueryMatchObjectId(RelationalOperator operator, String id, GetName ...names)
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
	public QueryMatchObjectId(RelationalOperator operator, String id, GetNVConfig ...gnvc)
	{
		super(operator, new ObjectId(id), gnvc);	
	}
	
}