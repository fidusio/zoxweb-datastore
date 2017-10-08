package org.zoxweb.server.ds.mongo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoxweb.server.ds.mongo.MongoQueryFormatter;
import org.zoxweb.server.ds.mongo.QueryMatchObjectId;
import org.zoxweb.shared.api.APITokenDAO;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.NVConfigEntity;

import com.mongodb.DBObject;



import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.util.Const.LogicalOperator;
import org.zoxweb.shared.util.Const.RelationalOperator;

public class JTestMongoQueryFormatter {

	private UserInfoDAO user;
	private NVConfigEntity nvce;
	private DBObject query;
	
	@Before
	public void setUp() throws Exception 
	{
//		user = DAOFactory.userInfoDAOBuilder();
		nvce = UserInfoDAO.NVC_USER_INFO_DAO;
	}

	@After
	public void tearDown() throws Exception 
	{
		
	}

	@Test
	public void test1() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				new QueryMatch<String>("first_name", "Mustapha", RelationalOperator.EQUAL));
		
		System.out.println("Query Test: " + query);
	}
	
	@Test
	public void test2() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				new QueryMatch<String>("first_name", "Mustapha", RelationalOperator.EQUAL),
				LogicalOperator.AND, 
				new QueryMatch<String>("last_name", "Zebib", RelationalOperator.NOT_EQUAL)
				);
		
		System.out.println("Query Test: " + query);
	}
	
	@Test
	public void test3() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				new QueryMatch<String>("first_name", "Mustapha", RelationalOperator.EQUAL),
				LogicalOperator.AND, 
				new QueryMatch<String>("last_name", "Zebib", RelationalOperator.EQUAL),
				LogicalOperator.AND,
				new QueryMatch<String>("date_of_birth", "1991-15-01", RelationalOperator.EQUAL),
				LogicalOperator.AND,
				new QueryMatch<String>(RelationalOperator.EQUAL, "542c55c7ea59bfebabe370b1", "reference_id")
				);
		
		System.out.println("Query Test3: " + query);
	}
	
	@Test
	public void test4() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				new QueryMatch<String>("first_name", "Mustapha", RelationalOperator.EQUAL),
				LogicalOperator.AND, 
				new QueryMatch<String>("last_name", "Zebib", RelationalOperator.EQUAL),
				LogicalOperator.AND,
				new QueryMatch<String>("date_of_birth", "1991-15-01", RelationalOperator.EQUAL),
				LogicalOperator.AND,
				new QueryMatch<String>("list_of_addresses", "{Purdue Ave, Veteran Ave}", RelationalOperator.EQUAL)
				);
		
		System.out.println("Query Test: " + query);
	}
	
	
	@Test
	public void test5() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				
				new QueryMatch<String>(RelationalOperator.EQUAL, "542c55c7ea59bfebabe370b1", "user_id"),
				LogicalOperator.AND, 
				new QueryMatch<String>("last_name", "Zebib", RelationalOperator.EQUAL),
				new QueryMatch<String>(RelationalOperator.EQUAL, "542c55c7ea59bfebabe370b1", "reference_id"),
				LogicalOperator.AND, 
				new QueryMatchString(RelationalOperator.EQUAL, "54e2aa3093a3154f6402f82c", APITokenDAO.Params.API_CONFIG_INFO_DAO.getNVConfig(), MetaToken.REFERENCE_ID)
				);
		
		System.out.println("Query Test5: " + query);
	}
	
	@Test
	public void test6() 
	{
		query = MongoQueryFormatter.formatQuery(nvce,
				
				new QueryMatch<String>(RelationalOperator.EQUAL, "542c55c7ea59bfebabe370b1", "user_id"),
				LogicalOperator.AND, 
				new QueryMatch<Long>("date_of_birth", (long) 0, RelationalOperator.GT),
				LogicalOperator.AND, 
				new QueryMatch<Long>("date_of_birth", (long) 51545845, RelationalOperator.LT)
				);
		
		System.out.println("Query Test: " + query);
	}
	
}
