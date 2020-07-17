package org.zoxweb.server.ds.mongo;

import com.mongodb.DBObject;


import org.junit.jupiter.api.Test;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.data.UserPreferenceDAO;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.MetaToken;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class MongoQueryFormatterTest {

	@Test
	public void testSingleQueryCriteria()
	{
        DBObject query = MongoQueryFormatter.formatQuery(UserInfoDAO.NVC_USER_INFO_DAO,
				new QueryMatch<>("first_name", "John", RelationalOperator.EQUAL));

        assertNotNull(query);
        assertNotNull(query.keySet());
        assertEquals(1, query.keySet().size());
        assertNotNull(query.get("first_name"));
	}
	
	@Test
	public void testMultipleQueryCriteria()
	{
        DBObject query = MongoQueryFormatter.formatQuery(UserInfoDAO.NVC_USER_INFO_DAO,
                new QueryMatch<>("first_name", "John", RelationalOperator.EQUAL),
                Const.LogicalOperator.AND,
				new QueryMatch<>("last_name", "Smith", RelationalOperator.NOT_EQUAL)
        );

        assertNotNull(query);
        assertNotNull(query.keySet());
        assertEquals(2, query.keySet().size());
        assertNotNull(query.get("first_name"));
        assertNotNull(query.get("last_name"));

        query = MongoQueryFormatter.formatQuery(UserInfoDAO.NVC_USER_INFO_DAO,
                new QueryMatch<>("first_name", "John", RelationalOperator.EQUAL),
                Const.LogicalOperator.AND,
                new QueryMatch<>("last_name", "Smith", RelationalOperator.EQUAL),
                Const.LogicalOperator.AND,
                new QueryMatch<>("reference_id", "542c55c7ea59bfebabe370b1", RelationalOperator.EQUAL)
        );

        assertNotNull(query);
        assertNotNull(query.keySet());
        assertEquals(3, query.keySet().size());
        assertNotNull(query.get("first_name"));
        assertNotNull(query.get("last_name"));
        assertNotNull(query.get("_id"));

        query = MongoQueryFormatter.formatQuery(UserPreferenceDAO.NVC_USER_PREFERENCE_DAO,
                new QueryMatchObjectId(RelationalOperator.EQUAL, "54e2aa3093a3154f6402f82c", UserPreferenceDAO.Param.APP_GID.getNVConfig(), MetaToken.REFERENCE_ID),
                Const.LogicalOperator.AND,
                new QueryMatch<>(RelationalOperator.EQUAL, "542c55c7ea59bfebabe370b1", "user_id")
        );

        assertNotNull(query);
        assertNotNull(query.keySet());
        assertEquals(2, query.keySet().size());
        assertNotNull(query.get("app_id.reference_id"));
        assertNotNull(query.get("_user_id"));
    }
	
}
