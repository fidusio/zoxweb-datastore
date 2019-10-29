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
package org.zoxweb.server.ds.derby;



import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.zoxweb.server.ds.data.DSTestClass;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.data.AddressDAO;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.NVConfigEntity;

import java.io.IOException;
import java.util.List;


public class DerbyDataStoreTest {

	// local datatore
    private static DerbyDataStore dataStore;
    private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String MEMORY_URL = "jdbc:derby:memory:test;create=true";
    private static final String DIR_URL = "jdbc:derby:/db/derby/test;create=true";
    private static final String USER ="APP";
    private static final String PASSWORD ="APP";

    @BeforeClass
    public static void setUp() {
//    	try
    	{
    		APIConfigInfo configInfo = new APIConfigInfoDAO();
    		configInfo.getProperties().add("driver", DRIVER);
    		configInfo.getProperties().add("url", MEMORY_URL);
    		configInfo.getProperties().add("user", USER);
    		configInfo.getProperties().add("password", PASSWORD);
    		dataStore = new DerbyDataStore(configInfo);
    		System.out.println(dataStore.connect());
    	}
//    	catch(Throwable e)
//    	{
//    		e.printStackTrace();
//    		
//    	}
    	System.out.println("Setup done");
    }

    @AfterClass
    public static void tearDown() {
        dataStore.close();
    }

    @Test
    public void testInsert() throws IOException {

        DSTestClass.AllTypes allTypes = null;


        for (int i = 0;i < 10; i++) {
            allTypes = DSTestClass.AllTypes.autoBuilder();
            long ts = System.nanoTime();
            allTypes = dataStore.insert(allTypes);
            ts = System.nanoTime() - ts;
            System.out.println("It took: " + Const.TimeInMillis.nanosToString(ts)  + " to insert");
        }

        assertNotNull(allTypes);
        assertNotNull(allTypes.getGlobalID());
        System.out.println("json:" + GSONUtil.toJSON(allTypes, true, false, false));
        List<DSTestClass.AllTypes> result = dataStore.searchByID((NVConfigEntity) allTypes.getNVConfig(), allTypes.getGlobalID());
        allTypes = result.get(0);
        System.out.println(allTypes.getBytes().length);
        String json = GSONUtil.toJSON(allTypes, true, false, true);
        System.out.println(json);
        DSTestClass.AllTypes.testValues(allTypes);
        allTypes = GSONUtil.fromJSON(json);
        DSTestClass.AllTypes.testValues(allTypes);
   }

    @Test
    public void testRead() {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO = dataStore.insert(addressDAO);
        assertNotNull(addressDAO);

        List<AddressDAO> result = dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGlobalID());


        //assertNotNull(result)

        System.out.println("Result: " + result);

    }

    @Test
    public void testUpdate() {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO = (AddressDAO) dataStore.insert(addressDAO);

        System.out.println("Original NVE: " + dataStore.lookupByReferenceID(AddressDAO.class.getName(), addressDAO.getReferenceID()));

        addressDAO.setCity("New York");
        addressDAO.setStateOrProvince("NY");
        addressDAO = (AddressDAO) dataStore.update(addressDAO);

        System.out.println("Updated NVE: " + dataStore.lookupByReferenceID(AddressDAO.class.getName(), addressDAO.getReferenceID()));
    }

    @Test
    public void testDelete() {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO = (AddressDAO) dataStore.insert(addressDAO);

        dataStore.delete(addressDAO, true);
        List<AddressDAO> result = dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGlobalID());
        assert(result.isEmpty());
    }

}