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


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.ds.data.DSConst;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.data.AddressDAO;
import org.zoxweb.shared.data.DeviceDAO;
import org.zoxweb.shared.data.Range;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.http.*;
import org.zoxweb.shared.iot.DeviceInfo;
import org.zoxweb.shared.iot.PortInfo;
import org.zoxweb.shared.iot.ProtocolInfo;
import org.zoxweb.shared.util.*;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class DerbyDataStoreTest {

	// local datatore
    private static DerbyDataStore dataStore;
    private static final String EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String CLIENT_DRIVER = "org.apache.derby.jdbc.ClientDriver";
   
    private static final String MEMORY_URL = "jdbc:derby:memory:test";
    private static final String EMBEDDED_DISK_URL = "jdbc:derby:/tmp/derby/test";
    private static final String CLIENT_URL = "jdbc:derby://localhost:1527/test-db";

    private static final String USER ="APP";
    private static final String PASSWORD ="APP";


    @BeforeAll
    public static void setUp() {
//    	try
    	{
    	    //NVEntity.GLOBAL_ID_AS_REF_ID = true;
    		APIConfigInfo configInfo = new APIConfigInfoDAO();
    		configInfo.getProperties().add("driver", CLIENT_DRIVER);
    		configInfo.getProperties().add("url", CLIENT_URL);
    		configInfo.getProperties().add("user", USER);
    		configInfo.getProperties().add("password", PASSWORD);
    		dataStore = new DerbyDataStore(configInfo);
    		System.out.println(dataStore.connect());
    		System.out.print("URLs:" + MEMORY_URL + " mem," + EMBEDDED_DISK_URL + " disk, " + CLIENT_URL);
    	}
//    	catch(Throwable e)
//    	{
//    		e.printStackTrace();
//    		
//    	}
    	System.out.println("Setup done");
    }

    @AfterAll
    public static void tearDown() {
        dataStore.close();
    }

    @Test
    public void testInsert() throws IOException {

        DSConst.AllTypes allTypes = null;


        for (int i = 0;i < 10; i++) {
            allTypes = DSConst.AllTypes.autoBuilder();
            long ts = System.nanoTime();
            allTypes = dataStore.insert(allTypes);
            ts = System.nanoTime() - ts;
            System.out.println("It took: " + Const.TimeInMillis.nanosToString(ts)  + " to insert");
        }

        assertNotNull(allTypes);
        assertNotNull(allTypes.getGUID());
        System.out.println("json:" + GSONUtil.toJSON(allTypes, true, false, false));
        List<DSConst.AllTypes> result = dataStore.searchByID((NVConfigEntity) allTypes.getNVConfig(), allTypes.getGUID());
        allTypes = result.get(0);
        System.out.println(allTypes.getBytes().length);
        String json = GSONUtil.toJSON(allTypes, true, false, true);
        System.out.println(json);
        DSConst.AllTypes.testValues(allTypes);
        allTypes = GSONUtil.fromJSON(json);
        DSConst.AllTypes.testValues(allTypes);
   }


    @Test
    public void testInsertComplex() throws IOException {
        DSConst.ComplexTypes complexTypes = null;

        for (int i = 0;i < 5; i++)
        {
            DSConst.AllTypes allTypes = DSConst.AllTypes.autoBuilder();
            long ts = System.nanoTime();
            complexTypes = DSConst.ComplexTypes.buildComplex(null);
            complexTypes.setAllTypes(i%2 == 0 ? allTypes: null);
            complexTypes = dataStore.insert(complexTypes);
            ts = System.nanoTime() - ts;
            System.out.println("It took: " + Const.TimeInMillis.nanosToString(ts)  + " to insert");
        }

        assertNotNull(complexTypes);
        assertNotNull(complexTypes.getGUID());
        String jsonOrig = GSONUtil.toJSON(complexTypes, true, false, false);

        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$ " + complexTypes.getGUID());
        List<DSConst.ComplexTypes> result = dataStore.searchByID((NVConfigEntity) complexTypes.getNVConfig(), complexTypes.getGUID());
        complexTypes = result.get(0);

        String json = GSONUtil.toJSON(complexTypes, true, false, false);
        assertEquals(jsonOrig,json);

    }

    @Test
    public void testRead() {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO.setZIPOrPostalCode("90001");
        addressDAO = dataStore.insert(addressDAO);
        assertNotNull(addressDAO);

        List<AddressDAO> result = dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGUID());


        //assertNotNull(result)

        System.out.println("Result: " + result);

    }

    @Test
    public void testUpdate() {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO = (AddressDAO) dataStore.update(addressDAO);

        System.out.println("Original NVE: " + dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGUID()));

        addressDAO.setCity("New York");
        addressDAO.setStateOrProvince("NY");
        addressDAO = (AddressDAO) dataStore.update(addressDAO);

        System.out.println("Updated NVE: " + dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGUID()));
    }

    @Test
    public void testSearchAll()
    {
        DeviceDAO device = DSConst.init(new DeviceDAO());
        device.setName(UUID.randomUUID().toString());
        device = dataStore.insert(device);

        List<DeviceDAO> result = dataStore.search(DeviceDAO.class.getName(), null, null);
        System.out.println("size:" + result.size());
        result = dataStore.search(DeviceDAO.class.getName(), null, new QueryMatch<String>(Const.RelationalOperator.EQUAL, device.getName(), "name"));
        System.out.println("size:" + result.size() + " " + result);

        DSConst.AllTypes at = DSConst.AllTypes.autoBuilder();
        at.setStatus(Const.Status.SUSPENDED);
        at = dataStore.insert(at);
        List<DSConst.AllTypes> resultAt = dataStore.search(DSConst.AllTypes.class.getName(), null,
                new QueryMatch<String>(Const.RelationalOperator.EQUAL, Const.Status.SUSPENDED.name(), "enum_val"), Const.LogicalOperator.AND, new QueryMatch<String>(Const.RelationalOperator.EQUAL, device.getName(), "name"));
        System.out.println("size:" + resultAt.size() + " " + resultAt);
        assert(resultAt.isEmpty());

        resultAt = dataStore.search(DSConst.AllTypes.class.getName(), null,
                new QueryMatch<String>(Const.RelationalOperator.EQUAL, Const.Status.SUSPENDED.name(), "enum_val"), Const.LogicalOperator.OR, new QueryMatch<String>(Const.RelationalOperator.EQUAL, device.getName(), "name"));
        System.out.println("size:" + resultAt.size() + " " + resultAt);
        assert(!resultAt.isEmpty());


    }

    @Test
    public void testUpdateComplex() throws IOException {
        DSConst.ComplexTypes nveTypes = null;

//        for (int i = 0;i < 10; i++)
        {
            DSConst.AllTypes allTypes = DSConst.AllTypes.autoBuilder();
            long ts = System.nanoTime();
            nveTypes = DSConst.ComplexTypes.buildComplex(null);
            nveTypes.setAllTypes(allTypes);
            nveTypes = dataStore.update(nveTypes);
            ts = System.nanoTime() - ts;
            System.out.println("It took: " + Const.TimeInMillis.nanosToString(ts)  + " to insert");
            System.out.println("json:" + GSONUtil.toJSON(nveTypes, true, false, false));
        }

        assertNotNull(nveTypes);
        assertNotNull(nveTypes.getGUID());
        nveTypes.setName("batata");
        nveTypes.getAllTypes().setName("harra");
        nveTypes =  dataStore.update(nveTypes);
        String jsonOrig = GSONUtil.toJSON(nveTypes, true, false, false);
        System.out.println("json:" + jsonOrig);
        List<DSConst.ComplexTypes> result = dataStore.searchByID((NVConfigEntity) nveTypes.getNVConfig(), nveTypes.getGUID());
        nveTypes = result.get(0);
        String json = GSONUtil.toJSON(nveTypes, true, false, false);
        assertEquals(jsonOrig, json);


        System.out.println(nveTypes);
    }

    @Test
    public void testDelete() throws IOException {
        AddressDAO addressDAO = new AddressDAO();
        addressDAO.setCity("Los Angeles");
        addressDAO.setStateOrProvince("CA");
        addressDAO.setCountry("USA");
        addressDAO = (AddressDAO) dataStore.insert(addressDAO);

        dataStore.delete(addressDAO, true);
        List<AddressDAO> result = dataStore.searchByID(AddressDAO.class.getName(), addressDAO.getGUID());
        assert (result.isEmpty());

        DSConst.AllTypes at = DSConst.AllTypes.autoBuilder();
        at.setName("to-be-deleted");
        at = dataStore.insert(at);
        assert (at.getGUID() != null);

        assert (dataStore.delete((NVConfigEntity) at.getNVConfig(), new QueryMatch<String>(Const.RelationalOperator.EQUAL, "to-be-deleted", "name")));
        assert (dataStore.delete((NVConfigEntity) at.getNVConfig(), new QueryMatch<Boolean>(Const.RelationalOperator.EQUAL, true, "boolean_val")));
        assert (!dataStore.delete((NVConfigEntity) at.getNVConfig(), new QueryMatch<String>(Const.RelationalOperator.EQUAL, "to-be-deleted", "name")));

        DSConst.AllTypes allTypes = DSConst.AllTypes.autoBuilder();
        String name = "NEVER" + UUID.randomUUID().toString();
        allTypes.setName(name);
        long ts = System.nanoTime();
        DSConst.ComplexTypes complex = DSConst.ComplexTypes.buildComplex(name);
        complex.setAllTypes(allTypes);
        complex = dataStore.update(complex);
        ts = System.nanoTime() - ts;
        System.out.println("It took: " + Const.TimeInMillis.nanosToString(ts)  + " to insert");
        System.out.println("json:" + GSONUtil.toJSON(complex, true, false, false));
        assert(dataStore.delete(complex, true));
        assert(dataStore.searchByID((NVConfigEntity) complex.getNVConfig(), complex.getGUID()).isEmpty());



    }
    @Test
    public void testInsertDeviceDAO() throws IOException {
        DeviceDAO device = DSConst.init(new DeviceDAO());
        device.getProperties().add("toto", "titi");
        device.getProperties().add(new NVInt("int_val", 100));
        device = dataStore.insert(device);
        assertNotNull(device.getGUID());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRange()
    {
        Range<Float> floatRange = new Range<Float>(1.0f, 6.666f);

        floatRange = dataStore.insert(floatRange);
        assertNotNull(floatRange.getGUID());


        floatRange = (Range<Float>) dataStore.searchByID(Range.class.getName(), floatRange.getGUID()).get(0);


        Range<Integer> intRange = new Range<Integer>(1, 200);

        intRange = dataStore.insert(intRange);
        assertNotNull(intRange.getGUID());


        intRange = (Range<Integer>) dataStore.searchByID(Range.class.getName(), intRange.getGUID()).get(0);


    }



    @Test
    public void testPassword() throws NoSuchAlgorithmException {
        CIPassword p = HashUtil.toPassword(CryptoConst.HASHType.BCRYPT, 0, 10, "P1ssw@rd");
        dataStore.insert(p);

        CIPassword found = dataStore.lookupByReferenceID(CIPassword.class.getName(), p.getGUID());
        Assertions.assertNotEquals(found, p);
        HashUtil.validatePassword(found, "P1ssw@rd");
        Assertions.assertEquals(GSONUtil.toJSONDefault(p), GSONUtil.toJSONDefault(found));
    }

    @Test
    public void testHMCI()
    {
        HTTPMessageConfigInterface hmci = HTTPMessageConfig.createAndInit("https://api.xlogistx.io", "login", HTTPMethod.PATCH);
        hmci.setAccept(HTTPMediaType.APPLICATION_JSON);
        hmci.setContentType(HTTPMediaType.APPLICATION_JSON);

        hmci.getHeaders().add("revision", "2023-07-15");
        HTTPAuthorization authorization = new HTTPAuthorization("XlogistX-KEY", "ABB-CC-DDSFS-664554");
        //dataStore.insert(authorization);


        hmci.setAuthorization(authorization);
        NVGenericMap nvgm = new NVGenericMap();
        nvgm.add("name", "mario");
        nvgm.add("email", "mario@mario.com");
        nvgm.add(new NVInt("age", 31));
        hmci.setContent(GSONUtil.toJSONDefault(nvgm));


        HTTPMessageConfig httpMessageConfig = dataStore.insert((HTTPMessageConfig)hmci);
        System.out.println(httpMessageConfig.getGUID());


        httpMessageConfig = (HTTPMessageConfig) dataStore.searchByID(HTTPMessageConfig.class.getName(), httpMessageConfig.getGUID()).get(0);

        System.out.println(SharedStringUtil.toString(httpMessageConfig.getContent()));

        String json = GSONUtil.toJSONDefault(hmci);
        String jsonFromDB = GSONUtil.toJSONDefault(httpMessageConfig);

        System.out.println((hmci == httpMessageConfig) + " " + json.equals(jsonFromDB));
        System.out.println(json);
        System.out.println(jsonFromDB);

        authorization = hmci.getAuthorization();
        //dataStore.delete(httpMessageConfig, true);

        System.out.println("Authorization meta: " + ((NVConfigEntity)authorization.getNVConfig()).getAttributes());
    }


    @Test
    public void deviceInfoTest()
    {
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setName("ATTINY84-M");
        deviceInfo.setDescription("14 pin Atmel microcontroller");
        deviceInfo.setManufacturer("Microchip");
        deviceInfo.setModel("PU");


        ProtocolInfo i2c = new ProtocolInfo();
        i2c.setName("I2C");
        i2c.setDescription("Inter-Integrated Circuit Protocol");
        deviceInfo.addProtocol(i2c);

        deviceInfo.addPort(new PortInfo("VCC", "Input voltage").setPort(1));
        deviceInfo.addPort(new PortInfo("PB0", "General pin").setPort(2).setFunctions("PCINT8", "10", "0", "XTAL1"));
        deviceInfo.addPort(new PortInfo("PB1", "General pin").setPort(3).setFunctions("PCINT9", "9", "1", "XTAL2"));
        deviceInfo.addPort(new PortInfo("PB3", "General pin").setPort(4).setFunctions("PCINT11", "11", "11", "RESET"));
        deviceInfo.addPort(new PortInfo("PB2", "General pin").setPort(5).setFunctions("PCINT10", "8", "2", "OC0A", "INT0"));
        deviceInfo.addPort(new PortInfo("PA7", "General pin").setPort(6).setFunctions("PCINT7", "7", "3", "OC0B", "ADC7"));
        deviceInfo.addPort(new PortInfo("PA6", "General pin").setPort(7).setFunctions("PCINT6", "6", "4", "MOSI", "DI", "SDA", "OC1A", "ADC6"));
        deviceInfo.addPort(new PortInfo("PA5", "General pin").setPort(8).setFunctions("PCINT5", "5", "5", "MISO", "DO", "OC1B", "ADC5"));
        deviceInfo.addPort(new PortInfo("PA4", "General pin").setPort(9).setFunctions("PCINT4", "4", "6", "SCK", "SCL", "ADC4"));
        deviceInfo.addPort(new PortInfo("PA3", "General pin").setPort(10).setFunctions("PCINT3", "3", "7", "ADC3"));
        deviceInfo.addPort(new PortInfo("PA2", "General pin").setPort(11).setFunctions("PCINT2", "2", "8", "AIN1", "ADC2"));
        deviceInfo.addPort(new PortInfo("PA1", "General pin").setPort(12).setFunctions("PCINT1", "1", "9", "AIN0", "ADC1"));
        deviceInfo.addPort(new PortInfo("PA0", "General pin").setPort(13).setFunctions("PCINT0", "0", "10", "AREF", "ADC0"));




        deviceInfo.addPort(new PortInfo("GND", "ground").setPort(14));




        //deviceInfo = dataStore.insert(deviceInfo);

        List<DeviceInfo> results = dataStore.search(DeviceInfo.NVC_DEVICE_INFO, null, new QueryMatch<String>(Const.RelationalOperator.EQUAL, "ATTINY84-M", "name"));


        System.out.println(GSONUtil.toJSONDefault(results.get(0), true));
    }

}