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

import org.zoxweb.shared.api.*;
import org.zoxweb.shared.util.*;

public class SyncMongoDSCreator
        implements APIServiceProviderCreator {

    public final static String API_NAME = "MongoDBSync";

    /**
     * Contains parameters needed to create the Mongo database.
     */
    public enum MongoParam
            implements GetNameValue<String> {
        DB_NAME("db_name", "no_sneak"),
        HOST("host", "localhost"),
        PORT("port", "27017"),
        // "mongodb://localhost:27017";
        DB_URI("db_uri", null),
        DATA_CACHE("data_cache", "false"),
        DATA_CACHE_CLASS_NAME("data_cache_class_name", null),
        GRIDFS_POSTFIX("gridfs_name", "_gridfs");;

        private final String name;
        private final String value;

        MongoParam(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public static String dataStoreName(APIConfigInfo aci) {
            return aci.getProperties().getValue(DB_NAME);
        }

        public static String dataStoreURI(APIConfigInfo aci) {
            String uri = aci.getProperties().getValue(DB_URI);
            if (SUS.isEmpty(uri))
                uri = "mongodb://" + aci.getProperties().getValue(HOST) + ":" + aci.getProperties().getValue(PORT) + "/?uuidRepresentation=standard";

            return uri;
        }

        public static String gridFSDataStoreName(APIConfigInfo aci) {
            return dataStoreName(aci) + aci.getProperties().getValue(GRIDFS_POSTFIX);
        }

    }

    @Override
    public APIConfigInfo createEmptyConfigInfo() {
        APIConfigInfo configInfo = new APIConfigInfoDAO();

        for (MongoParam mp : MongoParam.values()) {
            if (mp == MongoParam.PORT)
                configInfo.getProperties().build(new NVInt(mp.getName(), Integer.parseInt(mp.getValue())));
            else
                configInfo.getProperties().build(mp.getName(), mp.getValue());
        }


        @SuppressWarnings("unchecked")

        APIServiceType[] types = {APIServiceType.DATA_STORAGE, APIServiceType.DOCUMENT_STORAGE};
        configInfo.setServiceTypes(types);
        configInfo.setAPITypeName(API_NAME);
        configInfo.setDescription("MongoDBSync" + " java driver");
        configInfo.setVersion("10.0.0");

        return configInfo;
    }


    @Override
    public SyncMongoDS createAPI(APIDataStore<?, ?> dataStore, APIConfigInfo apiConfig)
            throws APIException {
        SyncMongoDS mongoDS = new SyncMongoDS();

        mongoDS.setAPIConfigInfo(apiConfig);
        mongoDS.setAPIExceptionHandler(SyncMongoExceptionHandler.SINGLETON);

        NVPair dcParam = (NVPair) mongoDS.getAPIConfigInfo().getProperties().get(MongoParam.DATA_CACHE.getName());

        if (dcParam != null && dcParam.getValue() != null && Boolean.parseBoolean(dcParam.getValue())) {
            NVPair dcClassNameParam = (NVPair) mongoDS.getAPIConfigInfo().getProperties().get(MongoParam.DATA_CACHE_CLASS_NAME.getName());
            try {
                mongoDS.setDataCacheMonitor((NVECRUDMonitor) Class.forName(dcClassNameParam.getValue()).newInstance());
                //log.info("Data Cache monitor created " + dcClassNameParam);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //log.info("Connect finished");
        // set the key
//		mongoDS.setKeyMaker(apiConfig.getKeyMaker());
//		mongoDS.setAPISecurityManager((APISecurityManager<Subject>) apiConfig.getAPISecurityManager());


        return mongoDS;
    }


    @Override
    public APIExceptionHandler getExceptionHandler() {
        return SyncMongoExceptionHandler.SINGLETON;
    }

    @Override
    public String getName() {
        return API_NAME;
    }

    @Override
    public APITokenManager getAPITokenManager() {
        return null;
    }

}