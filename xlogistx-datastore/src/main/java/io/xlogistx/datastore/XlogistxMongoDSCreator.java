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
package io.xlogistx.datastore;

import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.shared.api.*;
import org.zoxweb.shared.http.URLInfo;
import org.zoxweb.shared.util.*;

public class XlogistxMongoDSCreator
        implements APIServiceProviderCreator {

    public static final LogWrapper log = new LogWrapper(XlogistxMongoDSCreator.class);

    public final static String API_NAME = "XlogistxMongoDS";

    /**
     * Contains parameters needed to create the Mongo database.
     */
    public enum MongoParam
            implements GetNameValue<String> {
        DB_NAME("db_name", "no_sneak"),
        HOST("host", "localhost"),
        PORT("port", "27017"),
        // "mongodb://localhost:27017";
        //DB_URI("db_uri", null),
        // Credentials for authenticated deployments; percent-encoded into the connection string's
        // userinfo by dataStoreURI. authSource/authMechanism, when needed, ride in OPTIONS.
        USER("user", null),
        PASSWORD("password", null),
        DATA_CACHE("data_cache", "false"),
        DATA_CACHE_CLASS_NAME("data_cache_class_name", null),
        GRIDFS_POSTFIX("gridfs_name", "_gridfs"),
        // Extra connection-string options preserved from the input URL's query
        // (e.g. replicaSet=rs0, directConnection=true) — needed for transaction-capable deployments.
        OPTIONS("options", null);

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
            StringBuilder base = new StringBuilder("mongodb://");
            // Authenticated deployment: mongodb://user:password@host:port/... — userinfo must be
            // percent-encoded (':' '@' '/' '%' are structural in the connection string).
            String user = aci.getProperties().getValue(USER);
            if (SUS.isNotEmpty(user)) {
                base.append(percentEncode(user));
                String password = aci.getProperties().getValue(PASSWORD);
                if (SUS.isNotEmpty(password)) {
                    base.append(':').append(percentEncode(password));
                }
                base.append('@');
            }
            String host = aci.getProperties().getValue(HOST);
            Object port = aci.getProperties().getValue(PORT); // NVInt-typed -> Integer
            base.append(host).append(':').append(port);
            String dbName = aci.getProperties().getValue(DB_NAME);
            base.append('/');
            if (SUS.isNotEmpty(dbName)) {
                base.append(dbName); // empty db -> "mongodb://host:port/?query", never ".../null"
            }
            // Always enforce standard UUID representation; merge any options carried from the input
            // URL (replicaSet, directConnection, ...) so transaction-capable deployments can be targeted.
            String query = "uuidRepresentation=standard";
            Object optionsObj = aci.getProperties().getValue(OPTIONS);
            String options = optionsObj != null ? optionsObj.toString() : null;
            if (SUS.isNotEmpty(options)) {
                if (options.contains("uuidRepresentation")) {
                    if (!options.contains("uuidRepresentation=standard") && log.isEnabled()) {
                        log.getLogger().warning("dataStoreURI: OPTIONS overrides uuidRepresentation away from "
                                + "'standard' — UUID storage will be incompatible with this datastore's invariants: " + options);
                    }
                    query = options;
                } else {
                    query = query + "&" + options;
                }
            }
            return base + "?" + query;
        }

        /** RFC 3986 percent-encoding for connection-string userinfo. */
        private static String percentEncode(String s) {
            try {
                return java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20");
            } catch (java.io.UnsupportedEncodingException e) {
                throw new IllegalStateException(e); // UTF-8 always present
            }
        }

        /** Inverse of {@link #percentEncode} — a literal '+' must survive (not form-decode to space). */
        static String percentDecode(String s) {
            try {
                return java.net.URLDecoder.decode(s.replace("+", "%2B"), "UTF-8");
            } catch (java.io.UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }

        public static String gridFSDataStoreName(APIConfigInfo aci) {
            return dataStoreName(aci) + aci.getProperties().getValue(GRIDFS_POSTFIX);
        }

    }

    public APIConfigInfo toAPIConfigInfo(String mongoURL)
    {
        URLInfo urlInfo = URLInfo.parse(mongoURL);
        APIConfigInfo ret = createEmptyConfigInfo();
        ret.getProperties().build(MongoParam.HOST, urlInfo.ipAddress.getInetAddress())
                .build(new NVInt(MongoParam.PORT, urlInfo.ipAddress.getPort()))
                .build(MongoParam.DB_NAME, urlInfo.justPath());
        // Preserve credentials from an authenticated URL (mongodb://user:pass@host:...) — they
        // were previously dropped silently, making authenticated deployments unreachable.
        // URLInfo keeps the userinfo verbatim, so percent-decode here: the config stores the PLAIN
        // credential and dataStoreURI percent-encodes exactly once on rebuild.
        if (SUS.isNotEmpty(urlInfo.username)) {
            ret.getProperties().build(MongoParam.USER, MongoParam.percentDecode(urlInfo.username));
        }
        if (SUS.isNotEmpty(urlInfo.password)) {
            ret.getProperties().build(MongoParam.PASSWORD, MongoParam.percentDecode(urlInfo.password));
        }
        // Preserve the query string (replicaSet, directConnection, ...) so dataStoreURI can re-apply it.
        if (SUS.isNotEmpty(urlInfo.query)) {
            ret.getProperties().build(MongoParam.OPTIONS, urlInfo.query);
        }
        return ret;
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
        configInfo.setDescription("XlogistxMongoDS" + " java driver");
        configInfo.setVersion("10.0.0");

        return configInfo;
    }


    @Override
    public XlogistxMongoDataStore createAPI(APIDataStore<?, ?> dataStore, APIConfigInfo apiConfig)
            throws APIException {
        XlogistxMongoDataStore mongoDS = new XlogistxMongoDataStore();

        mongoDS.setAPIConfigInfo(apiConfig);
        mongoDS.setAPIExceptionHandler(XlogistxMongoExceptionHandler.SINGLETON);

        NVPair dcParam = (NVPair) mongoDS.getAPIConfigInfo().getProperties().get(MongoParam.DATA_CACHE.getName());

        if (dcParam != null && dcParam.getValue() != null && Boolean.parseBoolean(dcParam.getValue())) {
            NVPair dcClassNameParam = (NVPair) mongoDS.getAPIConfigInfo().getProperties().get(MongoParam.DATA_CACHE_CLASS_NAME.getName());
            try {
                mongoDS.setDataCacheMonitor((NVECRUDMonitor) Class.forName(dcClassNameParam.getValue()).getDeclaredConstructor().newInstance());
                //log.info("Data Cache monitor created " + dcClassNameParam);
            } catch (Exception e) {
                if (log.isEnabled()) log.getLogger().log(java.util.logging.Level.WARNING, "Failed to create data cache monitor", e);
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
        return XlogistxMongoExceptionHandler.SINGLETON;
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