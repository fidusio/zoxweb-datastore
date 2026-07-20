/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
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
package io.xlogistx.datastore.h2p;

import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIDataStore.DSType;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APIServiceProviderCreator;
import org.zoxweb.shared.api.APIServiceType;
import org.zoxweb.shared.api.APITokenManager;
import org.zoxweb.shared.util.GetNameValue;
import org.zoxweb.shared.util.NVInt;

public class H2PDSCreator
        implements APIServiceProviderCreator {

    public static final String API_NAME = "H2PDS";

    /**
     * H2 connection parameters.
     *
     * <p>The connection <b>{@link #TYPE}</b> selects the URL shape:
     * <ul>
     *   <li>{@code mem}  — in-memory DB {@code jdbc:h2:mem:<db_name>;DB_CLOSE_DELAY=-1}
     *       (keeps the DB alive between connections for the JVM lifetime)</li>
     *   <li>{@code file} — embedded file DB {@code jdbc:h2:file:<path>/<db_name>;DB_CLOSE_DELAY=-1}
     *       (keeps the DB open across connections so connection-per-op doesn't reopen the file each op)</li>
     *   <li>{@code tcp}  — remote server DB {@code jdbc:h2:tcp://<host>:<port>/<db_name>}</li>
     * </ul>
     *
     * <p>Settings appended as {@code ;KEY=VALUE} when set: <b>{@link #MODE}</b> (H2 SQL
     * compatibility mode, default {@code PostgreSQL}), <b>{@link #CIPHER}</b> (e.g. {@code AES}
     * for an encrypted DB), {@link #IF_EXISTS}, {@link #AUTO_SERVER}, and any raw {@link #OPTIONS}.
     * <b>{@link #FILE_PASSWORD}</b> (used with {@code CIPHER}) is not a URL setting — H2 expects
     * the connection password to be {@code "<filePassword> <userPassword>"}; see
     * {@link #dataStorePassword(APIConfigInfo)}.
     *
     * <p>A full {@link #URL}, when set, overrides all of the above and is used verbatim.
     */
    public enum H2PParam
            implements GetNameValue<String> {
        DRIVER("driver", "org.h2.Driver"),
        // Full JDBC URL. When set (non-empty) it takes precedence over TYPE/HOST/PORT/PATH/DB_NAME
        // and all settings — e.g. "jdbc:h2:mem:mydb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL".
        URL("url", null),
        TYPE("type", "mem"),                 // connection type: mem | file | tcp
        HOST("host", "localhost"),
        PORT("port", "9092"),
        PATH("path", "./data"),
        DB_NAME("db_name", "h2_test"),
        USER("user", "sa"),
        PASSWORD("password", ""),
        MODE("mode", "PostgreSQL"),          // H2 SQL compatibility mode (;MODE=...)
        CIPHER("cipher", null),              // encryption cipher, e.g. AES (;CIPHER=AES)
        FILE_PASSWORD("file_password", null),// file-encryption password (prefixed onto the password)
        IF_EXISTS("ifexists", null),          // ;IFEXISTS=TRUE — fail if the DB does not already exist
        AUTO_SERVER("auto_server", null),    // ;AUTO_SERVER=TRUE — automatic mixed mode
        OPTIONS("options", null),            // any additional raw ;KEY=VALUE settings
        POOL_MAX_SIZE("pool_max_size", "10"),// HikariCP max pool size (native PostgreSQL only)
        POOL_MIN_IDLE("pool_min_idle", "2"), // HikariCP min idle connections (native PostgreSQL only)
        ;

        private final String name;
        private final String value;

        H2PParam(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static String dataStoreName(APIConfigInfo aci) {
            return aci.getProperties().getValue(DB_NAME);
        }

        /** True when this config targets a native PostgreSQL server (by URL prefix or driver class). */
        static boolean isPostgres(APIConfigInfo aci) {
            String url = aci.getProperties().getValue(URL);
            if (url != null && url.toLowerCase().startsWith("jdbc:postgresql")) {
                return true;
            }
            String driver = aci.getProperties().getValue(DRIVER);
            return driver != null && driver.toLowerCase().contains("postgresql");
        }

        /**
         * The connection password. On an <b>encrypted</b> H2 file DB (i.e. {@link #CIPHER} is set),
         * H2 expects the file-encryption password and the user password in a single space-separated
         * value — {@code "<filePwd> <userPwd>"} — so the {@link #FILE_PASSWORD} is prefixed onto the
         * user {@link #PASSWORD}. Without {@code CIPHER} the DB is not encrypted and the plain user
         * password is returned (prefixing a file password there would make H2 reject the login).
         * Native PostgreSQL has no such convention.
         */
        public static String dataStorePassword(APIConfigInfo aci) {
            String pwd = aci.getProperties().getValue(PASSWORD);
            if (isPostgres(aci)) {
                return pwd;
            }
            // Only an encrypted DB (CIPHER set) takes the "<filePwd> <userPwd>" form.
            String cipher = aci.getProperties().getValue(CIPHER);
            if (cipher != null && !cipher.isEmpty()) {
                String filePwd = aci.getProperties().getValue(FILE_PASSWORD);
                return (filePwd != null ? filePwd : "") + " " + (pwd != null ? pwd : "");
            }
            return pwd;
        }

        public static String dataStoreURI(APIConfigInfo aci) {
            // A full JDBC URL, when provided, is used verbatim (either engine).
            String url = aci.getProperties().getValue(URL);
            if (url != null && !url.isEmpty()) {
                return url;
            }

            String dbName = aci.getProperties().getValue(DB_NAME);

            // Native PostgreSQL: jdbc:postgresql://host:port/db[?raw-options]. No H2-only settings.
            if (isPostgres(aci)) {
                String pg = "jdbc:postgresql://"
                        + aci.getProperties().getValue(HOST) + ":"
                        + aci.getProperties().getValue(PORT) + "/"
                        + dbName;
                String opts = aci.getProperties().getValue(OPTIONS);
                if (opts != null && !opts.isEmpty()) {
                    pg += "?" + opts;
                }
                return pg;
            }

            String type = aci.getProperties().getValue(TYPE);
            String base;
            if ("file".equalsIgnoreCase(type)) {
                // keep the embedded DB open across connections; without this the file DB is
                // closed (flush + file-lock release) whenever the last connection closes and
                // reopened on the next op — connection-per-op would otherwise churn it every op.
                base = "jdbc:h2:file:" + aci.getProperties().getValue(PATH) + "/" + dbName
                        + ";DB_CLOSE_DELAY=-1";
            } else if ("tcp".equalsIgnoreCase(type)) {
                base = "jdbc:h2:tcp://"
                        + aci.getProperties().getValue(HOST) + ":"
                        + aci.getProperties().getValue(PORT) + "/"
                        + dbName;
            } else {
                // in-memory; keep the DB alive across connections within the JVM
                base = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";
            }

            StringBuilder sb = new StringBuilder(base);
            appendSetting(sb, "MODE", aci.getProperties().getValue(MODE));
            appendSetting(sb, "CIPHER", aci.getProperties().getValue(CIPHER));
            appendSetting(sb, "IFEXISTS", aci.getProperties().getValue(IF_EXISTS));
            appendSetting(sb, "AUTO_SERVER", aci.getProperties().getValue(AUTO_SERVER));

            String options = aci.getProperties().getValue(OPTIONS);
            if (options != null && !options.isEmpty()) {
                sb.append(';').append(options);
            }
            return sb.toString();
        }

        private static void appendSetting(StringBuilder sb, String key, String value) {
            if (value != null && !value.isEmpty()) {
                sb.append(';').append(key).append('=').append(value);
            }
        }
    }

    /**
     * Resolve the target datastore engine from a config: {@code POSTGRES} when the URL is a
     * {@code jdbc:postgresql} URL or the driver is the Postgres driver; {@code H2} for a
     * {@code jdbc:h2} URL or the H2 driver; otherwise {@code UNKNOWN}.
     */
    public static DSType resolveDSType(APIConfigInfo aci) {
        if (aci == null) {
            return DSType.UNKNOWN;
        }
        if (H2PParam.isPostgres(aci)) {
            return DSType.POSTGRES;
        }
        String url = aci.getProperties().getValue(H2PParam.URL);
        if (url != null && url.toLowerCase().startsWith("jdbc:h2")) {
            return DSType.H2;
        }
        String driver = aci.getProperties().getValue(H2PParam.DRIVER);
        if (driver != null && driver.toLowerCase().contains("h2")) {
            return DSType.H2;
        }
        return DSType.UNKNOWN;
    }

    /**
     * Convenience: build a config from a single full JDBC URL (with default user/password).
     * e.g. {@code toAPIConfigInfo("jdbc:h2:mem:mydb;DB_CLOSE_DELAY=-1")}.
     */
    public APIConfigInfo toAPIConfigInfo(String jdbcURL) {
        APIConfigInfo ret = createEmptyConfigInfo();
        ret.getProperties().build(H2PParam.URL, jdbcURL);
        return ret;
    }

    /** Convenience: build a config from a full JDBC URL plus explicit credentials. */
    public APIConfigInfo toAPIConfigInfo(String jdbcURL, String user, String password) {
        APIConfigInfo ret = toAPIConfigInfo(jdbcURL);
        ret.getProperties().build(H2PParam.USER.getName(), user);
        ret.getProperties().build(H2PParam.PASSWORD.getName(), password);
        return ret;
    }

    @Override
    public APIConfigInfo createEmptyConfigInfo() {
        APIConfigInfo configInfo = new APIConfigInfoDAO();

        for (H2PParam hp : H2PParam.values()) {
            if (hp == H2PParam.PORT) {
                configInfo.getProperties().build(new NVInt(hp.getName(), Integer.parseInt(hp.getValue())));
            } else {
                configInfo.getProperties().build(hp.getName(), hp.getValue());
            }
        }

        APIServiceType[] types = {APIServiceType.DATA_STORAGE};
        configInfo.setServiceTypes(types);
        configInfo.setAPITypeName(API_NAME);
        configInfo.setDescription(API_NAME + " java driver");
        configInfo.setVersion("1.0.0");

        return configInfo;
    }

    @Override
    public H2PDataStore createAPI(APIDataStore<?, ?> ds, APIConfigInfo apiConfig)
            throws APIException {
        H2PDataStore h2DS = new H2PDataStore();
        h2DS.setAPIConfigInfo(apiConfig);
        h2DS.setAPIExceptionHandler(H2PExceptionHandler.SINGLETON);
        return h2DS;
    }

    @Override
    public APIExceptionHandler getExceptionHandler() {
        return H2PExceptionHandler.SINGLETON;
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