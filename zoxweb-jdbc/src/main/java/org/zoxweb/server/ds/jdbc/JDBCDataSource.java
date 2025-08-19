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
package org.zoxweb.server.ds.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBCDataSource
        implements DataSource {

    private String url = null;
    private String user = null;
    private String password = null;
    private Properties properties = new Properties();
    private String driverName;

    public JDBCDataSource() {

    }

    public JDBCDataSource(String driverName,
                          Properties driverProperties,
                          String url,
                          String defaultUser,
                          String defaultPassword) throws ClassNotFoundException, SQLException {
        this.url = url;
        setUser(defaultUser);
        setPassword(defaultPassword);
        setDriverName(driverName);
        this.properties = driverProperties;
    }


    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    @Override
    public Connection getConnection(String username, String password)
            throws SQLException {
        Properties prop = properties;
        if (username != null || password != null) {
            prop = (Properties) properties.clone();
        }

        if (username != null) {
            prop.setProperty("user", username);
        }

        if (password != null) {
            prop.setProperty("password", password);
        }

        return DriverManager.getConnection(url, prop);
    }

    @Override
    public PrintWriter getLogWriter()
            throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out)
            throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds)
            throws SQLException {

    }

    @Override
    public int getLoginTimeout()
            throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException {
        return false;
    }

    public String getURL() {
        return url;
    }

    public void setURL(String url) {
        this.url = url;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName)
            throws ClassNotFoundException {
        this.driverName = driverName;
        Class.forName(driverName);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}