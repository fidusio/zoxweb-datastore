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

import org.postgresql.util.PGobject;
import org.zoxweb.shared.api.APIDataStore.DSType;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Dialect-specific mapping for schemaless (JSON) columns — the "meta type converter" selected
 * by the datastore's {@link DSType}. The JSON <em>content</em> is produced uniformly by
 * {@code GSONUtil.toJSONDefault}/{@code fromJSONDefault}; only the SQL column type and the JDBC
 * binding differ by target engine:
 * <ul>
 *   <li><b>H2</b> — a {@code varchar} column bound with {@code setString}; the column reads back as a {@link String}.</li>
 *   <li><b>PostgreSQL</b> — a native {@code jsonb} column bound via a {@link PGobject} (so the value is stored
 *       and queryable as jsonb); the column reads back as a {@code PGobject} whose {@code getValue()} is the JSON text.</li>
 * </ul>
 * Everything else (uuid, bytea, typed scalars, FK columns, join tables) is identical across both engines.
 */
public enum H2PDialect {

    H2 {
        @Override
        public String schemalessColumnType() {
            return "varchar";
        }

        @Override
        public void bindSchemaless(PreparedStatement ps, int idx, String json) throws SQLException {
            ps.setString(idx, json);
        }
    },

    POSTGRES {
        @Override
        public String schemalessColumnType() {
            return "jsonb";
        }

        @Override
        public void bindSchemaless(PreparedStatement ps, int idx, String json) throws SQLException {
            if (json == null) {
                ps.setObject(idx, null);
                return;
            }
            PGobject pg = new PGobject();
            pg.setType("jsonb");
            pg.setValue(json);
            ps.setObject(idx, pg);
        }
    };

    /** SQL type for a schemaless (JSON) column in this dialect. */
    public abstract String schemalessColumnType();

    /** Bind a JSON string to a schemaless column parameter. */
    public abstract void bindSchemaless(PreparedStatement ps, int idx, String json) throws SQLException;

    /** Normalize a schemaless column value read from the DB into JSON text ({@link String} or {@link PGobject}). */
    public String readSchemaless(Object column) {
        if (column == null) return null;
        if (column instanceof PGobject) return ((PGobject) column).getValue();
        return column.toString();
    }

    /** Select the dialect for a resolved datastore type; anything but POSTGRES uses the H2 mapping. */
    public static H2PDialect forDSType(DSType dsType) {
        return dsType == DSType.POSTGRES ? POSTGRES : H2;
    }
}
