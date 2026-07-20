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

import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIException.Category;
import org.zoxweb.shared.api.APIException.Code;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.util.GetValue;

import java.sql.SQLException;

public class H2PExceptionHandler
        implements APIExceptionHandler {

    /**
     * H2 reports errors via standard SQLState values. The mappings below match
     * the SQL:2008 states H2 emits (unique / not-null / connection).
     */
    public enum H2PError
            implements GetValue<String> {
        UNIQUE_VIOLATION("Already exists.", "23505", Category.OPERATION, Code.DUPLICATE_ENTRY_NOT_ALLOWED),
        NOT_NULL_VIOLATION("Missing required field.", "23502", Category.OPERATION, Code.MISSING_PARAMETERS),
        CONNECTION_FAILED("Failed to connect.", "08001", Category.CONNECTION, Code.CONNECTION_FAILED),
        ;

        private final String message;
        private final String value;
        private final Category category;
        private final Code code;

        H2PError(String message, String value, Category category, Code code) {
            this.message = message;
            this.value = value;
            this.category = category;
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String getValue() {
            return value;
        }

        public Category getCategory() {
            return category;
        }

        public Code getCode() {
            return code;
        }
    }

    public static final H2PExceptionHandler SINGLETON = new H2PExceptionHandler();

    private H2PExceptionHandler() {
    }

    @Override
    public void throwException(Exception e)
            throws APIException {
        APIException apiException = mapException(e);
        if (apiException != null)
            throw apiException;
    }

    @Override
    public APIException mapException(Exception e) {
        if (e instanceof SQLException) {
            String sqlState = ((SQLException) e).getSQLState();
            if (sqlState != null) {
                for (H2PError he : H2PError.values()) {
                    if (he.getValue().equals(sqlState)) {
                        return new APIException(he.getMessage(), he.getCategory(), he.getCode());
                    }
                }
            }
            return new APIException("" + e);
        }
        return new APIException(e.getMessage());
    }
}
