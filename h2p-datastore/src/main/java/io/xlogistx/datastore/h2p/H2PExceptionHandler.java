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
     * H2 and PostgreSQL both report errors via standard SQLState values. Exact states first;
     * unmatched states fall back on the SQLState class (first two chars) in {@link #mapException}.
     */
    public enum H2PError
            implements GetValue<String> {
        UNIQUE_VIOLATION("Already exists.", "23505", Category.OPERATION, Code.DUPLICATE_ENTRY_NOT_ALLOWED),
        NOT_NULL_VIOLATION("Missing required field.", "23502", Category.OPERATION, Code.MISSING_PARAMETERS),
        // FK violation: 23503 on PostgreSQL (and H2 child-exists), 23506 on H2 (parent missing).
        FK_VIOLATION("Referential integrity violation.", "23503", Category.OPERATION, Code.PROVIDER_EXCEPTION),
        FK_VIOLATION_H2("Referential integrity violation.", "23506", Category.OPERATION, Code.PROVIDER_EXCEPTION),
        SERIALIZATION_FAILURE("Transaction conflict, retry.", "40001", Category.OPERATION, Code.RETRY),
        DEADLOCK_DETECTED("Deadlock detected, retry.", "40P01", Category.OPERATION, Code.RETRY),
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
                        return withCause(new APIException(he.getMessage(), he.getCategory(), he.getCode()), e);
                    }
                }
                // Fall back on the SQLState class (first two chars).
                if (sqlState.startsWith("23")) {
                    return withCause(new APIException(e.getMessage(), Category.OPERATION, Code.PROVIDER_EXCEPTION), e);
                }
                if (sqlState.startsWith("08")) {
                    return withCause(new APIException(e.getMessage(), Category.CONNECTION, Code.CONNECTION_FAILED), e);
                }
                if (sqlState.startsWith("40")) {
                    return withCause(new APIException(e.getMessage(), Category.OPERATION, Code.RETRY), e);
                }
            }
            return withCause(new APIException("" + e), e);
        }
        return withCause(new APIException(e.getMessage()), e);
    }

    private static APIException withCause(APIException apiException, Exception cause) {
        if (apiException.getCause() == null && apiException != cause) {
            apiException.initCause(cause);
        }
        return apiException;
    }
}
