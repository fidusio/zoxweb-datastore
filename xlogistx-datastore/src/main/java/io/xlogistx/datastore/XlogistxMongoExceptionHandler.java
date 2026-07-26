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

import com.mongodb.MongoException;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIException.Category;
import org.zoxweb.shared.api.APIException.Code;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.util.GetValue;

/**
 * This class handles exceptions in MongoDB.
 */
public class XlogistxMongoExceptionHandler
        implements APIExceptionHandler {

    /**
     * Contains Mongo error codes.
     */
    public enum MongoError
            implements GetValue<Integer> {
        DUPLICATE_KEY("Already exists.", 11000, Category.OPERATION, Code.DUPLICATE_ENTRY_NOT_ALLOWED),
        DUPLICATE_KEY_BULK("Already exists.", 11001, Category.OPERATION, Code.DUPLICATE_ENTRY_NOT_ALLOWED),
        WRITE_CONFLICT("Write conflict, retry.", 112, Category.OPERATION, Code.RETRY),
        NO_SUCH_TRANSACTION("Transaction expired or aborted, retry.", 251, Category.OPERATION, Code.RETRY),
        EXCEEDED_TIME_LIMIT("Operation timed out.", 50, Category.OPERATION, Code.RETRY),
        // Legacy (pre-3.x) codes kept for old servers.
        INVALID_FIELD_NAME("Invalid field name.", 10333, Category.OPERATION, Code.MISSING_PARAMETERS),
        CONNECTION_FAILED("Failed to connect.", 13328, Category.CONNECTION, Code.CONNECTION_FAILED),

        ;

        private final String message;
        private final Integer value;
        private final Category category;
        private final Code code;

        MongoError(String message, Integer value, Category category, Code code) {
            this.message = message;
            this.value = value;
            this.category = category;
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public Integer getValue() {
            return value;
        }

        public Category getCategory() {
            return category;
        }

        public Code getCode() {
            return code;
        }
    }


    /**
     * This variable declares that only one instance of this class can be created.
     */
    public static final XlogistxMongoExceptionHandler SINGLETON = new XlogistxMongoExceptionHandler();

    /**
     * The default constructor is declared private to prevent
     * outside instantiation of this class.
     */
    private XlogistxMongoExceptionHandler() {

    }

    /**
     * Throws an API exception.
     *
     * @param e to be thrown
     */
    @Override
    public void throwException(Exception e)
            throws APIException {
        APIException apiException = mapException(e);

        if (apiException != null)
            throw apiException;
    }

    /**
     * Maps an exception to an API exception.
     *
     * @param e to be mapped
     */
    @Override
    public APIException mapException(Exception e) {
        APIException apiException = null;

        if (e instanceof MongoException) {
            MongoException me = (MongoException) e;

            int code = me.getCode();

            for (MongoError mError : MongoError.values()) {
                if (mError.getValue() == code) {
                    apiException = new APIException(mError.getMessage(), mError.getCategory(), mError.getCode());
                    break;
                }
            }

            // Driver-labeled transient transaction failures are retryable regardless of code.
            if (apiException == null && (me.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
                    || me.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL))) {
                apiException = new APIException("Transient transaction failure, retry.", Category.OPERATION, Code.RETRY);
            }

            if (apiException == null) {
                apiException = new APIException("" + e);
            }
        }

        if (apiException == null) {
            apiException = new APIException(e.getMessage());
        }

        // Always preserve the original failure for diagnostics.
        if (apiException.getCause() == null && apiException != e) {
            apiException.initCause(e);
        }

        return apiException;
    }

}
