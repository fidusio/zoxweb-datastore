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

import org.bson.Document;
import org.bson.types.ObjectId;
import org.zoxweb.server.filters.TimestampFilter;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const.LogicalOperator;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.SharedStringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Includes utility methods to format Mongo query requests.
 */
public class MongoQueryFormatter {




    private static Document buildCondition(QueryMatch<?> qm, NVConfigEntity nvce) {
        NVConfig nvc = nvce.lookup(qm.getName());
        String key = MongoUtil.ReservedID.map(nvc, qm.getName());
        Object val = map(nvc, qm);

        switch (qm.getOperator()) {
            case EQUAL:
                // { key: value }
                return new Document(key, val);
            case GT:
                // { key: { $gt: value } }
                return new Document(key, new Document("$gt", val));
            case GTE:
                return new Document(key, new Document("$gte", val));
            case LT:
                return new Document(key, new Document("$lt", val));
            case LTE:
                return new Document(key, new Document("$lte", val));
            case NOT_EQUAL:
                return new Document(key, new Document("$ne", val));
            default:
                throw new IllegalArgumentException("Unsupported operator: " + qm.getOperator());
        }
    }


    /**
     * Top‐level formatter: processes QueryMatch and LogicalOperator markers in sequence,
     * building up a list of Documents and combining them with $and / $or.
     */
    public static Document formatQuery(NVConfigEntity nvce, QueryMarker... criteria) {
        List<Document> filters = new ArrayList<>();

        for (int i = 0; i < criteria.length; i++) {
            QueryMarker marker = criteria[i];

            if (marker instanceof QueryMatch) {
                // Simple field match

                Document cond = buildCondition((QueryMatch<?>) marker, nvce);
                filters.add(cond);

            } else if (marker instanceof LogicalOperator) {
                LogicalOperator op = (LogicalOperator) marker;

                if (op == LogicalOperator.OR && i + 1 < criteria.length &&
                        criteria[i + 1] instanceof QueryMatch) {

                    // Pop the last filter, combine with the next one under $or
                    Document left = filters.remove(filters.size() - 1);

                    Document right = buildCondition((QueryMatch<?>) criteria[++i], nvce);

                    filters.add(new Document("$or", Arrays.asList(left, right)));
                }
                // AND is the default behavior—no special action needed
            }
        }

        // Nothing matched → empty filter
        if (filters.isEmpty()) {
            return new Document();
        }
        // Single filter → return it directly
        if (filters.size() == 1) {
            return filters.get(0);
        }
        // Multiple filters → combine under $and
        return new Document("$and", filters);
    }



    private static Object map(NVConfig nvc, QueryMatch<?> queryMatch) {
        if (nvc != null && nvc.getMetaTypeBase() == Date.class && queryMatch.getValue() instanceof String) {
            return TimestampFilter.SINGLETON.validate((String) queryMatch.getValue());
        }

        if (nvc != null && nvc.isTypeReferenceID() && queryMatch.getValue() instanceof String) {
            return new ObjectId((String) queryMatch.getValue());
        }

        if (nvc == null && MongoUtil.ReservedID.lookupByName(SharedStringUtil.valueAfterRightToken(queryMatch.getName(), ".")) == MongoUtil.ReservedID.REFERENCE_ID
                && queryMatch.getValue() instanceof String) {
            return new ObjectId((String) queryMatch.getValue());
        }

        return queryMatch.getValue();
    }

}