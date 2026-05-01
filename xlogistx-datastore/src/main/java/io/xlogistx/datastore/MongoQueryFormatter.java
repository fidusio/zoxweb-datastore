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

import org.bson.Document;
import org.zoxweb.server.filters.TimestampFilter;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const.LogicalOperator;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.SharedStringUtil;

import java.util.ArrayList;
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
     * Top-level formatter: processes QueryMatch and LogicalOperator markers using
     * SQL-style precedence — AND binds tighter than OR.
     * E.g. {@code A OR B AND C} is parsed as {@code A OR (B AND C)}.
     */
    public static Document formatQuery(NVConfigEntity nvce, QueryMarker... criteria) {
        if (criteria == null || criteria.length == 0) {
            return new Document();
        }

        // Split criteria into OR-groups; within each group, conditions are AND-ed.
        List<List<Document>> orGroups = new ArrayList<>();
        List<Document> currentAndGroup = new ArrayList<>();

        for (QueryMarker marker : criteria) {
            if (marker instanceof QueryMatch) {
                currentAndGroup.add(buildCondition((QueryMatch<?>) marker, nvce));
            } else if (marker instanceof LogicalOperator) {
                LogicalOperator op = (LogicalOperator) marker;
                if (op == LogicalOperator.OR) {
                    if (!currentAndGroup.isEmpty()) {
                        orGroups.add(currentAndGroup);
                        currentAndGroup = new ArrayList<>();
                    }
                }
                // LogicalOperator.AND is the default joiner — no action needed.
            }
        }
        if (!currentAndGroup.isEmpty()) {
            orGroups.add(currentAndGroup);
        }

        if (orGroups.isEmpty()) {
            return new Document();
        }

        // Collapse each AND-group to a single Document.
        List<Document> orTerms = new ArrayList<>(orGroups.size());
        for (List<Document> andGroup : orGroups) {
            if (andGroup.size() == 1) {
                orTerms.add(andGroup.get(0));
            } else {
                orTerms.add(new Document("$and", andGroup));
            }
        }

        if (orTerms.size() == 1) {
            return orTerms.get(0);
        }
        return new Document("$or", orTerms);
    }


    private static Object map(NVConfig nvc, QueryMatch<?> queryMatch) {
        if (nvc != null && nvc.getMetaTypeBase() == Date.class && queryMatch.getValue() instanceof String) {
            return TimestampFilter.SINGLETON.validate((String) queryMatch.getValue());
        }

        if (nvc != null && nvc.isTypeReferenceID() && queryMatch.getValue() instanceof String) {
            return IDGs.UUIDV7.decode((String) queryMatch.getValue());
        }

        if (nvc == null && MongoUtil.ReservedID.lookupByName(SharedStringUtil.valueAfterRightToken(queryMatch.getName(), ".")) == MongoUtil.ReservedID.REFERENCE_ID
                && queryMatch.getValue() instanceof String) {
            return IDGs.UUIDV7.decode((String) queryMatch.getValue());
        }

        return queryMatch.getValue();
    }

}