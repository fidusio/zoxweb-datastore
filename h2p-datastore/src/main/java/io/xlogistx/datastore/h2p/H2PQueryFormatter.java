/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.xlogistx.datastore.h2p;

import org.zoxweb.server.util.IDGs;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Builds H2 {@code WHERE} clauses + parameter binding from {@link QueryMarker}
 * sequences. NVConfigEntity-aware: reserved-ID / reference-ID criteria bind as
 * native {@code UUID}, enums bind as their name.
 */
public final class H2PQueryFormatter {

    private H2PQueryFormatter() {
    }

    /** WHERE fragment (no leading "WHERE") for the given criteria, identifiers quoted. */
    public static String formatWhere(QueryMarker... queryCriteria) {
        if (queryCriteria == null || queryCriteria.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (QueryMarker qm : queryCriteria) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            if (qm instanceof QueryMatch) {
                QueryMatch<?> qMatch = (QueryMatch<?>) qm;
                sb.append(H2PUtil.q(qMatch.getName()))
                  .append(' ').append(qMatch.getOperator().getValue()).append(" ?");
            } else if (qm instanceof Const.LogicalOperator) {
                sb.append(((Const.LogicalOperator) qm).getValue());
            }
        }
        return sb.toString();
    }

    /**
     * Binds each QueryMatch value starting at {@code startIndex}; returns the next
     * free parameter index. Reserved-ID / reference-ID fields decode String → UUID.
     */
    public static int bindWhere(PreparedStatement ps, int startIndex, NVConfigEntity nvce,
                                QueryMarker... queryCriteria) throws SQLException {
        int index = startIndex;
        if (queryCriteria == null) {
            return index;
        }
        for (QueryMarker qm : queryCriteria) {
            if (qm instanceof QueryMatch) {
                QueryMatch<?> qMatch = (QueryMatch<?>) qm;
                Object value = qMatch.getValue();
                NVConfig nvc = nvce != null ? nvce.lookup(qMatch.getName()) : null;
                ps.setObject(index++, normalize(nvc, value));
            }
        }
        return index;
    }

    /** Convert a query value into what H2 expects for its column type. */
    static Object normalize(NVConfig nvc, Object value) {
        if (value == null) {
            return null;
        }
        if (H2PUtil.isUUIDField(nvc)) {
            if (value instanceof UUID) {
                return value;
            }
            String s = value.toString();
            return s.isEmpty() ? null : IDGs.UUIDV7.decode(s);
        }
        if (value instanceof Enum) {
            return ((Enum<?>) value).name();
        }
        return value;
    }
}
