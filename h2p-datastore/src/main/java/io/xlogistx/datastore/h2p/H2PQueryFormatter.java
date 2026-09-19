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
import org.zoxweb.shared.db.QueryGroup;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.db.QueryMatchIn;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Builds H2 {@code WHERE} clauses + parameter binding from {@link QueryMarker}
 * sequences. NVConfigEntity-aware: reserved-ID / reference-ID criteria bind as
 * native {@code UUID}, enums bind as their name.
 *
 * <p>Supported markers: {@link QueryMatch} (all {@code Const.RelationalOperator}s including
 * {@code LIKE}/{@code NOT_LIKE}), {@link QueryMatchIn} ({@code [NOT] IN (...)}; an empty value
 * list renders as a constant — matches nothing, or everything when negated), {@link QueryGroup}
 * (explicit parentheses, balance-validated) and {@code Const.LogicalOperator}. Any other
 * {@link QueryMarker} is REJECTED — silently skipping an unknown criterion would widen the
 * result set (a data-exposure bug on version skew), so it fails loudly instead.
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
        int groupDepth = 0;
        for (QueryMarker qm : queryCriteria) {
            if (qm == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(' ');
            }
            if (qm instanceof QueryMatch) {
                QueryMatch<?> qMatch = (QueryMatch<?>) qm;
                if (isNullCheck(qMatch)) {
                    // A null value with =/!= can never match via a bound parameter (SQL null semantics,
                    // and pgjdbc rejects untyped nulls); emit the IS [NOT] NULL form instead.
                    sb.append(H2PUtil.q(qMatch.getName()))
                      .append(qMatch.getOperator() == Const.RelationalOperator.EQUAL ? " IS NULL" : " IS NOT NULL");
                } else {
                    sb.append(H2PUtil.q(qMatch.getName()))
                      .append(' ').append(qMatch.getOperator().getValue()).append(" ?");
                }
            } else if (qm instanceof QueryMatchIn) {
                QueryMatchIn<?> in = (QueryMatchIn<?>) qm;
                List<?> values = in.getValues();
                if (values == null || values.isEmpty()) {
                    // IN () is invalid SQL; an empty set matches nothing (or everything when negated).
                    sb.append(in.isNot() ? "1 = 1" : "1 = 0");
                } else {
                    sb.append(H2PUtil.q(in.getName()));
                    if (in.isNot()) sb.append(" NOT");
                    sb.append(" IN (");
                    for (int i = 0; i < values.size(); i++) {
                        sb.append(i == 0 ? "?" : ", ?");
                    }
                    sb.append(')');
                }
            } else if (qm instanceof QueryGroup) {
                sb.append(((QueryGroup) qm).getValue());
                if (qm == QueryGroup.OPEN) {
                    groupDepth++;
                } else if (--groupDepth < 0) {
                    throw new IllegalArgumentException("unbalanced QueryGroup: CLOSE without matching OPEN");
                }
            } else if (qm instanceof Const.LogicalOperator) {
                sb.append(((Const.LogicalOperator) qm).getValue());
            } else {
                throw new IllegalArgumentException("unsupported QueryMarker "
                        + qm.getClass().getName() + ": " + qm);
            }
        }
        if (groupDepth != 0) {
            throw new IllegalArgumentException("unbalanced QueryGroup: " + groupDepth + " unclosed OPEN");
        }
        return sb.toString();
    }

    /**
     * Binds each QueryMatch/QueryMatchIn value starting at {@code startIndex}; returns the next
     * free parameter index. Reserved-ID / reference-ID fields and single entity-reference columns
     * ({@code AttrKind.ENTITY_REF}, stored as {@code uuid}) decode String → UUID; an entity-reference
     * criterion may also carry the child entity itself, in which case its GUID is bound.
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
                if (isNullCheck(qMatch)) {
                    continue; // rendered as IS [NOT] NULL — no parameter to bind
                }
                Object value = qMatch.getValue();
                NVConfig nvc = nvce != null ? nvce.lookup(qMatch.getName()) : null;
                ps.setObject(index++, normalize(nvc, value));
            } else if (qm instanceof QueryMatchIn) {
                QueryMatchIn<?> in = (QueryMatchIn<?>) qm;
                List<?> values = in.getValues();
                if (values == null || values.isEmpty()) {
                    continue; // rendered as a constant — no parameters to bind
                }
                NVConfig nvc = nvce != null ? nvce.lookup(in.getName()) : null;
                for (Object v : values) {
                    ps.setObject(index++, normalize(nvc, v));
                }
            }
        }
        return index;
    }

    /** True when the match must render as {@code IS NULL} / {@code IS NOT NULL} instead of a bound parameter. */
    static boolean isNullCheck(QueryMatch<?> qMatch) {
        return qMatch.getValue() == null
                && (qMatch.getOperator() == Const.RelationalOperator.EQUAL
                || qMatch.getOperator() == Const.RelationalOperator.NOT_EQUAL);
    }

    /**
     * Convert a query value into what the column type stores. Public so tests can assert the
     * bound type: H2 in PostgreSQL mode coerces a varchar against a {@code uuid} column, native
     * PostgreSQL does not.
     */
    public static Object normalize(NVConfig nvc, Object value) {
        if (value == null) {
            return null;
        }
        if (H2PUtil.isUUIDField(nvc) || H2PUtil.classify(nvc) == H2PUtil.AttrKind.ENTITY_REF) {
            if (value instanceof UUID) {
                return value;
            }
            if (value instanceof NVEntity) {
                value = ((NVEntity) value).getGUID();
            }
            String s = value == null ? "" : value.toString();
            return s.isEmpty() ? null : IDGs.UUIDV7.decode(s);
        }
        if (value instanceof Enum) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof Date) {
            return ((Date) value).getTime(); // Date columns are bigint (epoch millis)
        }
        // NVNumber columns are type-tagged varchar; criteria must use the same encoding (equality only).
        if (value instanceof Number && nvc != null && nvc.getMetaType() == Number.class) {
            return H2PUtil.encodeNumber((Number) value);
        }
        return value;
    }
}
