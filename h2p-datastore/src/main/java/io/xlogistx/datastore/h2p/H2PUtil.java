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

import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.NVConfig;
import org.zoxweb.shared.util.NVEntity;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Relational mapping helpers for the (PostgreSQL-portable) H2P datastore.
 *
 * <p>Each attribute of an {@link org.zoxweb.shared.util.NVConfigEntity} maps to a
 * storage kind ({@link #classify}):
 * <ul>
 *   <li>{@code SCALAR} — a typed column ({@code varchar}/{@code bigint}/{@code integer}/
 *       {@code real}/{@code double precision}/{@code boolean}/{@code uuid} for reserved/ref-id)</li>
 *   <li>{@code BLOB} — a {@code bytea} column (raw {@code byte[]} field data)</li>
 *   <li>{@code ENTITY_REF} — a {@code uuid} column with a FOREIGN KEY to the referenced type's table</li>
 *   <li>{@code ENTITY_COLLECTION} — a join table (no column on the parent)</li>
 *   <li>{@code SCHEMALESS} — a {@code varchar} column holding JSON (NVGenericMap, NamedValue,
 *       primitive lists/sets) via {@code GSONUtil.toJSONDefault} / {@code fromJSONDefault}</li>
 * </ul>
 */
public final class H2PUtil {

    private H2PUtil() {
    }

    /** Storage kind of an entity attribute. */
    public enum AttrKind {
        PK,               // the guid primary key
        EXCLUDED,         // not persisted (reference_id)
        SCALAR,           // typed column
        BLOB,             // bytea column
        ENTITY_REF,       // uuid FK column -> child table
        ENTITY_COLLECTION,// join table
        SCHEMALESS        // json (varchar) column
    }

    public static final Set<String> META_INSERT_EXCLUSION = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(MetaToken.REFERENCE_ID.getName())));

    /**
     * Reserved-ID attributes persisted as native {@code uuid} columns — same set
     * as {@code xlogistx-datastore.ReservedID}. Membership here is what makes these
     * names UUID-stored; their zoxweb-core NVConfigs are not flagged isTypeReferenceID.
     */
    private static final Set<String> RESERVED_UUID_NAMES;
    static {
        Set<String> s = new LinkedHashSet<>();
        for (GetName gn : new GetName[]{
                MetaToken.BROKER_GUID, MetaToken.GUID, MetaToken.PERMISSION_GUID,
                MetaToken.ROLE_GROUP_GUID, MetaToken.ROLE_GUID, MetaToken.SUBJECT_GUID}) {
            s.add(gn.getName());
        }
        RESERVED_UUID_NAMES = Collections.unmodifiableSet(s);
    }

    /** True if this attribute must be persisted as a native uuid (reserved-ID member or ref-ID flag). */
    public static boolean isUUIDField(NVConfig nvc) {
        return nvc != null && (RESERVED_UUID_NAMES.contains(nvc.getName()) || nvc.isTypeReferenceID());
    }

    public static boolean excludeMeta(Set<String> exclusion, String name) {
        return exclusion.contains(name);
    }

    /** Quote an identifier (case preserved) — portable across H2 and PostgreSQL. */
    public static String q(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    /** Classify how an attribute is stored. */
    public static AttrKind classify(NVConfig nvc) {
        if (nvc == null) {
            return AttrKind.EXCLUDED;
        }
        String name = nvc.getName();
        if (MetaToken.GUID.getName().equals(name)) {
            return AttrKind.PK;
        }
        if (META_INSERT_EXCLUSION.contains(name)) {
            return AttrKind.EXCLUDED;
        }
        Class<?> mt = nvc.getMetaType();
        if (mt == byte[].class) {
            return AttrKind.BLOB;
        }
        if (isUUIDField(nvc)) {
            return AttrKind.SCALAR; // uuid column
        }
        Class<?> base = nvc.getMetaTypeBase();
        boolean entity = base != null && NVEntity.class.isAssignableFrom(base);
        if (entity) {
            return nvc.isArray() ? AttrKind.ENTITY_COLLECTION : AttrKind.ENTITY_REF;
        }
        if (!nvc.isArray() && isScalarType(mt)) {
            return AttrKind.SCALAR;
        }
        // NVGenericMap, NamedValue, NVStringList, NVIntList, NVEnumList, ... -> JSON
        return AttrKind.SCHEMALESS;
    }

    private static boolean isScalarType(Class<?> mt) {
        return mt == Boolean.class || mt == String.class || mt == Integer.class
                || mt == Long.class || mt == Float.class || mt == Double.class
                || mt == Date.class || mt == Number.class || (mt != null && Enum.class.isAssignableFrom(mt));
    }

    /** The SQL column type for a {@code SCALAR} attribute. */
    public static String scalarColumnType(NVConfig nvc) {
        if (isUUIDField(nvc)) {
            return "uuid";
        }
        Class<?> mt = nvc.getMetaType();
        if (mt == Boolean.class) return "boolean";
        if (mt == String.class) return "varchar";
        if (mt == Integer.class) return "integer";
        if (mt == Long.class) return "bigint";
        if (mt == Float.class) return "real";
        if (mt == Double.class) return "double precision";
        if (mt == Date.class) return "bigint";
        if (mt == Number.class) return "varchar";
        if (mt != null && Enum.class.isAssignableFrom(mt)) return "varchar";
        return "varchar";
    }

    /** The referenced entity class for {@code ENTITY_REF} / {@code ENTITY_COLLECTION} attributes. */
    @SuppressWarnings("unchecked")
    public static Class<? extends NVEntity> childEntityClass(NVConfig nvc) {
        return (Class<? extends NVEntity>) nvc.getMetaTypeBase();
    }
}
