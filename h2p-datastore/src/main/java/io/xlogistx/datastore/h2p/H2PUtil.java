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

import org.zoxweb.shared.util.*;

import java.io.File;
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



    /** Substitution token for the file location (dir/dbName) in {@link #DEFAULT_H2_URL}. */
    public static final String LOCATION_TOK  = "$$LOCATION$$";
    /** Default encrypted H2 file-DB URL template; see {@link #defaultH2JdbcURL(String, String)}. */
    public static final String DEFAULT_H2_URL = "jdbc:h2:file:" + LOCATION_TOK + ";MODE=PostgreSQL;CIPHER=AES";

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

    // ---------- JDBC URL parsing ----------

    /** Keys used by {@link #parseJdbcURL(String)} in the returned {@link NVGenericMap}. */
    public static final String JDBC_PREFIX      = "jdbc:";
    public static final String JDBC_URL         = "url";          // the original URL, verbatim
    public static final String JDBC_SUBPROTOCOL = "subprotocol";  // e.g. "h2", "postgresql", "mysql" (lowercased)
    public static final String JDBC_TYPE        = "type";         // H2 sub-type: mem | file | tcp | ssl | zip | nio
    public static final String JDBC_HOST        = "host";         // hostname (or raw multi-host authority)
    public static final String JDBC_PORT        = "port";         // NVInt when numeric, else the raw string
    public static final String JDBC_DATABASE    = "database";     // database / schema name
    public static final String JDBC_PATH        = "path";         // filesystem path (H2 file/embedded forms)
    public static final String JDBC_PARAMS      = "params";       // nested map of driver settings/options

    /**
     * Parse a JDBC URL into an {@link NVGenericMap}. The JDBC spec only fixes the
     * {@code jdbc:<subprotocol>:<subname>} prefix; the subname is vendor-specific, so this parser
     * understands the two shapes this datastore targets (and most others that follow them):
     * <ul>
     *   <li><b>H2</b> — {@code jdbc:h2:mem:<db>}, {@code jdbc:h2:file:<path>}, {@code jdbc:h2:tcp://<host>:<port>/<db>},
     *       or a bare {@code jdbc:h2:<path>}, with {@code ;KEY=VALUE} settings.</li>
     *   <li><b>PostgreSQL / MySQL</b> — {@code jdbc:postgresql://<host>:<port>/<db>} with {@code ?opt=val&opt2=val2}
     *       options (also a db-only {@code jdbc:postgresql:<db>}).</li>
     * </ul>
     * The returned map always carries {@link #JDBC_URL} and {@link #JDBC_SUBPROTOCOL}; the remaining
     * structural keys ({@link #JDBC_TYPE}/{@link #JDBC_HOST}/{@link #JDBC_PORT}/{@link #JDBC_DATABASE}/
     * {@link #JDBC_PATH}) are present only when applicable. All {@code ;}- or {@code ?&}-delimited
     * settings go into a nested {@link #JDBC_PARAMS} map, keys verbatim (e.g. {@code CIPHER}, {@code MODE},
     * {@code DB_CLOSE_DELAY}, {@code ssl}).
     *
     * @throws IllegalArgumentException if {@code jdbcURL} is null or does not start with {@code jdbc:}.
     */
    public static NVGenericMap parseJdbcURL(String jdbcURL) {
        if (jdbcURL == null || !jdbcURL.regionMatches(true, 0, JDBC_PREFIX, 0, JDBC_PREFIX.length())) {
            throw new IllegalArgumentException("Not a JDBC URL: " + jdbcURL);
        }
        NVGenericMap ret = new NVGenericMap("jdbc-url");
        ret.build(JDBC_URL, jdbcURL);

        // subprotocol = the token between the first two ':' (jdbc:<subprotocol>:<subname>)
        String rest = jdbcURL.substring(JDBC_PREFIX.length());
        int c = rest.indexOf(':');
        String subprotocol = c >= 0 ? rest.substring(0, c) : rest;
        ret.build(JDBC_SUBPROTOCOL, subprotocol.toLowerCase());
        String subname = c >= 0 ? rest.substring(c + 1) : "";

        // Split off the settings section: ';' (H2 style) or '?...&' (query style), whichever comes first.
        int semi = subname.indexOf(';');
        int q = subname.indexOf('?');
        int cut = -1;
        boolean queryStyle = false;
        if (semi >= 0 && (q < 0 || semi < q)) {
            cut = semi;
        } else if (q >= 0) {
            cut = q;
            queryStyle = true;
        }
        String base = cut >= 0 ? subname.substring(0, cut) : subname;
        String paramPart = cut >= 0 ? subname.substring(cut + 1) : null;

        if ("h2".equalsIgnoreCase(subprotocol)) {
            parseH2Base(ret, base);
        } else {
            parseNetworkOrDbBase(ret, base);
        }

        if (paramPart != null && !paramPart.isEmpty()) {
            NVGenericMap params = new NVGenericMap(JDBC_PARAMS);
            for (String tok : paramPart.split(queryStyle ? "&" : ";", -1)) {
                if (tok.isEmpty()) {
                    continue;
                }
                int eq = tok.indexOf('=');
                if (eq >= 0) {
                    params.build(tok.substring(0, eq), tok.substring(eq + 1));
                } else {
                    params.build(tok, "");
                }
            }
            ret.build(params);
        }
        return ret;
    }

    /** H2 subname: {@code mem:<db>} | {@code file:<path>} | {@code tcp://<host>:<port>/<db>} | bare {@code <path>}. */
    private static void parseH2Base(NVGenericMap ret, String base) {
        int c = base.indexOf(':');
        String subtype = c >= 0 ? base.substring(0, c) : null;
        if (subtype != null && (subtype.equalsIgnoreCase("mem") || subtype.equalsIgnoreCase("file")
                || subtype.equalsIgnoreCase("tcp") || subtype.equalsIgnoreCase("ssl")
                || subtype.equalsIgnoreCase("zip") || subtype.equalsIgnoreCase("nio")
                || subtype.equalsIgnoreCase("nioMapped"))) {
            ret.build(JDBC_TYPE, subtype.toLowerCase());
            String remainder = base.substring(c + 1);
            if (subtype.equalsIgnoreCase("tcp") || subtype.equalsIgnoreCase("ssl")) {
                parseNetworkOrDbBase(ret, remainder); // remainder is //host:port/db
            } else if (subtype.equalsIgnoreCase("mem")) {
                if (!remainder.isEmpty()) ret.build(JDBC_DATABASE, remainder);
            } else if (!remainder.isEmpty()) {
                ret.build(JDBC_PATH, remainder); // file / zip / nio -> a filesystem path
            }
        } else if (!base.isEmpty()) {
            ret.build(JDBC_PATH, base); // bare path form: jdbc:h2:~/test or jdbc:h2:./data/db
        }
    }

    /** Network authority ({@code //host[:port][,host2:port2]/db}) or a db-only subname. */
    private static void parseNetworkOrDbBase(NVGenericMap ret, String base) {
        if (!base.startsWith("//")) {
            if (!base.isEmpty()) ret.build(JDBC_DATABASE, base); // e.g. jdbc:postgresql:mydb
            return;
        }
        String s = base.substring(2);
        int slash = s.indexOf('/');
        String authority = slash >= 0 ? s.substring(0, slash) : s;
        String db = slash >= 0 ? s.substring(slash + 1) : "";
        if (!db.isEmpty()) ret.build(JDBC_DATABASE, db);
        if (authority.isEmpty()) {
            return;
        }
        if (authority.indexOf(',') >= 0) {
            ret.build(JDBC_HOST, authority); // multi-host: keep raw, don't split a single port
            return;
        }
        String host = authority;
        String port = null;
        if (authority.startsWith("[")) {                 // IPv6 literal: [::1]:5432
            int close = authority.indexOf(']');
            if (close > 0) {
                host = authority.substring(0, close + 1);
                int colon = authority.indexOf(':', close);
                if (colon >= 0) port = authority.substring(colon + 1);
            }
        } else {
            int colon = authority.lastIndexOf(':');
            if (colon >= 0) {
                host = authority.substring(0, colon);
                port = authority.substring(colon + 1);
            }
        }
        ret.build(JDBC_HOST, host);
        if (port != null && !port.isEmpty()) {
            try {
                ret.build(new NVInt(JDBC_PORT, Integer.parseInt(port.trim())));
            } catch (NumberFormatException e) {
                ret.build(JDBC_PORT, port);
            }
        }
    }


    /**
     * Build the default <b>encrypted</b> H2 file-DB URL from a directory and a database name:
     * {@code jdbc:h2:file:<location>/<dbName>;MODE=PostgreSQL;CIPHER=AES} (the {@link #DEFAULT_H2_URL}
     * template with {@link #LOCATION_TOK} replaced by {@code <location>/<dbName>}).
     *
     * <p>Because the URL ships {@code ;CIPHER=AES}, the resulting DB is encrypted — the caller must
     * supply a file password when opening it (e.g. via
     * {@code H2PDSCreator.toAPIConfigInfo(url, user, password, filePassword)}). {@code MODE=PostgreSQL}
     * keeps H2 speaking the same SQL dialect as the Postgres target.
     *
     * <p>Both arguments are trimmed ({@link SUS#trimOrNull}); {@code location} must be an <b>existing
     * directory</b> (the DB file is created inside it on first connection).
     *
     * @param location an existing directory that will hold the DB file (trimmed)
     * @param dbName   the database file base name, appended under {@code location} (trimmed)
     * @return the composed encrypted H2 file URL
     * @throws NullPointerException     if {@code location} or {@code dbName} is null or blank
     * @throws IllegalArgumentException if {@code location} is not an existing directory
     */
    public static String defaultH2JdbcURL(String location, String dbName) {
        location = SUS.trimOrNull(location);
        dbName = SUS.trimOrNull(dbName);
        SUS.checkIfNulls("location or db name can't be null", location, dbName);
        File file = new File(location);
        if (!file.isDirectory())
            throw new IllegalArgumentException(location + " is not a directory");
        String combo = location + "/" + dbName;
        return SharedStringUtil.embedText(DEFAULT_H2_URL, LOCATION_TOK, combo);
    }

}
