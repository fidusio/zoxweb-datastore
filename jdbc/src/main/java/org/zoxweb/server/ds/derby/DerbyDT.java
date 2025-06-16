package org.zoxweb.server.ds.derby;


import org.zoxweb.shared.util.*;

import java.sql.Types;
import java.util.Date;


public enum DerbyDT
        implements GetValue<String> {
    ACCOUNT_ID("VARCHAR(64)", Types.VARCHAR, String.class, null),
    BIGINT("BIGINT", Types.BIGINT, Long.class, NVLong.class),
    BLOB("BLOB", Types.BLOB, byte[].class, NVBlob.class),
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, NVBoolean.class),
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, NVDouble.class),
    ENUM("VARCHAR(256)", Types.VARCHAR, Enum.class, NVEnum.class),
    FLOAT("FLOAT", Types.FLOAT, Float.class, NVFloat.class),
    GLOBAL_ID("VARCHAR(64) NOT NULL PRIMARY KEY", Types.VARCHAR, String.class, null),
    INNER_ARRAY("LONG VARCHAR", Types.LONGNVARCHAR, null, null),
    INTEGER("INTEGER", Types.INTEGER, Integer.class, NVInt.class),
    //LONG_VARCHAR("LONG VARCHAR", Types.LONGNVARCHAR, String.class, NVPair.class),
    K4_VARCHAR("VARCHAR(4096)", Types.LONGNVARCHAR, String.class, NVPair.class),
    OUTER_ARRAY("LONG VARCHAR", Types.LONGNVARCHAR, null, null),
    REMOTE_REFERENCE("VARCHAR(512)", Types.LONGNVARCHAR, null, null),
    STRING_LIST("LONG VARCHAR", Types.LONGNVARCHAR, null, NVStringList.class),
    TIMESTAMP("BIGINT", Types.BIGINT, Date.class, NVLong.class),
    SUBJECT_GUID("VARCHAR(64)", Types.VARCHAR, String.class, null),
    SUBJECT_ID("VARCHAR(256)", Types.VARCHAR, String.class, null),
    NUMBER("VARCHAR(256)", Types.NUMERIC, Number.class, NVNumber.class),


    ;


    private String dbType;
    private int sqlType;
    private Class<?> javaClass;
    private Class<?> nvbClass;

    DerbyDT(String dbValue, int type, Class<?> javaClass, Class<?> nvbClass) {
        this.dbType = dbValue;
        this.javaClass = javaClass;
        this.nvbClass = nvbClass;
        this.sqlType = type;
    }


    public String getValue() {
        return dbType;
    }

    public int getSQLType() {
        return sqlType;
    }

    public static int javaClassToSQLType(Class<?> clazz) {
        SUS.checkIfNulls("Null class is not allowed", clazz);
        for (DerbyDT ddt : DerbyDT.values()) {
            if (ddt.getJavaClass() == clazz) {
                return ddt.getSQLType();
            }
        }

        throw new IllegalArgumentException("class " + clazz.getName() + " not supported");
    }


    @SuppressWarnings("unchecked")
    public <T> Class<T> getJavaClass() {
        return (Class<T>) javaClass;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getNVBaseClass() {
        return (Class<T>) nvbClass;
    }


}
