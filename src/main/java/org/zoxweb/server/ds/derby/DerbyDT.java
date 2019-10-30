package org.zoxweb.server.ds.derby;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.io.UByteArrayOutputStream;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.util.*;


public enum DerbyDT
  implements GetValue<String>
{
  ACCOUNT_ID("VARCHAR(64)", Types.VARCHAR, String.class, null),
  BIGINT("BIGINT", Types.BIGINT, Long.class, NVLong.class),
  BLOB("BLOB", Types.BLOB, byte[].class, NVBlob.class),
  BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, NVBoolean.class),
  DOUBLE("DOUBLE", Types.DOUBLE,  Double.class, NVDouble.class),
  ENUM("VARCHAR(256)", Types.VARCHAR, Enum.class, NVEnum.class),
  FLOAT("FLOAT", Types.FLOAT, Float.class, NVFloat.class),
  GLOBAL_ID("VARCHAR(64) NOT NULL PRIMARY KEY", Types.VARCHAR, String.class, null),
  INNER_ARRAY("LONG VARCHAR", Types.LONGNVARCHAR,null, null),
  INTEGER("INTEGER", Types.INTEGER, Integer.class, NVInt.class),
  LONG_VARCHAR("LONG VARCHAR", Types.LONGNVARCHAR, String.class, NVPair.class),
  OUTER_ARRAY("LONG VARCHAR", Types.LONGNVARCHAR,null, null),
  REMOTE_REFERENCE("LONG VARCHAR", Types.LONGNVARCHAR,null, null),
  STRING_LIST("LONG VARCHAR", Types.LONGNVARCHAR, null, NVStringList.class),
  TIMESTAMP("BIGINT", Types.BIGINT, Date.class, NVLong.class),
  USER_ID("VARCHAR(64)", Types.VARCHAR, String.class, null),


  ;


  public static final Set<String> META_INSERT_EXCLUSION = new HashSet<String>(Arrays.asList(new String[] {MetaToken.REFERENCE_ID.getName()}));
  public static final Set<String> META_UPDATE_EXCLUSION = new HashSet<String>(Arrays.asList(new String[] {MetaToken.REFERENCE_ID.getName(), MetaToken.GLOBAL_ID.getName()}));
  private String dbType;
  private int sqlType;
  private Class<?> javaClass;
  private Class<?> nvbClass;
  DerbyDT(String dbValue, int type, Class<?> javaClass, Class<?> nvbClass)
  {
    this.dbType = dbValue;
    this.javaClass = javaClass;
    this.nvbClass = nvbClass;
    this.sqlType = type;
  }
  
  
  public String getValue()
  {
    return dbType;
  }
  public int getSQLType()
  {
    return sqlType;
  }

  public static int javaClassToSQLType(Class<?> clazz)
  {
    SharedUtil.checkIfNulls("Null class is not allowed", clazz);
    for(DerbyDT ddt: DerbyDT.values()){
      if(ddt.getJavaClass() == clazz)
      {
        return ddt.getSQLType();
      }
    }

    throw new IllegalArgumentException("class " + clazz.getName() + " not supported");
  }


   @SuppressWarnings("unchecked")
  public <T> Class<T> getJavaClass()
   {
     return (Class<T>) javaClass;
   }

   @SuppressWarnings("unchecked")
  public <T> Class<T> getNVBaseClass()
   {
     return (Class<T>)  nvbClass;
   }


  public static boolean excludeMeta(Set<String> exclusion, NVBase<?> nvb)
  {
    return excludeMeta(exclusion, nvb.getName());
  }

  public static boolean excludeMeta(Set<String> exclusion, NVConfig nvc)
  {
    return excludeMeta(exclusion, nvc.getName());
  }
   public static boolean excludeMeta(Set<String> exclusion, String name)
   {
     return exclusion.contains(name);
   }
  
}
