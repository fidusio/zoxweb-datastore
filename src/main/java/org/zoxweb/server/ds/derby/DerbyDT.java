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
  TIMESTAMP("TIMESTAMP", Types.BIGINT, Date.class, NVLong.class),
  USER_ID("VARCHAR(64)", Types.VARCHAR, String.class, null),


  ;


  public static final Set<String> META_EXCLUSION = new HashSet<String>(Arrays.asList(new String[] {MetaToken.REFERENCE_ID.getName()}));
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


  public static DerbyDT metaToDerbyDT(NVConfig nvc)
  {
    Class<?> nvcJavaClass = nvc.getMetaType();
    DerbyDT ddt = null;
    if (nvcJavaClass == Boolean.class)
    {
      ddt = BOOLEAN;
    }
    else if (nvcJavaClass == byte[].class)
    {
      ddt = BLOB;
      return ddt;
    }
    else if (nvcJavaClass == String.class)
    {
      if (nvc.getName().equals(MetaToken.GLOBAL_ID.getName()))
      {
        ddt = GLOBAL_ID;
      }
      else if (nvc.getName().equals(MetaToken.USER_ID.getName()))
      {
        ddt = USER_ID;
      }
      else if (nvc.getName().equals(MetaToken.ACCOUNT_ID.getName()))
      {
        ddt = ACCOUNT_ID;
      }
      else
      {
        ddt = LONG_VARCHAR;
      }
    }
    else if (nvcJavaClass == Integer.class)
    {
      ddt = INTEGER;
    }
    else if (nvcJavaClass == Long.class)
    {
      ddt = BIGINT;
    }
    else if (nvcJavaClass == Float.class)
    {
      ddt = FLOAT;
    }
    else if (nvcJavaClass == Double.class)
    {
      ddt = DOUBLE;
    }
    else if (nvcJavaClass == Date.class)
    {
      ddt = TIMESTAMP;
    }
    else if (nvcJavaClass == NVStringList.class)
    {
      ddt = STRING_LIST;
    }
    else if (Enum.class.isAssignableFrom(nvcJavaClass))
    {
      ddt = ENUM;
    }
    else if (ArrayValues.class.isAssignableFrom(nvcJavaClass))
    {
      ddt = INNER_ARRAY;
    }
    else if (NVEntity.class.isAssignableFrom(nvcJavaClass))
    {
      ddt = REMOTE_REFERENCE;
    }



    if (ddt != null)
    {
      if (nvc.isArray())
        return INNER_ARRAY;
      else
        return ddt;
    }


    throw new IllegalArgumentException("Type not found " + nvc);
  }


  public static void toDerbyValue(StringBuilder sb, NVBase<?> nvb) throws IOException {
    Object value = nvb.getValue();
    if (value == null)
    {
      sb.append("NULL");
    }
    else if (value instanceof String)
    {
      sb.append("'" + value + "'");
    }
    else if (value instanceof Enum)
    {
      sb.append("'" +  ((Enum) value).name()+ "'");
    }
    else if (value instanceof Boolean)
    {
      sb.append(value);
    }
    else if(value instanceof Number)
    {
      sb.append(value);
    }
    else if (value instanceof NVGenericMap)
    {
      String json = GSONUtil.toJSONGenericMap((NVGenericMap) value, false, false, true);
      sb.append("'");
      sb.append(json);
      sb.append("'");
    }
  }


  public static void toDerbyValue(PreparedStatement ps, int index, NVBase<?> nvb) throws IOException, SQLException {
    Object value = nvb.getValue();
    if (nvb.getValue() == null)
    {
      ps.setObject(index, null);
    }
    if (nvb instanceof NVBlob)
    {
      ps.setBinaryStream(index, new ByteArrayInputStream(((NVBlob)nvb).getValue()));
    }
    else if (nvb.getValue() instanceof String)
    {
      ps.setString(index, (String) nvb.getValue());
    }
    else if (nvb instanceof NVEnum)
    {
      ps.setString(index, ((Enum) nvb.getValue()).name());
    }
    else if (nvb instanceof NVBoolean)
    {
      ps.setBoolean(index, (Boolean) nvb.getValue());
    }
    else if(value instanceof Number)
    {
      ps.setObject(index, value, javaClassToSQLType(value.getClass()));
    }
    else if (nvb instanceof NVGenericMap)
    {
      String json = GSONUtil.toJSONGenericMap((NVGenericMap) nvb, false, false, true);
      ps.setString(index, json);
    }
    else if (nvb instanceof NVStringList)
    {
      NVGenericMap nvgm = new NVGenericMap();
      nvgm.add((GetNameValue<?>) nvb);
      ps.setString(index, GSONUtil.toJSONGenericMap(nvgm, false, false, false));
    }

  }

//  public static DerbyDT toType(Object obj)
//  {
//    for (DerbyDT ddt : DerbyDT.values())
//    {
//      for(Object o : ddt.types)
//      {
//        if (obj == o)
//          return ddt;
//      }
//    }
//    return null;
//  }

  public static void mapValue(ResultSet rs, NVConfig nvc, NVBase<?> nvb) throws SQLException, IOException {
    //Object value = rs.getObject(nvb.getName());
    if (nvb instanceof NVGenericMap)
    {
      NVGenericMap nvgm = GSONUtil.fromJSONGenericMap(rs.getString(nvb.getName()), null, null);
      ((NVGenericMap)nvb).setValue(nvgm.getValue());
    }
    else if (nvb instanceof NVStringList)
    {
      NVGenericMap nvgm = GSONUtil.fromJSONGenericMap(rs.getString(nvb.getName()), null, null);
      ((NVStringList)nvb).setValue(((NVStringList)nvgm.values()[0]).getValue());
    }
    else if (nvb instanceof NVEnum)
    {
      ((NVEnum)nvb).setValue(SharedUtil.enumValue(nvc.getMetaType(), rs.getString(nvb.getName())));
    }
    else if(nvb instanceof NVBlob)
    {
      Blob b = rs.getBlob(nvb.getName());
      InputStream is = b.getBinaryStream();
      UByteArrayOutputStream baos = new UByteArrayOutputStream();
      IOUtil.relayStreams(is, baos, true);
      ((NVBlob)nvb).setValue(baos.toByteArray());
    }
    else if (nvb instanceof NVFloat)
    {
      ((NVFloat)nvb).setValue(rs.getFloat(nvb.getName()));
    }
    else if (nvb instanceof NVDouble)
    {
      ((NVDouble)nvb).setValue(rs.getDouble(nvb.getName()));
    }
    else if (nvb instanceof NVLong)
    {
      ((NVLong)nvb).setValue(rs.getLong(nvb.getName()));
    }
    else if (nvb instanceof NVInt)
    {
      ((NVInt)nvb).setValue(rs.getInt(nvb.getName()));
    }
    else if (nvb instanceof NVBoolean)
    {
      ((NVBoolean)nvb).setValue(rs.getBoolean(nvb.getName()));
    }
    else
      ((NVBase<Object>)nvb).setValue(rs.getObject(nvb.getName()));
  }

   public <T> Class<T> getJavaClass()
   {
     return (Class<T>) javaClass;
   }

   public <T> Class<T> getNVBaseClass()
   {
     return (Class<T>)  nvbClass;
   }


  public static boolean excludeMeta(NVBase<?> nvb)
  {
    return excludeMeta(nvb.getName());
  }

  public static boolean excludeMeta(NVConfig nvc)
  {
    return excludeMeta(nvc.getName());
  }
   public static boolean excludeMeta(String name)
   {
     return META_EXCLUSION.contains(name);
   }
  
}
