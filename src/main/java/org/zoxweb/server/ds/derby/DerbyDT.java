package org.zoxweb.server.ds.derby;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.zoxweb.shared.util.*;

public enum DerbyDT
  implements GetValue<String>
{
  ACCOUNT_ID("VARCHAR(64)", String.class, null),
  BIGINT("BIGINT", Long.class, NVLong.class),
  BLOB("BLOB", byte[].class, NVBlob.class),
  BOOLEAN("BOOLEAN", Boolean.class, NVBoolean.class),
  DOUBLE("DOUBLE", Double.class, NVDouble.class),
  ENUM("VARCHAR(256)", Enum.class, NVEnum.class),
  FLOAT("FLOAT", Float.class, NVFloat.class),
  GLOBAL_ID("VARCHAR(64) NOT NULL PRIMARY KEY", String.class, null),
  INNER_ARRAY("LONG VARCHAR", null, null),
  INTEGER("INTEGER", Integer.class, NVInt.class),
  LONG_VARCHAR("LONG VARCHAR", String.class, NVPair.class),
  OUTER_ARRAY("LONG VARCHAR", null, null),
  REMOTE_REFERENCE("LONG VARCHAR", null, null),
  TIMESTAMP("TIMESTAMP", Date.class, NVLong.class),
  USER_ID("VARCHAR(64)", String.class, null),


  ;
  
  private String dbType;
  private Class<?> javaClass;
  private Class<?> nvbClass;
  DerbyDT(String dbValue, Class<?> javaClass, Class<?> nvbClass)
  {
    this.dbType = dbValue;
    this.javaClass = javaClass;
    this.nvbClass = nvbClass;
  }
  
  
  public String getValue()
  {
    return dbType;
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


  public static void toDerbyValue(StringBuilder sb, NVBase<?> nvb)
  {
    Object value = nvb.getValue();
    if (value == null)
    {
      sb.append("NULL");
    }
    else if (value instanceof String)
    {
      sb.append("'" + value + "'");
    }
    else if(value instanceof Number)
    {
      sb.append(value);
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

  public static void mapValue(ResultSet rs, NVBase<Object> nvb) throws SQLException {
    Object value = rs.getObject(nvb.getName());
    nvb.setValue(value);
  }

   public <T> Class<T> getJavaClass()
   {
     return (Class<T>) javaClass;
   }

   public <T> Class<T> getNVBaseClass()
   {
     return (Class<T>)  nvbClass;
   }
  
}
