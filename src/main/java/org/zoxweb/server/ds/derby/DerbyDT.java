package org.zoxweb.server.ds.derby;

import java.util.Date;

import org.zoxweb.shared.util.*;

public enum DerbyDT
{
  BOOLEAN("BOOLEAN", Boolean.class, NVBoolean.class),
  BLOB("BLOB", byte[].class, NVBlob.class),
  LONG_VARCHAR("LONG VARCHAR", String.class),
  INTEGER("INTEGER", Integer.class, NVInt.class),
  BIGINT("BIGINT", Long.class, NVLong.class),
  FLOAT("FLOAT", Float.class, NVFloat.class),
  DOUBLE("DOUBLE", Double.class, NVDouble.class),
  TIMESTAMP("TIMESTAMP", Date.class),
  
  
  
  
  
  
  ;
  
  private String dbType;
  private Object types[];
  DerbyDT(String dbName, Object ...types)
  {
    this.dbType = dbName;
    this.types = types;
  }
  
  
  public String getDBType()
  {
    return dbType;
  }


  public static DerbyDT toType(Object obj)
  {
    for (DerbyDT ddt : DerbyDT.values())
    {
      for(Object o : ddt.types)
      {
        if (obj == o)
          return ddt;
      }
    }
    return null;
  }

  
}
