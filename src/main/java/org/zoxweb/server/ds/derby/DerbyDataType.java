package org.zoxweb.server.ds.derby;

import java.util.Date;
import org.zoxweb.shared.util.NVBoolean;

public enum DerbyDataType
{
  BOOLEAN("BOOLEAN", Boolean.class, NVBoolean.class),
  BLOB("BLOB", byte[].class),
  LONG_VARCHAR("LONG VARCHAR", String.class),
  INTEGER("INTEGER", Integer.class),
  BIGINT("BIGINT", Long.class),
  FLOAT("FLOAT", Float.class),
  DOUBLE("DOUBLE", Double.class),
  TIMESTAMP("TIMESTAMP", Date.class),
  
  
  
  
  
  
  ;
  
  private String dbType;
  private Object types;
  DerbyDataType(String dbName, Object ...types)
  {
    this.dbType = dbName;
    this.types = types;
  }
  
  
  public String getDBType()
  {
    return dbType;
  }
  
  
  

  
}
