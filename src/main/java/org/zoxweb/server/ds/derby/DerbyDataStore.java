package org.zoxweb.server.ds.derby;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;


import org.zoxweb.server.ds.derby.DerbyDataStoreCreator.DerbyParam;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.util.IDGeneratorUtil;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIExceptionHandler;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.data.LongSequence;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.util.*;

@SuppressWarnings("serial")
public class DerbyDataStore implements APIDataStore<Connection> {

  private volatile APIConfigInfo apiConfig = null;
  private volatile boolean driverLoaded = false;
  private volatile Set<Connection> connections = new HashSet<Connection>();

  public static final String CACHE_DATA_TB  = "CACHE_DATA_TB";
  private Lock lock = new ReentrantLock();

  private static transient  Logger log = Logger.getLogger(DerbyDataStore.class.getName());
  private volatile HashMap<String, NVConfigEntity> metaTables = new HashMap<String, NVConfigEntity>();




  class DerbyDBData{
    final StringBuilder columns = new StringBuilder();
    final StringBuilder values = new StringBuilder();
    final StringBuilder genericValues = new StringBuilder();
    int genericValuesCount = 0;
  }
  public DerbyDataStore()
  {
    
  }
  
  
  public DerbyDataStore(APIConfigInfo configInfo)
  {
    setAPIConfigInfo(configInfo);
    init();
  }


  private void init()
  {
    Connection con = null;
    Statement stmt = null;
    try {
      con = connect();
      stmt = con.createStatement();
      if (!doesTableExists(CACHE_DATA_TB))
      stmt.execute("create table " + CACHE_DATA_TB + " (GLOBAL_ID VARCHAR(64), CANONICAL_ID VARCHAR(1024), DATA_TXT LONG VARCHAR, PRIMARY KEY(GLOBAL_ID))");
    }
    catch(Exception e)
    {

    }
    finally {
      close(con, stmt);
    }
  }


  public boolean createTable(NVConfigEntity nvce) throws SQLException {

    if (!doesTableExists(nvce))
    {


      Connection con = null;
      Statement stmt = null;
      try {
        lock.lock();
        String tableName = nvce.getName().toUpperCase();
        // to prevent double penetration
        if (!doesTableExists(nvce)) {
          // create table
          StringBuilder sb = new StringBuilder();//new StringBuilder("CREATE TABLE "+ tableName +" (");

          for (NVConfig nvc : nvce.getAttributes())
          {
            // skip referenceID
            if (!DerbyDT.excludeMeta(DerbyDT.META_INSERT_EXCLUSION, nvc))
            {
              if (nvc instanceof NVConfigEntity)
                createTable((NVConfigEntity)nvc);
              DerbyDT derbyDT = DerbyDT.metaToDerbyDT(nvc);
              if (derbyDT != null)
              {
                if(sb.length() > 0)
                {
                  sb.append(',');
                }
                sb.append(nvc.getName() + " " + derbyDT.getValue());
                if (nvc.isUnique())
                {
                  sb.append(" UNIQUE");
                }
              }
            }
          }

          sb.insert(0, "CREATE TABLE " + tableName + " (");
          sb.append(')');
          String createTable = sb.toString();

          log.info(createTable);
          con = connect();
          stmt = con.createStatement();
          stmt.execute(createTable);





          boolean ret =  doesTableExists(nvce);
          log.info("Table: " + tableName + " created " + ret);
          return ret;
        }
      } finally {
        lock.unlock();
        close(con, stmt);
      }
    }
    return false;
  }


  private boolean doesTableExists(NVConfigEntity nvce) throws SQLException {
    String tableName = nvce.getName().toUpperCase();
    if (metaTables.get(tableName) != null)
    {
      return true;
    }

    synchronized (metaTables) {
      if (doesTableExists(tableName)) {
        // this will occur once at start up after the table creation
        metaTables.put(tableName, nvce);
        return true;
      }
    }
    return false;
  }

  public boolean doesTableExists(String sTablename)
          throws SQLException
  {

    sTablename = sTablename.toUpperCase();
    if (metaTables.get(sTablename) != null)
    {
      return true;
    }
    synchronized (metaTables) {
      if (metaTables.get(sTablename) != null)
      {
        return true;
      }
      Connection con = null;
      ResultSet rs = null;
      try {
        con = connect();

        DatabaseMetaData dbmd = con.getMetaData();
        rs = dbmd.getTables(null, null, sTablename, null);
        if (rs.next()) {
          log.info("Table " + rs.getString("TABLE_NAME") + "already exists !!");
          return true;
        } else {
          log.info("Table " + sTablename + " do not exist.");
          return false;
        }
      } finally {
        close(rs, con);
      }
    }
  }


  private void close(AutoCloseable ...autoCloseables)
  {
    for (AutoCloseable au : autoCloseables) {
      if(au != null) {
        if (au instanceof Connection)
          synchronized (connections) {
            connections.remove(au);
          }
        IOUtil.close(au);
      }
    }
  }

  @Override
  public APIConfigInfo getAPIConfigInfo() {
    // TODO Auto-generated method stub
    return apiConfig;
  }

  @Override
  public void setAPIConfigInfo(APIConfigInfo configInfo) {
    // TODO Auto-generated method stub
    apiConfig = configInfo;
  }

  @Override
  public Connection connect() throws APIException {
    // TODO Auto-generated method stub
    if (!driverLoaded)
    {

      try
      {
        lock.lock();
        if(!driverLoaded)
        {
          SharedUtil.checkIfNulls("Configuration null", getAPIConfigInfo());
          
          String driverClassName = getAPIConfigInfo().getProperties().getValue(DerbyParam.DRIVER);
          try 
          {
            Class.forName(driverClassName);
            driverLoaded = true;
          } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new APIException("Driver not loaded");
          }
        }
      }
      finally {
        lock.unlock();
      }
    }
    return newConnection();
  }

  @Override
  public Connection newConnection() throws APIException {
    // TODO Auto-generated method stub
    try 
    {
      synchronized(connections)
      {
        Connection ret =  DriverManager.getConnection(getAPIConfigInfo().getProperties().getValue(DerbyParam.URL),

                                         getAPIConfigInfo().getProperties().getValue(DerbyParam.USER),
                                         getAPIConfigInfo().getProperties().getValue(DerbyParam.PASSWORD));
        
        connections.add(ret);
        return ret;
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    throw new APIException("Connection failed");
  }

  @Override
  public void close() throws APIException {
    // TODO Auto-generated method stub
    synchronized (connections) {
      connections.stream().forEach(c -> IOUtil.close(c));
      connections.clear();

      try {
        DriverManager.getConnection(getAPIConfigInfo().getProperties().getValue(DerbyParam.URL) +";shutdown=true",

                getAPIConfigInfo().getProperties().getValue(DerbyParam.USER),
                getAPIConfigInfo().getProperties().getValue(DerbyParam.PASSWORD));
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public boolean isProviderActive() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public APIExceptionHandler getAPIExceptionHandler() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setAPIExceptionHandler(APIExceptionHandler exceptionHandler) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public <T> T lookupProperty(GetName propertyName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long lastTimeAccessed() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long inactivityDuration() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isBusy() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setDescription(String str) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getDescription() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setName(String name) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toCanonicalID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getStoreName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<String> getStoreTables() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames,
      QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> search(String className, List<String> fieldNames,
      QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> APISearchResult<T> batchSearch(String className, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> results,
      int startIndex, int batchSize)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> userSearch(String userID, NVConfigEntity nvce,
      List<String> fieldNames, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> userSearch(String userID, String className,
      List<String> fieldNames, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return searchByID(nvce.getMetaType().getName(), ids);
  }

  @Override
  public <V extends NVEntity> List<V> searchByID(String className, String... ids)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    Connection con = null;
    Statement stmt = null;
    ResultSet rs = null;
    NVEntity retType = null;
    List<NVEntity> ret = new ArrayList<NVEntity>();
    try
    {
      retType = (NVEntity) Class.forName(className).newInstance();
      NVConfigEntity nvce = (NVConfigEntity) retType.getNVConfig();


      for (int i = 0; i < ids.length; i++)
      {
        if (ids[i] != null)
          ids[i] = "'" + ids[i] +"'";
      }
      con = connect();
      stmt = con.createStatement();
      String select = "SELECT * FROM " + retType.getNVConfig().getName() +
              " WHERE GLOBAL_ID IN(" + SharedUtil.toCanonicalID(',', ids)+ ")" ;
      rs = stmt.executeQuery(select);
      while(rs.next())
      {
        for (NVBase<?> nvb : retType.getAttributes().values())
        {
          if(!DerbyDT.excludeMeta(DerbyDT.META_INSERT_EXCLUSION, nvb)) {
            if (nvb instanceof NVEntityReference)
            {
              if (rs.getString(nvb.getName()) != null)
              {
                String values[] = rs.getString(nvb.getName()).split(":");
                String tableName = values[0];
                String globalID = values[1];
                List<NVEntity> innerValues = searchByID((NVConfigEntity) nvce.lookup(nvb.getName()), globalID);
                if(innerValues.size() == 1)
                {
                  ((NVEntityReference)nvb).setValue(innerValues.get(0));
                }
              }
            }
            else {
              DerbyDT.mapValue(rs, nvce.lookup(nvb.getName()), nvb);
            }
          }
        }
        ret.add(retType);
        retType = (NVEntity) Class.forName(className).newInstance();
      }

    }
    catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e)
    {
      e.printStackTrace();
      throw new APIException(e.getMessage());
    }
    finally {
      close(con, stmt, rs);
    }



    return (List<V>) ret;
  }

  @Override
  public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce,
      String... ids)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> V insert(V nve)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    Connection con = null;
    PreparedStatement stmt = null;
    try {


      createTable((NVConfigEntity) nve.getNVConfig());
      con = connect();
      if(SharedStringUtil.isEmpty(nve.getGlobalID()))
      {
        nve.setGlobalID(UUID.randomUUID().toString());
      }
      else if (isRefCreated(con, nve.getNVConfig().getName(), nve.getGlobalID()))
      {
        // already exit we must update
        log.info("invoke update for " + nve.getGlobalID());
        return update(nve);
      }



      DerbyDBData ddbd = formatInsertStatement(nve, false);

      String statementToken = "INSERT INTO " + nve.getNVConfig().getName() + " VALUES (" + ddbd.genericValues.toString() + ")";
      log.info(statementToken);
      log.info("parameter count " + ddbd.genericValuesCount);


      stmt = con.prepareStatement(statementToken);
      int index = 0;
      for(NVBase<?> nvb : nve.getAttributes().values()) {
        if(!DerbyDT.excludeMeta(DerbyDT.META_INSERT_EXCLUSION, nvb)) {
          if (nvb instanceof NVEntityReference) {
            NVEntity innerNVE = insert(((NVEntityReference) nvb).getValue());
            ((NVEntityReference) nvb).setValue(innerNVE);
          }
          DerbyDT.toDerbyValue(stmt, ++index, nvb);
        }
      }


      stmt.execute();


    }
    catch (SQLException | IOException e)
    {
      e.printStackTrace();
      throw new APIException(e.getMessage());
    }
    finally {
      close(con, stmt);
    }
    // TODO Auto-generated method stub
    return nve;
  }


  private DerbyDBData formatInsertStatement(NVEntity nve, boolean nullsAllowed) throws IOException {
    DerbyDBData ret = new DerbyDBData();

    int index = 0;
    for(NVBase<?> nvb : nve.getAttributes().values())
    {
      if (!DerbyDT.excludeMeta(DerbyDT.META_INSERT_EXCLUSION, nvb)) {
        if (ret.genericValues.length() > 0) {
          ret.genericValues.append(", ");
        }
        ret.genericValues.append('?');
        index++;
        if (nullsAllowed || nvb.getValue() != null) {
          if (ret.columns.length() > 0) {
            ret.columns.append(',');
          }
          ret.columns.append(nvb.getName());
          if (ret.values.length() > 0) {
            ret.values.append(',');
          }
          DerbyDT.toDerbyValue(ret.values, nvb, false);
        }
      }
    }
    ret.genericValuesCount = index;
    return ret;
  }


//  private DerbyDBData formatUpdateStatement(NVEntity nve) throws IOException {
//    DerbyDBData ret = new DerbyDBData();
//
//    int index = 0;
//    for(NVBase<?> nvb : nve.getAttributes().values())
//    {
//      if (!DerbyDT.excludeMeta(nvb)) {
//        if (ret.genericValues.length() > 0) {
//          ret.genericValues.append(", ");
//        }
//        ret.genericValues.append('?');
//        index++;
//        if (nullsAllowed || nvb.getValue() != null) {
//          if (ret.columns.length() > 0) {
//            ret.columns.append(',');
//          }
//          ret.columns.append(nvb.getName());
//          if (ret.values.length() > 0) {
//            ret.values.append(',');
//          }
//          DerbyDT.toDerbyValue(ret.values, nvb);
//        }
//      }
//    }
//    ret.genericValuesCount = index;
//    return ret;
//  }

  @Override
  public <V extends NVEntity> boolean delete(V nve, boolean withReference)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto generated method stub
    String sql = "DELETE FROM " + nve.getNVConfig().getName() + " WHERE GLOBAL_ID = '" + nve.getGlobalID() + "'";
    Connection con = null;
    Statement stmt = null;
    try
    {
      con = connect();
      stmt = con.createStatement();
      return stmt.execute(sql);

    } catch (SQLException e) {
      throw new APIException(e.getMessage());
    } finally {
      close(stmt, con);
    }

  }

  @Override
  public <V extends NVEntity> boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <V extends NVEntity> V update(V nve)
      throws NullPointerException, IllegalArgumentException, APIException {
    SharedUtil.checkIfNulls("Can't update null nve", nve);
    Connection con = null;
    Statement stmt = null;
    try {


      createTable((NVConfigEntity) nve.getNVConfig());
      con = connect();
      if(SharedStringUtil.isEmpty(nve.getGlobalID()) || !isRefCreated(con, nve.getNVConfig().getName(), nve.getGlobalID()))
      {
       return insert(nve);
      }

      // update table table_name set nvb.name = nvb.value ... where global_id = 'nvb.GLOBAL_ID'

      StringBuilder values = new StringBuilder();
      for(NVBase<?> nvb : nve.getAttributes().values())
      {
        if (!DerbyDT.excludeMeta(DerbyDT.META_UPDATE_EXCLUSION, nvb))
        {
          if(nvb instanceof NVEntityReference)
          {
            NVEntity innerNVE = update(nve);
            ((NVEntityReference) nvb).setValue(innerNVE);
          }
          if(values.length() > 0)
          {
            values.append(", ");
          }
          DerbyDT.toDerbyValue(values, nvb, true);
        }
      }
      String updateStatement = "UPDATE " + nve.getNVConfig().getName() + " set " + values.toString() + " WHERE GLOBAL_ID='" + nve.getGlobalID() + "'";
      log.info(updateStatement);
      stmt = con.createStatement();
      stmt.executeUpdate(updateStatement);



//      DerbyDBData ddbd = formatInsertStatement(nve, false);
//
//      String statementToken = "INSERT INTO " + nve.getNVConfig().getName() + " VALUES (" + ddbd.genericValues.toString() + ")";
//      log.info(statementToken);
//      log.info("parameter count " + ddbd.genericValuesCount);
//
//
//      stmt = con.prepareStatement(statementToken);
//      int index = 0;
//      for(NVBase<?> nvb : nve.getAttributes().values()) {
//        if(!DerbyDT.excludeMeta(nvb)) {
//          if (nvb instanceof NVEntityReference) {
//            NVEntity innerNVE = insert(((NVEntityReference) nvb).getValue());
//            ((NVEntityReference) nvb).setValue(innerNVE);
//          }
//          DerbyDT.toDerbyValue(stmt, ++index, nvb);
//        }
//      }
//
//
//      stmt.execute();


    }
    catch (SQLException | IOException e)
    {
      e.printStackTrace();
      throw new APIException(e.getMessage());
    }
    finally {
      close(con, stmt);
    }
    // TODO Auto-generated method stub
    return nve;
  }

  @Override
  public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateRefOnly,
      boolean includeParam, String... nvConfigNames)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long countMatch(NVConfigEntity nvce, QueryMarker... queryCriteria)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DynamicEnumMap searchDynamicEnumMapByName(String name)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteDynamicEnumMap(String name)
      throws NullPointerException, IllegalArgumentException, APIException {
    // TODO Auto-generated method stub
    
  }

  private boolean isRefCreated(Connection con, String tableName, String globalID) throws SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = con.createStatement();
      rs = stmt.executeQuery("select GLOBAL_ID from " + tableName + " where GLOBAL_ID='" + globalID + "'");
      if (rs.next()) {
        return true;
      }

    }
    finally {
      // Do NOT close  con the connection
      close(rs, stmt);
    }
    return false;


  }


  @SuppressWarnings("unchecked")
  @Override
  public IDGenerator<String, UUID> getIDGenerator() {
    // TODO Auto-generated method stub
    return IDGeneratorUtil.UUIDV4;
  }

  @Override
  public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {

    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LongSequence createSequence(String sequenceName)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LongSequence createSequence(String sequenceName, long startValue, long defaultIncrement)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteSequence(String sequenceName)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long currentSequenceValue(String sequenceName)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long nextSequenceValue(String sequenceName)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long nextSequenceValue(String sequenceName, long increment)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isValidReferenceID(String refID) {
    // TODO Auto-generated method stub
    return false;
  }

}
