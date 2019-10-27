package org.zoxweb.server.ds.derby;

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
import org.zoxweb.shared.data.StatInfo;
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
            if (!MetaToken.REFERENCE_ID.getName().equals(nvc.getName()))
            {
              DerbyDT derbyDT = DerbyDT.metaToDerbyDT(nvc);
              if (derbyDT != null)
              {
                if(sb.length() > 0)
                {
                  sb.append(',');
                }
                sb.append(nvc.getName() + " " + derbyDT.getValue());
              }
            }
          }

          //sb.append(" PRIMARY KEY(" + MetaToken.GLOBAL_ID.getName()+ ")");
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
          if(!nvb.getName().equals(MetaToken.REFERENCE_ID.getName()))
            DerbyDT.mapValue(rs, (NVBase<Object>) nvb);
        }
        ret.add(retType);
        retType = (NVEntity) Class.forName(className).newInstance();
      }

    }
    catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException e)
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
    Statement stmt = null;
    try {


      createTable((NVConfigEntity) nve.getNVConfig());
      if(SharedStringUtil.isEmpty(nve.getGlobalID()))
      {
        nve.setGlobalID(UUID.randomUUID().toString());
      }

      DerbyDBData ddbd = formatStatement(nve, false);
      String statementToken = "INSERT INTO " + nve.getNVConfig().getName() + " (" + ddbd.columns.toString() +
              ") VALUES (" + ddbd.values.toString() + ")";

      log.info(statementToken);

      con = connect();
      stmt = con.createStatement();
      stmt.execute(statementToken);


    }
    catch (SQLException e)
    {
      throw new APIException(e.getMessage());
    }
    finally {
      close(con, stmt);
    }
    // TODO Auto-generated method stub
    return nve;
  }


  private DerbyDBData formatStatement(NVEntity nve, boolean nullsAllowed)
  {
    DerbyDBData ret = new DerbyDBData();

    for(NVBase<?> nvb : nve.getAttributes().values())
    {
      if (nullsAllowed ||  nvb.getValue() != null)
      {
        if (ret.columns.length() > 0)
        {
          ret.columns.append(',');
        }
        ret.columns.append(nvb.getName());
        if (ret.values.length() > 0)
        {
          ret.values.append(',');
        }
        DerbyDT.toDerbyValue(ret.values, nvb);
      }
    }

    return ret;
  }

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
    // TODO Auto-generated method stub
    return null;
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
