package org.zoxweb.server.ds.derby;

import org.zoxweb.server.ds.derby.DerbyDataStoreCreator.DerbyParam;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.util.IDGs;
import org.zoxweb.server.util.MetaUtil;
import org.zoxweb.shared.api.*;
import org.zoxweb.shared.data.LongSequence;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.db.QueryMatch;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.util.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("serial")
public class DerbyDataStore implements APIDataStore<Connection, Connection> {

    private volatile APIConfigInfo apiConfig = null;
    private volatile boolean driverLoaded = false;
    private final Set<Connection> connections = new HashSet<Connection>();

    public static final String CACHE_DATA_TB = "CACHE_DATA_TB";
    private final Lock lock = new ReentrantLock();

    public static LogWrapper log = new LogWrapper(DerbyDataStore.class);
    private final Map<String, NVConfigEntity> metaTables = new HashMap<String, NVConfigEntity>();
    private final Map<String, DerbyDBData> ddbdCache = new HashMap<String, DerbyDBData>();


    class DerbyDBData {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        String insertStatement = null;
        int genericValuesCount = 0;
    }

    public DerbyDataStore() {

    }


    public DerbyDataStore(APIConfigInfo configInfo) {
        setAPIConfigInfo(configInfo);
        init();
    }


    private void init() {
        Connection con = null;
        Statement stmt = null;
        try {
            con = connect();
//      stmt = con.createStatement();
//      if (!doesTableExists(CACHE_DATA_TB))
//      stmt.execute("create table " + CACHE_DATA_TB + " (GUID VARCHAR(64), CANONICAL_ID VARCHAR(1024), DATA_TXT LONG VARCHAR, PRIMARY KEY(GUID))");
        } catch (Exception e) {

        } finally {
            close(con, stmt);
        }
    }


//  public DataSource getDataSource()
//  {
//
//  }


    public boolean createTable(NVConfigEntity nvce) throws SQLException {

        if (!doesTableExists(nvce)) {


            Connection con = null;
            Statement stmt = null;
            try {
                lock.lock();
                String tableName = nvce.getName().toUpperCase();
                // to prevent double penetration
                if (!doesTableExists(nvce)) {
                    // create table
                    StringBuilder sb = new StringBuilder();//new StringBuilder("CREATE TABLE "+ tableName +" (");

                    for (NVConfig nvc : nvce.getAttributes()) {
                        // skip referenceID
                        if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_INSERT_EXCLUSION, nvc)) {
                            if (nvc instanceof NVConfigEntity && ((NVConfigEntity) nvc).getArrayType() == NVConfigEntity.ArrayType.NOT_ARRAY) {
                                //log.getLogger().info("Table to be inserted: " + nvc + "\n" + nvc.getName() + ":" +  ((NVConfigEntity)nvc).getAttributes());


                                try {
                                    NVEntity toAdd = (NVEntity) nvc.getMetaType().getDeclaredConstructor().newInstance();

                                    createTable((NVConfigEntity) toAdd.getNVConfig());
                                } catch (InstantiationException e) {
                                    throw new RuntimeException(e);
                                } catch (IllegalAccessException e) {
                                    throw new RuntimeException(e);
                                } catch (InvocationTargetException e) {
                                    throw new RuntimeException(e);
                                } catch (NoSuchMethodException e) {
                                    throw new RuntimeException(e);
                                }

                            }
                            DerbyDT derbyDT = DerbyDBMeta.metaToDerbyDT(nvc);
                            if (derbyDT != null) {
                                if (sb.length() > 0) {
                                    sb.append(',');
                                }
                                sb.append(nvc.getName() + " " + derbyDT.getValue());
                                if (nvc.isUnique() && !nvc.getName().equalsIgnoreCase("guid")) {
                                    sb.append(" UNIQUE");
                                }
                            }
                        }
                    }

                    sb.insert(0, "CREATE TABLE " + tableName + " (");
                    sb.append(')');
                    String createTable = sb.toString();
                    if (log.isEnabled())
                        log.getLogger().info("Table: " + tableName + "  to be created SQL \n" + createTable);
                    con = connect();
                    stmt = con.createStatement();
                    if (log.isEnabled()) log.getLogger().info(createTable);
                    stmt.execute(createTable);

                    boolean ret = doesTableExists(nvce);
                    if (log.isEnabled())
                        log.getLogger().info("Table: " + tableName + " stat " + ret + " created SQL " + createTable);
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
        if (metaTables.get(tableName) != null) {
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
            throws SQLException {

        sTablename = sTablename.toUpperCase();
        if (metaTables.get(sTablename) != null) {
            return true;
        }
        synchronized (metaTables) {
            if (metaTables.get(sTablename) != null) {
                return true;
            }
            Connection con = null;
            ResultSet rs = null;
            try {
                con = connect();

                DatabaseMetaData dbmd = con.getMetaData();
                rs = dbmd.getTables(null, null, sTablename, null);
                if (rs.next()) {
                    if (log.isEnabled())
                        log.getLogger().info("Table " + rs.getString("TABLE_NAME") + " already exists !!");
                    return true;
                } else {
                    if (log.isEnabled()) log.getLogger().info("Table " + sTablename + " do not exist.");
                    return false;
                }
            } finally {
                close(rs, con);
            }
        }
    }


    private void close(AutoCloseable... autoCloseables) {
        for (AutoCloseable au : autoCloseables) {
            if (au != null) {
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
        if (!driverLoaded) {

            try {
                lock.lock();
                if (!driverLoaded) {
                    SUS.checkIfNulls("Configuration null", getAPIConfigInfo());

                    String driverClassName = getAPIConfigInfo().getProperties().getValue(DerbyParam.DRIVER);
                    try {
                        Class.forName(driverClassName);
                        driverLoaded = true;
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        throw new APIException("Driver not loaded");
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        return newConnection();
    }

    @Override
    public Connection newConnection() throws APIException {
        // TODO Auto-generated method stub
        try {
            synchronized (connections) {
                Connection ret = DriverManager.getConnection(getAPIConfigInfo().getProperties().getValue(DerbyParam.URL) + ";create=true",

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

//      try {
//        DriverManager.getConnection(getAPIConfigInfo().getProperties().getValue(DerbyParam.URL) +";shutdown=true",
//
//                getAPIConfigInfo().getProperties().getValue(DerbyParam.USER),
//                getAPIConfigInfo().getProperties().getValue(DerbyParam.PASSWORD));
//      } catch (SQLException e) {
//        e.printStackTrace();
//      }
        }
        if (log.isEnabled()) log.getLogger().info("Closed");
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
        return search(nvce.getMetaType().getName(), fieldNames, queryCriteria);
    }

    @Override
    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames,
                                               QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        NVEntity retType = null;

        Connection con = null;
        try {
            retType = (NVEntity) Class.forName(className).newInstance();
            NVConfigEntity nvce = (NVConfigEntity) retType.getNVConfig();
            if (doesTableExists(nvce)) {
                con = connect();
                StringBuilder select = new StringBuilder("SELECT GUID FROM " + retType.getNVConfig().getName());
                if (queryCriteria != null && queryCriteria.length > 0) {
                    select.append(" WHERE ");
                    select.append(DerbyDBMeta.formatQuery(queryCriteria));
                }
                //if(log.isEnabled()) log.getLogger().info(select.toString());
                stmt = con.prepareStatement(select.toString());
                DerbyDBMeta.conditionsSetup(stmt, queryCriteria);
//      if (queryCriteria != null) {
//        int index = 0;
//        for (QueryMarker qm : queryCriteria) {
//          if (qm instanceof QueryMatch) {
//            Object value = ((QueryMatch) qm).getValue();
//            if(value instanceof Enum)
//            {
//              value = ((Enum<?>) value).name();
//            }
//            stmt.setObject(++index, value);
//          }
//        }
//      }
                rs = stmt.executeQuery();
                List<String> ids = new ArrayList<String>();
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }

                return innerSearchByID(con, className, ids.toArray(new String[0]));
            } else {
                return (List<V>) new ArrayList<NVEntity>();
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new APIException(e.getMessage());
        } finally {
            close(stmt, rs, con);
        }

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
        try {
            con = connect();
            return innerSearchByID(con, className, ids);
        } finally {
            close(con);
        }
    }


    @SuppressWarnings("unchecked")
    private <V extends NVEntity> List<V> innerSearchByID(Connection con, String className, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        // TODO Auto-generated method stub

        Statement stmt = null;
        ResultSet rs = null;
        NVEntity retType = null;
        List<NVEntity> ret = new ArrayList<NVEntity>();
        if (ids == null || ids.length == 0) {
            return (List<V>) ret;
        }
        try {
            retType = (NVEntity) Class.forName(className).newInstance();
            NVConfigEntity nvce = (NVConfigEntity) retType.getNVConfig();


            for (int i = 0; i < ids.length; i++) {
                if (ids[i] != null)
                    ids[i] = "'" + ids[i] + "'";
            }
            con = connect();
            stmt = con.createStatement();
            String select = "SELECT * FROM " + retType.getNVConfig().getName() +
                    " WHERE GUID IN(" + SharedUtil.toCanonicalID(',', (Object[]) ids) + ")";
//      log.getLogger().info("select: " + select);
//      log.getLogger().info("IDS: " + Arrays.toString(ids));
            rs = stmt.executeQuery(select);
            while (rs.next()) {
                for (NVBase<?> nvb : retType.getAttributes().values()) {
                    if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_INSERT_EXCLUSION, nvb)) {
                        if (nvb instanceof NVEntityReference) {
                            if (rs.getString(nvb.getName()) != null) {
                                NVEntityRefMeta nverm = DerbyDBMeta.toNVEntityRefMeta(rs.getString(nvb.getName()));
                                List<NVEntity> innerValues = innerSearchByID(con, nverm.className, nverm.globalID);
                                if (innerValues.size() == 1) {
                                    ((NVEntityReference) nvb).setValue(innerValues.get(0));
                                }
                            }
                        } else if (MetaToken.isNVEntityArray(nvb)) {
                            if (rs.getString(nvb.getName()) != null) {
                                NVStringList nveList = new NVStringList(nvb.getName());
                                DerbyDBMeta.mapValue(rs, null, nveList);
                                List<NVEntityRefMeta> nveRefMetas = DerbyDBMeta.toNVEntityRefMetaList(nveList);
                                for (NVEntityRefMeta nverm : nveRefMetas) {
                                    List<NVEntity> nves = innerSearchByID(con, nverm.className, nverm.globalID);
                                    if (nves.size() == 1) {
                                        ((ArrayValues<NVEntity>) nvb).add(nves.get(0));
                                    }
                                }
                                // the NVE List
                            }
                        } else {
                            DerbyDBMeta.mapValue(rs, nvce.lookup(nvb.getName()), nvb);
                        }
                    }
                }
                ret.add(retType);
                retType = (NVEntity) Class.forName(className).newInstance();
            }

        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException |
                 IOException e) {
            e.printStackTrace();
            throw new APIException(e.getMessage());
        } finally {
            close(stmt, rs);
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
        try {
            con = connect();
            return innerInsert(con, nve);
        } finally {
            close(con);
        }

    }

    private <V extends NVEntity> V innerInsert(Connection con, V nve)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {

        PreparedStatement stmt = null;

        try {

            createTable((NVConfigEntity) nve.getNVConfig());
            con = connect();
            MetaUtil.initTimeStamp(nve);
            if (SUS.isEmpty(nve.getGUID())) {
                nve.setGUID(IDGs.UUIDV4.generateID());
            } else if (isRefCreated(con, nve.getNVConfig().getName(), nve.getGUID())) {
                // already exit we must update
                //if(log.isEnabled()) log.getLogger().info("invoke update for " + nve.getGlobalID());
                return innerUpdate(con, nve);
            }

            //DerbyDBData ddbd = formatInsertStatement(nve, false);

            //String statementToken = "INSERT INTO " + nve.getNVConfig().getName() + " VALUES (" + ddbd.genericValues + ")";
            //if(log.isEnabled()) log.getLogger().info(statementToken);
            //if(log.isEnabled()) log.getLogger().info("parameter count " + ddbd.genericValuesCount);


            stmt = con.prepareStatement(formatInsertStatement(nve, false).insertStatement);
            int index = 0;
            for (NVBase<?> nvb : nve.getAttributes().values()) {
                if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_INSERT_EXCLUSION, nvb)) {
                    if (MetaToken.isNVEntityArray(nvb)) {
                        NVStringList nveList = new NVStringList(nvb.getName());
                        for (NVEntity innerNVE : ((ArrayValues<NVEntity>) nvb).values()) {
                            if (innerNVE != null) {
                                innerNVE = innerInsert(con, innerNVE);
                                nveList.getValue().add(DerbyDBMeta.toNVEntityDBToken(innerNVE));
                            }
                        }
                        // make as a StringList to be converted
                        nvb = nveList;
                    } else if (nvb instanceof NVEntityReference) {
                        if (nvb.getValue() != null) {
                            NVEntity innerNVE = innerInsert(con, ((NVEntityReference) nvb).getValue());
                            ((NVEntityReference) nvb).setValue(innerNVE);
                        }
                    }
                    DerbyDBMeta.toDerbyValue(stmt, ++index, nvb);
                }
            }
            stmt.execute();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new APIException(e.getMessage());
        } finally {
            close(stmt);
        }
        // TODO Auto-generated method stub
        return nve;
    }


    private DerbyDBData formatInsertStatement(NVEntity nve, boolean nullsAllowed) throws IOException {
        DerbyDBData ret = ddbdCache.get(nve.getNVConfig().getName());


        if (ret == null) {
            ret = new DerbyDBData();
            int index = 0;
            StringBuilder genValues = new StringBuilder();
            for (NVBase<?> nvb : nve.getAttributes().values()) {
                if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_INSERT_EXCLUSION, nvb)) {
                    if (genValues.length() > 0) {
                        genValues.append(", ");
                    }
                    genValues.append('?');
                    index++;
                    if (nullsAllowed || nvb.getValue() != null) {
                        if (ret.columns.length() > 0) {
                            ret.columns.append(',');
                        }
                        ret.columns.append(nvb.getName());
                        if (ret.values.length() > 0) {
                            ret.values.append(',');
                        }
                        DerbyDBMeta.toDerbyValue(ret.values, nvb, false);
                    }
                }
            }
            ret.genericValuesCount = index;

            ret.insertStatement = "INSERT INTO " + nve.getNVConfig().getName() + " VALUES (" + genValues.toString() + ")";
            ddbdCache.put(nve.getNVConfig().getName(), ret);
        }
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

        if (nve == null)
            return false;

        if (withReference) {
            for (NVBase<?> nvb : nve.getAttributes().values()) {
                if (nvb.getValue() != null) {
                    if (nvb instanceof NVEntityReference) {
                        delete((V) nvb.getValue(), withReference);
                    } else if (MetaToken.isNVEntityArray(nvb)) {
                        for (NVEntity nvbVals : ((ArrayValues<NVEntity>) nvb).values()) {
                            delete(nvbVals, withReference);
                        }
                    }
                }
            }
        }

        return delete((NVConfigEntity) nve.getNVConfig(),
                new QueryMatch<UUID>(Const.RelationalOperator.EQUAL, IDGs.UUIDV4.decode(nve.getGUID()), MetaToken.GUID));
    }

    @Override
    public <V extends NVEntity> boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        SUS.checkIfNulls("nvce and queryCriteria can not be null", nvce, queryCriteria);
        if (queryCriteria.length == 0) {
            throw new IllegalArgumentException("queryCriteria can not be empty");
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection con = null;
        try {
            con = connect();
            StringBuilder select = new StringBuilder("DELETE FROM " + nvce.getName());
            if (queryCriteria != null && queryCriteria.length > 0) {
                select.append(" WHERE ");
                select.append(DerbyDBMeta.formatQuery(queryCriteria));
            }
            //if(log.isEnabled()) log.getLogger().info(select.toString());
            stmt = con.prepareStatement(select.toString());
            DerbyDBMeta.conditionsSetup(stmt, queryCriteria);

            return stmt.executeUpdate() != 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new APIException(e.getMessage());
        } finally {
            close(stmt, rs, con);
        }
    }

    @Override
    public <V extends NVEntity> V update(V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Can't update null nve", nve);
        Connection con = null;
        try {
            con = connect();
            return innerUpdate(con, nve);
        } finally {
            close(con);
        }
    }


    private <V extends NVEntity> V innerUpdate(Connection con, V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        SUS.checkIfNulls("Can't update null nve", nve);
        PreparedStatement stmt = null;
        try {
            createTable((NVConfigEntity) nve.getNVConfig());
            con = connect();
            if (SUS.isEmpty(nve.getGUID()) || !isRefCreated(con, nve.getNVConfig().getName(), nve.getGUID())) {
                return innerInsert(con, nve);
            }

            // update table table_name set nvb.name = nvb.value ... where GUID = 'nvb.GUID'

            StringBuilder values = new StringBuilder();
            for (NVBase<?> nvb : nve.getAttributes().values()) {
                if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_UPDATE_EXCLUSION, nvb)) {

                    if (values.length() > 0) {
                        values.append(", ");
                    }
                    DerbyDBMeta.toDerbyValue(values, nvb, true);
                }
            }

            String updateStatement = "UPDATE " + nve.getNVConfig().getName() + " set " + values.toString() + " WHERE " + MetaToken.GUID.name() + "='" + nve.getGUID() + "'";
            //if(log.isEnabled()) log.getLogger().info(updateStatement);
            stmt = con.prepareStatement(updateStatement);
            int index = 0;
            for (NVBase<?> nvb : nve.getAttributes().values()) {
                if (!DerbyDBMeta.excludeMeta(DerbyDBMeta.META_UPDATE_EXCLUSION, nvb)) {
                    if (nvb instanceof NVEntityReference) {
                        NVEntity innerNVE = innerUpdate(con, ((NVEntityReference) nvb).getValue());
                        ((NVEntityReference) nvb).setValue(innerNVE);
                    } else if (MetaToken.isNVEntityArray(nvb)) {
                        NVStringList nveList = new NVStringList(nvb.getName());
                        for (NVEntity innerNVE : ((ArrayValues<NVEntity>) nvb).values()) {
                            if (innerNVE != null) {
                                innerNVE = innerUpdate(con, innerNVE);
                                nveList.getValue().add(DerbyDBMeta.toNVEntityDBToken(innerNVE));
                            }
                        }
                        nvb = nveList;
                    }

                    DerbyDBMeta.toDerbyValue(stmt, ++index, nvb);
                }
            }
            stmt.execute();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            throw new APIException(e.getMessage());
        } finally {
            close(stmt);
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
            rs = stmt.executeQuery("select GUID from " + tableName + " where GUID='" + globalID + "'");
            if (rs.next()) {
                return true;
            }
        } finally {
            // Do NOT close  con the connection
            close(rs, stmt);
        }
        return false;


    }


    @SuppressWarnings("unchecked")
    @Override
    public IDGenerator<String, UUID> getIDGenerator() {
        // TODO Auto-generated method stub
        return IDGs.UUIDV4;
    }

    @Override
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {

        List<?> ret = searchByID(metaTypeName, (String) objectId);
        // TODO Auto-generated method stub
        return (NT) (ret.size() > 0 ? ret.get(0) : null);
    }

    @Override
    public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection) {
        // TODO Auto-generated method stub
        throw new NotFoundException("Method not implements");
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
