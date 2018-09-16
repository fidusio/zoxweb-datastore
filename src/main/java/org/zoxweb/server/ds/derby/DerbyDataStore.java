package org.zoxweb.server.ds.derby;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.IDGenerator;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;

@SuppressWarnings("serial")
public class DerbyDataStore implements APIDataStore<Connection> {

  @Override
  public APIConfigInfo getAPIConfigInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setAPIConfigInfo(APIConfigInfo configInfo) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Connection connect() throws APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Connection newConnection() throws APIException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() throws APIException {
    // TODO Auto-generated method stub
    
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
    return null;
  }

  @Override
  public <V extends NVEntity> List<V> searchByID(String className, String... ids)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends NVEntity> boolean delete(V nve, boolean withReference)
      throws NullPointerException, IllegalArgumentException, AccessException, APIException {
    // TODO Auto-generated method stub
    return false;
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
