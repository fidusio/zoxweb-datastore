package org.zoxweb.server.ds.proxy;

import org.zoxweb.server.api.APIDocumentStore;
import org.zoxweb.server.api.APIServiceProviderBase;
import org.zoxweb.shared.api.APIBatchResult;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIException;
import org.zoxweb.shared.api.APIFileInfoMap;
import org.zoxweb.shared.api.APISearchResult;
import org.zoxweb.shared.db.QueryMarker;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.util.DynamicEnumMap;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class DataStoreProxy
    extends APIServiceProviderBase<Void>
    implements APIDataStore<Void>, APIDocumentStore<Void> {

    private String name;
    private String description;
    private APIConfigInfo configInfo;

    // FS database via HTTP
    // Required Parameters: URL, URIs

    public DataStoreProxy(APIConfigInfo configInfo) {
        this();
        setAPIConfigInfo(configInfo);
    }

    public DataStoreProxy() {

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public APIConfigInfo getAPIConfigInfo() {
        return configInfo;
    }

    @Override
    public void setAPIConfigInfo(APIConfigInfo configInfo) {
        this.configInfo = configInfo;
    }

    @Override
    public APIFileInfoMap createFile(String folderID, APIFileInfoMap file, InputStream is, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {
        return null;
    }

    @Override
    public APIFileInfoMap createFolder(String folderFullPath)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {
        return null;
    }

    @Override
    public APIFileInfoMap readFile(APIFileInfoMap map, OutputStream os, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {
        return null;
    }

    @Override
    public APIFileInfoMap updateFile(APIFileInfoMap map, InputStream is, boolean closeStream)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {
        return null;
    }

    @Override
    public void deleteFile(APIFileInfoMap map)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {

    }

    @Override
    public Map<String, APIFileInfoMap> discover()
            throws IOException, AccessException, APIException {
        return null;
    }

    @Override
    public List<APIFileInfoMap> search(String... args)
            throws NullPointerException, IllegalArgumentException, IOException, AccessException, APIException {
        return null;
    }

    @Override
    public String getStoreName() {
        return null;
    }

    @Override
    public Set<String> getStoreTables() {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> search(NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> search(String className, List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <T> APISearchResult<T> batchSearch(String className, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <T, V extends NVEntity> APIBatchResult<V> nextBatch(APISearchResult<T> results, int startIndex, int batchSize)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, NVConfigEntity nvce, List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearch(String userID, String className, List<String> fieldNames, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> searchByID(String className, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> List<V> userSearchByID(String userID, NVConfigEntity nvce, String... ids)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <V extends NVEntity> V insert(V nve)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return nve;
    }

    @Override
    public <V extends NVEntity> boolean delete(V nve, boolean withReference)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return false;
    }

    @Override
    public <V extends NVEntity> boolean delete(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return false;
    }

    @Override
    public <V extends NVEntity> V update(V nve)
            throws NullPointerException, IllegalArgumentException, APIException {
        return nve;
    }

    @Override
    public <V extends NVEntity> V patch(V nve, boolean updateTS, boolean sync, boolean updateRefOnly, boolean includeParam, String... nvConfigNames)
            throws NullPointerException, IllegalArgumentException, APIException {
        return nve;
    }

    @Override
    public long countMatch(NVConfigEntity nvce, QueryMarker... queryCriteria)
            throws NullPointerException, IllegalArgumentException, APIException {
        return 0;
    }

    @Override
    public DynamicEnumMap insertDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        return null;
    }

    @Override
    public DynamicEnumMap updateDynamicEnumMap(DynamicEnumMap dynamicEnumMap)
            throws NullPointerException, IllegalArgumentException, APIException {
        return null;
    }

    @Override
    public DynamicEnumMap searchDynamicEnumMapByName(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
        return null;
    }

    @Override
    public void deleteDynamicEnumMap(String name)
            throws NullPointerException, IllegalArgumentException, APIException {
    }

    @Override
    public List<DynamicEnumMap> getAllDynamicEnumMap(String domainID, String userID)
            throws NullPointerException, IllegalArgumentException, AccessException, APIException {
        return null;
    }

    @Override
    public <NT, RT> NT lookupByReferenceID(String metaTypeName, RT objectId) {
        return null;
    }

    @Override
    public <NT, RT, NIT> NT lookupByReferenceID(String metaTypeName, RT objectId, NIT projection) {
        return null;
    }

    @Override
    public Void connect()
            throws APIException {
        return null;
    }

    @Override
    public Void newConnection()
            throws APIException {
        return null;
    }

    @Override
    public void close()
            throws APIException {
    }

    @Override
    public boolean isProviderActive() {
        return false;
    }

    @Override
    public String toCanonicalID() {
        return null;
    }

}