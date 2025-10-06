package org.zoxweb.datastore.common;

import org.zoxweb.shared.annotation.EndPointProp;
import org.zoxweb.shared.annotation.ParamProp;
import org.zoxweb.shared.annotation.SecurityProp;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.http.HTTPMethod;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.SUS;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataStoreMonitor {

    private final Map<String, APIDataStore<?, ?>> dataStores = new LinkedHashMap<>();

    @EndPointProp(methods = {HTTPMethod.GET}, name = "ds-monitor", uris = "/ds/ping/{ds-id}")
    @SecurityProp(authentications = {CryptoConst.AuthenticationType.ALL}, permissions = "ds:check")
    public NVGenericMap ping(@ParamProp(name = "ds-id", optional = true) String dsID) {
        if (SUS.isNotEmpty(dsID)) {
            APIDataStore ds = dataStores.get(dsID);
            if (ds != null)
                return ds.ping(false);
        } else {
            if (!dataStores.values().isEmpty()) {
                return dataStores.values().toArray(new APIDataStore[0])[0].ping(false);
            }
        }
        return new NVGenericMap().build("message", "No data store found");
    }


    public void addAPIDataStore(APIDataStore<?, ?> ds) {
        dataStores.put(ds.getStoreName(), ds);

    }
}
