package org.zoxweb.datastore.common;

import org.zoxweb.shared.annotation.EndPointProp;
import org.zoxweb.shared.annotation.ParamProp;
import org.zoxweb.shared.annotation.SecurityProp;
import org.zoxweb.shared.api.APIRegistrar;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.http.HTTPMethod;
import org.zoxweb.shared.util.NVGenericMap;

public class DataStoreMonitor {

    @EndPointProp(methods = {HTTPMethod.GET}, name = "ds-monitor", uris = "/ds/ping/{detailed}")
    @SecurityProp(authentications = {CryptoConst.AuthenticationType.ALL}, permissions = "ds:check")
    public NVGenericMap ping(@ParamProp(name = "detailed", optional = true) boolean detailed) {

        return APIRegistrar.SINGLETON.stats(detailed);
    }

}
