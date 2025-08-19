package org.zoxweb.datastore.test;

import org.zoxweb.server.security.SecUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.http.*;
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.SharedStringUtil;

import java.security.NoSuchAlgorithmException;

public class CommonDataStoreTest<P,S> {
    public final APIDataStore<P,S> dataStore;
    public CommonDataStoreTest(APIDataStore<P, S> ds)
    {
        this.dataStore = ds;
    }

    public void testHMCI()
    {
        HTTPMessageConfigInterface hmci = HTTPMessageConfig.createAndInit("https://api.xlogistx.io", "login", HTTPMethod.PATCH);
        hmci.setAccept(HTTPMediaType.APPLICATION_JSON);
        hmci.setContentType(HTTPMediaType.APPLICATION_JSON);

        //hmci.getHeaders().add("revision", "2023-07-15");
        HTTPAuthorization authorization = new HTTPAuthorization("XlogistX-KEY", "ABB-CC-DDSFS-664554");
        //dataStore.insert(authorization);


        hmci.setAuthorization(authorization);
        NVGenericMap nvgm = new NVGenericMap();
        nvgm.add("name", "mario");
        nvgm.add("email", "mario@mario.com");
        nvgm.add(new NVInt("age", 31));
        hmci.setContent(GSONUtil.toJSONDefault(nvgm));


        System.out.println(GSONUtil.toJSONDefault(hmci, true ));

        HTTPMessageConfig httpMessageConfig = dataStore.insert((HTTPMessageConfig)hmci);
        System.out.println(httpMessageConfig.getGUID());


        httpMessageConfig = (HTTPMessageConfig) dataStore.searchByID(HTTPMessageConfig.class.getName(), httpMessageConfig.getGUID()).get(0);

        System.out.println(SharedStringUtil.toString(httpMessageConfig.getContent()));

        String json = GSONUtil.toJSONDefault(hmci);
        String jsonFromDB = GSONUtil.toJSONDefault(httpMessageConfig );

        assert(hmci != httpMessageConfig);
        assert json.equals(jsonFromDB);
        System.out.println(json);
        System.out.println(jsonFromDB);

        authorization = hmci.getAuthorization();
        //dataStore.delete(httpMessageConfig, true);

        System.out.println("Authorization meta: " + ((NVConfigEntity)authorization.getNVConfig()).getAttributes());
    }

    public void testBCryptPassword() throws NoSuchAlgorithmException {
        CredentialHasher<CIPassword> credentialHasher =  SecUtil.SINGLETON.lookupCredentialHasher(CryptoConst.HashType.BCRYPT.getName());
        CIPassword p = credentialHasher.hash("P1ssw@rd");
        dataStore.insert(p);

        CIPassword found = dataStore.lookupByReferenceID(CIPassword.class.getName(), p.getGUID());
        assert found != p;
        assert credentialHasher.isPasswordValid(found, "P1ssw@rd");
        assert GSONUtil.toJSONDefault(p).equals(GSONUtil.toJSONDefault(found));
    }

    public void testArgonPassword() throws NoSuchAlgorithmException {
        CredentialHasher<CIPassword> credentialHasher =  SecUtil.SINGLETON.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        CIPassword p = credentialHasher.hash("P1ssw@rd");
        dataStore.insert(p);

        CIPassword found = dataStore.lookupByReferenceID(CIPassword.class.getName(), p.getGUID());
        assert found != p;
        assert credentialHasher.isPasswordValid(found, "P1ssw@rd");
        assert GSONUtil.toJSONDefault(p).equals(GSONUtil.toJSONDefault(found));
    }
}
