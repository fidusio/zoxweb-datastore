package org.zoxweb.datastore.test;

import org.zoxweb.server.security.SecUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APIRegistrar;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.http.*;
import org.zoxweb.shared.util.*;

import java.security.NoSuchAlgorithmException;

public class CommonDataStoreTest<P,S> {
    //public final APIDataStore<P,S> dataStore;
    public final RateCounter rc = new RateCounter("Test");
    public CommonDataStoreTest(APIDataStore<P, S> ds)
    {
        APIRegistrar.SINGLETON.setDefault(ds);
    }
    private static final String PASSWORD = "P1ssw@rd";
    private static final String NEW_PASSWORD = "P1ssw@rd123$";



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

        HTTPMessageConfig httpMessageConfig = ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert((HTTPMessageConfig)hmci);
        System.out.println(httpMessageConfig.getGUID());


        httpMessageConfig = (HTTPMessageConfig) ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).searchByID(HTTPMessageConfig.class.getName(), httpMessageConfig.getGUID()).get(0);

        System.out.println(SharedStringUtil.toString(httpMessageConfig.getContent()));

        String json = GSONUtil.toJSONDefault(hmci);
        String jsonFromDB = GSONUtil.toJSONDefault(httpMessageConfig);

        assert(hmci != httpMessageConfig);
        assert json.equals(jsonFromDB);
        System.out.println(json);
        System.out.println(jsonFromDB);

        authorization = hmci.getAuthorization();
        //dataStore.delete(httpMessageConfig, true);

        System.out.println("Authorization meta: " + ((NVConfigEntity)authorization.getNVConfig()).getAttributes());
        NamedValue<String> token = hmci.getAuthorization().lookup(HTTPAuthorization.NVC_TOKEN.getName());
        token.getProperties().build("test", "test-value");
        httpMessageConfig = ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).update((HTTPMessageConfig) hmci);

    }

    public void testBCryptPassword() throws NoSuchAlgorithmException {
        CredentialHasher<CIPassword> credentialHasher =  SecUtil.lookupCredentialHasher(CryptoConst.HashType.BCRYPT.getName());
        CIPassword p = credentialHasher.hash(PASSWORD);
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert(p);

        CIPassword found = (CIPassword) ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).searchByID(CIPassword.class.getName(), p.getGUID()).get(0);
        assert found != p;
        assert credentialHasher.validate(found, PASSWORD);
        assert GSONUtil.toJSONDefault(p).equals(GSONUtil.toJSONDefault(found));
    }

    public void testArgonPassword() throws NoSuchAlgorithmException {
        CredentialHasher<CIPassword> credentialHasher =  SecUtil.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        CIPassword p = credentialHasher.hash(PASSWORD);
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert(p);

        CIPassword found = (CIPassword) ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).searchByID(CIPassword.class.getName(), p.getGUID()).get(0);
        assert found != p;
        assert credentialHasher.validate(found, PASSWORD);
        assert GSONUtil.toJSONDefault(p).equals(GSONUtil.toJSONDefault(found));
    }

    public void testUpdatePassword() throws NoSuchAlgorithmException {
        long count = ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).countMatch(CIPassword.NVCE_CI_PASSWORD);
        CredentialHasher<CIPassword> credentialHasher =  SecUtil.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        CIPassword p = credentialHasher.hash(PASSWORD);
        p.setName("ToBeUpdated-" + count);
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert(p);

        CIPassword found = (CIPassword) ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).searchByID(CIPassword.class.getName(), p.getGUID()).get(0);
        assert found != p;
        assert credentialHasher.validate(found, PASSWORD);
        assert GSONUtil.toJSONDefault(p).equals(GSONUtil.toJSONDefault(found));
        credentialHasher =  SecUtil.lookupCredentialHasher(CryptoConst.HashType.BCRYPT.getName());
        CIPassword updated = credentialHasher.update(found, PASSWORD, NEW_PASSWORD);
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).update(updated);

        found = (CIPassword) ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).searchByID(CIPassword.class.getName(), p.getGUID()).get(0);
        assert found != updated;
        assert credentialHasher.validate(found, NEW_PASSWORD);
        assert GSONUtil.toJSONDefault(updated).equals(GSONUtil.toJSONDefault(found));
    }


    public void insertAllType()
    {
        DSConst.AllTypes allTypes = DSConst.AllTypes.autoBuilder();
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert(allTypes);
    }
    public void insertComplexType()
    {
        DSConst.ComplexTypes complexTypes = DSConst.ComplexTypes.buildComplex("batata");
        ((APIDataStore<?,?>)APIRegistrar.SINGLETON.getDefault()).insert(complexTypes);
    }


}
