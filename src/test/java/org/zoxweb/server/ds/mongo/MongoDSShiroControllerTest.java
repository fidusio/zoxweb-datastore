package org.zoxweb.server.ds.mongo;

import com.mongodb.DB;
import io.xlogistx.shiro.mgt.ShiroRealmManager;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.HashUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.crypto.PasswordDAO;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.util.BaseSubjectID;
import org.zoxweb.shared.util.RateCounter;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.SharedBase64;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

public class MongoDSShiroControllerTest {

    private static final Logger log = Logger.getLogger(MongoDSShiroTest.class.getName());

    private static final SecretKey MS_KEY = CryptoUtil.toSecretKey(SharedBase64.decode("Lq/bGZ2qRJZMEIQqi3OeKth8+IpoZRd5/bevzaRVRVE="), "AES");
    private static final String MONGO_CONF = "mongo_conf.json";
    private static final String KEYSTORE_INFO = "key_store_info.json";
    private static final String SHIRO_INI = "shiro.ini";
    private static final String TEST_USER = "test@xlogistx.io";
    private static final String TEST_PASSWORD= "T!st2s3r";
    private static final String ILLEGAL_USER = "illegal@xlogistx.io";
    private static final String ILLEGAL_PASSWORD= "T!st2s3r";
    private static final String DEFAULT_API_KEY = "test_default_api_key";

    private static final String SUPER_ADMIN = "superadmin@xlogistx.io";
    private static final String SUPER_PASSWORD = "T!st2s3r";
    private static final String DOMAIN_ID = "test.com";
    private static final String APP_ID = "testapp";
    private static MongoDataStore mongoDS = null;
    private static ShiroRealmManager shiroRM = null;



    @BeforeAll
    public static void start()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
//        KeyStoreInfo ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(KEYSTORE_INFO)), KeyStoreInfo.class);
//        KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
//                CryptoConst.KEY_STORE_TYPE,
//                ksid.getKeyStorePassword().toCharArray());
//
//        KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());
//
//        apiSecurityManager = new APISecurityManagerProvider();


        String mongoJSONConfig = IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroControllerTest.class.getClassLoader(), MONGO_CONF));
        log.info(mongoJSONConfig);
        APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(mongoJSONConfig, APIConfigInfoDAO.class);
//        dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
//        dsConfig.setAPISecurityManager(apiSecurityManager);

//        Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
//        SecurityManager securityManager = factory.getInstance();
//        SecurityUtils.setSecurityManager(securityManager);

        MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
        dsConfig.setSecurityController(new ShiroSecurityController());
        mongoDS = mdsc.createAPI(null, dsConfig);

        shiroRM = new ShiroRealmManager();
        shiroRM.setDataStore(mongoDS);

        KeyMakerProvider.SINGLETON.setMasterKey(MS_KEY);
        shiroRM.setKeyMaker(KeyMakerProvider.SINGLETON);
        ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, mongoDS);
//        dataStore = ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE);
//        documentStore = ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE);
//
//
//        appManager.setAPIDataStore(ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE));
//        appManager.setAPISecurityManager(apiSecurityManager);
//
//        ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
//        realm.setAPISecurityManager(apiSecurityManager);
//        realm.setDataStore(ResourceManager.lookupResource(ResourceManager.Resource.DATA_STORE));
//
//        apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
    }

    @Test
    public void testDBConnection()
    {
        DB db = mongoDS.connect();
    }

    @Test
    public void createUser()
    {
        if(shiroRM.lookupSubjectIdentifier(SUPER_ADMIN) == null) {
            SubjectIdentifier subjectID = new SubjectIdentifier();
            subjectID.setSubjectID(SUPER_ADMIN);
            subjectID.setSubjectType(BaseSubjectID.SubjectType.USER);
            shiroRM.addSubjectIdentifier(subjectID);
        }

        if(shiroRM.lookupSubjectIdentifier("mario@bros.com") == null) {
            PasswordDAO password = HashUtil.toBCryptPassword("password");
            shiroRM.addSubjectIdentifier("mario@bros.com", BaseSubjectID.SubjectType.USER, password);
        }
    }

    @Test
    public void lookupTest()
    {
        RateCounter rc = new RateCounter("reading users");
        for (int i = 0; i < 10; i++)
        {
            rc.start();
            SubjectIdentifier subjectIdentifier = shiroRM.lookupSubjectIdentifier(SUPER_ADMIN);
            rc.stop();
            assert subjectIdentifier != null;

        }
        System.out.println(rc);
        rc.reset();
        for (int i = 0; i < 100; i++)
        {
            rc.start();
            SubjectIdentifier subjectIdentifier = shiroRM.lookupSubjectIdentifier(SUPER_ADMIN);
            rc.stop();
            assert subjectIdentifier != null;

        }

        System.out.println(rc);
    }
}
