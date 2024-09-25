package org.zoxweb.server.ds.mongo;

import com.mongodb.DB;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.DomainUsernamePasswordToken;
import io.xlogistx.shiro.mgt.ShiroRealmController;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import io.xlogistx.shiro.mgt.ShiroXlogistXRealm;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.env.BasicIniEnvironment;
import org.apache.shiro.env.Environment;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.ds.data.DSConst;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.data.SecureDocumentDAO;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.security.AccessException;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.Const;
import org.zoxweb.shared.util.MetaToken;
import org.zoxweb.shared.util.RateCounter;
import org.zoxweb.shared.util.ResourceManager;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MongoDSShiroControllerTest {

    private static final Logger log = Logger.getLogger(MongoDSShiroTest.class.getName());


    private static MongoDataStore mongoDS = null;
    private static ShiroRealmController shiroRM = null;



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


        String mongoJSONConfig = IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroControllerTest.class.getClassLoader(), DSConst.MONGO_CONF));
        log.info(mongoJSONConfig);
        APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(mongoJSONConfig, APIConfigInfoDAO.class);
//        dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
//        dsConfig.setAPISecurityManager(apiSecurityManager);

//        Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
//        SecurityManager securityManager = factory.getInstance();
//        SecurityUtils.setSecurityManager(securityManager);

        MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
        dsConfig.setSecurityController(new ShiroSecurityController());


        KeyMakerProvider.SINGLETON.setMasterKey(DSConst.MS_KEY);
        dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
        mongoDS = mdsc.createAPI(null, dsConfig);

        shiroRM = new ShiroRealmController();
        shiroRM.setDataStore(mongoDS);
        shiroRM.setKeyMaker(KeyMakerProvider.SINGLETON);
        ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, mongoDS);

        Environment env = new BasicIniEnvironment("classpath:shiro-xlog.ini");
        SecurityManager securityManager = env.getSecurityManager();
        System.out.println(securityManager.getClass().getName());
        SecurityUtils.setSecurityManager(securityManager);
        ShiroXlogistXRealm xlogRealm = ShiroUtil.getRealm(ShiroXlogistXRealm.class);
        System.out.println("" + xlogRealm);
        xlogRealm.setRealmController(shiroRM);
        xlogRealm.setCachePersistenceEnabled(false);
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
        DSConst.createUser(shiroRM, DSConst.SUPER_ADMIN, DSConst.TEST_PASSWORD);
        DSConst.createUser(shiroRM, DSConst.TEST_USER, DSConst.TEST_PASSWORD);
        DSConst.createUser(shiroRM, DSConst.TEST_USER_TWO, DSConst.TEST_PASSWORD);
//        if(shiroRM.lookupSubjectIdentifier(DSConst.SUPER_ADMIN) == null) {
//            SubjectIdentifier subjectID = new SubjectIdentifier();
//            subjectID.setSubjectID(DSConst.SUPER_ADMIN);
//            subjectID.setSubjectType(BaseSubjectID.SubjectType.USER);
//            shiroRM.addSubjectIdentifier(subjectID,
//                    HashUtil.toBCryptPassword(DSConst.TEST_PASSWORD));
//        }
//
//        if(shiroRM.lookupSubjectIdentifier(DSConst.TEST_USER) == null) {
//            shiroRM.addSubjectIdentifier(DSConst.TEST_USER,
//                    BaseSubjectID.SubjectType.USER,
//                    HashUtil.toBCryptPassword(DSConst.TEST_PASSWORD));
//        }
    }

    @Test
    public void lookupTest()
    {
        RateCounter rc = new RateCounter("reading users");
        for (int i = 0; i < 10; i++)
        {
            rc.start();
            SubjectIdentifier subjectIdentifier = shiroRM.lookupSubjectIdentifier(DSConst.SUPER_ADMIN);
            rc.stop();
            assert subjectIdentifier != null;

        }
        System.out.println(rc);
        rc.reset();
        for (int i = 0; i < 100; i++)
        {
            rc.start();
            SubjectIdentifier subjectIdentifier = shiroRM.lookupSubjectIdentifier(DSConst.SUPER_ADMIN);
            rc.stop();
            assert subjectIdentifier != null;

        }

        System.out.println(rc);
    }

    @Test
    public void testLogin()
    {
        if(shiroRM.lookupSubjectIdentifier(DSConst.TEST_USER) != null)
        {
            System.out.println("we will try to login");
            DomainUsernamePasswordToken token =  new DomainUsernamePasswordToken(DSConst.TEST_USER, DSConst.TEST_PASSWORD, false, null, null);

            Subject subject = SecurityUtils.getSubject();
            subject.login(token);
            subject.isPermitted("read:batata");
            String resourceAccessViaSubjectGUID = SecurityModel.SecToken.updateToken(SecurityModel.PERM_RESOURCE_ACCESS_VIA_SUBJECT_GUID,
                    SecurityModel.SecToken.SUBJECT_GUID.toGNV(subject.getPrincipals().oneByType(UUID.class).toString()),
                    SecurityModel.SecToken.CRUD.toGNV(SecurityModel.READ));
            subject.logout();
            ShiroXlogistXRealm.log.setEnabled(false);
            RateCounter rc = new RateCounter("login");



            System.out.println(resourceAccessViaSubjectGUID);
            for(int i = 0; i < 10; i++)
            {
                SecurityUtils.getSubject().login(token);
                rc.start();
                SecurityUtils.getSubject().checkPermission(resourceAccessViaSubjectGUID);
                SecurityUtils.getSubject().logout();
                rc.stop();
            }


            System.out.println(rc);
        }
    }


    @Test
    public void createPermissions()
    {

    }

    @Test
    public void testEncryptedDocument()
    {
        SecureDocumentDAO sd = new SecureDocumentDAO();
        sd.setName("Test");
        sd.setContent("Hello word " + new Date());

        DomainUsernamePasswordToken token =  new DomainUsernamePasswordToken(DSConst.TEST_USER, DSConst.TEST_PASSWORD, false, null, null);

        Subject subject = SecurityUtils.getSubject();
        subject.login(token);
        try {
            sd = mongoDS.insert(sd);

            SecureDocumentDAO sd1 = mongoDS.findOne(SecureDocumentDAO.NVC_SECURE_DOCUMENT_DAO, null, new QueryMatchString(MetaToken.GUID, sd.getGUID(), Const.RelationalOperator.EQUAL));
            System.out.println(sd1.getContent());
        }

        finally {
            subject.logout();
        }
    }
    @Test
    public void testEncryptedDocumentFail()
    {
        SecureDocumentDAO sd = new SecureDocumentDAO();
        sd.setName("Test");
        sd.setContent("Hello word " + new Date());

        DomainUsernamePasswordToken token =  new DomainUsernamePasswordToken(DSConst.TEST_USER, DSConst.TEST_PASSWORD, false, null, null);

        Subject subject = SecurityUtils.getSubject();
        subject.login(token);
        try {
            sd = mongoDS.insert(sd);
        }

        finally {
            subject.logout();
        }

        token =  new DomainUsernamePasswordToken(DSConst.TEST_USER_TWO, DSConst.TEST_PASSWORD, false, null, null);
        subject = SecurityUtils.getSubject();
        subject.login(token);
        String guid = sd.getGUID();
        System.out.println(Thread.currentThread());
        assertThrows(AccessException.class, ()->{
            System.out.println(Thread.currentThread());
            SecureDocumentDAO sd1 = mongoDS.findOne(SecureDocumentDAO.NVC_SECURE_DOCUMENT_DAO, null, new QueryMatchString(MetaToken.GUID, guid, Const.RelationalOperator.EQUAL));
        });
        try {
            SecureDocumentDAO sd1 = mongoDS.findOne(SecureDocumentDAO.NVC_SECURE_DOCUMENT_DAO, null, new QueryMatchString(MetaToken.GUID, sd.getGUID(), Const.RelationalOperator.EQUAL));
            System.out.println(sd1.getContent());
        }
        finally {
            subject.logout();
        }

        System.out.println(Thread.currentThread());

        if(!subject.isAuthenticated())
            subject.logout();

    }
}
