package org.zoxweb.server.ds.mongo;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.shiro.SecurityUtils;

import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.junit.Before;
import org.junit.Test;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.UserIDCredentialsDAO.UserStatus;
import org.zoxweb.server.security.shiro.DefaultAPISecurityManager;
import org.zoxweb.server.security.shiro.ShiroUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.data.AppDeviceDAO;
import org.zoxweb.shared.data.AppIDDAO;
import org.zoxweb.shared.data.DeviceDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.security.KeyStoreInfoDAO;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;
import org.zoxweb.shared.security.AccessException;


public class MongoDSShiroTest
{
	private static final transient Logger log = Logger.getLogger(MongoDSShiroTest.class.getName());
//	mongo_conf.json
//	key_store_info.json
//	shiro.ini
//	nael@xlogistx.io
//	W1r2l3ss
	
	private static final String MONGO_CONF = "mongo_conf.json";
	private static final String KEYSTORE_INFO = "key_store_info.json";
	private static final String SHIRO_INI = "shiro.ini";
	private static final String TEST_USER = "test@xlosistx.io";
	private static final String TEST_PASSWORD= "T!st2s3r";
	private static final String ILLEGAL_USER = "illegal@xlosistx.io";
	private static final String ILLEGAL_PASSWORD= "T!st2s3r";
	private static final String DEFAULT_API_KEY = "test_default_api_key";
	
	private static final String SUPER_ADMIN = "superadmid@xlogistx.io";
	private static final String SUPER_PASSWORD = "T!st2s3r";
	private static final String DOMAIN_ID = "test.com";
	private static final String APP_ID = "testapp";
	
	
	private static  APISecurityManager<Subject> apiSecurityManager;
	private static  APIAppManagerProvider appManager;

	
	@Before
	public void start() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
	{
		if (appManager == null)
		{
		APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF)), APIConfigInfoDAO.class);
		
		// load the Master Key
		KeyStoreInfoDAO ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(KEYSTORE_INFO)), KeyStoreInfoDAO.class);
		
		KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
			 	  CryptoUtil.KEY_STORE_TYPE,
			 	  ksid.getKeyStorePassword().toCharArray());
	
		
		KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getKeyPassword());
		// setup the ms
		// load the mongo config file
		// create the data store
		dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
		apiSecurityManager = new DefaultAPISecurityManager();
		dsConfig.setAPISecurityManager(apiSecurityManager);
		
		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);

	    //2.
	    SecurityManager securityManager = factory.getInstance();
	    log.info("security manager " + securityManager);

	    //3.
	    SecurityUtils.setSecurityManager(securityManager);
	    
	    
	    ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
		
		MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
		
		
		ResourceManager.SINGLETON.map(Resource.DATA_STORE, (MongoDataStore) mdsc.createAPI(null, dsConfig));
		
		
		
		realm.setAPISecurityManager(apiSecurityManager);
		realm.setDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
		
		appManager = new APIAppManagerProvider();
		
		appManager.setAPIDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
		appManager.setAPISecurityManager(apiSecurityManager);
		
		
		
		createAPP();
		
		try 
		{
			createUser(TEST_USER, TEST_PASSWORD);
			createUser(ILLEGAL_USER, ILLEGAL_PASSWORD);
			if (appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, false) == null)
			{
				registerSubjectAPIKey(DEFAULT_API_KEY, TEST_USER, TEST_PASSWORD);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			log.info("Create error " + e);
		}
		
		
		
		apiSecurityManager.login(TEST_USER, TEST_PASSWORD, DOMAIN_ID, APP_ID, false);
		}
	}
	
//	@Test
//	public void testGenerateKeystoreInfo() throws NoSuchAlgorithmException, IOException
//	{
//		log.info("\n"+GSONUtil.toJSON(CryptoUtil.generateKeyStoreInfo("test", "test"), true, false, false));
//	}

	@Test
    public void testRegisterSubjectAPIKey() {
//
//	    UserInfoDAO userInfoDAO = new UserInfoDAO();
//	    userInfoDAO.setFirstName("John");
//	    userInfoDAO.setLastName("Smith");
//
//	    AppIDDAO appIDDAO = new AppIDDAO(DOMAIN_ID, APP_ID);
//        DeviceDAO deviceDAO = new DeviceDAO();
//        deviceDAO.setDeviceID(UUID.randomUUID().toString());
//        deviceDAO.setManufacturer("Apple");
//        deviceDAO.setModel("7");
//        deviceDAO.setVersion("10");
//
//        AppDeviceDAO appDeviceDAO = new AppDeviceDAO();
//        appDeviceDAO.setAppIDDAO(appIDDAO);
//        appDeviceDAO.setDevice(deviceDAO);
//        appDeviceDAO.setSubjectID(DEFAULT_API_KEY);
//
//
//        apiSecurityManager.logout();
//        AppDeviceDAO temp = (AppDeviceDAO) appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, TEST_USER, TEST_PASSWORD);
//       
//        AppDeviceDAO val = appManager.lookupSubjectAPIKey(temp.getAPIKey());
//        log.info(""+val);
        
        
        registerSubjectAPIKey(null, TEST_USER, TEST_PASSWORD);
    }
	
	
	
	
	 public void registerSubjectAPIKey(String apiKey, String userID, String password) {

		    UserInfoDAO userInfoDAO = new UserInfoDAO();
		    userInfoDAO.setFirstName("John");
		    userInfoDAO.setLastName("Smith");

		    AppIDDAO appIDDAO = new AppIDDAO(DOMAIN_ID, APP_ID);
	        DeviceDAO deviceDAO = new DeviceDAO();
	        deviceDAO.setDeviceID(UUID.randomUUID().toString());
	        deviceDAO.setManufacturer("Apple");
	        deviceDAO.setModel("7");
	        deviceDAO.setVersion("10");

	        AppDeviceDAO appDeviceDAO = new AppDeviceDAO();
	        appDeviceDAO.setAppIDDAO(appIDDAO);
	        appDeviceDAO.setDevice(deviceDAO);
	        if (apiKey != null)
	        	appDeviceDAO.setAPIKey(apiKey);


	        apiSecurityManager.logout();
	        AppDeviceDAO temp = (AppDeviceDAO) appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, userID, password);
	       
	        AppDeviceDAO val = appManager.lookupSubjectAPIKey(temp.getAPIKey(), false);
	        log.info(""+val);
	    }
	
	
	
	
	
	private void createUser(String subjectID, String password)
	{
		
		UserIDDAO userID = new UserIDDAO();
		
		userID.setSubjectID(subjectID);
		UserInfoDAO userInfo = new UserInfoDAO();
		userInfo.setFirstName("N/S");
		userInfo.setLastName("N/S");
		userID.setUserInfo(userInfo);
		appManager.createUserIDDAO(userID, UserStatus.ACTIVE, password);
		///appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, subjectID, password);
	}
	
	public void createAPP()
	{
		String subjectID = SUPER_ADMIN;
		String password  = SUPER_PASSWORD;
		UserIDDAO userID = new UserIDDAO();
		
		userID.setSubjectID(subjectID);
		UserInfoDAO userInfo = new UserInfoDAO();
		userInfo.setFirstName("N/S");
		userInfo.setLastName("N/S");
		userID.setUserInfo(userInfo);
		try
		{
			appManager.createUserIDDAO(userID, UserStatus.ACTIVE, password);
		}
		catch(Exception e)
		{
		}
		
		apiSecurityManager.logout();
		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, null, null, false);
		
		appManager.createAppIDDAO(DOMAIN_ID, APP_ID);
		apiSecurityManager.logout();
		
	}
	
	@Test
	public void loadKeySuccess()
	{
		
		apiSecurityManager.logout();
		apiSecurityManager.login(TEST_USER, TEST_PASSWORD, DOMAIN_ID, APP_ID, true);
	
		appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, true);
		
	}
	
	@Test(expected = AccessException.class)
	public void loadKeyFailed()
	{
		
		apiSecurityManager.logout();
		apiSecurityManager.login(ILLEGAL_USER, ILLEGAL_PASSWORD, DOMAIN_ID, APP_ID, true);
	
	
		appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, true);
		
	}

}
