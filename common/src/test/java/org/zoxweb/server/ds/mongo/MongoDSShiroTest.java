package org.zoxweb.server.ds.mongo;


import io.xlogistx.shiro.APISecurityManagerProvider;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.DomainUsernamePasswordToken;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.data.*;
import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.http.HTTPEncoder;
import org.zoxweb.shared.http.HTTPMessageConfig;
import org.zoxweb.shared.http.HTTPMethod;
import org.zoxweb.shared.security.KeyStoreInfo;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.security.model.SecurityModel.Role;
import org.zoxweb.shared.security.shiro.ShiroAssociationRule;
import org.zoxweb.shared.security.shiro.ShiroAssociationType;
import org.zoxweb.shared.security.shiro.ShiroPermission;
import org.zoxweb.shared.security.shiro.ShiroRole;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.Const.Status;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;
import org.zoxweb.shared.util.SharedUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class MongoDSShiroTest
{
	private static final Logger log = Logger.getLogger(MongoDSShiroTest.class.getName());
//	mongo_conf.json
//	key_store_info.json
//	shiro.ini
//	nael@xlogistx.io
//
	
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

    private static final String PROPANEXP_DOMAIN_ID = "propanexp.com";
    private static final String PROPANEXP_APP_ID = "propanexp";

	private static  APISecurityManager<Subject, AuthorizationInfo, PrincipalCollection> apiSecurityManager;
	private static  APIAppManagerProvider appManager;

	
	@BeforeAll
	public static void start() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
	{
		if (appManager == null)
		{
		APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF)), APIConfigInfoDAO.class);
		
		// load the Master Key
		KeyStoreInfo ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(KEYSTORE_INFO)), KeyStoreInfo.class);
		
		KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
				CryptoConst.KEY_STORE_TYPE,
			 	  ksid.getKeyStorePassword().toCharArray());
	
		
		KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());
		// setup the ms
		// load the mongo config file
		// create the data store
		dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
		apiSecurityManager = new APISecurityManagerProvider();
		dsConfig.setSecurityController(new ShiroSecurityController());
		
		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);

	    //2.
	    SecurityManager securityManager = factory.getInstance();
	    log.info("security manager " + securityManager);

	    //3.
	    SecurityUtils.setSecurityManager(securityManager);
	    
	    
	    ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
		
		MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
		
		
		ResourceManager.SINGLETON.register(Resource.DATA_STORE, (MongoDataStore) mdsc.createAPI(null, dsConfig));
		
		
		
		realm.setAPISecurityManager(apiSecurityManager);
		realm.setDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));
		
		appManager = new APIAppManagerProvider();
		
		appManager.setAPIDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));
		appManager.setAPISecurityManager(apiSecurityManager);

        
		
		
		try
		{
			
			createUser(SUPER_ADMIN, SUPER_PASSWORD);
			createSuperAdminRole();
			associateAdminRole();
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		createAPP(DOMAIN_ID, APP_ID);
		createAPP(PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID);
		
		
		try 
		{
			createUser(TEST_USER, TEST_PASSWORD);
			createUser(ILLEGAL_USER, ILLEGAL_PASSWORD);
//			if (appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, false) == null)
//			{
//				registerSubjectAPIKey(DEFAULT_API_KEY, TEST_USER, TEST_PASSWORD);
//				apiSecurityManager.logout();
//				registerSubjectAPIKey(null, ILLEGAL_USER, ILLEGAL_PASSWORD);
//				apiSecurityManager.logout();
//			}
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
	
	
	
	@Test
	public void createPropaneXPUser()
	{
		String admin = "appadmin@propanexp.com";
		String sp = "sp@propanexp.com";
		String pwd = "T1stpwd!";
		apiSecurityManager.logout();
		try {
			String adminUserID = createUser(admin, pwd).getSubjectID();
		
		
			String spUserID = createUser(sp, pwd).getSubjectID();//apiSecurityManager.currentUserID();
			
			
			apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
			
			ShiroAssociationRule sard = new ShiroAssociationRule();
			sard.setName(SecurityModel.Role.APP_ADMIN.getName());
			sard.setAssociatedTo(adminUserID);
			sard.setAssociate(SecurityModel.toSubjectID(PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, SecurityModel.Role.APP_ADMIN));
			sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
			//sard.setExpiration(null);
			apiSecurityManager.addShiroRule(sard);
			
			sard = new ShiroAssociationRule();
			sard.setName(SecurityModel.Role.APP_SERVICE_PROVIDER.getName());
			sard.setAssociatedTo(adminUserID);
			sard.setAssociate(SecurityModel.toSubjectID(PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, SecurityModel.Role.APP_SERVICE_PROVIDER));
			sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
			//sard.setExpiration(null);
			apiSecurityManager.addShiroRule(sard);
			
			
			
			
			sard = new ShiroAssociationRule();
			sard.setName(SecurityModel.Role.APP_SERVICE_PROVIDER.getName());
			sard.setAssociatedTo(spUserID);
			sard.setAssociate(SecurityModel.toSubjectID(PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, SecurityModel.Role.APP_SERVICE_PROVIDER));
			sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
			sard.setExpiration(null);
			apiSecurityManager.addShiroRule(sard);
			
			
			
			
			
			apiSecurityManager.logout();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		
		
		
		
	}
	
	
	
	@Test
	public void testPemissions()
	{
		apiSecurityManager.logout();
		//apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
		Subject currentUser = SecurityUtils.getSubject();
//		ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
		DomainUsernamePasswordToken token = new DomainUsernamePasswordToken(SUPER_ADMIN, SUPER_PASSWORD, false, null, DOMAIN_ID, APP_ID);
        //token.setAutoAuthenticationEnabled(autoLogin);

        //this is all you have to do to support 'remember me' (no config - built in!):
        token.setRememberMe(true);

        currentUser.login(token);
		
		
		
		//System.out.println("Principals:" + realm.getAuthenticationInfo(token).getClass().getName());
		System.out.println("Principals:" + currentUser.getPrincipals().getClass().getName());
		
		//System.out.println("AutorizationInfo:" + realm.lookupAuthorizationInfo(currentUser.getPrincipals()).getClass().getName());
		
		
		
		String permissions[]  = 
			{
	    		"nventity:read:batata",
	    		"write:batata", 
	    		"nventity:update:batata",
	    		"batata:update",
	    		"batata:update:all"
	    	};
	
		for (String permission : permissions)
		{
			System.out.println(permission + ":" +ShiroUtil.isPermitted(permission) );
		}
//		AuthorizationInfo ai =  realm.lookupAuthorizationInfo(currentUser.getPrincipals());
//		ai.getStringPermissions().add("write:batata");
//		System.out.println("=================================");
//		for (String permission : permissions)
//		{
//			System.out.println(permission + ":" +ShiroUtil.isPermitted(permission) );
//		}
		
		apiSecurityManager.logout();
	}
	
	
	
	
	 public static void registerSubjectAPIKey(String apiKey, String userID, String password)
	 {

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
        appDeviceDAO.setAppID(appIDDAO.getAppID());
		appDeviceDAO.setDomainID(appIDDAO.getDomainID());

        appDeviceDAO.setDevice(deviceDAO);
        if (apiKey != null)
        	appDeviceDAO.setSubjectID(apiKey);


        apiSecurityManager.logout();
        AppDeviceDAO temp = (AppDeviceDAO) appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, userID, password);
       
//        AppDeviceDAO val = appManager.lookupSubjectAPIKey(temp.getSubjectID(), false);
//        log.info(""+val);
    }
	
	private static UserIDDAO createUser(String subjectID, String password)
	{
		
		UserIDDAO userID = new UserIDDAO();
		
		userID.setSubjectID(subjectID);
		UserInfoDAO userInfo = new UserInfoDAO();
		userInfo.setFirstName("N/S");
		userInfo.setLastName("N/S");
		userID.setUserInfo(userInfo);
		return appManager.createUserIDDAO(userID, CryptoConst.SubjectStatus.ACTIVE, password);
		///appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, subjectID, password);
	}
	
	public static void createAPP(String domainID, String appID)
	{
//		String subjectID = SUPER_ADMIN;
//		String password  = SUPER_PASSWORD;
//		UserIDDAO userID = new UserIDDAO();
//		
//		userID.setSubjectID(subjectID);
//		UserInfoDAO userInfo = new UserInfoDAO();
//		userInfo.setFirstName("N/S");
//		userInfo.setLastName("N/S");
//		userID.setUserInfo(userInfo);
//		try
//		{
//			appManager.createUserIDDAO(userID, UserStatus.ACTIVE, password);
//		}
//		catch(Exception e)
//		{
//		}
//		
		apiSecurityManager.logout();
		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, null, null, false);
		
		AppIDDAO aid = appManager.createAppIDDAO(domainID, appID);
		log.info("App created:" + aid.toCanonicalID());
		apiSecurityManager.logout();
	}
	
	@Test
	public void loadKeySuccess()
	{
		
		apiSecurityManager.logout();
		apiSecurityManager.login(TEST_USER, TEST_PASSWORD, DOMAIN_ID, APP_ID, true);
	
		//appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, true);
		
	}
	
//	@Test()
//	public void loadKeyFailed()
//	{
//		assertThrows(AccessException.class, ()->{apiSecurityManager.logout();
//			apiSecurityManager.login(ILLEGAL_USER, ILLEGAL_PASSWORD, DOMAIN_ID, APP_ID, true);
//
//
//			appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, true);});
//
//
//	}
	
//	@Test
//	public void loadKeySuccessBySuperAdmin()
//	{
//
//		apiSecurityManager.logout();
//		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
//
//		appManager.lookupSubjectAPIKey(DEFAULT_API_KEY, true);
//
//	}
	
	public static void createSuperAdminRole()
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
		
		
		ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
		
		
		// set the permission manually
		AuthorizationInfo ai =  realm.lookupAuthorizationInfo(SecurityUtils.getSubject().getPrincipals());
		for (SecurityModel.Permission permission : SecurityModel.Permission.values())
		{
			ai.getStringPermissions().add(permission.getValue());
		}
		
		
//		ShiroRoleDAO superAdminRole = new ShiroRoleDAO(DOMAIN_ID, APP_ID, "super_admin_role", "Super admin role");
//		
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermissionDAO(DOMAIN_ID, APP_ID, "nve_read_all", "Read all nves", "nventity:read:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermissionDAO(DOMAIN_ID, APP_ID, "nve_delete_all", "Delete all nves", "nventity:delete:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermissionDAO(DOMAIN_ID, APP_ID, "nve_update_all", "Update all nves", "nventity:update:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermissionDAO(DOMAIN_ID, APP_ID, "nve_create_all", "Create all nves", "nventity:create:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermissionDAO(DOMAIN_ID, APP_ID, "nve_move_all", "Move all nves", "nventity:move:*")));
//		realm.addRole(superAdminRole);
		
		
		ShiroRole superAdminRole = SecurityModel.Role.SUPER_ADMIN.toRole(DOMAIN_ID, APP_ID);
		
		for (SecurityModel.Permission permission : SecurityModel.Permission.values())
		{
			ShiroPermission permDAO = permission.toPermission(DOMAIN_ID, APP_ID);
			apiSecurityManager.addPermission(permDAO);
			SecurityModel.Role.addPermission(superAdminRole, permDAO);
		}
		
		apiSecurityManager.addRole(superAdminRole);
		
		
		try {
			System.out.println(GSONUtil.toJSON(superAdminRole, true, false, false));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		apiSecurityManager.logout();

	}
	
	
	public static void associateAdminRole()
	{
		
		apiSecurityManager.logout();
		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
		
		log.info("USER ID********************************:" + ShiroUtil.subjectUserID());
		ShiroAssociationRule sard = new ShiroAssociationRule();
		sard.setAssociatedTo(ShiroUtil.subjectUserID());
		sard.setAssociate(SecurityModel.toSubjectID(DOMAIN_ID, APP_ID, Role.SUPER_ADMIN));
		sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
		sard.setName("SuperAdminRule");
		sard.setExpiration(null);
		sard.setAssociationStatus(Status.ACTIVE);
		
		
		
		
		apiSecurityManager.addShiroRule(sard);
		
		apiSecurityManager.logout();
		
		
	}
	
	@Test
	public void adminPermissionCheck()
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
		String permissions[]  = 
				{
		    		"nventity:read:batata",
		    		"write:batata", 
		    		"nventity:update:batata",
		    		"batata:update",
		    		"batata:update:all"
		    	};
		
		for (String permission : permissions)
		{
			System.out.println(permission + ":" +ShiroUtil.isPermitted(permission) );
		}
		
	}
	
	
	@Test
	public void appAdminPermissionCheck()
	{
		
		String admin = "appadmin@propanexp.com";
		//String sp = "sp@propanexp.com";
		String pwd = "T1stpwd!";
		apiSecurityManager.logout();
		apiSecurityManager.login(admin, pwd, PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, false);
//		String permissions[]  = 
//				{
//		    		"nventity:read:batata",
//		    		"write:batata", 
//		    		"nventity:update:batata",
//		    		"batata:update",
//		    		"batata:update:all"
//		    	};
//		
//		for (String permission : permissions)
//		{
//			
//			System.out.println(permission + ":" +ShiroUtil.isPermitted(permission) );
//		}

		System.out.println("*****************************************************************************");
		SecurityModel.Role[] roles = {SecurityModel.Role.APP_ADMIN, SecurityModel.Role.APP_USER, SecurityModel.Role.APP_SERVICE_PROVIDER};
		
		for (SecurityModel.Role role : roles) {
            String roleSubjectID = SecurityModel.toSubjectID(PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, role);
            System.out.println(roleSubjectID);
            System.out.println(SharedUtil.toCanonicalID('-', apiSecurityManager.currentSubjectID(), apiSecurityManager.currentUserID(), apiSecurityManager.currentDomainID(), apiSecurityManager.currentAppID()));

            if (apiSecurityManager.hasRole(roleSubjectID)) {
            	System.out.println("Role Exists: " + roleSubjectID);
                //appAccessMode.getRolesAsList().getValue().add(role);
            }
            
        }
		
		apiSecurityManager.logout();
	}

	@Test
    public void createHTTPMessageConfig() throws IOException {
        String admin = "appadmin@propanexp.com";
        //String sp = "sp@propanexp.com";
        String pwd = "T1stpwd!";
        apiSecurityManager.logout();
        apiSecurityManager.login(admin, pwd, PROPANEXP_DOMAIN_ID, PROPANEXP_APP_ID, false);

	    HTTPMessageConfig hmc = (HTTPMessageConfig) HTTPMessageConfig.createAndInit("http://localhost:8080", "/api", HTTPMethod.GET);
        hmc.setName("HTTPMessageConfig test");
        hmc.setHTTPParameterFormatter(HTTPEncoder.URI_REST_ENCODED);
        hmc.getParameters().add(new NVPair("name", "John Smith"));
        hmc.getParameters().add(new NVPair("year", "2017"));
        hmc.getHeaders().add(new NVPair("bozo", "the clown"));
        hmc.getHeaders().add(new NVPair("bozo", "the donkey"));
        hmc.getHeaders().add(new NVPair("toto", "the clown"));
       
        hmc = appManager.create(hmc);
        String str = GSONUtil.toJSON(hmc, true);
        List<HTTPMessageConfig> ret = appManager.search(HTTPMessageConfig.NVC_HTTP_MESSAGE_CONFIG, new QueryMatchString(RelationalOperator.EQUAL, hmc.getReferenceID(), HTTPMessageConfig.NVC_REFERENCE_ID));
        assert(ret.size() == 1);
        System.out.println(GSONUtil.toJSON(ret.get(0), true));
        assert(GSONUtil.toJSON(ret.get(0), true).equals(str));
        
        System.out.println(ret.get(0).getParameters().getClass().getName());
        System.out.println(ret.get(0).getHeaders().getClass().getName());
    }

}