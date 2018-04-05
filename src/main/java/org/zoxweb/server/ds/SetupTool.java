package org.zoxweb.server.ds;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.logging.Logger;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.Factory;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.mongo.MongoDataStore;
import org.zoxweb.server.ds.mongo.MongoDataStoreCreator;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;

import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.UserIDCredentialsDAO.UserStatus;
import org.zoxweb.server.security.shiro.APISecurityManagerProvider;
import org.zoxweb.server.security.shiro.ShiroUtil;
import org.zoxweb.server.task.TaskUtil;
import org.zoxweb.server.util.ApplicationConfigManager;
import org.zoxweb.server.util.GSONUtil;

import org.zoxweb.shared.api.APIAppManager;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.data.AppIDDAO;
import org.zoxweb.shared.data.ApplicationConfigDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.security.KeyStoreInfoDAO;
import org.zoxweb.shared.security.model.PPEncoder;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.security.model.SecurityModel.PermissionToken;
import org.zoxweb.shared.security.model.SecurityModel.Role;
import org.zoxweb.shared.security.shiro.ShiroAssociationRuleDAO;
import org.zoxweb.shared.security.shiro.ShiroAssociationType;
import org.zoxweb.shared.security.shiro.ShiroPermissionDAO;
import org.zoxweb.shared.security.shiro.ShiroRoleDAO;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.Const.Status;
import org.zoxweb.shared.util.ResourceManager.Resource;

/**
 * The setup tool is used to create and setup a new system
 * @author javaconsigliere
 *
 */


public class SetupTool 
{
	
	private static final transient Logger log = Logger.getLogger(SetupTool.class.getName());
	public static final String MONGO_CONF = "mongod_conf";
	public static final String KEYSTORE_INFO = "key_store_info";
	public static final String SHIRO_INI = "shiro_ini";
	
	private ApplicationConfigDAO appConfig = null; 
	private APIConfigInfoDAO dsConfig = null;
	private APIAppManager appManager = null;
	APISecurityManagerProvider apiSecurityManager = null;
	
	
	
	
	public void createAppID(String subjectID, String password, String domainID, String appID)
	{


		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, null, null, false);
		
		AppIDDAO aid = appManager.createAppIDDAO(domainID, appID);
		log.info("App created:" + aid.getAppGID());
		apiSecurityManager.logout();
	}
	
	public void associateAdminRole(String subjectID, String password, String domainID, String appID)
	{
		
		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, domainID, appID, true);
		
		log.info("USER ID********************************:" + ShiroUtil.subjectUserID());
		ShiroAssociationRuleDAO sard = new ShiroAssociationRuleDAO();
		sard.setAssociatedTo(ShiroUtil.subjectUserID());
		sard.setAssociate(SecurityModel.toSubjectID(domainID, appID, Role.SUPER_ADMIN));
		sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
		sard.setName("SuperAdminRule");
		sard.setExpiration(null);
		sard.setAssociationStatus(Status.ACTIVE);
		
		
		
		
		apiSecurityManager.addShiroRule(sard);
		
		apiSecurityManager.logout();
		
		
	}
	
	
	public void createSuperAdminRole(String subjectID, String password, String domainID, String appID)
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, domainID, appID, true);
		
		
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
		
		
		ShiroRoleDAO superAdminRole = SecurityModel.Role.SUPER_ADMIN.toRole(domainID, appID);
		
		for (SecurityModel.Permission permission : SecurityModel.Permission.values())
		{
			ShiroPermissionDAO permDAO = permission.toPermission(domainID, appID);
			
			permDAO.setPermissionPattern(PPEncoder.SINGLETON.encodePattern(permDAO.getPermissionPattern(), PermissionToken.APP_ID, "*"));
			
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
	
	
	public SetupTool initApp() throws NullPointerException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
	{
		
		appConfig = ApplicationConfigManager.SINGLETON.loadDefault();
		
		
		
		log.info("" + appConfig);
		
		
		 dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(ApplicationConfigManager.SINGLETON.locateFile(appConfig, MONGO_CONF)), APIConfigInfoDAO.class);
         
         // load the Master Key
         KeyStoreInfoDAO ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(ApplicationConfigManager.SINGLETON.locateFile(appConfig, KEYSTORE_INFO)), KeyStoreInfoDAO.class);

         KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(ApplicationConfigManager.locateFile(ksid.getKeyStore())),
         		ksid.getKeyStoreType(),
                 ksid.getKeyStorePassword().toCharArray());


         KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());
         // setup the ms
         // load the mongo config file
         // create the data store
         dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
         apiSecurityManager = new APISecurityManagerProvider();
         dsConfig.setAPISecurityManager(apiSecurityManager);

//		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
//
//	    //2.
//	    SecurityManager securityManager = factory.getInstance();
//	    log.info("security manager " + securityManager);
//
//	    //3.
//	    SecurityUtils.setSecurityManager(securityManager);


         //ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);

         MongoDataStoreCreator mdsc = new MongoDataStoreCreator();


         ResourceManager.SINGLETON.map(Resource.DATA_STORE, (MongoDataStore) mdsc.createAPI(null, dsConfig));
//		realm.setAPISecurityManager(apiSecurityManager);
//		realm.setDataStore(DefaultMongoDS.SIGLETON.getDataStore());

         appManager = new APIAppManagerProvider();

         appManager.setAPIDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
         appManager.setAPISecurityManager(apiSecurityManager);
         ResourceManager.SINGLETON.map(Resource.API_APP_MANAGER, appManager);
         ResourceManager.SINGLETON.map(Resource.API_SECURITY_MANAGER, apiSecurityManager);
         TaskUtil.getDefaultTaskProcessor();
         TaskUtil.getDefaultTaskScheduler();
         
         
         File shiroFile = ApplicationConfigManager.locateFile(appConfig.lookupValue(SHIRO_INI));
			
		 Factory<SecurityManager> factory = new IniSecurityManagerFactory(shiroFile.getAbsolutePath());

		    //2.
		    SecurityManager securityManager = factory.getInstance();
		    log.info("security manager " + securityManager);

		    //3.
		    SecurityUtils.setSecurityManager(securityManager);
		    
		    
		    ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
			
		
			
			
			
			realm.setAPISecurityManager(apiSecurityManager);
			realm.setDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
			
			appManager = new APIAppManagerProvider();
			
			appManager.setAPIDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
			appManager.setAPISecurityManager(apiSecurityManager);

	        
			
			
			return this;
	}
	
	
	private UserIDDAO createUser(String subjectID, String name, String lastname, String password)
	{
		
		UserIDDAO userID = new UserIDDAO();
		
		userID.setSubjectID(subjectID);
		UserInfoDAO userInfo = new UserInfoDAO();
		userInfo.setFirstName(name);
		userInfo.setLastName(lastname);
		userID.setUserInfo(userInfo);
		APIAppManager appManager = ResourceManager.SINGLETON.lookup(Resource.API_APP_MANAGER);
		return appManager.createUserIDDAO(userID, UserStatus.ACTIVE, password);
		///appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, subjectID, password);
	}
	
	
	private static void usage()
	{
		System.err.println("setup subjectid password domain appID name lastname");
		System.err.println("create_app subjectid password domain appID");
	}
	
	public static void main(String[] args) 
	{
		try
		{
			SetupTool setupTool = new SetupTool().initApp();
			int index = 0;
			String command = args[index++];
			String subjectID = args[index++];
			String password = args[index++];
			String domainID = args[index++];
			String appID = args[index++];
		
			
			
			switch(command.toLowerCase())
			{
			case "setup":
				String name = args[index++];
				String lastname = args[index++];
				
				setupTool.createUser(subjectID, name, lastname, password);
				setupTool.createSuperAdminRole(subjectID, password, domainID, appID);
				setupTool.associateAdminRole(subjectID, password, domainID, appID);
				break;
			case "create_app":
				setupTool.createAppID(subjectID, password, domainID, appID);
				break;
				
				default:
					System.err.println("No command found");
					usage();
					
			}
			
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
			usage();
		}
		
		System.exit(0);
	}

}
