package org.zoxweb.server.ds;

import io.xlogistx.shiro.APISecurityManagerProvider;
import io.xlogistx.shiro.ShiroUtil;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.mongo.MongoDataStore;
import org.zoxweb.server.ds.mongo.MongoDataStoreCreator;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.task.TaskUtil;
import org.zoxweb.server.util.ApplicationConfigManager;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIAppManager;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.data.AppIDDAO;
import org.zoxweb.shared.data.ApplicationConfigDAO;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.security.KeyStoreInfo;
import org.zoxweb.shared.security.model.PPEncoder;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.security.model.SecurityModel.PermissionToken;
import org.zoxweb.shared.security.model.SecurityModel.Role;
import org.zoxweb.shared.security.shiro.ShiroAssociationRule;
import org.zoxweb.shared.security.shiro.ShiroAssociationType;
import org.zoxweb.shared.security.shiro.ShiroPermission;
import org.zoxweb.shared.security.shiro.ShiroRole;
import org.zoxweb.shared.util.Const.Status;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashSet;
import java.util.Set;

/**
 * The setup tool is used to create and setup a new system
 * @author javaconsigliere
 *
 */
public class SetupTool 
{
	public static final LogWrapper log = new LogWrapper(SetupTool.class);

	public static final String MONGO_CONF = "mongod_conf";
	public static final String KEYSTORE_INFO = "key_store_info";
	public static final String SHIRO_INI = "shiro_ini";
	
	private ApplicationConfigDAO appConfig = null; 
	private APIConfigInfoDAO dsConfig = null;
	public APIAppManager appManager = null;
	public APISecurityManager<Subject> apiSecurityManager = null;
	
	private SetupTool()
	{
		
	}

	public void createAppID(String subjectID, String password, String domainID, String appID)
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, null, null, false);
		
		AppIDDAO aid = appManager.createAppIDDAO(domainID, appID);
		if(log.isEnabled()) log.getLogger().info("App created:" + aid.getAppGID());
		apiSecurityManager.logout();
	}
	
	public void assignSuperAdminRole(String subjectID, String password, String domainID, String appID)
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, domainID, appID, true);
		
		if(log.isEnabled()) log.getLogger().info("USER ID********************************:" + ShiroUtil.subjectUserID());
		ShiroAssociationRule sard = new ShiroAssociationRule();
		sard.setAssociatedTo(ShiroUtil.subjectUserID());
		sard.setAssociate(SecurityModel.toSubjectID(domainID, appID, Role.SUPER_ADMIN));
		sard.setAssociationType(ShiroAssociationType.ROLE_TO_SUBJECT);
		sard.setName("SuperAdminRule");
		sard.setExpiration(null);
		sard.setAssociationStatus(Status.ACTIVE);

		apiSecurityManager.addShiroRule(sard);
		apiSecurityManager.logout();
	}

	public void createBasicRoles(String subjectID, String password, String domainID, String appID)
	{
		apiSecurityManager.logout();
		apiSecurityManager.login(subjectID, password, domainID, appID, true);

		// set the permission manually
		AuthorizationInfo ai =  ShiroUtil.lookupAuthorizationInfo(SecurityUtils.getSubject().getPrincipals());
		for (SecurityModel.Permission permission : SecurityModel.Permission.values())
		{
			ai.getStringPermissions().add(permission.getValue());
		}

//		ShiroRoleDAO superAdminRole = new ShiroRoleDAO(DOMAIN_ID, APP_ID, "super_admin_role", "Super admin role");
//		
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermission(DOMAIN_ID, APP_ID, "nve_read_all", "Read all nves", "nventity:read:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermission(DOMAIN_ID, APP_ID, "nve_delete_all", "Delete all nves", "nventity:delete:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermission(DOMAIN_ID, APP_ID, "nve_update_all", "Update all nves", "nventity:update:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermission(DOMAIN_ID, APP_ID, "nve_create_all", "Create all nves", "nventity:create:*")));
//		superAdminRole.getPermissions().add(realm.addPermission(new ShiroPermission(DOMAIN_ID, APP_ID, "nve_move_all", "Move all nves", "nventity:move:*")));
//		realm.addRole(superAdminRole);
		
		
		ShiroRole superAdminRole = SecurityModel.Role.SUPER_ADMIN.toRole(domainID, appID);
		Set<SecurityModel.Permission> exclusion = new HashSet<SecurityModel.Permission>();
		exclusion.add(SecurityModel.Permission.RESOURCE_READ_PRIVATE);
		exclusion.add(SecurityModel.Permission.RESOURCE_READ_PUBLIC);
		exclusion.add(SecurityModel.Permission.NVE_CREATE_ALL);
		exclusion.add(SecurityModel.Permission.NVE_DELETE_ALL);
		exclusion.add(SecurityModel.Permission.NVE_UPDATE_ALL);
		exclusion.add(SecurityModel.Permission.NVE_READ_ALL);
		exclusion.add(SecurityModel.Permission.SELF_USER);
		PermissionToken pTokens[] = {PermissionToken.APP_ID, PermissionToken.RESOURCE_ID};
		for (SecurityModel.Permission permission : SecurityModel.Permission.values())
		{
			if (!exclusion.contains(permission))
			{
				ShiroPermission permDAO = permission.toPermission(domainID, appID);
				
				for (PermissionToken pr: pTokens)
					permDAO.setPermissionPattern(PPEncoder.SINGLETON.encodePattern(permDAO.getPermissionPattern(), pr, "*"));
				
				//apiSecurityManager.addPermission(permDAO);
				SecurityModel.Role.addPermission(superAdminRole, permDAO);
			}
		}
		SecurityModel.Role.addPermission(superAdminRole, SecurityModel.toPermission(domainID, appID, "assign_all", "Assign super admin mode", SecurityModel.PERM_ASSIGN_PERMISSION + ":*"));
		
		apiSecurityManager.addRole(superAdminRole);
		
		
		ShiroRole userRole = SecurityModel.Role.USER.toRole(domainID, appID);
		ShiroPermission permDAO = apiSecurityManager.lookupPermission(SecurityModel.Permission.SELF.toPermission(domainID, appID).getSubjectID());
		
		SecurityModel.Role.addPermission(userRole, permDAO);
		permDAO = SecurityModel.Permission.SELF_USER.toPermission(domainID, appID);
		apiSecurityManager.addPermission(permDAO);
		SecurityModel.Role.addPermission(userRole, permDAO);
		apiSecurityManager.addRole(userRole);
	
//		ShiroRole resourceRole = SecurityModel.Role.RESOURCE_ROLE.toRole(domainID, appID);
//		permDAO = SecurityModel.AppPermission.RESOURCE_READ_PRIVATE.toPermission(domainID, appID);
//		apiSecurityManager.addPermission(permDAO);
//		SecurityModel.Role.addPermission(resourceRole, permDAO);
//		permDAO = SecurityModel.AppPermission.RESOURCE_READ_PUBLIC.toPermission(domainID, appID);
//		apiSecurityManager.addPermission(permDAO);
//		SecurityModel.Role.addPermission(resourceRole, permDAO);	
//		apiSecurityManager.addRole(resourceRole);
		
		try {
			System.out.println(GSONUtil.toJSON(superAdminRole, true, false, false));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		apiSecurityManager.logout();

	}
	
	
	public static SetupTool initApp() throws NullPointerException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException
	{
		
		SetupTool ret = new SetupTool();
		ret.appConfig = ApplicationConfigManager.SINGLETON.loadDefault();
		
		
		
		if(log.isEnabled()) log.getLogger().info("" + ret.appConfig);
		
		
		ret.dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(ApplicationConfigManager.SINGLETON.locateFile(ret.appConfig, MONGO_CONF)), APIConfigInfoDAO.class);
         
         // load the Master Key
         KeyStoreInfo ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(ApplicationConfigManager.SINGLETON.locateFile(ret.appConfig, KEYSTORE_INFO)), KeyStoreInfo.class);

         KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(ApplicationConfigManager.locateFile(ksid.getKeyStore())),
         		ksid.getKeyStoreType(),
                 ksid.getKeyStorePassword().toCharArray());


         KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());
         // setup the ms
         // load the mongo config file
         // create the data store
         ret.dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
         ret.apiSecurityManager = new APISecurityManagerProvider();
         ret.dsConfig.setAPISecurityManager(ret.apiSecurityManager);

//		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
//
//	    //2.
//	    SecurityManager securityManager = factory.getInstance();
//	    if(log.isEnabled()) log.getLogger().info("security manager " + securityManager);
//
//	    //3.
//	    SecurityUtils.setSecurityManager(securityManager);


         //ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);

         MongoDataStoreCreator mdsc = new MongoDataStoreCreator();


         ResourceManager.SINGLETON.register(Resource.DATA_STORE, (MongoDataStore) mdsc.createAPI(null, ret.dsConfig));
//		realm.setAPISecurityManager(apiSecurityManager);
//		realm.setDataStore(DefaultMongoDS.SIGLETON.getDataStore());

         ret.appManager = new APIAppManagerProvider();

         ret.appManager.setAPIDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));
         ret.appManager.setAPISecurityManager(ret.apiSecurityManager);
         ResourceManager.SINGLETON.register(Resource.API_APP_MANAGER, ret.appManager);
         ResourceManager.SINGLETON.register(Resource.API_SECURITY_MANAGER, ret.apiSecurityManager);
         TaskUtil.defaultTaskProcessor();
         TaskUtil.defaultTaskScheduler();
         
         
         File shiroFile = ApplicationConfigManager.locateFile(ret.appConfig.lookupValue(SHIRO_INI));
			
		 Factory<SecurityManager> factory = new IniSecurityManagerFactory(shiroFile.getAbsolutePath());

		    //2.
		    SecurityManager securityManager = factory.getInstance();
		    if(log.isEnabled()) log.getLogger().info("security manager " + securityManager);

		    //3.
		    SecurityUtils.setSecurityManager(securityManager);
		    
		    
		    ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
			
		
			
			
			
			realm.setAPISecurityManager(ret.apiSecurityManager);
			realm.setDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));
			
			ret.appManager = new APIAppManagerProvider();
			
			ret.appManager.setAPIDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));
			ret.appManager.setAPISecurityManager(ret.apiSecurityManager);

	        
			
			
			return ret;
	}
	
	
	private UserIDDAO createUser(String subjectID, String name, String lastname, String password)
	{
		
		UserIDDAO userID = new UserIDDAO();
		
		userID.setSubjectID(subjectID);
		UserInfoDAO userInfo = new UserInfoDAO();
		userInfo.setFirstName(name);
		userInfo.setLastName(lastname);
		userID.setUserInfo(userInfo);
		APIAppManager appManager = ResourceManager.lookupResource(Resource.API_APP_MANAGER);
		return appManager.createUserIDDAO(userID, CryptoConst.SubjectStatus.ACTIVE, password);
		///appManager.registerSubjectAPIKey(userInfoDAO, appDeviceDAO, subjectID, password);
	}
	
	public void readUserAuthz(String subjectID, String password, String domainID, String appID)
	{
		
		apiSecurityManager.login(subjectID, password, domainID, appID, false);
		Subject subject = SecurityUtils.getSubject();
		
		// set the permission manually
		AuthorizationInfo ai =  ShiroUtil.lookupAuthorizationInfo(subject);
		for (String role : ai.getRoles())
		{
			System.out.println("Role: " + role);
		}
		for(String permission : ai.getStringPermissions())
		{
			System.out.println("Permission: " + permission);
		}
		apiSecurityManager.logout();
				
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
			SetupTool setupTool = initApp();
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
				setupTool.createBasicRoles(subjectID, password, domainID, appID);
				setupTool.assignSuperAdminRole(subjectID, password, domainID, appID);
				break;
			case "create_app":
				setupTool.createAppID(subjectID, password, domainID, appID);
				break;
			case "read_user_authz":
				setupTool.readUserAuthz(subjectID, password, domainID, appID);
				
				
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
