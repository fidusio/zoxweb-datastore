package org.zoxweb.server.ds.mongo;

import java.io.File;
import java.io.FileInputStream;

import java.security.KeyStore;
import java.util.Collection;
import java.util.logging.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.RealmSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.shiro.APISecurityManagerProvider;
import org.zoxweb.server.security.shiro.ShiroUtil;

import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.data.StatCounter;
import org.zoxweb.shared.data.UserIDDAO;
import org.zoxweb.shared.data.UserInfoDAO;
import org.zoxweb.shared.security.KeyStoreInfoDAO;
import org.zoxweb.shared.security.SecurityConsts;
import org.zoxweb.shared.security.SubjectAPIKey;

public class MongoDSTest 
{
	
	private static final transient Logger log = Logger.getLogger(MongoDSTest.class.getName());
	
	
	
	
	public static  void main(String ...args)
	{
		log.info("started");
		

		try
		{
		
			int index = 0;
			
			// load the mongo db config file
			APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(args[index++])), APIConfigInfoDAO.class);
			
			// load the Master Key
			KeyStoreInfoDAO ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(args[index++])), KeyStoreInfoDAO.class);
			
			KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
				 	  CryptoUtil.KEY_STORE_TYPE,
				 	  ksid.getKeyStorePassword().toCharArray());
			
			System.out.println(GSONUtil.toJSON(CryptoUtil.generateKeyStoreInfo("test", "test", CryptoUtil.PKCS12), true, false, false));
			
			KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());
			// setup the ms
			// load the mongo config file
			// create the data store
			dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
			APISecurityManager<Subject> apiSecurityManager = new APISecurityManagerProvider();
			dsConfig.setAPISecurityManager(apiSecurityManager);
			
			
			
			
			
			
			ClassLoader classLoader = MongoDSTest.class.getClassLoader();
			String filename = args[index++];
			File file = new File(classLoader.getResource(filename).getFile());
			System.out.println(IOUtil.inputStreamToString(file));
			
			
	
			//1.
		    Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + filename);

		    //2.
		    SecurityManager securityManager = factory.getInstance();
		    log.info("security manager " + securityManager);

		    //3.
		    SecurityUtils.setSecurityManager(securityManager);
		    
		    
		    ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
			
			MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
			APIDataStore<?> ds = mdsc.createAPI(null, dsConfig);
			
			realm.setAPISecurityManager(apiSecurityManager);
			realm.setDataStore(ds);
			
			APIAppManagerProvider appManager = new APIAppManagerProvider();
			
			appManager.setAPIDataStore(ds);
			appManager.setAPISecurityManager(apiSecurityManager);
			
			
			
			
		    
			String subjectID = args[index++];
			String password  = args[index++];
			UserIDDAO userID = new UserIDDAO();
			
			userID.setPrimaryEmail(subjectID);
			UserInfoDAO userInfo = new UserInfoDAO();
			userInfo.setFirstName("N/S");
			userInfo.setLastName("N/S");
			userID.setUserInfo(userInfo);
			appManager.createUserIDDAO(userID, SecurityConsts.UserStatus.ACTIVE, password);
			
		    
			Subject currentUser = apiSecurityManager.login(subjectID, password, null, null, false);
//		    Session session = currentUser.getSession();
//		    session.setAttribute( "someKey", "aValue" );
//
//		  
//		    if (!currentUser.isAuthenticated() ) {
//		        //collect user principals and credentials in a gui specific manner
//		        //such as username/password html form, X509 certificate, OpenID, etc.
//		        //We'll use the username/password example here since it is the most common.
//		    	DomainUsernamePasswordToken token = new DomainUsernamePasswordToken(subjectID, password, false, null, null, null);
//		        //token.setAutoAuthenticationEnabled(true);
//
//		        //this is all you have to do to support 'remember me' (no config - built in!):
//		        token.setRememberMe(true);
//
//		        currentUser.login(token);
//		        log.info(""+SecurityUtils.getSubject().getPrincipals().getClass());
//		    }
		    
		    
		    Collection<Realm> realms =  ((RealmSecurityManager)SecurityUtils.getSecurityManager()).getRealms();
		    System.out.println(realms);
		    
		    String permissions[]=
		    	{
		    		"read:batata",
		    		"write:batata", 
		    		"update:batata",
		    		"batata:update",
		    		"batata:update:all"
		    	};
		    
		    StatCounter sc = new StatCounter();
		    for(int i=0 ; i < 1; i++)
		    for (String permission : permissions)
		    {
		    		//currentUser.isPermitted(permission);
		    		
		    		System.out.println(permission + " stat:" + currentUser.isPermitted(permission));
		    }
		    System.out.println(sc.deltaSinceCreation());
		    
			
		    
			
		    appManager.createSubjectAPIKey(new SubjectAPIKey());
			ds.createSequence("mz");
			StatCounter stat = new StatCounter();
			for (int i=0; i<100; i++)
			{
				ds.nextSequenceValue("MZ");
				stat.increment();
			}
			
			
			
			if (index + 1 < args.length && args[index++].equals("-d"))
			{
				// we need to delete a user
				appManager.deleteUser(args[index++]);
			}
			
			
			System.out.println("it took:" + stat.deltaSinceCreation() + " " + ds.currentSequenceValue("MZ"));
			
			
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		System.exit(0);
	}
}
