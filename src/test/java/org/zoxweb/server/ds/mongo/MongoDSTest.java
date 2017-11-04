package org.zoxweb.server.ds.mongo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Collection;
import java.util.logging.Logger;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.RealmSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.shiro.DefaultAPISecurityManager;
import org.zoxweb.server.security.shiro.ShiroUtil;
import org.zoxweb.server.security.shiro.authc.DomainUsernamePasswordToken;
import org.zoxweb.server.util.ApplicationConfigManager;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.data.ApplicationConfigDAO;
import org.zoxweb.shared.data.StatCounter;

public class MongoDSTest 
{
	
	private static final transient Logger log = Logger.getLogger(MongoDSTest.class.getName());
	
	public static  void main(String ...args)
	{
		log.info("started");
		
//		LogManager.getLogManager().getLogger("").setLevel(Level.OFF);
//		log.info("after update");
//		log.severe("Maniac");
		try
		{
			//int index = 0;
			// get the app config file name
			
			
			
			//String configFile = args[index++];
			
			ApplicationConfigDAO appConfig = ApplicationConfigManager.SINGLETON.loadDefault();
			System.out.println(appConfig);
		
			// load the keystore for MS
			String mongoConfigName = appConfig.lookupValue("mongod_xlogistx_conf");
			System.out.println(":" + mongoConfigName);
			
			APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(ApplicationConfigManager.SINGLETON.readConfigurationContent(appConfig, mongoConfigName), APIConfigInfoDAO.class);
			
			
			
			
			File keyStoreFile = ApplicationConfigManager.SINGLETON.locateFile(null, "key_store");
			if (keyStoreFile == null || !keyStoreFile.isFile())
			{
				throw new IOException("cache dir " + keyStoreFile + " is not a directory");
			}
			KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(keyStoreFile),
				 	  CryptoUtil.KEY_STORE_TYPE,
				 	  ApplicationConfigManager.SINGLETON.loadDefault().lookupValue("key_store_password").toCharArray());
			

			
			KeyMakerProvider.SINGLETON.setMasterKey(ks, ApplicationConfigManager.SINGLETON.loadDefault().lookupValue("mk_alias"),
					 ApplicationConfigManager.SINGLETON.loadDefault().lookupValue("mk_alias_password"));
			// setup the ms
			// load the mongo config file
			// create the data store
			dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
			APISecurityManager<Subject> apiSecurityManager = new DefaultAPISecurityManager();
			dsConfig.setAPISecurityManager(apiSecurityManager);
			
			
			
			
			
			int index = 0;
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
		    
		    
		    Subject currentUser = SecurityUtils.getSubject();
		    Session session = currentUser.getSession();
		    session.setAttribute( "someKey", "aValue" );

		    String subjectID = args[index++];
		    String password  = args[index++];
		    if ( !currentUser.isAuthenticated() ) {
		        //collect user principals and credentials in a gui specific manner
		        //such as username/password html form, X509 certificate, OpenID, etc.
		        //We'll use the username/password example here since it is the most common.
		        UsernamePasswordToken token = new DomainUsernamePasswordToken(subjectID, password, false, null, null, null);

		        //this is all you have to do to support 'remember me' (no config - built in!):
		        token.setRememberMe(true);

		        currentUser.login(token);
		    }
		    
		    
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
		    
			
		    log.info(""+SecurityUtils.getSubject().getPrincipals().getClass());
			
		
			ds.createSequence("mz");
			StatCounter stat = new StatCounter();
			for (int i=0; i<100; i++)
			{
				ds.nextSequenceValue("MZ");
				stat.increment();
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
