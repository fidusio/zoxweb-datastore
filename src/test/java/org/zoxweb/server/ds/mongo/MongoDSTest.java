package org.zoxweb.server.ds.mongo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;

import java.util.logging.Logger;

import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.shiro.DefaultAPISecurityManager;
import org.zoxweb.server.util.ApplicationConfigManager;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.data.ApplicationConfigDAO;

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
			dsConfig.setAPISecurityManager(new DefaultAPISecurityManager());
			
			
			
			MongoDataStoreCreator mdsc = new MongoDataStoreCreator();
			APIDataStore<?> ds = mdsc.createAPI(null, dsConfig);
			ds.createSequence("mz");
			System.out.println(ds.nextSequenceValue("MZ"));
			
			
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
