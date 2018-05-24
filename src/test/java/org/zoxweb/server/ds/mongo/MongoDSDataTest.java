package org.zoxweb.server.ds.mongo;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoxweb.server.api.APIDocumentStore;
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

import org.zoxweb.shared.db.QueryMatchString;
import org.zoxweb.shared.security.JWT;
import org.zoxweb.shared.security.JWTHeader;
import org.zoxweb.shared.security.JWTPayload;
import org.zoxweb.shared.security.KeyStoreInfoDAO;
import org.zoxweb.shared.security.SecurityConsts.JWTAlgorithm;
import org.zoxweb.shared.util.Const.RelationalOperator;
import org.zoxweb.shared.util.NVBigDecimal;
import org.zoxweb.shared.util.NVBlob;
import org.zoxweb.shared.util.NVBoolean;
import org.zoxweb.shared.util.NVDouble;
import org.zoxweb.shared.util.NVEnum;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.NVStringList;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;

import java.io.FileInputStream;

import java.io.IOException;

import java.math.BigDecimal;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;


public class MongoDSDataTest {

    private static final String MONGO_CONF = "mongo_conf.json";
    private static final String KEYSTORE_INFO = "key_store_info.json";
    private static final String SHIRO_INI = "shiro.ini";

    private static final String SUPER_ADMIN = "superadmin@xlogistx.io";
    private static final String SUPER_PASSWORD = "T!st2s3r";
    private static final String DOMAIN_ID = "test.com";
    private static final String APP_ID = "testapp";

    protected static APISecurityManager<Subject> apiSecurityManager;
    protected static APIDocumentStore<?> documentStore;
    protected static APIDataStore<?> dataStore;

    @BeforeClass
    public static void start()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        KeyStoreInfoDAO ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(KEYSTORE_INFO)), KeyStoreInfoDAO.class);
        KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
                CryptoUtil.KEY_STORE_TYPE,
                ksid.getKeyStorePassword().toCharArray());

        KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());

        apiSecurityManager = new APISecurityManagerProvider();

        APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF)), APIConfigInfoDAO.class);
        dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
        dsConfig.setAPISecurityManager(apiSecurityManager);

        Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
        SecurityManager securityManager = factory.getInstance();
        SecurityUtils.setSecurityManager(securityManager);

        MongoDataStoreCreator mdsc = new MongoDataStoreCreator();

        ResourceManager.SINGLETON.map(Resource.DATA_STORE, mdsc.createAPI(null, dsConfig));
        dataStore = ResourceManager.SINGLETON.lookup(Resource.DATA_STORE);
        documentStore = ResourceManager.SINGLETON.lookup(Resource.DATA_STORE);

        ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
        realm.setAPISecurityManager(apiSecurityManager);
        realm.setDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));

        apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
    }

    @AfterClass
    public static void close()
    {
        apiSecurityManager.logout();
    }

    @Test
    public void testCreateNVGM()
    {
    	int index = 1;
    	JWT jwtHS256 = new JWT();
    	jwtHS256.setName("jwtHS256");
		JWTHeader header = jwtHS256.getHeader();
		
		header.setJWTAlgorithm(JWTAlgorithm.HS256);
		header.setTokenType("JWT");
		header.getNVGenericMap().add(new NVBoolean("boolean", true));
		header.getNVGenericMap().add(new NVEnum("enum", JWTAlgorithm.HS256));
		header.getNVGenericMap().add(new NVInt("int", 1000));
		header.getNVGenericMap().add(new NVLong("long", 1000000));
		header.getNVGenericMap().add(new NVFloat("float", (float) 32.554));
		header.getNVGenericMap().add(new NVDouble("double", 32.554));
		header.getNVGenericMap().add(new NVBlob("blob", new byte[] {0,1,2,3,4,5,6,7,8,9}));
		header.getNVGenericMap().add(new NVBigDecimal("bigdecimal", new BigDecimal(1254.5856584)));
		List<String> stringListValue = new ArrayList<String>();
		stringListValue.add("mario");
		stringListValue.add("taza");
		header.getNVGenericMap().add(new NVStringList("stringlist", stringListValue));
		
		JWTPayload payload = jwtHS256.getPayload();
		payload.setDomainID("xlogistx.io");
		payload.setAppID("xlogistx");
		payload.setNonce(index++);
		//payload.setRandom(new byte[] {0,1,2,3});
		payload.setSubjectID("support@xlogistx.io");
		
		
		
		
		JWT jwtNONE = new JWT();
		jwtNONE.setName("jwtNONE");
		header = jwtNONE.getHeader();
		header.setJWTAlgorithm(JWTAlgorithm.none);
		
		payload = jwtNONE.getPayload();
		payload.setDomainID("xlogistx.io");
		payload.setAppID("xlogistx");
		payload.setNonce(index++);
		//payload.setRandom(new byte[] {0,1,2,3});
		payload.setSubjectID("none@xlogistx.io");
		
	
		
		JWT jwtHS512 = new JWT();
		jwtHS512.setName("jwtHS512");
		
		header = jwtHS512.getHeader();
			
		header.setJWTAlgorithm(JWTAlgorithm.HS512);
		header.setTokenType("JWT");
	
		payload =jwtHS512.getPayload();
		payload.setDomainID("xlogistx.io");
		payload.setAppID("xlogistx");
		payload.setNonce(index++);
		//payload.setRandom(new byte[] {0,1,2,3});
		payload.setSubjectID("support@xlogistx.io");
		dataStore.insert(jwtHS256);
		dataStore.insert(jwtNONE);
		dataStore.insert(jwtHS512);
        
    }
    
    @Test
    public void testReadNVGM() throws IOException
    {
    	List<JWT> results = dataStore.search(JWT.NVC_JWT, null, new QueryMatchString(RelationalOperator.EQUAL, "jwtHS256", "name" ));
    	assertNotNull(results);
    	for (JWT jwt : results)
    	{
    		System.out.println(GSONUtil.toJSON(jwt, true, false, false));
    	}
    	
    }

  
}
