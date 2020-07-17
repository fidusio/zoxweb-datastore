package org.zoxweb.server.ds.mongo;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.api.APIAppManagerProvider;
import org.zoxweb.shared.api.APIDocumentStore;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.security.shiro.APISecurityManagerProvider;
import org.zoxweb.server.security.shiro.ShiroUtil;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIAppManager;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDataStore;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.crypto.EncryptedDAO;
import org.zoxweb.shared.crypto.EncryptedKeyDAO;
import org.zoxweb.shared.data.AppIDDAO;
import org.zoxweb.shared.data.Range;
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
import org.zoxweb.shared.util.NVConfigEntity;
import org.zoxweb.shared.util.NVDouble;
import org.zoxweb.shared.util.NVEnum;
import org.zoxweb.shared.util.NVFloat;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.NVInt;
import org.zoxweb.shared.util.NVLong;
import org.zoxweb.shared.util.NVPair;
import org.zoxweb.shared.util.NVStringList;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;
import org.zoxweb.shared.util.SharedStringUtil;
import java.io.FileInputStream;

import java.io.IOException;

import java.math.BigDecimal;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import static org.junit.jupiter.api.Assertions.assertNotNull;


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
    protected static APIAppManager appManager = new APIAppManagerProvider();

    @BeforeAll
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
        

        appManager.setAPIDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));
        appManager.setAPISecurityManager(apiSecurityManager);

        ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
        realm.setAPISecurityManager(apiSecurityManager);
        realm.setDataStore(ResourceManager.SINGLETON.lookup(Resource.DATA_STORE));

        apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
    }

    @AfterAll
    public static void close()
    {
        apiSecurityManager.logout();
    }

    @Test
    public void testCreateNVGM() throws IOException
    {
    	int index = 1;
    	JWT jwtHS256 = new JWT();
    	jwtHS256.setName("jwtHS256");
		JWTHeader header = jwtHS256.getHeader();
		
		header.setJWTAlgorithm(JWTAlgorithm.HS256);
		header.setTokenType("JWT");
		header.getProperties().add(new NVBoolean("boolean", true));
		header.getProperties().add(new NVEnum("enum", JWTAlgorithm.HS256));
		header.getProperties().add(new NVInt("int", 1000));
		header.getProperties().add(new NVLong("long", 1000000));
		header.getProperties().add(new NVFloat("float", (float) 32.554));
		header.getProperties().add(new NVDouble("double", 32.554));
		header.getProperties().add(new NVBlob("blob", new byte[] {0,1,2,3,4,5,6,7,8,9}));
		header.getProperties().add(new NVBigDecimal("bigdecimal", new BigDecimal(1254.5856584)));
		List<String> stringListValue = new ArrayList<String>();
		stringListValue.add("mario");
		stringListValue.add("taza");
		header.getProperties().add(new NVStringList("stringlist", stringListValue));
		AppIDDAO appID = appManager.lookupAppIDDAO("batata.com", "banadoura", false);
		if (appID == null)
		{
			appID = new AppIDDAO("batata.com", "banadoura");
		}
		
		
		header.getProperties().add("nve", appID);
		
		NVGenericMap inner = new NVGenericMap("innerNVG");
		inner.add(new NVLong("innerLong", 5000));
		inner.add("innerString", "v");
		
		
		header.getProperties().add(inner);
		
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
		System.out.println(GSONUtil.toJSON(jwtHS256, true, false, false));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRangeInt()
    {
        Range<Integer> intRange = new Range<Integer>(1,1000);
        intRange.setName("INT_RANGE");
        dataStore.insert(intRange);
        System.out.println(intRange.getReferenceID());
        intRange = (Range<Integer>) dataStore.searchByID(Range.class.getName(), intRange.getReferenceID()).get(0);
        System.out.println(intRange + " " + intRange.getStart().getClass() + " " + intRange.getEnd().getClass());
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testRangeLong()
    {
        Range<Long> longRange = new Range<Long>(1l, 5000l);
        longRange.setName("LONG_RANGE");
        dataStore.insert(longRange);
        System.out.println(longRange.getReferenceID());
        longRange = (Range<Long>) dataStore.searchByID(Range.class.getName(), longRange.getReferenceID()).get(0);
        System.out.println(longRange + " " + longRange.getStart().getClass() + " " + longRange.getEnd().getClass());
    }
    
    @Test
    public void testEncryptedKeyDOA() throws InvalidKeyException, NullPointerException, IllegalArgumentException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException
    {
      EncryptedDAO ed = CryptoUtil.encryptDAO(new EncryptedKeyDAO(), SharedStringUtil.getBytes("PASSWORD"), null, 1);
      ed.getSubjectProperties().add(new NVPair("mario", "taza"));
      ed.getAlgoProperties().add(new NVPair("taza", "mario"));
      ed.getAlgoProperties().add(new NVInt("int", -2));
      ed.getAlgoProperties().add(new NVLong("long", Long.MAX_VALUE));
      ed.getAlgoProperties().add(new NVFloat("float", (float) 3.54));
      ed.getAlgoProperties().add(new NVDouble("double",  Double.MIN_VALUE));
      ed.getAlgoProperties().add(new NVBoolean("bolean",  false));
      ed.getAlgoProperties().add(new NVBlob("blob",  "password".getBytes()));
      System.out.println(GSONUtil.toJSON(ed, false));
      ed = dataStore.insert(ed);
      System.out.println(GSONUtil.toJSON(ed, false));
      System.out.println(ed.getNVConfig().getName() + "," + ed.getReferenceID());
      List<EncryptedKeyDAO> l = dataStore.searchByID((NVConfigEntity)ed.getNVConfig(), ed.getReferenceID());
      assert(l != null && l.size() == 1 );
      System.out.println(GSONUtil.toJSON(l.get(0), false));
      
    }
    
    //@Test
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
