package org.zoxweb.server.ds.mongo;

import io.xlogistx.shiro.APISecurityManagerProvider;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.mgt.ShiroSecurityController;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zoxweb.server.ds.shiro.ShiroDSRealm;
import org.zoxweb.server.io.IOUtil;
import org.zoxweb.server.security.CryptoUtil;
import org.zoxweb.server.security.KeyMakerProvider;
import org.zoxweb.server.util.GSONUtil;
import org.zoxweb.shared.api.APIConfigInfoDAO;
import org.zoxweb.shared.api.APIDocumentStore;
import org.zoxweb.shared.api.APISecurityManager;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.data.FileInfoDAO;
import org.zoxweb.shared.security.KeyStoreInfo;
import org.zoxweb.shared.util.ResourceManager;
import org.zoxweb.shared.util.ResourceManager.Resource;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import static org.junit.jupiter.api.Assertions.assertNotNull;


public class MongoDSFileTest {

    private static final String MONGO_CONF = "mongo_conf.json";
    private static final String KEYSTORE_INFO = "key_store_info.json";
    private static final String SHIRO_INI = "shiro.ini";

    private static final String SUPER_ADMIN = "superadmin@xlogistx.io";
    private static final String SUPER_PASSWORD = "T!st2s3r";
    private static final String DOMAIN_ID = "test.com";
    private static final String APP_ID = "testapp";

    private static APISecurityManager<Subject, AuthorizationInfo, PrincipalCollection> apiSecurityManager;
    private static APIDocumentStore<?> documentStore;

    @BeforeAll
    public static void start()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        KeyStoreInfo ksid = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(KEYSTORE_INFO)), KeyStoreInfo.class);
        KeyStore ks = CryptoUtil.loadKeyStore(new FileInputStream(IOUtil.locateFile(ksid.getKeyStore())),
                CryptoConst.KEY_STORE_TYPE,
                ksid.getKeyStorePassword().toCharArray());

        KeyMakerProvider.SINGLETON.setMasterKey(ks, ksid.getAlias(), ksid.getAliasPassword());

        apiSecurityManager = new APISecurityManagerProvider();

        APIConfigInfoDAO dsConfig = GSONUtil.fromJSON(IOUtil.inputStreamToString(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF)), APIConfigInfoDAO.class);
        dsConfig.setKeyMaker(KeyMakerProvider.SINGLETON);
        dsConfig.setSecurityController(new ShiroSecurityController());

        Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:" + SHIRO_INI);
        SecurityManager securityManager = factory.getInstance();
        SecurityUtils.setSecurityManager(securityManager);

        MongoDataStoreCreator mdsc = new MongoDataStoreCreator();

        ResourceManager.SINGLETON.register(Resource.DATA_STORE, mdsc.createAPI(null, dsConfig));

        documentStore = ResourceManager.lookupResource(Resource.DATA_STORE);

        ShiroDSRealm realm = ShiroUtil.getRealm(ShiroDSRealm.class);
        realm.setAPISecurityManager(apiSecurityManager);
        realm.setDataStore(ResourceManager.lookupResource(Resource.DATA_STORE));

        apiSecurityManager.login(SUPER_ADMIN, SUPER_PASSWORD, DOMAIN_ID, APP_ID, true);
    }

    @AfterAll
    public static void close() {
        apiSecurityManager.logout();
    }

    @Test
    public void testCreateFile() throws IOException {
        FileInfoDAO createdFileInfo = new FileInfoDAO();
        
        createdFileInfo.setFullPathName("mongo_conf");
        createdFileInfo.setDescription("MongoDB configuration file");
        createdFileInfo.setFileType(FileInfoDAO.FileType.FILE);
       
        createdFileInfo.setCreationTime(System.currentTimeMillis());

        InputStream is = new FileInputStream(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF));

        createdFileInfo = (FileInfoDAO) documentStore.createFile(null, createdFileInfo, is, true);
        assertNotNull(createdFileInfo);
        assertNotNull(createdFileInfo.getReferenceID());
        assertNotNull(createdFileInfo.getName());
        assertNotNull(createdFileInfo.getDescription());
        assert(createdFileInfo.getLength() > 0);
    }

    @Test
    public void testReadFile() throws IOException {
        FileInfoDAO createdFileInfo = new FileInfoDAO();
        createdFileInfo.setFullPathName("mongo_conf");
        createdFileInfo.setDescription("MongoDB configuration file");
        createdFileInfo.setFileType(FileInfoDAO.FileType.FILE);
        createdFileInfo.setLength(400);
        createdFileInfo.setCreationTime(System.currentTimeMillis());

        InputStream is = new FileInputStream(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF));

        createdFileInfo = (FileInfoDAO) documentStore.createFile(null, createdFileInfo, is, true);

        FileInfoDAO readFileInfo = (FileInfoDAO) documentStore.readFile(createdFileInfo, new FileOutputStream("src/test/resources/read_mongo_conf.json"), true);
        assertNotNull(readFileInfo);
        assertNotNull(readFileInfo.getReferenceID());
        assertNotNull(readFileInfo.getName());
        assertNotNull(readFileInfo.getDescription());
        assert(readFileInfo.getLength() > 0);
    }

    @Test
    public void testUpdateFile() throws IOException {
        FileInfoDAO createdFileInfo = new FileInfoDAO();
        createdFileInfo.setFullPathName("mongo_conf");
        createdFileInfo.setFileType(FileInfoDAO.FileType.FILE);
        
        createdFileInfo.setCreationTime(System.currentTimeMillis());

        InputStream is = new FileInputStream(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF));

        createdFileInfo = (FileInfoDAO) documentStore.createFile(null, createdFileInfo, is, false);
        createdFileInfo.setDescription("MongoDB configuration file");

        FileInfoDAO updatedFileInfo = (FileInfoDAO) documentStore.updateFile(createdFileInfo, is, true);
        assertNotNull(updatedFileInfo);
        assertNotNull(updatedFileInfo.getReferenceID());
        assertNotNull(updatedFileInfo.getName());
        assertNotNull(updatedFileInfo.getDescription());
        assert(updatedFileInfo.getLength() > 0);
    }

    @Test
    public void testDeleteFile() throws IOException {
        FileInfoDAO createdFileInfo = new FileInfoDAO();
        createdFileInfo.setFullPathName("mongo_conf");
        createdFileInfo.setDescription("MongoDB configuration file");
        createdFileInfo.setFileType(FileInfoDAO.FileType.FILE);
        
        createdFileInfo.setCreationTime(System.currentTimeMillis());

        InputStream is = new FileInputStream(IOUtil.locateFile(MongoDSShiroTest.class.getClassLoader(), MONGO_CONF));

        createdFileInfo = (FileInfoDAO) documentStore.createFile(null, createdFileInfo, is, true);

        documentStore.deleteFile( createdFileInfo);
    }

}
