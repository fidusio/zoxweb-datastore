package io.xlogistx.datastore.h2p.test;

import io.xlogistx.datastore.h2p.H2PSetup;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.opsec.tools.ds.SecurityAdminTool;
import io.xlogistx.shiro.ShiroUtil;
import org.apache.shiro.mgt.SecurityManager;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link H2PSetup} run without a console, every answer on the command line: the vault, the
 * encrypted H2 file, the database set-up and the INI, then the refusals that must write nothing.
 * The set-up is the thing under test, so each run gets a fresh directory under {@code target/}.
 */
public class H2PSetupTest {

    private static final String STORE_PASSWORD = "Setup-" + UUID.randomUUID();

    private static File freshDirectory() {
        File ret = new File("target/h2psetup-test/" + UUID.randomUUID().toString().substring(0, 8));
        assertTrue(ret.mkdirs() || ret.isDirectory(), "cannot create " + ret);
        return ret;
    }

    private static int setup(ByteArrayOutputStream out, ByteArrayOutputStream err, String... args) {
        return H2PSetup.run(new PrintStream(out, true), new PrintStream(err, true), args);
    }

    @Test
    public void encryptedH2File_vaultDatabaseAndIni() throws Exception {
        File dir = freshDirectory();
        File store = new File(dir, "deploy.store");
        File data = new File(dir, "data").getAbsoluteFile(); // an operator's relative answer is checked in urlChecks
        File ini = new File(dir, "deploy-shiro.ini");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int code = setup(out, err,
                "store=" + store, "store.password=" + STORE_PASSWORD,
                "db.type=h2", "db.path=" + data, "db.name=app", "db.encrypt=yes",
                "db.user=sa", "db.password=", "db.enc-password=",      // empty = generated
                "super-admin-id=admin.local", "super-admin-password=", // empty = generated
                "app.id=xlogistx.io-setuptest",
                "shiro.ini=" + ini);
        assertEquals(H2PSetup.EXIT_OK, code, err + "\n" + out);
        String printed = out.toString();
        assertTrue(printed.contains("wildcard=verified"), printed);
        assertTrue(printed.contains("login=verified"), printed);
        assertTrue(printed.contains("super-admin-password (generated)"), printed);

        // the vault: every mandatory entry, the generated secrets, the URL with the cipher
        assertTrue(store.isFile());
        String url;
        try (SecretStore vault = SecretStore.open(store, STORE_PASSWORD.toCharArray())) {
            assertTrue(vault.missingMandatory().isEmpty(), "missing " + vault.missingMandatory());
            assertEquals("admin.local", vault.getSuperAdminID());
            assertNotNull(vault.getSecretKey(SecretStore.StoreParam.MASTER_KEY.getName()));
            assertEquals("sa", vault.get(SecretStore.StoreParam.DB_USER.getName()));
            assertEquals(H2PSetup.GENERATED_PASSWORD_LENGTH, vault.get(SecretStore.StoreParam.DB_PASSWORD.getName()).length());
            assertEquals(H2PSetup.GENERATED_PASSWORD_LENGTH, vault.get(SecretStore.StoreParam.DB_ENC_PASSWORD.getName()).length());
            assertEquals(H2PSetup.GENERATED_PASSWORD_LENGTH, vault.get(SecretStore.StoreParam.SUPER_ADMIN_PASSWORD.getName()).length());
            url = vault.get(SecretStore.StoreParam.DB_URL.getName());
            assertEquals("xlogistx.io-setuptest", vault.get(H2PSetup.APP_ID_ENTRY), "default app id stored");
            assertFalse(printed.contains(vault.get(SecretStore.StoreParam.DB_PASSWORD.getName())), "secret printed");
            assertFalse(printed.contains(vault.get(SecretStore.StoreParam.SUPER_ADMIN_PASSWORD.getName())), "secret printed");
        }
        assertTrue(url.startsWith("jdbc:h2:file:" + data.getPath().replace('\\', '/') + "/app;"), url);
        assertEquals(data.getPath().replace('\\', '/'), H2PSetup.h2Path(data.getPath()), "an absolute path is kept");
        assertTrue(url.contains(";CIPHER=AES"), url);
        assertTrue(url.contains(";MODE=PostgreSQL"), url);

        // the database: the H2 file exists and a new run through the vault reads it (file + user passwords right)
        assertTrue(new File(data, "app.mv.db").isFile(), "H2 file not created");
        assertTrue(printed.contains("does not exist: creating it"), printed);
        assertTrue(printed.contains("created H2 database file " + new File(data, "app.mv.db")), printed);
        assertEquals(new File(data, "app.mv.db"), H2PSetup.h2DatabaseFile(url));
        out.reset();
        err.reset();
        code = SecurityAdminTool.run(new PrintStream(out, true), new PrintStream(err, true),
                "store=" + store, "store.password=" + STORE_PASSWORD, "command=list-apps");
        assertEquals(SecurityAdminTool.EXIT_OK, code, err.toString());
        assertTrue(out.toString().contains("xlogistx.com-common"), out.toString());
        assertTrue(out.toString().contains("xlogistx.io-setuptest"), "the default app was created: " + out);
        assertTrue(printed.contains("creating app xlogistx.io-setuptest"), printed);

        // read mode: the super-admin id and the db.url, nothing else
        out.reset();
        err.reset();
        code = setup(out, err, "mode=read", "store=" + store, "store.password=" + STORE_PASSWORD);
        assertEquals(H2PSetup.EXIT_OK, code, err.toString());
        String[] lines = out.toString().trim().split("\\r?\\n");
        assertEquals(2, lines.length, out.toString());
        assertEquals("super-admin-id=admin.local", lines[0]);
        assertEquals("db.url=" + url, lines[1]);
        assertFalse(out.toString().contains("app-id") || out.toString().contains("password"), out.toString());
        err.reset();
        assertEquals(H2PSetup.EXIT_FAILURE, setup(out, err, "mode=read", "store=" + store, "store.password=wrong"), "wrong password");
        assertTrue(err.toString().contains("cannot open secret store"), err.toString());
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "mode=read", "store=" + new File(dir, "none.store"), "store.password=x"), "missing store");
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "mode=other", "store=" + store), "unknown mode");

        // the INI: written where asked, names the realm, loads
        assertTrue(ini.isFile());
        String text = new String(Files.readAllBytes(ini.toPath()), StandardCharsets.UTF_8);
        assertTrue(text.contains("dsRealm.name = " + ShiroUtil.REALM_NAME), text);
        assertTrue(text.contains("io.xlogistx.shiro.ds.DSAuthorizingRealm"), text);
        assertTrue(text.contains("# " + ini.getName() + " written by"), text);
        assertFalse(text.contains("admin.local"), "the super-admin id is never in the INI");
        assertTrue(text.contains("[" + H2PSetup.INI_APP_SECTION + "]\n" + H2PSetup.INI_APP_ID_KEY + " = xlogistx.io-setuptest"), text);
        SecurityManager sm = ShiroUtil.loadSecurityManager(ini.getPath());
        assertNotNull(sm);
        assertEquals("xlogistx.io-setuptest", org.apache.shiro.config.Ini.fromResourcePath(ini.getPath())
                .getSection(H2PSetup.INI_APP_SECTION).get(H2PSetup.INI_APP_ID_KEY), "the application reads the app id from its own section");
    }

    @Test
    public void givenURL_encPasswordImpliesCipher() throws Exception {
        File dir = freshDirectory().getAbsoluteFile();
        File store = new File(dir, "given.store");
        String plainURL = "jdbc:h2:file:" + dir.getPath().replace('\\', '/') + "/data/given;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = setup(out, err, "store=" + store, "store.password=" + STORE_PASSWORD,
                "db.url=" + plainURL, "db.password=", "db.enc-password=", "super-admin-id=admin.local", "super-admin-password=");
        assertEquals(H2PSetup.EXIT_OK, code, err + "\n" + out);
        try (SecretStore vault = SecretStore.open(store, STORE_PASSWORD.toCharArray())) {
            assertEquals(plainURL + ";CIPHER=AES", vault.get(SecretStore.StoreParam.DB_URL.getName()), "the cipher is added to the stored URL");
            assertNotNull(vault.get(SecretStore.StoreParam.DB_ENC_PASSWORD.getName()));
            assertNull(vault.get(H2PSetup.APP_ID_ENTRY), "no default app was asked for: no entry");
        }
        assertTrue(new File(dir, "data/given.mv.db").isFile());
        assertTrue(new File(dir, "shiro.ini").isFile(), "default INI beside the store");
        String ini = new String(Files.readAllBytes(new File(dir, "shiro.ini").toPath()), StandardCharsets.UTF_8);
        assertFalse(ini.contains("[" + H2PSetup.INI_APP_SECTION + "]"), "no app section without a default app");
        assertTrue(out.toString().contains("no default app"), out.toString());
    }

    @Test
    public void refusals_writeNothing() throws Exception {
        File dir = freshDirectory();
        File existing = new File(dir, "existing.store");
        assertTrue(existing.createNewFile());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        // an existing store is never overwritten (the store is asked after the database and the super-admin)
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + existing, "store.password=x",
                "db.path=" + new File(dir, "d"), "db.name=x", "super-admin-id=a", "super-admin-password="), err.toString());
        assertTrue(err.toString().contains("already exists"), err.toString());
        assertEquals(0, existing.length());

        File store = new File(dir, "new.store");
        // an in-memory database would not survive the run
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.url=jdbc:h2:mem:gone;MODE=PostgreSQL", "super-admin-id=a"), err.toString());
        assertTrue(err.toString().contains("in-memory"), err.toString());
        // a PostgreSQL URL must name its database
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.url=jdbc:postgresql://localhost:5432", "super-admin-id=a"), err.toString());
        assertTrue(err.toString().contains("must name the database"), err.toString());
        // the default app id must be <domain>-<app>, and never the common app
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.path=" + new File(dir, "d"), "db.name=x", "super-admin-id=a", "super-admin-password=", "app.id=nodash"), err.toString());
        assertTrue(err.toString().contains("app.id must be <domain>-<app>"), err.toString());
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.path=" + new File(dir, "d"), "db.name=x", "super-admin-id=a", "super-admin-password=", "app.id=xlogistx.com-common"), err.toString());
        assertTrue(err.toString().contains("is the common app"), err.toString());
        // a value without default and without a console stops the run
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.path=" + new File(dir, "d"), "db.name=x"), err.toString());
        assertTrue(err.toString().contains("super-admin-id= required"), err.toString());
        // an existing INI is never overwritten
        File ini = new File(dir, "shiro.ini");
        assertTrue(ini.createNewFile());
        err.reset();
        assertEquals(H2PSetup.EXIT_USAGE, setup(out, err, "store=" + store, "store.password=x",
                "db.path=" + new File(dir, "d"), "db.name=x", "db.password=", "db.enc-password=", "super-admin-id=a", "super-admin-password=",
                "shiro.ini=" + ini), err.toString());
        assertTrue(err.toString().contains("shiro ini already exists"), err.toString());

        assertFalse(store.exists(), "a refused run must not create the store");
        assertFalse(new File(dir, "d").exists(), "a refused run must not create the H2 directory");
        assertEquals(0, ini.length());
    }

    @Test
    public void urlChecks() {
        assertEquals(H2PSetup.DBType.H2, H2PSetup.typeOf("jdbc:h2:file:./data/x;CIPHER=AES"));
        assertEquals(H2PSetup.DBType.H2, H2PSetup.typeOf("jdbc:h2:./data/x"));
        assertEquals(H2PSetup.DBType.POSTGRES, H2PSetup.typeOf("jdbc:postgresql://host:5432/db"));
        assertEquals(new File("./data"), H2PSetup.h2FileDirectory("jdbc:h2:file:./data/x;MODE=PostgreSQL"));
        assertEquals(new File("."), H2PSetup.h2FileDirectory("jdbc:h2:./x"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.h2FileDirectory("jdbc:h2:x"), "implicitly relative: H2 refuses it");
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.typeOf("jdbc:h2:tcp://host/x"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.typeOf("jdbc:h2:file:data/x"), "implicitly relative: H2 refuses it");
        // a typed directory becomes what H2 accepts: ./name, forward slashes, absolute kept
        assertEquals("./data", H2PSetup.h2Path("data"));
        assertEquals("./data", H2PSetup.h2Path("./data/"));
        assertEquals("./deploy/data", H2PSetup.h2Path("deploy\\data"));
        assertEquals("../data", H2PSetup.h2Path("../data"));
        assertEquals("~/data", H2PSetup.h2Path("~/data"));
        assertEquals("C:/deploy/data", H2PSetup.h2Path("C:\\deploy\\data"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.typeOf("jdbc:mysql://host/x"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.typeOf("not a url"));
        // PostgreSQL host:port, port defaulting to 5432
        assertArrayEquals(new String[]{"localhost", "5432"}, H2PSetup.splitHostPort("localhost"));
        assertArrayEquals(new String[]{"lax-2", "5433"}, H2PSetup.splitHostPort(" lax-2:5433 "));
        assertArrayEquals(new String[]{"[::1]", "5432"}, H2PSetup.splitHostPort("[::1]"));
        assertArrayEquals(new String[]{"[::1]", "6000"}, H2PSetup.splitHostPort("[::1]:6000"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.splitHostPort("host:abc"));
        assertThrows(IllegalArgumentException.class, () -> H2PSetup.splitHostPort(":5432"));
        char[] generated = H2PSetup.generatePassword();
        assertEquals(H2PSetup.GENERATED_PASSWORD_LENGTH, generated.length);
        assertTrue(new String(generated).matches("[A-Za-z0-9]+"), new String(generated));
    }
}
