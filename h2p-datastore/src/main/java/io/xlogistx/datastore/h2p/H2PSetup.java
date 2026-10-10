/*
 * Copyright (c) 2012-2026 ZoxWeb.com LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.xlogistx.datastore.h2p;

import io.xlogistx.datastore.h2p.H2PDSCreator.H2PParam;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.opsec.tools.ds.SecurityAdminTool;
import io.xlogistx.shiro.ShiroUtil;
import io.xlogistx.shiro.authc.CredentialsInfoMatcher;
import io.xlogistx.shiro.ds.DSAuthorizingRealm;
import org.apache.shiro.cache.MemoryConstrainedCacheManager;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.ParamUtil;
import org.zoxweb.shared.util.SUS;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;

/**
 * One-shot, interactive set-up of a deployment from the command line: it creates the
 * {@link SecretStore}, creates the H2 database file or connects to the PostgreSQL database
 * the secret store names, sets the database up (catalog, super-admin, common app and its registrar) and
 * writes the {@code shiro.ini} the application is bootstrapped with later.
 * <pre>
 *   java io.xlogistx.datastore.h2p.H2PSetup                         fully interactive
 *   java io.xlogistx.datastore.h2p.H2PSetup store=... db.type=h2 ... any value given as key=value is not asked for
 *   java io.xlogistx.datastore.h2p.H2PSetup mode=read store=...    print the super-admin id and db.url of an existing store
 * </pre>
 * Every value is taken from the command line when given, else asked on the console; without a
 * console a value that has no default stops the run. Secrets are read without echo and never
 * printed. The steps, in order:
 * <ol>
 * <li><b>The secret store.</b> A new {@link SecretStore} (a {@link SecretStore#TYPE BCFKS} keystore) is created, never
 *     overwritten, with every {@link SecretStore.StoreParam}: a generated {@code master-key}
 *     (AES), the {@code super-admin-id}, the initial {@code super-admin-password}, {@code db.url},
 *     {@code db.user}, {@code db.password} and, for an encrypted H2 file, {@code db.enc-password}.
 *     A password left empty at the prompt is generated and lives only in the secret store (read it back
 *     with the {@code SecretStore} tool, {@code command=get}).</li>
 * <li><b>The database URL</b> is built from the answers, or taken as given ({@code db.url=}):
 *     H2 = {@code jdbc:h2:file:<file>;DB_CLOSE_DELAY=-1;MODE=PostgreSQL[;CIPHER=AES]} from the
 *     file location and database name ({@code db.path}, {@code db.name}; the file is created by
 *     the set-up), PostgreSQL =
 *     {@code jdbc:postgresql://host:port/name} from {@code db.host=host[:port]} (no port =
 *     {@value #DEFAULT_PG_PORT}) and {@code db.name}
 *     (the database must exist; nothing is created on the server). An in-memory H2 URL is refused:
 *     nothing would survive the run.</li>
 * <li><b>The database set-up</b> is {@link SecurityAdminTool} {@code bootstrap-super-admin}
 *     through the new secret store, the only sanctioned way to create the super-admin: the datastore is
 *     opened with the master key and the Shiro security controller (this creates the H2 file),
 *     the catalog is seeded, the super-admin account is created with the secret store's initial password
 *     and granted the reserved role, the common app and its registrar are created. The registrar
 *     key is printed once by that tool. The step is idempotent: when it fails the secret store is kept
 *     (its master key already wraps whatever was written) and the same command is rerun by hand.
 *     When a default app id was answered ({@code app.id}, optional, {@code <domain>-<app>}), that
 *     app is then created the same way ({@code create-app}: starter catalog, registrar key printed
 *     once); the id is also the secret store's {@value #APP_ID_ENTRY} entry and the
 *     {@code [xlogistx] app.id} line of the INI.</li>
 * <li><b>{@code shiro.ini}</b> is written beside the secret store (or at {@code shiro.ini=}), never
 *     overwritten, and loaded once through {@link ShiroUtil#loadSecurityManager(String)} to prove
 *     it parses. It holds the realm, matcher, cache and session manager only: the data store is
 *     registered by the application at start-up and the super-admin id is read from the secret store
 *     (user rule 2026-10-03), neither is in the INI.</li>
 * </ol>
 * Exit codes: 0 success, 1 usage error or refused answer, 2 a step failed.
 */
public class H2PSetup {
    private static final LogWrapper log = new LogWrapper(H2PSetup.class);

    public static final int EXIT_OK = 0;
    public static final int EXIT_USAGE = 1;
    public static final int EXIT_FAILURE = 2;

    public static final String DEFAULT_STORE = "xlogistx.store";
    public static final String DEFAULT_SHIRO_INI = "shiro.ini";
    /** The H2 database file location: the directory of the file. */
    public static final String DEFAULT_H2_PATH = "./data";
    /** The H2 database name: the file is {@code <path>/<name>.mv.db}. */
    public static final String DEFAULT_H2_DB_NAME = "xlogistx";
    /** The suffix H2 gives its database file. */
    public static final String H2_FILE_SUFFIX = ".mv.db";
    /** Text entry of the secret store that names the deployment's default app ({@code <domain>-<app>}); absent when none. */
    public static final String APP_ID_ENTRY = "app-id";
    /** Section of the generated INI that carries the deployment's own settings; Shiro ignores it, the application reads it. */
    public static final String INI_APP_SECTION = "xlogistx";
    /** Key of the default app id in {@link #INI_APP_SECTION}. */
    public static final String INI_APP_ID_KEY = "app.id";
    public static final String DEFAULT_H2_USER = "sa";
    public static final String DEFAULT_PG_HOST = "localhost";
    public static final String DEFAULT_PG_PORT = "5432";
    public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
    /** Length of a generated password (letters and digits only: an H2 file password must not contain a space). */
    public static final int GENERATED_PASSWORD_LENGTH = 32;

    /** The database engines the set-up knows. */
    public enum DBType implements GetName {
        H2("h2"),
        POSTGRES("postgres"),
        ;
        private final String name;

        DBType(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    /** What a run does: the set-up (default), or a read of an existing store. */
    public enum Mode implements GetName {
        /** Create the secret store, the database and the INI. */
        SETUP("setup"),
        /** Open an existing secret store and print its super-admin id and database URL, nothing else. */
        READ("read"),
        ;
        private final String name;

        Mode(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    /** Command-line parameters; each one is asked on the console when absent. */
    public enum Param implements GetName {
        /** {@code setup} (default) or {@code read}. */
        MODE("mode"),
        STORE("store"),
        STORE_PASSWORD("store.password"),
        DB_TYPE("db.type"),
        DB_URL("db.url"),
        /** H2: the database file location, the directory of the file, e.g. {@code ./data}. */
        DB_PATH("db.path"),
        /** PostgreSQL: {@code host} or {@code host:port}; no port = {@value #DEFAULT_PG_PORT}. */
        DB_HOST("db.host"),
        /** The database name: H2, the file {@code <db.path>/<db.name>.mv.db}; PostgreSQL, the database, which must exist. */
        DB_NAME("db.name"),
        DB_ENCRYPT("db.encrypt"),
        DB_USER(SecretStore.StoreParam.DB_USER.getName()),
        DB_PASSWORD(SecretStore.StoreParam.DB_PASSWORD.getName()),
        DB_ENC_PASSWORD(SecretStore.StoreParam.DB_ENC_PASSWORD.getName()),
        SUPER_ADMIN_ID(SecretStore.StoreParam.SUPER_ADMIN_ID.getName()),
        SUPER_ADMIN_PASSWORD(SecretStore.StoreParam.SUPER_ADMIN_PASSWORD.getName()),
        /** The default app of the deployment, {@code <domain>-<app>}; optional. */
        APP_ID("app.id"),
        SHIRO_INI("shiro.ini"),
        ;
        private final String name;

        Param(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    public static final String USAGE =
            "Usage: H2PSetup [key=value ...]   every value not given is asked on the console\n"
                    + "       H2PSetup mode=read store=<file> [store.password=<pw>]   print the super-admin id and the db.url of an\n"
                    + "                                                               existing secret store, nothing else\n"
                    + "\n"
                    + "  mode=setup|read              default setup\n"
                    + "  store=<file>                 the secret store to create (never overwritten); default " + DEFAULT_STORE + "\n"
                    + "  store.password=<pw>          its password (asked twice on the console)\n"
                    + "  db.type=h2|postgres          the database engine; default h2\n"
                    + "  db.url=<jdbc url>            a full JDBC URL instead of the parts below (H2 file or PostgreSQL only)\n"
                    + "  db.path=<dir> db.name=<name> db.encrypt=yes|no  H2: file location and database name (jdbc:h2:file:<dir>/<name>;...[;CIPHER=AES])\n"
                    + "                               defaults " + DEFAULT_H2_PATH + ", " + DEFAULT_H2_DB_NAME + ", yes\n"
                    + "  db.host=<host[:port]> db.name=<database>        PostgreSQL: jdbc:postgresql://<host>:<port>/<database>\n"
                    + "                               no port = " + DEFAULT_PG_PORT + "; default host " + DEFAULT_PG_HOST + "; the database must exist\n"
                    + "  db.user=<user>               database user; H2 default " + DEFAULT_H2_USER + "\n"
                    + "  db.password=<pw>             database password; H2: empty = generated\n"
                    + "  db.enc-password=<pw>         encrypted H2 file password; empty = generated\n"
                    + "  super-admin-id=<id>          the super-admin's username or email address\n"
                    + "  super-admin-password=<pw>    its initial password (asked twice); empty = generated\n"
                    + "  app.id=<domain-app>          the deployment's default app, e.g. xlogistx.io-shop; optional (empty = none):\n"
                    + "                               created with its starter catalog and registrar (create-app), stored as the\n"
                    + "                               secret store's " + APP_ID_ENTRY + " entry and written to shiro.ini [" + INI_APP_SECTION + "] " + INI_APP_ID_KEY + "\n"
                    + "  shiro.ini=<file>             the Shiro INI to write (never overwritten); default " + DEFAULT_SHIRO_INI + " beside the store\n"
                    + "\n"
                    + "Steps: create the secret store (master-key, super-admin-id, super-admin-password, db.*), create the H2 file or\n"
                    + "connect to PostgreSQL and set the database up (SecurityAdminTool bootstrap-super-admin), write shiro.ini.\n"
                    + "Generated passwords are only in the store: SecretStore store=<file> command=get name=<entry>.\n"
                    + "Exit codes: 0 success, 1 usage error, 2 a step failed.";

    public static void main(String... args) {
        System.exit(run(System.out, System.err, args));
    }

    /**
     * In-process entry point; returns the exit code instead of exiting. Values absent from
     * {@code args} are asked on {@link System#console()}.
     */
    public static int run(PrintStream out, PrintStream err, String... args) {
        ParamUtil.ParamMap params;
        try {
            params = ParamUtil.parse("=", args);
            params.hide(Param.STORE_PASSWORD, Param.DB_PASSWORD, Param.DB_ENC_PASSWORD, Param.SUPER_ADMIN_PASSWORD);
            if (params.namelessCount() > 0) {
                throw new IllegalArgumentException("unexpected argument: every argument is key=value");
            }
        } catch (RuntimeException e) {
            err.println(e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        OPSecUtil.singleton();
        Prompter in = new Prompter(params, System.console(), out);
        Mode mode;
        try {
            mode = params.nameExists(Param.MODE.getName()) ? params.enumValue(Param.MODE, Mode.values()) : Mode.SETUP;
            if (mode == null) {
                throw new IllegalArgumentException(Param.MODE.getName() + " must be setup or read");
            }
        } catch (RuntimeException e) {
            err.println(e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        if (mode == Mode.READ) {
            return read(in, out, err);
        }
        Plan plan;
        try {
            plan = ask(in);
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        try {
            return execute(plan, out, err);
        } finally {
            plan.wipe();
        }
    }

    /**
     * Read mode (user, 2026-10-09): opens the existing secret store named by {@code store=} with
     * its password ({@code store.password=} or a console prompt) and prints two lines,
     * {@code super-admin-id=<id>} and {@code db.url=<url>}. No other entry is shown or named.
     */
    static int read(Prompter in, PrintStream out, PrintStream err) {
        File file;
        char[] password;
        try {
            file = new File(in.value(Param.STORE, "Secret store file", DEFAULT_STORE));
            if (!file.isFile()) {
                throw new IllegalArgumentException("secret store not found: " + file);
            }
            password = in.secret(Param.STORE_PASSWORD, "Secret store password", false, false);
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }
        try (SecretStore store = SecretStore.open(file, password)) {
            String superAdminID = store.getSuperAdminID();
            String dbURL = store.get(SecretStore.StoreParam.DB_URL.getName());
            out.println(SecretStore.StoreParam.SUPER_ADMIN_ID.getName() + "=" + (superAdminID != null ? superAdminID : ""));
            out.println(SecretStore.StoreParam.DB_URL.getName() + "=" + (dbURL != null ? dbURL : ""));
            return EXIT_OK;
        } catch (Exception e) {
            err.println("cannot open secret store " + file + ": " + e.getMessage());
            return EXIT_FAILURE;
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    // ------------------------------------------------------------------
    // the answers
    // ------------------------------------------------------------------

    /** Everything the set-up needs, collected before anything is written. */
    static final class Plan {
        File store;
        char[] storePassword;
        DBType dbType;
        String dbURL;
        /**
         * The H2 database file ({@code <dir>/<name>.mv.db}); null for PostgreSQL. Its directory is
         * created before the set-up, the file itself by the set-up's first connection when it
         * does not exist (user, 2026-10-09: "for the h2 file if does not exist you have to create it").
         */
        File h2File;
        String dbUser;
        char[] dbPassword;
        boolean dbPasswordGenerated;
        char[] dbEncPassword;
        boolean dbEncPasswordGenerated;
        String superAdminID;
        char[] superAdminPassword;
        boolean superAdminPasswordGenerated;
        /** The default app, canonical {@code <domain>-<app>}; null when none was given. */
        String appID;
        File shiroIni;

        void wipe() {
            for (char[] secret : new char[][]{storePassword, dbPassword, dbEncPassword, superAdminPassword}) {
                if (secret != null) {
                    Arrays.fill(secret, '\0');
                }
            }
        }
    }

    /**
     * Collects the answers (user, 2026-10-09): first the database type, {@code h2} or
     * {@code postgres}; for H2 the file location, for PostgreSQL {@code host:port} (no port =
     * {@value #DEFAULT_PG_PORT}) and the database; then the credentials, the super-admin, the secret store
     * and the INI. Each check (bad URL, existing file) stops before the next question.
     *
     * @throws IllegalArgumentException on a refused answer or a missing one without a console
     */
    static Plan ask(Prompter in) {
        Plan plan = new Plan();

        String givenURL = in.given(Param.DB_URL);
        if (givenURL != null) {
            plan.dbType = typeOf(givenURL);
            plan.dbURL = givenURL;
        } else {
            plan.dbType = in.choice(Param.DB_TYPE, "Database", DBType.H2, DBType.values());
            plan.dbURL = plan.dbType == DBType.H2 ? askH2URL(in, plan) : askPostgresURL(in);
        }
        // a given db.enc-password implies encryption (the creator's rule): the URL then carries the cipher
        boolean encrypted = plan.dbType == DBType.H2 && (H2PParam.hasCipher(plan.dbURL) || in.named(Param.DB_ENC_PASSWORD));
        if (encrypted && !H2PParam.hasCipher(plan.dbURL)) {
            plan.dbURL = plan.dbURL + ";CIPHER=AES";
        }
        if (plan.dbType == DBType.H2) {
            plan.h2File = h2DatabaseFile(plan.dbURL);
        }

        plan.dbUser = in.value(Param.DB_USER, "Database user", plan.dbType == DBType.H2 ? DEFAULT_H2_USER : null);
        if (plan.dbType == DBType.H2) {
            plan.dbPassword = in.secret(Param.DB_PASSWORD, "Database password (empty = generated)", false, true);
            plan.dbPasswordGenerated = plan.dbPassword == null;
            if (plan.dbPasswordGenerated) {
                plan.dbPassword = generatePassword();
            }
            if (encrypted) {
                plan.dbEncPassword = in.secret(Param.DB_ENC_PASSWORD, "H2 file encryption password (empty = generated)", false, true);
                plan.dbEncPasswordGenerated = plan.dbEncPassword == null;
                if (plan.dbEncPasswordGenerated) {
                    plan.dbEncPassword = generatePassword();
                }
            }
        } else {
            plan.dbPassword = in.secret(Param.DB_PASSWORD, "Database password", false, false);
        }

        plan.superAdminID = in.value(Param.SUPER_ADMIN_ID, "Super-admin id (username or email address)", null);
        plan.superAdminPassword = in.secret(Param.SUPER_ADMIN_PASSWORD, "Super-admin initial password (empty = generated)", true, true);
        plan.superAdminPasswordGenerated = plan.superAdminPassword == null;
        if (plan.superAdminPasswordGenerated) {
            plan.superAdminPassword = generatePassword();
        }

        String appID = in.optional(Param.APP_ID, "Default app id (<domain>-<app>, e.g. xlogistx.io-shop; empty = none)");
        if (appID != null) {
            try {
                plan.appID = AppIDDefault.create(appID).getDomainAppID();
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(Param.APP_ID.getName() + " must be <domain>-<app>: " + e.getMessage(), e);
            }
            if (ShiroUtil.COMMON_SCOPE.equalsIgnoreCase(plan.appID)) {
                throw new IllegalArgumentException(Param.APP_ID.getName() + " " + plan.appID + " is the common app, which the set-up creates anyway");
            }
        }

        plan.store = new File(in.value(Param.STORE, "Secret store file to create", DEFAULT_STORE));
        if (plan.store.exists()) {
            throw new IllegalArgumentException("secret store already exists: " + plan.store + " (it is never overwritten)");
        }
        plan.storePassword = in.secret(Param.STORE_PASSWORD, "Secret store password", true, false);

        File storeDirectory = plan.store.getAbsoluteFile().getParentFile();
        plan.shiroIni = new File(in.value(Param.SHIRO_INI, "Shiro INI file to write",
                new File(storeDirectory, DEFAULT_SHIRO_INI).getPath()));
        if (plan.shiroIni.exists()) {
            throw new IllegalArgumentException("shiro ini already exists: " + plan.shiroIni + " (it is never overwritten)");
        }
        return plan;
    }

    /**
     * The H2 file URL from the file location ({@code db.path}, the directory), the database name
     * ({@code db.name}) and {@code db.encrypt}, through the creator's own URL builder.
     */
    private static String askH2URL(Prompter in, Plan plan) {
        String path = in.value(Param.DB_PATH, "H2 database file location (a directory, absolute or relative to the working directory of the application)", DEFAULT_H2_PATH);
        if (path.indexOf(';') >= 0) {
            throw new IllegalArgumentException("H2 database file location must be a path, without URL settings: " + path);
        }
        path = h2Path(path);
        String name = in.value(Param.DB_NAME, "Database name", DEFAULT_H2_DB_NAME);
        if (name.toLowerCase(Locale.ROOT).endsWith(H2_FILE_SUFFIX)) {
            name = name.substring(0, name.length() - H2_FILE_SUFFIX.length());
        }
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0 || name.indexOf(';') >= 0) {
            throw new IllegalArgumentException("H2 database name must be a plain name, not a path: " + name);
        }
        boolean encrypt = in.yesNo(Param.DB_ENCRYPT, "Encrypt the H2 file (AES)", true);
        APIConfigInfo cfg = new H2PDSCreator().createEmptyConfigInfo();
        cfg.getProperties().build(H2PParam.TYPE, "file");
        cfg.getProperties().build(H2PParam.PATH, path);
        cfg.getProperties().build(H2PParam.DB_NAME, name);
        if (encrypt) {
            cfg.getProperties().build(H2PParam.CIPHER, "AES");
        }
        return H2PParam.dataStoreURI(cfg);
    }

    /**
     * The PostgreSQL URL from {@code db.host} ({@code host} or {@code host:port}; no port =
     * {@value #DEFAULT_PG_PORT}) and {@code db.name}, through the creator's own URL builder.
     */
    private static String askPostgresURL(Prompter in) {
        String[] hostPort = splitHostPort(in.value(Param.DB_HOST, "PostgreSQL host:port (no port = " + DEFAULT_PG_PORT + ")", DEFAULT_PG_HOST));
        String host = hostPort[0];
        String port = hostPort[1];
        String name = in.value(Param.DB_NAME, "Database name (must exist on the server)", null);
        if (name.indexOf('/') >= 0 || name.indexOf('?') >= 0) {
            throw new IllegalArgumentException("PostgreSQL database must be a plain name: " + name);
        }
        APIConfigInfo cfg = new H2PDSCreator().createEmptyConfigInfo();
        cfg.getProperties().build(H2PParam.DRIVER, POSTGRES_DRIVER);
        cfg.getProperties().build(H2PParam.HOST, host);
        cfg.getProperties().build(H2PParam.PORT, port);
        cfg.getProperties().build(H2PParam.DB_NAME, name);
        return H2PParam.dataStoreURI(cfg);
    }

    /**
     * {@code host} or {@code host:port} split in two; no port means {@value #DEFAULT_PG_PORT}.
     * An IPv6 literal goes in brackets ({@code [::1]:5432}).
     *
     * @throws IllegalArgumentException on an empty host or a port that is not a number
     */
    public static String[] splitHostPort(String answer) {
        String value = answer.trim();
        String host = value;
        String port = DEFAULT_PG_PORT;
        int colon = value.lastIndexOf(':');
        if (colon >= 0 && (value.startsWith("[") ? value.indexOf(']') < colon : value.indexOf(':') == colon)) {
            host = value.substring(0, colon);
            port = value.substring(colon + 1).trim();
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL host is empty: " + answer);
        }
        try {
            Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("PostgreSQL port must be a number: " + answer);
        }
        return new String[]{host, port};
    }

    /**
     * The engine of a given URL: an H2 file database or PostgreSQL; anything else (in-memory or
     * server H2, another engine, not a JDBC URL) is refused.
     */
    public static DBType typeOf(String url) {
        String subprotocol;
        try {
            subprotocol = H2PUtil.parseJdbcURL(url).getValue(H2PUtil.JDBC_SUBPROTOCOL);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("not a JDBC URL: " + url);
        }
        if ("postgresql".equalsIgnoreCase(subprotocol)) {
            if (SUS.isEmpty(SecurityAdminToolURL.postgresDatabase(url))) {
                throw new IllegalArgumentException("PostgreSQL URL must name the database: jdbc:postgresql://host:port/dbname");
            }
            return DBType.POSTGRES;
        }
        if ("h2".equalsIgnoreCase(subprotocol)) {
            h2FileDirectory(url); // refuses mem:, tcp: and a path H2 itself would refuse
            return DBType.H2;
        }
        throw new IllegalArgumentException("unsupported database URL (H2 file or PostgreSQL only): " + url);
    }

    /**
     * A file location answer as H2 accepts it in a URL: separators as {@code /}, and a relative path
     * made explicit with {@code ./} (H2 refuses an implicitly relative path such as {@code data/x};
     * it takes an absolute path, {@code ~/name} or {@code ./name}).
     */
    public static String h2Path(String path) {
        String ret = path.trim().replace('\\', '/');
        while (ret.length() > 1 && ret.endsWith("/")) {
            ret = ret.substring(0, ret.length() - 1);
        }
        if (new File(ret).isAbsolute() || ret.startsWith("/") || ret.startsWith("~/") || ret.startsWith("./") || ret.startsWith("../")
                || ret.equals(".") || ret.equals("..")) {
            return ret;
        }
        return "./" + ret;
    }

    /** The directory of an H2 file URL: the parent of {@link #h2DatabaseFile}. */
    public static File h2FileDirectory(String url) {
        File parent = h2DatabaseFile(url).getParentFile();
        return parent != null ? parent : new File(".");
    }

    /**
     * The database file an H2 file URL names ({@code jdbc:h2:file:<dir>/<name>;...} or
     * {@code jdbc:h2:<dir>/<name>;...}): {@code <dir>/<name>.mv.db}, which H2 creates on the first
     * connection when it does not exist. Refuses {@code mem:}, {@code tcp:}, {@code ssl:} and
     * {@code zip:} URLs.
     */
    public static File h2DatabaseFile(String url) {
        String rest = url.substring("jdbc:h2:".length());
        int semi = rest.indexOf(';');
        if (semi >= 0) {
            rest = rest.substring(0, semi);
        }
        String lower = rest.toLowerCase(Locale.ROOT);
        if (lower.startsWith("mem:")) {
            throw new IllegalArgumentException("an in-memory H2 database cannot be set up: nothing survives the run (" + url + ")");
        }
        if (lower.startsWith("tcp:") || lower.startsWith("ssl:") || lower.startsWith("zip:")) {
            throw new IllegalArgumentException("only an H2 file database can be set up here: " + url);
        }
        if (lower.startsWith("file:")) {
            rest = rest.substring("file:".length());
        }
        if (rest.isEmpty()) {
            throw new IllegalArgumentException("H2 URL names no file: " + url);
        }
        if (!h2Path(rest).equals(rest.replace('\\', '/'))) {
            throw new IllegalArgumentException("H2 refuses an implicitly relative path in a URL; use an absolute path, ~/name or ./name: " + url);
        }
        return new File(rest + H2_FILE_SUFFIX);
    }

    // ------------------------------------------------------------------
    // the steps
    // ------------------------------------------------------------------

    /** Runs the three steps on a collected plan; prints progress on {@code out}, the failure on {@code err}. */
    static int execute(Plan plan, PrintStream out, PrintStream err) {
        // 1. the secret store
        try {
            File storeDirectory = plan.store.getAbsoluteFile().getParentFile();
            if (storeDirectory != null && !storeDirectory.isDirectory() && !storeDirectory.mkdirs()) {
                throw new IOException("cannot create directory " + storeDirectory);
            }
            File h2Directory = plan.h2File != null ? plan.h2File.getAbsoluteFile().getParentFile() : null;
            if (h2Directory != null && !h2Directory.isDirectory() && !h2Directory.mkdirs()) {
                throw new IOException("cannot create H2 directory " + h2Directory);
            }
            try (SecretStore store = SecretStore.create(plan.store, plan.storePassword)) {
                store.createSecretKey(SecretStore.StoreParam.MASTER_KEY.getName());
                store.setSuperAdminID(plan.superAdminID);
                store.put(SecretStore.StoreParam.SUPER_ADMIN_PASSWORD.getName(), new String(plan.superAdminPassword));
                store.put(SecretStore.StoreParam.DB_URL.getName(), plan.dbURL);
                store.put(SecretStore.StoreParam.DB_USER.getName(), plan.dbUser);
                store.put(SecretStore.StoreParam.DB_PASSWORD.getName(), new String(plan.dbPassword));
                if (plan.dbEncPassword != null) {
                    store.put(SecretStore.StoreParam.DB_ENC_PASSWORD.getName(), new String(plan.dbEncPassword));
                }
                if (plan.appID != null) {
                    store.put(APP_ID_ENTRY, plan.appID);
                }
                store.save();
                if (!store.missingMandatory().isEmpty()) {
                    throw new IllegalStateException("secret store lacks " + store.missingMandatory());
                }
            }
        } catch (Exception e) {
            err.println("secret store not created: " + e.getMessage());
            log.getLogger().severe("secret store not created: " + e);
            return EXIT_FAILURE;
        }
        out.println("created secret store " + plan.store + " (" + SecretStore.TYPE + "): " + SecretStore.StoreParam.MASTER_KEY.getName()
                + ", " + SecretStore.StoreParam.SUPER_ADMIN_ID.getName() + "=" + plan.superAdminID
                + ", " + SecretStore.StoreParam.SUPER_ADMIN_PASSWORD.getName() + (plan.superAdminPasswordGenerated ? " (generated)" : "")
                + ", " + SecretStore.StoreParam.DB_URL.getName() + "=" + plan.dbURL
                + ", " + SecretStore.StoreParam.DB_USER.getName() + "=" + plan.dbUser
                + ", " + SecretStore.StoreParam.DB_PASSWORD.getName() + (plan.dbPasswordGenerated ? " (generated)" : "")
                + (plan.dbEncPassword != null ? ", " + SecretStore.StoreParam.DB_ENC_PASSWORD.getName() + (plan.dbEncPasswordGenerated ? " (generated)" : "") : "")
                + (plan.appID != null ? ", " + APP_ID_ENTRY + "=" + plan.appID : ""));

        // 2. the database: the H2 file is created when it does not exist (H2 does it on the first
        //    connection), PostgreSQL is connected to; then the set-up through the secret store
        boolean h2FileExisted = plan.h2File != null && plan.h2File.isFile();
        if (plan.h2File != null) {
            out.println("H2 database file " + plan.h2File + (h2FileExisted ? " exists: setting it up ..." : " does not exist: creating it and setting it up ..."));
        } else {
            out.println("connecting to PostgreSQL " + plan.dbURL + " and setting it up ...");
        }
        int code = SecurityAdminTool.run(out, err,
                SecurityAdminTool.Param.STORE.getName() + "=" + plan.store,
                SecurityAdminTool.Param.STORE_PASSWORD.getName() + "=" + new String(plan.storePassword),
                SecurityAdminTool.Param.COMMAND.getName() + "=" + SecurityAdminTool.Command.BOOTSTRAP_SUPER_ADMIN.getName());
        if (code != SecurityAdminTool.EXIT_OK) {
            err.println("database set-up failed (exit " + code + "); the secret store " + plan.store
                    + " is kept: fix the database and rerun\n  " + SecurityAdminTool.class.getName() + " store=" + plan.store
                    + " command=" + SecurityAdminTool.Command.BOOTSTRAP_SUPER_ADMIN.getName());
            return EXIT_FAILURE;
        }
        if (plan.h2File != null) {
            if (!plan.h2File.isFile()) {
                err.println("database set-up reported success but the H2 database file " + plan.h2File + " does not exist");
                return EXIT_FAILURE;
            }
            out.println((h2FileExisted ? "set up H2 database file " : "created H2 database file ") + plan.h2File);
        }
        if (plan.appID != null) {
            // the deployment's default app: its own starter catalog and registrar (key printed once by the tool)
            out.println("creating app " + plan.appID + " ...");
            code = SecurityAdminTool.run(out, err,
                    SecurityAdminTool.Param.STORE.getName() + "=" + plan.store,
                    SecurityAdminTool.Param.STORE_PASSWORD.getName() + "=" + new String(plan.storePassword),
                    SecurityAdminTool.Param.COMMAND.getName() + "=" + SecurityAdminTool.Command.CREATE_APP.getName(),
                    SecurityAdminTool.Param.APP_ID.getName() + "=" + plan.appID);
            if (code != SecurityAdminTool.EXIT_OK) {
                err.println("app " + plan.appID + " not created (exit " + code + "); the secret store " + plan.store
                        + " and the database set-up are kept: rerun\n  " + SecurityAdminTool.class.getName() + " store=" + plan.store
                        + " command=" + SecurityAdminTool.Command.CREATE_APP.getName() + " app.id=" + plan.appID);
                return EXIT_FAILURE;
            }
        }

        // 3. shiro.ini
        try {
            File iniDirectory = plan.shiroIni.getAbsoluteFile().getParentFile();
            if (iniDirectory != null && !iniDirectory.isDirectory() && !iniDirectory.mkdirs()) {
                throw new IOException("cannot create directory " + iniDirectory);
            }
            Files.write(plan.shiroIni.toPath(), shiroIni(plan.store, plan.shiroIni, plan.appID).getBytes(StandardCharsets.UTF_8));
            ShiroUtil.loadSecurityManager(plan.shiroIni.getPath()); // proves the INI parses and the realm builds
        } catch (Exception e) {
            err.println("shiro ini not written: " + e.getMessage());
            log.getLogger().severe("shiro ini not written: " + e);
            return EXIT_FAILURE;
        }
        out.println("wrote " + plan.shiroIni + " (realm " + ShiroUtil.REALM_NAME + ", loads)");
        out.println("setup complete: secret store " + plan.store + ", database " + plan.dbURL + ", super-admin " + plan.superAdminID
                + (plan.appID != null ? ", app " + plan.appID : ", no default app") + ", shiro ini " + plan.shiroIni);
        return EXIT_OK;
    }

    /**
     * The Shiro INI of a deployment: realm, credentials matcher, cache and session manager. The
     * data store and the super-admin id are not in it (see the class comment).
     */
    static String shiroIni(File store, File ini, String appID) {
        String appSection = appID == null ? "" : "\n"
                + "# The deployment's own settings. Shiro ignores this section; the application reads it\n"
                + "# (org.apache.shiro.config.Ini: getSection(\"" + INI_APP_SECTION + "\")). The same app id is the secret store's\n"
                + "# " + APP_ID_ENTRY + " entry; the app itself was created by the set-up (create-app).\n"
                + "[" + INI_APP_SECTION + "]\n"
                + INI_APP_ID_KEY + " = " + appID + "\n";
        return "# " + ini.getName() + " written by " + H2PSetup.class.getName() + " on "
                + ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\n"
                + "# Secret store of this deployment: " + store + "\n"
                + "#   (db.url, db.user, db.password, db.enc-password, master-key, super-admin-id, super-admin-password)\n"
                + "#\n"
                + "# The data store is NOT configured here. Before the first login the application registers it\n"
                + "# under ResourceManager.Resource.DATA_STORE (\"DataStore\"), or calls\n"
                + "# ShiroDSDomainSecurityManager.attach(realm, dataStore) / fromGlobal(dataStore); the super-admin\n"
                + "# id is the secret store's super-admin-id entry, handed to the manager at start-up, never written here.\n"
                + "#\n"
                + "#   ResourceManager.SINGLETON.register(ResourceManager.Resource.DATA_STORE, dataStore);\n"
                + "#   SecurityUtils.setSecurityManager(ShiroUtil.loadSecurityManager(\"" + ini.getName() + "\"));\n"
                + "#   ShiroDSDomainSecurityManager dsm = ShiroDSDomainSecurityManager.fromGlobal();\n"
                + "#\n"
                + "[main]\n"
                + "# authorization info is cached per subject GUID; without a cacheManager the realm re-flattens\n"
                + "# grants on every isPermitted / hasRole call\n"
                + "cacheManager = " + MemoryConstrainedCacheManager.class.getName() + "\n"
                + "securityManager.cacheManager = $cacheManager\n"
                + "\n"
                + "# JWT policy knobs (defaults: clockSkewMillis 60000, JWTTimestampWindowMillis 300000)\n"
                + "matcher = " + CredentialsInfoMatcher.class.getName() + "\n"
                + "matcher.clockSkewMillis = 60000\n"
                + "\n"
                + "dsRealm = " + DSAuthorizingRealm.class.getName() + "\n"
                + "dsRealm.name = " + ShiroUtil.REALM_NAME + "\n"
                + "dsRealm.credentialsMatcher = $matcher\n"
                + "# ResourceManager key of the APIDataStore; \"DataStore\" is the default\n"
                + "dsRealm.dataStoreResource = DataStore\n"
                + "# load roles/permissions into the cache at login instead of on the first isPermitted/hasRole\n"
                + "dsRealm.eagerAuthorization = false\n"
                + "\n"
                + "# other realms can sit beside it: securityManager.realms = $dsRealm, $otherRealm\n"
                + "securityManager.realm = $dsRealm\n"
                + "\n"
                + "sessionManager = " + DefaultSessionManager.class.getName() + "\n"
                + "sessionManager.globalSessionTimeout = 1800000\n"
                + "securityManager.sessionManager = $sessionManager\n"
                + appSection;
    }

    /** Letters and digits from a {@link SecureRandom}, with at least one of each class. */
    public static char[] generatePassword() {
        String upper = "ABCDEFGHJKLMNPQRSTUVWXYZ";
        String lower = "abcdefghijkmnopqrstuvwxyz";
        String digits = "23456789";
        String all = upper + lower + digits;
        SecureRandom random = new SecureRandom();
        char[] ret = new char[GENERATED_PASSWORD_LENGTH];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = all.charAt(random.nextInt(all.length()));
        }
        // one of each class at random positions, so a policy that asks for them is met
        int u = random.nextInt(ret.length), l, d;
        do { l = random.nextInt(ret.length); } while (l == u);
        do { d = random.nextInt(ret.length); } while (d == u || d == l);
        ret[u] = upper.charAt(random.nextInt(upper.length()));
        ret[l] = lower.charAt(random.nextInt(lower.length()));
        ret[d] = digits.charAt(random.nextInt(digits.length()));
        return ret;
    }

    // ------------------------------------------------------------------
    // console
    // ------------------------------------------------------------------

    /**
     * Answers: the command-line value when given, else a console question. Without a console a
     * question with a default takes it, one without stops the run.
     */
    static final class Prompter {
        private final ParamUtil.ParamMap params;
        private final Console console;
        private final PrintStream out;

        Prompter(ParamUtil.ParamMap params, Console console, PrintStream out) {
            this.params = params;
            this.console = console;
            this.out = out;
        }

        /** True when the parameter was named on the command line, with or without a value. */
        boolean named(Param param) {
            return params.nameExists(param.getName());
        }

        /** The trimmed command-line value, or null when absent or empty. */
        String given(Param param) {
            String ret = params.stringValue(param, null);
            return SUS.isEmpty(ret) ? null : ret.trim();
        }

        /** An optional value: given, else asked once (empty answer = null), else null without a console. */
        String optional(Param param, String question) {
            String ret = given(param);
            if (ret != null || console == null) {
                return ret;
            }
            String answer = console.readLine("%s: ", question);
            if (answer == null) {
                throw new IllegalArgumentException(param.getName() + ": no answer (end of input)");
            }
            answer = answer.trim();
            return answer.isEmpty() ? null : answer;
        }

        /** A plain value: given, else asked (empty answer = {@code defaultValue}), else the default without a console. */
        String value(Param param, String question, String defaultValue) {
            String ret = given(param);
            if (ret != null) {
                return ret;
            }
            if (console == null) {
                if (defaultValue != null) {
                    return defaultValue;
                }
                throw new IllegalArgumentException(param.getName() + "= required: no console to ask on");
            }
            while (true) {
                String answer = console.readLine("%s%s: ", question, defaultValue != null ? " [" + defaultValue + "]" : "");
                if (answer == null) {
                    throw new IllegalArgumentException(param.getName() + ": no answer (end of input)");
                }
                answer = answer.trim();
                if (!answer.isEmpty()) {
                    return answer;
                }
                if (defaultValue != null) {
                    return defaultValue;
                }
                out.println("a value is required");
            }
        }

        /** One of {@code choices} by name, case-insensitive. */
        <E extends Enum<E> & GetName> E choice(Param param, String question, E defaultValue, E[] choices) {
            StringBuilder names = new StringBuilder();
            for (E choice : choices) {
                names.append(names.length() > 0 ? "|" : "").append(choice.getName());
            }
            while (true) {
                String answer = value(param, question + " (" + names + ")", defaultValue.getName());
                for (E choice : choices) {
                    if (choice.getName().equalsIgnoreCase(answer) || choice.name().equalsIgnoreCase(answer)) {
                        return choice;
                    }
                }
                if (console == null || given(param) != null) {
                    throw new IllegalArgumentException(param.getName() + " must be one of " + names + ": " + answer);
                }
                out.println("answer one of " + names);
            }
        }

        /** yes/no (also y/n, true/false, on/off). */
        boolean yesNo(Param param, String question, boolean defaultValue) {
            while (true) {
                String answer = value(param, question + " (yes|no)", defaultValue ? "yes" : "no").toLowerCase(Locale.ROOT);
                switch (answer) {
                    case "y": case "yes": case "true": case "on": case "1":
                        return true;
                    case "n": case "no": case "false": case "off": case "0":
                        return false;
                    default:
                        if (console == null || given(param) != null) {
                            throw new IllegalArgumentException(param.getName() + " must be yes or no: " + answer);
                        }
                        out.println("answer yes or no");
                }
            }
        }

        /**
         * A secret: given, else read without echo (twice when {@code confirm}). Null means "generate
         * one" and is only possible when {@code emptyOk}; otherwise an empty answer is asked again.
         */
        char[] secret(Param param, String question, boolean confirm, boolean emptyOk) {
            String ret = params.stringValue(param, null);
            if (!SUS.isEmpty(ret)) {
                return ret.toCharArray();
            }
            if (params.nameExists(param.getName()) && emptyOk) {
                return null; // key= given empty: generate
            }
            if (console == null) {
                if (emptyOk) {
                    return null;
                }
                throw new IllegalArgumentException(param.getName() + "= required: no console to ask on");
            }
            while (true) {
                char[] first = console.readPassword("%s: ", question);
                if (first == null) {
                    throw new IllegalArgumentException(param.getName() + ": no answer (end of input)");
                }
                if (first.length == 0) {
                    if (emptyOk) {
                        return null;
                    }
                    out.println("a value is required");
                    continue;
                }
                if (!confirm) {
                    return first;
                }
                char[] second = console.readPassword("Repeat: ");
                boolean same = Arrays.equals(first, second);
                if (second != null) {
                    Arrays.fill(second, '\0');
                }
                if (same) {
                    return first;
                }
                Arrays.fill(first, '\0');
                out.println("values do not match, again");
            }
        }
    }

    /** The PostgreSQL URL check the admin tool applies, so a refused URL is refused before the secret store exists. */
    private static final class SecurityAdminToolURL {
        /** The database a PostgreSQL URL names ({@code jdbc:postgresql://host:port/db[?options]} or {@code jdbc:postgresql:db}), or empty. */
        static String postgresDatabase(String url) {
            String rest = url.substring("jdbc:".length());
            int colon = rest.indexOf(':');
            String base = colon >= 0 ? rest.substring(colon + 1) : "";
            int cut = base.indexOf('?');
            int semi = base.indexOf(';');
            if (semi >= 0 && (cut < 0 || semi < cut)) {
                cut = semi;
            }
            if (cut >= 0) {
                base = base.substring(0, cut);
            }
            if (!base.startsWith("//")) {
                return base;
            }
            base = base.substring(2);
            int slash = base.indexOf('/');
            return slash >= 0 ? base.substring(slash + 1) : "";
        }
    }
}
