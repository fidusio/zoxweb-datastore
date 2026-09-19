package io.xlogistx.shiro.ds.tools;

import io.xlogistx.datastore.h2p.H2PDSCreator;
import io.xlogistx.datastore.h2p.H2PDataStore;
import io.xlogistx.datastore.h2p.H2PUtil;
import io.xlogistx.opsec.OPSecUtil;
import io.xlogistx.opsec.SecretStore;
import io.xlogistx.shiro.ds.SecurityBootstrap;
import io.xlogistx.shiro.ds.SecurityCatalogSeeder;
import io.xlogistx.shiro.ds.ShiroDSDomainSecurityManager;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.shared.api.APIConfigInfo;
import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.security.PermissionGrant;
import org.zoxweb.shared.security.PermissionInfo;
import org.zoxweb.shared.security.RoleGrant;
import org.zoxweb.shared.security.RoleGroupGrant;
import org.zoxweb.shared.security.RoleGroupInfo;
import org.zoxweb.shared.security.RoleInfo;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.GetName;
import org.zoxweb.shared.util.NVGenericMap;
import org.zoxweb.shared.util.ParamUtil;
import org.zoxweb.shared.util.SUS;

import java.io.Console;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Command-line administration of the security catalog and the admin accounts, the only
 * sanctioned way to create the super-admin. Arguments are {@code key=value} pairs;
 * {@code password}, {@code db.password}, {@code db.enc-key} and {@code store.password} are hidden from logs.
 * <pre>
 *   command=seed-catalog               db.url=&lt;jdbc url&gt; [db.user= db.password= db.enc-key=] | store=&lt;vault&gt; [store.password=]
 *   command=bootstrap-super-admin      db.url=... [principal.id=&lt;super-admin&gt;] [password=&lt;pw&gt;]
 *   command=reset-super-admin-password db.url=... [principal.id=&lt;super-admin&gt;] [password=&lt;pw&gt;]
 *   command=create-subject             db.url=... principal.id=&lt;principal&gt; [password=&lt;pw&gt;] [role=&lt;role name&gt; [app.id=&lt;domain-app&gt;]]
 *   command=grant-role                 db.url=... principal.id=&lt;principal&gt; role=&lt;role name&gt; [app.id=&lt;domain-app&gt;]
 *   command=revoke-role                db.url=... principal.id=&lt;principal&gt; role=&lt;role name&gt; [app.id=&lt;domain-app&gt;]
 *   command=revoke-app                 db.url=... principal.id=&lt;principal&gt; app.id=&lt;domain-app&gt;
 *   command=list-grants                db.url=... principal.id=&lt;principal&gt;
 *   command=reset-password             db.url=... principal.id=&lt;principal&gt;
 *   command=list-catalog               db.url=...
 * </pre>
 * {@code app.id} scopes a role grant to one app ({@code <domain>-<app>} as {@link AppIDDefault#create(String)}
 * reads it, or {@code domain.id=} plus a bare {@code app.id=}): that grant is the subject's
 * assignment to the app, flattened as {@code <domain-app>:<role>} / {@code <domain-app>:<token>},
 * and revoking it ({@code revoke-role}, or {@code revoke-app} for every grant under the app) removes
 * the assignment. The tool runs unenforced, so {@code broker_guid} stays empty here; grants made
 * through the API by an app admin carry that admin as broker.
 * {@code principal.id} names the account the command works on. For the two super-admin commands
 * it is the super-admin principal of that store (default
 * {@link ShiroDSDomainSecurityManager#DEFAULT_SUPER_ADMIN_PRINCIPAL_ID}); the realm that later
 * serves the store must name the same principal ({@code dsRealm.superAdminPrincipalID}), or the
 * flattener drops its wildcard. Only that one account may ever hold a bare {@code *}. Domain
 * admins such as {@code remote-admin} or {@code local-admin} (the latter on devices that run an
 * offline H2 store) are ordinary subjects made with {@code create-subject}: their reach is
 * domain-driven, {@code <domain-app-id>:*} at most, which the guards allow because only a token
 * whose first part is {@code *} is reserved ({@code SecurityModel.isWildcardToken}).
 * <p>
 * The datastore URL is resolved from {@code db.url=}, then the {@code db.url} text secret of the
 * {@link SecretStore} vault named by {@code store=} (or the {@code XLOGISTX_SECRET_STORE}
 * environment variable), then the {@code XLOGISTX_SHIRO_DB_URL} environment variable, then
 * {@code -Dshiro.ds.db.url}; there is no default. {@code db.user}, {@code db.password} and
 * {@code db.enc-key} come from the command line, else from the vault entries of the same name,
 * so an operator with a vault types one password ({@code store.password=} or a console prompt)
 * instead of the database credentials. A PostgreSQL URL must name its database
 * ({@code jdbc:postgresql://host:5432/dbname}); nothing is created.
 * {@code db.enc-key} is the file password of an encrypted H2 database.
 * When {@code password=} is absent and a console is attached, the password is prompted twice.
 * Permission enforcement is off inside the tool: it is the trusted bootstrap path.
 */
public class SecurityAdminTool {

    private static final LogWrapper log = new LogWrapper(SecurityAdminTool.class);
    public static final String DB_URL_ENV = "XLOGISTX_SHIRO_DB_URL";
    public static final String DB_URL_PROPERTY = "shiro.ds.db.url";
    public static final String SECRET_STORE_ENV = "XLOGISTX_SECRET_STORE";

    public static final String USAGE =
            "Usage: command=<command> db.url=<jdbc url> [db.user=<user>] [db.password=<pw>] [db.enc-key=<h2 file password>] [options]\n"
                    + "   or: command=<command> store=<secret store file> [store.password=<pw>] [options]\n"
                    + "\n"
                    + "Commands:\n"
                    + "  seed-catalog                 create or repair the built-in permissions, roles and role groups\n"
                    + "  bootstrap-super-admin        seed the catalog, create the super-admin account (principal.id=<principal>,\n"
                    + "                               password=<pw> or console prompt) and grant it the super_admin role; idempotent\n"
                    + "  reset-super-admin-password   replace the super-admin password (password=<pw> or console prompt)\n"
                    + "  create-subject               principal.id=<principal> [password=<pw> or console prompt] [role=<role name> [app.id=]]:\n"
                    + "                               create an account (e.g. remote-admin / local-admin with role=domain_admin);\n"
                    + "                               never super_admin: a domain admin's reach is <domain-app-id>:* at most\n"
                    + "  grant-role                   principal.id=<principal> role=<role name> [app.id=<domain-app>]\n"
                    + "  revoke-role                  principal.id=<principal> role=<role name> [app.id=<domain-app>]\n"
                    + "  revoke-app                   principal.id=<principal> app.id=<domain-app>: delete every grant under that app\n"
                    + "  list-grants                  principal.id=<principal>: roles, role groups and permissions with their app scope\n"
                    + "  reset-password               principal.id=<principal>: admin reset; prints the one-time token (4 h) to hand out\n"
                    + "  list-catalog                 print permissions, roles and role groups\n"
                    + "\n"
                    + "Options:\n"
                    + "  principal.id=<principal>     the account to work on; for the super-admin commands the super-admin\n"
                    + "                               principal of this store (default "
                    + ShiroDSDomainSecurityManager.DEFAULT_SUPER_ADMIN_PRINCIPAL_ID + ")\n"
                    + "  app.id=<domain-app>          scope of a role grant (e.g. xlogistx.io-shop); with domain.id=<domain> it is\n"
                    + "                               the bare app name. A scoped grant flattens as <domain-app>:<token>\n"
                    + "  db.url=<url>                 H2 (jdbc:h2:...) or PostgreSQL (jdbc:postgresql://host:port/db)\n"
                    + "                               or set " + DB_URL_ENV + " in the environment, or -D" + DB_URL_PROPERTY + "\n"
                    + "  db.enc-key=<pw>              H2 only: file password of an encrypted database\n"
                    + "  store=<file>                 SecretStore vault (BCFKS) whose text secrets db.url, db.user, db.password\n"
                    + "                               and db.enc-key fill in whatever is not on the command line;\n"
                    + "                               or set " + SECRET_STORE_ENV + " in the environment\n"
                    + "  store.password=<pw>          the vault password, prompted on the console when absent";

    public enum Command implements GetName {
        SEED_CATALOG("seed-catalog"),
        BOOTSTRAP_SUPER_ADMIN("bootstrap-super-admin"),
        RESET_SUPER_ADMIN_PASSWORD("reset-super-admin-password"),
        CREATE_SUBJECT("create-subject"),
        GRANT_ROLE("grant-role"),
        REVOKE_ROLE("revoke-role"),
        REVOKE_APP("revoke-app"),
        LIST_GRANTS("list-grants"),
        LIST_CATALOG("list-catalog"),
        RESET_PASSWORD("reset-password"),
        ;
        private final String name;

        Command(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    public enum Param implements GetName {
        COMMAND("command"),
        DB_URL("db.url"),
        DB_USER("db.user"),
        DB_PASSWORD("db.password"),
        DB_ENC_KEY("db.enc-key"),
        STORE("store"),
        STORE_PASSWORD("store.password"),
        PRINCIPAL_ID("principal.id"),
        PASSWORD("password"),
        ROLE("role"),
        APP_ID("app.id"),
        DOMAIN_ID("domain.id"),
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

    public static final int EXIT_OK = 0;
    public static final int EXIT_USAGE = 1;
    public static final int EXIT_FAILURE = 2;

    public static void main(String... args) {
        System.exit(run(System.out, System.err, args));
    }

    /**
     * In-process entry point; returns the exit code instead of exiting.
     */
    public static int run(String... args) {
        return run(System.out, System.err, args);
    }

    public static int run(PrintStream out, PrintStream err, String... args) {
        ParamUtil.ParamMap params;
        Command command;
        String dbURL;
        NVGenericMap vault;
        AppIDDefault app;
        try {
            params = ParamUtil.parse("=", args);
            params.hide(Param.PASSWORD, Param.DB_PASSWORD, Param.DB_ENC_KEY, Param.STORE_PASSWORD);
            command = params.enumValue(Param.COMMAND, Command.values());
            if (command == null) {
                throw new IllegalArgumentException("command is missing or unknown");
            }
            vault = loadVault(params, System.getenv(SECRET_STORE_ENV));
            app = appOf(params);
            dbURL = resolveDbUrl(params.stringValue(Param.DB_URL, null), vault.getValue(Param.DB_URL.getName()),
                    System.getenv(DB_URL_ENV), System.getProperty(DB_URL_PROPERTY));
        } catch (RuntimeException e) {
            err.println(e.getMessage());
            err.println(USAGE);
            return EXIT_USAGE;
        }

        String principalID = params.stringValue(Param.PRINCIPAL_ID, null);
        principalID = SUS.isEmpty(principalID) ? null : principalID.trim();
        H2PDataStore ds = null;
        try {
            ds = openStore(dbURL, dbSetting(params, vault, Param.DB_USER), dbSetting(params, vault, Param.DB_PASSWORD),
                    dbSetting(params, vault, Param.DB_ENC_KEY));
            ShiroDSDomainSecurityManager dsm = new ShiroDSDomainSecurityManager(ds);
            switch (command) {
                case SEED_CATALOG: {
                    SecurityCatalogSeeder.Report report = dsm.seedCatalog();
                    out.println("catalog: " + report);
                    return EXIT_OK;
                }
                case BOOTSTRAP_SUPER_ADMIN: {
                    if (principalID != null) {
                        dsm.setSuperAdminPrincipalID(principalID);
                    }
                    boolean exists = dsm.lookupSuperAdminSubject() != null;
                    String password = exists ? null : password(params, "Super-admin password: ", err);
                    if (!exists && password == null) {
                        return EXIT_USAGE;
                    }
                    SecurityBootstrap.Result result = SecurityBootstrap.bootstrapSuperAdmin(dsm, password);
                    out.println(result);
                    if (exists) {
                        out.println("account already existed: password unchanged");
                    }
                    return result.wildcardVerified ? EXIT_OK : EXIT_FAILURE;
                }
                case RESET_SUPER_ADMIN_PASSWORD: {
                    if (principalID != null) {
                        dsm.setSuperAdminPrincipalID(principalID);
                    }
                    String password = password(params, "New super-admin password: ", err);
                    if (password == null) {
                        return EXIT_USAGE;
                    }
                    SecurityBootstrap.resetSuperAdminPassword(dsm, password);
                    out.println("super-admin password replaced for " + dsm.getSuperAdminPrincipalID());
                    return EXIT_OK;
                }
                case CREATE_SUBJECT: {
                    if (principalID == null) {
                        err.println("create-subject needs principal.id=<principal>");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    String roleName = params.stringValue(Param.ROLE, null);
                    RoleInfo role = null;
                    if (!SUS.isEmpty(roleName)) {
                        if (SecurityModel.Role.SUPER_ADMIN.getName().equalsIgnoreCase(roleName.trim())) {
                            err.println("create-subject never grants " + SecurityModel.Role.SUPER_ADMIN.getName()
                                    + ": use bootstrap-super-admin for the super-admin account");
                            return EXIT_FAILURE;
                        }
                        role = dsm.lookupRole(null, roleName);
                        if (role == null) {
                            err.println("unknown role: " + roleName);
                            return EXIT_FAILURE;
                        }
                    }
                    SubjectIdentifier subject = dsm.lookupSubjectID(principalID);
                    boolean created = false;
                    if (subject == null) {
                        String password = password(params, "Password for " + principalID + ": ", err);
                        if (password == null) {
                            return EXIT_USAGE;
                        }
                        subject = dsm.createSubjectID(principalID, password, CryptoConst.HashType.ARGON2);
                        created = true;
                    }
                    boolean granted = false;
                    if (role != null && !holdsRole(dsm, subject, role, app)) {
                        dsm.addRoleGrant(subject, role, app);
                        granted = true;
                    }
                    out.println("subject " + principalID + " subject=" + subject.getGUID()
                            + (created ? " (created)" : " (existing, password unchanged)")
                            + (role != null ? " role " + roleName + scopeLabel(app) + "=" + (granted ? "granted" : "existing") : ""));
                    return EXIT_OK;
                }
                case GRANT_ROLE: {
                    String roleName = params.stringValue(Param.ROLE, null);
                    if (principalID == null || SUS.isEmpty(roleName)) {
                        err.println("grant-role needs principal.id=<principal> role=<role name>");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    SubjectIdentifier subject = dsm.lookupSubjectID(principalID);
                    if (subject == null) {
                        err.println("unknown subject: " + principalID);
                        return EXIT_FAILURE;
                    }
                    RoleInfo role = dsm.lookupRole(null, roleName);
                    if (role == null) {
                        err.println("unknown role: " + roleName);
                        return EXIT_FAILURE;
                    }
                    if (holdsRole(dsm, subject, role, app)) {
                        out.println("role " + roleName + scopeLabel(app) + " already granted to " + principalID);
                        return EXIT_OK;
                    }
                    dsm.addRoleGrant(subject, role, app);
                    out.println("granted role " + roleName + scopeLabel(app) + " to " + principalID);
                    return EXIT_OK;
                }
                case REVOKE_ROLE: {
                    String roleName = params.stringValue(Param.ROLE, null);
                    if (principalID == null || SUS.isEmpty(roleName)) {
                        err.println("revoke-role needs principal.id=<principal> role=<role name> [app.id=<domain-app>]");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    SubjectIdentifier subject = dsm.lookupSubjectID(principalID);
                    if (subject == null) {
                        err.println("unknown subject: " + principalID);
                        return EXIT_FAILURE;
                    }
                    RoleInfo role = dsm.lookupRole(null, roleName);
                    if (role == null) {
                        err.println("unknown role: " + roleName);
                        return EXIT_FAILURE;
                    }
                    int revoked = 0;
                    for (RoleGrant g : roleGrants(dsm, subject, role, app)) {
                        if (dsm.deleteRoleGrant(g)) {
                            revoked++;
                        }
                    }
                    if (revoked == 0) {
                        err.println("no grant of role " + roleName + scopeLabel(app) + " for " + principalID);
                        return EXIT_FAILURE;
                    }
                    out.println("revoked " + revoked + " grant(s) of role " + roleName + scopeLabel(app) + " from " + principalID);
                    return EXIT_OK;
                }
                case REVOKE_APP: {
                    if (principalID == null || app == null) {
                        err.println("revoke-app needs principal.id=<principal> app.id=<domain-app>");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    SubjectIdentifier subject = dsm.lookupSubjectID(principalID);
                    if (subject == null) {
                        err.println("unknown subject: " + principalID);
                        return EXIT_FAILURE;
                    }
                    int revoked = dsm.revokeAppGrants(subject.getGUID(), app);
                    out.println("revoked " + revoked + " grant(s)" + scopeLabel(app) + " from " + principalID);
                    return EXIT_OK;
                }
                case LIST_GRANTS: {
                    if (principalID == null) {
                        err.println("list-grants needs principal.id=<principal>");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    SubjectIdentifier subject = dsm.lookupSubjectID(principalID);
                    if (subject == null) {
                        err.println("unknown subject: " + principalID);
                        return EXIT_FAILURE;
                    }
                    listGrants(dsm, subject, out);
                    return EXIT_OK;
                }
                case LIST_CATALOG: {
                    listCatalog(dsm, out);
                    return EXIT_OK;
                }
                case RESET_PASSWORD: {
                    if (principalID == null) {
                        err.println("reset-password needs principal.id=<principal>");
                        err.println(USAGE);
                        return EXIT_USAGE;
                    }
                    org.zoxweb.shared.security.PasswordResetRequest req = dsm.adminResetPassword(principalID);
                    out.println("reset token for " + req.getPrincipalID() + " (valid until " + new java.util.Date(req.getExpiryTS()) + "):");
                    out.println(req.getToken());
                    out.println("the account is locked until the reset completes or the token expires; complete with "
                            + "completePasswordReset(principal, token, newPassword)");
                    return EXIT_OK;
                }
                default:
                    err.println(USAGE);
                    return EXIT_USAGE;
            }
        } catch (RuntimeException e) {
            err.println(command.getName() + " failed: " + e.getMessage());
            log.getLogger().severe(command.getName() + " failed: " + e);
            return EXIT_FAILURE;
        } finally {
            if (ds != null) {
                try {
                    ds.close();
                } catch (Exception ignore) {
                    // best effort
                }
            }
        }
    }

    /**
     * The datastore URL from the first non-blank source: param, vault entry, environment variable,
     * system property.
     *
     * @throws IllegalArgumentException when none is set; there is deliberately no default
     */
    static String resolveDbUrl(String param, String vaultEntry, String env, String property) {
        for (String candidate : new String[]{param, vaultEntry, env, property}) {
            if (candidate != null && !candidate.trim().isEmpty()) {
                return candidate.trim();
            }
        }
        throw new IllegalArgumentException("no datastore URL: pass db.url=<url>, store it as db.url in a store=<vault>, set "
                + DB_URL_ENV + ", or run with -D" + DB_URL_PROPERTY + "=<url>");
    }

    /** A {@code db.*} setting: the command-line value when present, else the vault's text secret of the same name. */
    static String dbSetting(ParamUtil.ParamMap params, NVGenericMap vault, Param param) {
        String ret = params.stringValue(param, null);
        return SUS.isEmpty(ret) ? vault.getValue(param.getName()) : ret;
    }

    /**
     * The {@code db.*} text secrets of the vault named by {@code store=} (else {@code env}), or an
     * empty map when no vault is named. The vault password is {@code store.password=} or a single
     * console prompt; the vault is closed again before this returns, only the copied values live on.
     *
     * @throws IllegalArgumentException when a vault is named but cannot be opened (missing file,
     *                                  wrong password, no password source)
     */
    static NVGenericMap loadVault(ParamUtil.ParamMap params, String env) {
        String store = params.stringValue(Param.STORE, null);
        if (SUS.isEmpty(store)) {
            store = env;
        }
        if (SUS.isEmpty(store)) {
            return new NVGenericMap();
        }
        File file = new File(store.trim());
        if (!file.isFile()) {
            throw new IllegalArgumentException("secret store not found: " + file);
        }
        char[] password = vaultPassword(params, file);
        if (password == null) {
            throw new IllegalArgumentException("store.password required for " + file
                    + ": pass store.password=<value> or run from an interactive console");
        }
        try (SecretStore vault = SecretStore.open(file, password)) {
            NVGenericMap ret = vault.toNVGenericMap("db.");
            log.getLogger().info("secret store " + file + " supplied " + ret.size() + " db.* setting(s)");
            return ret;
        } catch (Exception e) {
            throw new IllegalArgumentException("cannot open secret store " + file + ": " + e.getMessage(), e);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static char[] vaultPassword(ParamUtil.ParamMap params, File file) {
        String password = params.stringValue(Param.STORE_PASSWORD, null);
        if (!SUS.isEmpty(password)) {
            return password.toCharArray();
        }
        Console console = System.console();
        if (console == null) {
            return null;
        }
        char[] ret = console.readPassword("Password for secret store " + file.getName() + ": ");
        return ret == null || ret.length == 0 ? null : ret;
    }

    static H2PDataStore openStore(String url, String user, String password, String filePassword) {
        OPSecUtil.singleton();
        NVGenericMap parsed = H2PUtil.parseJdbcURL(url);
        String subprotocol = parsed.getValue(H2PUtil.JDBC_SUBPROTOCOL);
        APIConfigInfo cfg;
        if ("postgresql".equals(subprotocol)) {
            String database = parsed.getValue(H2PUtil.JDBC_DATABASE);
            if (SUS.isEmpty(database)) {
                throw new IllegalArgumentException("PostgreSQL URL must name the database: jdbc:postgresql://host:port/dbname");
            }
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("PostgreSQL driver not on the classpath", e);
            }
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password);
        } else {
            cfg = H2PDSCreator.toAPIConfigInfo(url, user, password, filePassword);
        }
        return new H2PDSCreator().createAPI(null, cfg);
    }

    private static boolean holdsRole(ShiroDSDomainSecurityManager dsm, SubjectIdentifier subject, RoleInfo role, AppIDDefault app) {
        return !roleGrants(dsm, subject, role, app).isEmpty();
    }

    /** The subject's grants of {@code role} under exactly {@code app} (null = the global grants). */
    private static List<RoleGrant> roleGrants(ShiroDSDomainSecurityManager dsm, SubjectIdentifier subject, RoleInfo role, AppIDDefault app) {
        List<RoleGrant> ret = new ArrayList<>();
        for (RoleGrant g : dsm.getRoleGrants(subject.getGUID())) {
            if (role.getGUID().equals(g.getRoleGUID()) && sameScope(g.getAppIdDAO(), app)) {
                ret.add(g);
            }
        }
        return ret;
    }

    private static boolean sameScope(AppIDDefault stored, AppIDDefault wanted) {
        return wanted == null ? stored == null : wanted.equals(stored);
    }

    private static String scopeLabel(AppIDDefault app) {
        return app == null ? "" : " [app " + ShiroDSDomainSecurityManager.appScope(app) + "]";
    }

    /**
     * {@code app.id=} as {@code <domain>-<app>} ({@link AppIDDefault#create(String)}), or with
     * {@code domain.id=} the bare app name; null when absent.
     *
     * @throws IllegalArgumentException when the value is malformed
     */
    static AppIDDefault appOf(ParamUtil.ParamMap params) {
        String appID = params.stringValue(Param.APP_ID, null);
        if (SUS.isEmpty(appID)) {
            return null;
        }
        String domainID = params.stringValue(Param.DOMAIN_ID, null);
        try {
            return SUS.isEmpty(domainID) ? AppIDDefault.create(appID.trim()) : new AppIDDefault(domainID.trim(), appID.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("app.id must be <domain>-<app> (or domain.id=<domain> app.id=<app>): " + e.getMessage(), e);
        }
    }

    private static void listGrants(ShiroDSDomainSecurityManager dsm, SubjectIdentifier subject, PrintStream out) {
        out.println("grants of " + subject.getGUID() + ":");
        for (RoleGrant g : dsm.getRoleGrants(subject.getGUID())) {
            RoleInfo r = dsm.lookupRoleByGUID(g.getRoleGUID());
            out.println("  role " + (r != null ? r.getName() : g.getRoleGUID()) + scopeLabel(g.getAppIdDAO()) + brokerLabel(g.getBrokerGUID()));
        }
        for (RoleGroupGrant g : dsm.getRoleGroupGrants(subject.getGUID())) {
            RoleGroupInfo r = dsm.lookupRoleGroupByGUID(g.getRoleGroupGUID());
            out.println("  role group " + (r != null ? r.getName() : g.getRoleGroupGUID()) + scopeLabel(g.getAppIdDAO()) + brokerLabel(g.getBrokerGUID()));
        }
        for (PermissionGrant g : dsm.getPermissionGrants(subject.getGUID())) {
            String token = g.getPermissionToken();
            if (SUS.isEmpty(token)) {
                PermissionInfo p = dsm.lookupPermissionByGUID(g.getPermissionGUID());
                token = p != null ? p.getName() + " = " + p.getPermissionToken() : g.getPermissionGUID();
            }
            out.println("  permission " + token
                    + (g.getResourceMap() != null ? " on " + g.getResourceMap().getResourceGUID() : "")
                    + scopeLabel(g.getAppIdDAO()) + brokerLabel(g.getBrokerGUID()));
        }
    }

    private static String brokerLabel(String brokerGUID) {
        return SUS.isEmpty(brokerGUID) ? "" : " by " + brokerGUID;
    }

    /**
     * {@code password=} if given, else a double console prompt, else null after an error message.
     */
    private static String password(ParamUtil.ParamMap params, String prompt, PrintStream err) {
        String password = params.stringValue(Param.PASSWORD, null);
        if (!SUS.isEmpty(password)) {
            return password;
        }
        Console console = System.console();
        if (console == null) {
            err.println("password required: pass password=<value> or run from an interactive console");
            return null;
        }
        char[] first = console.readPassword(prompt);
        char[] second = console.readPassword("Repeat: ");
        try {
            if (first == null || first.length == 0 || !Arrays.equals(first, second)) {
                err.println("passwords are empty or do not match");
                return null;
            }
            return new String(first);
        } finally {
            if (first != null) Arrays.fill(first, '\0');
            if (second != null) Arrays.fill(second, '\0');
        }
    }

    private static void listCatalog(ShiroDSDomainSecurityManager dsm, PrintStream out) {
        out.println("permissions:");
        for (PermissionInfo p : dsm.getPermissions()) {
            out.println("  " + p.getName() + " = " + p.getPermissionToken()
                    + (p.getAppIdDAO() != null ? "  [app " + p.getAppIdDAO().getAppID() + "]" : ""));
        }
        out.println("roles:");
        for (RoleInfo r : dsm.getRoles()) {
            StringBuilder sb = new StringBuilder();
            if (r.getPermissions() != null) {
                for (PermissionInfo p : r.getPermissions()) {
                    if (sb.length() > 0) sb.append(", ");
                    sb.append(p.getName());
                }
            }
            out.println("  " + r.getName() + " -> [" + sb + "]");
        }
        out.println("role groups:");
        for (RoleGroupInfo g : dsm.getRoleGroups()) {
            StringBuilder sb = new StringBuilder();
            if (g.getRoles() != null) {
                for (RoleInfo r : g.getRoles()) {
                    if (sb.length() > 0) sb.append(", ");
                    sb.append(r.getName());
                }
            }
            out.println("  " + g.getName() + " -> [" + sb + "]");
        }
    }
}
