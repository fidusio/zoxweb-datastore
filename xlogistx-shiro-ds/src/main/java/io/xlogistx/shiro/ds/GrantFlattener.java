package io.xlogistx.shiro.ds;

import org.zoxweb.shared.app.AppIDDefault;
import org.zoxweb.shared.security.*;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.server.logging.LogWrapper;
import org.zoxweb.shared.util.DataEncoder;
import org.zoxweb.shared.util.SUS;
import org.zoxweb.shared.util.SharedStringUtil;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Flattens a subject's grants into the role names and Shiro permission strings the realm hands to
 * Shiro. A {@link PermissionGrant} contributes its inlined token or its catalog token, plus
 * {@code :<resource guid>} when it embeds a {@link ResourceMap}; a {@link RoleGrant} contributes
 * the role name and the role's permission tokens; a {@link RoleGroupGrant} does the same for every
 * role of the group (each re-read by GUID so nested permission references resolve).
 * <p>
 * <b>Login scope (user decision 2026-09-18).</b> A grant whose {@code app_id} is set is the
 * subject's assignment to that app. Which grants apply is decided by the login: a login with
 * domain and app loads <em>only</em> the grants scoped to that app, a login without loads
 * <em>only</em> the global (unscoped) grants. <b>The super-admin is the exception:</b> its global
 * grants (the {@code *}) apply in every login, app or not. Loaded grants flatten as plain tokens,
 * never prefixed. A wildcard token is emitted only for the super-admin subject (defense in depth
 * behind the manager guards).
 */
public final class GrantFlattener {

    public static final class Result {
        public final Set<String> roles = new LinkedHashSet<>();
        public final Set<String> permissions = new LinkedHashSet<>();

        public boolean isEmpty() {
            return roles.isEmpty() && permissions.isEmpty();
        }
    }

    private GrantFlattener() {
    }

    public static final LogWrapper log = new LogWrapper(GrantFlattener.class).setEnabled(true);

    /** The global grants of the subject (a login without domain/app). */
    public static Result flatten(ShiroDSDomainSecurityManager dsm, String subjectGUID) {
        return flatten(dsm, subjectGUID, null);
    }

    /**
     * The grants that apply to a login scope: {@code scope == null} selects the global grants only,
     * otherwise only the grants scoped to that app (domain and app compared case-insensitively),
     * plus, for the super-admin subject, its global grants in any scope.
     */
    public static Result flatten(ShiroDSDomainSecurityManager dsm, String subjectGUID, AppIDDefault scope) {
        Result ret = new Result();
        if (dsm == null || SUS.isEmpty(subjectGUID)) {
            return ret;
        }
        boolean superAdmin = dsm.isSuperAdminSubject(subjectGUID);
        for (PermissionGrant g : dsm.getPermissionGrants(subjectGUID)) {
            if (inScope(g, scope, superAdmin)) {
                addPermission(ret, permissionString(dsm, g), superAdmin, subjectGUID);
            }
        }
        for (RoleGrant g : dsm.getRoleGrants(subjectGUID)) {
            if (inScope(g, scope, superAdmin)) {
                addRole(ret, dsm.lookupRoleByGUID(g.getRoleGUID()), superAdmin, subjectGUID);
            }
        }
        for (RoleGroupGrant g : dsm.getRoleGroupGrants(subjectGUID)) {
            if (!inScope(g, scope, superAdmin)) {
                continue;
            }
            RoleGroupInfo group = dsm.lookupRoleGroupByGUID(g.getRoleGroupGUID());
            RoleInfo[] roles = group != null ? group.getRoles() : null;
            if (roles != null) {
                for (RoleInfo role : roles) {
                    if (role != null && !SUS.isEmpty(role.getGUID())) {
                        RoleInfo full = dsm.lookupRoleByGUID(role.getGUID());
                        addRole(ret, full != null ? full : role, superAdmin, subjectGUID);
                    }
                }
            }
        }
        return ret;
    }

    /**
     * True if the grant belongs to the login scope: a global grant for a null scope, else the same
     * app; the super-admin's global grants belong to every scope.
     */
    static boolean inScope(AuthzInfo grant, AppIDDefault scope, boolean superAdmin) {
        AppIDDefault app = grant.getAppIdDAO();
        if (app == null) {
            return scope == null || superAdmin;
        }
        return scope != null && scope.equals(app);
    }

    static String permissionString(ShiroDSDomainSecurityManager dsm, PermissionGrant permissionGrant) {
        String token = permissionGrant.getPermissionToken();
        if (SUS.isEmpty(token)) {
            PermissionInfo p = dsm.lookupPermissionByGUID(permissionGrant.getPermissionGUID());
            token = p != null ? p.getPermissionToken() : null;
        }
        if (SUS.isEmpty(token)) {
            return null;
        }
        ResourceMap rm = permissionGrant.getResourceMap();
        if (rm != null && !SUS.isEmpty(rm.getResourceGUID())) {
            token = token + SecurityModel.PART_SEP + rm.getResourceGUID();
        }
        return DataEncoder.StringLower.encode(token);
    }

    static void addRole(Result ret, RoleInfo role, boolean wildcardOK, String subjectGUID) {
        if (role == null) {
            return;
        }
        if (!SUS.isEmpty(role.getName())) {
            ret.roles.add(role.getName());
        }
        PermissionInfo[] permissions = role.getPermissions();
        if (permissions != null) {
            for (PermissionInfo p : permissions) {
                if (p != null) {
                    addPermission(ret, SharedStringUtil.toLowerCase(p.getPermissionToken()), wildcardOK, subjectGUID);
                }
            }
        }
    }

    /** Adds one permission string; a wildcard is dropped for anyone but the super-admin. */
    static void addPermission(Result ret, String token, boolean wildcardOK, String subjectGUID) {
        if (SUS.isEmpty(token)) {
            return;
        }
        if (SecurityModel.isWildcardToken(token) && !wildcardOK) {
            log.getLogger().warning("dropped wildcard permission for subject " + subjectGUID);
            return;
        }
        ret.permissions.add(token);
    }
}
