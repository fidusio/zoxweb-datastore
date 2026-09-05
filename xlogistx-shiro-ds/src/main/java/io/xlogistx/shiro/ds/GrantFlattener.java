package io.xlogistx.shiro.ds;

import org.zoxweb.shared.security.*;
import org.zoxweb.shared.util.SUS;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Turns a subject's grants into the flat role names and permission strings Shiro authorizes on.
 * <ul>
 *   <li>{@link PermissionGrant} contributes the permission's token.</li>
 *   <li>{@link RoleGrant} contributes the role name and every permission token the role carries.</li>
 *   <li>{@link RoleGroupGrant} contributes every role in the group, each expanded as above.</li>
 * </ul>
 * Roles reached through a group are re-read by GUID so their permission references are resolved
 * regardless of how deeply the store materialised the group. Missing catalog rows (a grant whose
 * permission or role was deleted) are skipped, not failed.
 */
public final class GrantFlattener {

    /** Flattened authorization: insertion-ordered, duplicate-free. */
    public static final class Result {
        public final Set<String> roles = new LinkedHashSet<>();
        public final Set<String> permissions = new LinkedHashSet<>();

        public boolean isEmpty() {
            return roles.isEmpty() && permissions.isEmpty();
        }
    }

    private GrantFlattener() {
    }

    public static Result flatten(ShiroDSDomainSecurityManager dsm, String subjectGUID) {
        Result ret = new Result();
        if (dsm == null || SUS.isEmpty(subjectGUID)) {
            return ret;
        }
        for (PermissionGrant g : dsm.getPermissionGrants(subjectGUID)) {
            addPermission(ret, dsm.lookupPermissionByGUID(g.getPermissionGUID()));
        }
        for (RoleGrant g : dsm.getRoleGrants(subjectGUID)) {
            addRole(ret, dsm.lookupRoleByGUID(g.getRoleGUID()));
        }
        for (RoleGroupGrant g : dsm.getRoleGroupGrants(subjectGUID)) {
            RoleGroupInfo group = dsm.lookupRoleGroupByGUID(g.getRoleGroupGUID());
            RoleInfo[] roles = group != null ? group.getRoles() : null;
            if (roles != null) {
                for (RoleInfo role : roles) {
                    if (role != null && !SUS.isEmpty(role.getGUID())) {
                        RoleInfo full = dsm.lookupRoleByGUID(role.getGUID());
                        addRole(ret, full != null ? full : role);
                    }
                }
            }
        }
        return ret;
    }

    static void addRole(Result ret, RoleInfo role) {
        if (role == null) {
            return;
        }
        if (!SUS.isEmpty(role.getName())) {
            ret.roles.add(role.getName());
        }
        PermissionInfo[] permissions = role.getPermissions();
        if (permissions != null) {
            for (PermissionInfo p : permissions) {
                addPermission(ret, p);
            }
        }
    }

    static void addPermission(Result ret, PermissionInfo permission) {
        if (permission != null && !SUS.isEmpty(permission.getPermissionToken())) {
            ret.permissions.add(permission.getPermissionToken());
        }
    }
}
