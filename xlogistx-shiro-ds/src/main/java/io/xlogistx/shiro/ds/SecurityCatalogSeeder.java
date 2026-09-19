package io.xlogistx.shiro.ds;

import org.zoxweb.shared.security.PermissionInfo;
import org.zoxweb.shared.security.RoleGroupInfo;
import org.zoxweb.shared.security.RoleInfo;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.SUS;
import org.zoxweb.shared.util.SharedStringUtil;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Materialises the built-in catalog of {@link SecurityModel} into the manager's store:
 * one global (app-less) {@link PermissionInfo} per {@link SecurityModel.Permission}, one
 * {@link RoleInfo} per {@link SecurityModel.Role} carrying exactly the permissions the model
 * declares, and one {@link RoleGroupInfo} per {@link SecurityModel.RoleGroup}.
 * <p>
 * Idempotent: rows are found by {@code (appID = null, name)}; missing ones are created, drifted
 * ones (token, description, permission set, role set) are repaired, matching ones are left alone.
 * Everything runs in one transaction. The seeder never grants anything to any subject; the reserved
 * {@code super_admin_all} permission and {@code super_admin} role are created here and granted only
 * by {@link SecurityBootstrap}. With enforcement on, the caller needs the catalog permissions (or
 * the wildcard).
 */
public final class SecurityCatalogSeeder {

    /** What one run did, per kind. */
    public static final class Report {
        public int permissionsCreated, permissionsUpdated, permissionsExisting;
        public int rolesCreated, rolesUpdated, rolesExisting;
        public int roleGroupsCreated, roleGroupsUpdated, roleGroupsExisting;

        public boolean isNoOp() {
            return permissionsCreated + permissionsUpdated + rolesCreated + rolesUpdated
                    + roleGroupsCreated + roleGroupsUpdated == 0;
        }

        @Override
        public String toString() {
            return "permissions[created=" + permissionsCreated + ", updated=" + permissionsUpdated + ", existing=" + permissionsExisting
                    + "] roles[created=" + rolesCreated + ", updated=" + rolesUpdated + ", existing=" + rolesExisting
                    + "] roleGroups[created=" + roleGroupsCreated + ", updated=" + roleGroupsUpdated + ", existing=" + roleGroupsExisting + "]";
        }
    }

    private SecurityCatalogSeeder() {
    }

    /**
     * Seeds or repairs the catalog.
     *
     * @param dsm the manager whose store receives the catalog
     * @return counts of what was created, repaired and left alone
     */
    public static Report seed(ShiroDSDomainSecurityManager dsm) {
        SUS.checkIfNulls("manager can't be null", dsm);
        Report report = new Report();
        dsm.inTransaction(() -> {
            Map<SecurityModel.Permission, PermissionInfo> permissions = seedPermissions(dsm, report);
            Map<SecurityModel.Role, RoleInfo> roles = seedRoles(dsm, permissions, report);
            seedRoleGroups(dsm, roles, report);
            return null;
        });
        dsm.getRealm().evictAllAuthorization();
        return report;
    }

    private static Map<SecurityModel.Permission, PermissionInfo> seedPermissions(ShiroDSDomainSecurityManager dsm, Report report) {
        Map<SecurityModel.Permission, PermissionInfo> ret = new LinkedHashMap<>();
        for (SecurityModel.Permission model : SecurityModel.Permission.values()) {
            PermissionInfo existing = dsm.lookupPermission(null, model.getName());
            if (existing == null) {
                ret.put(model, dsm.createPermission(model.toPermissionInfo()));
                report.permissionsCreated++;
                continue;
            }
            boolean drift = !model.getValue().equals(existing.getPermissionToken())
                    || !SharedStringUtil.equals(model.getDescription(), existing.getDescription(), false);
            if (drift) {
                if (model.isReserved()) {
                    // the reserved row is immutable through the manager; a drifted one is a corrupt catalog
                    throw new IllegalStateException("reserved permission row drifted: " + existing.getPermissionToken());
                }
                existing.setPermissionToken(model.getValue());
                existing.setDescription(model.getDescription());
                dsm.updatePermission(existing);
                report.permissionsUpdated++;
            } else {
                report.permissionsExisting++;
            }
            ret.put(model, existing);
        }
        return ret;
    }

    private static Map<SecurityModel.Role, RoleInfo> seedRoles(ShiroDSDomainSecurityManager dsm,
                                                              Map<SecurityModel.Permission, PermissionInfo> permissions,
                                                              Report report) {
        Map<SecurityModel.Role, RoleInfo> ret = new LinkedHashMap<>();
        for (SecurityModel.Role model : SecurityModel.Role.values()) {
            PermissionInfo[] wanted = new PermissionInfo[model.getPermissions().length];
            for (int i = 0; i < wanted.length; i++) {
                wanted[i] = permissions.get(model.getPermissions()[i]);
            }
            RoleInfo existing = dsm.lookupRole(null, model.getName());
            if (existing == null) {
                ret.put(model, dsm.createRole(new RoleInfo(model.getName(), model.getDescription(), wanted)));
                report.rolesCreated++;
                continue;
            }
            Set<String> wantedGUIDs = new HashSet<>();
            for (PermissionInfo p : wanted) {
                wantedGUIDs.add(p.getGUID());
            }
            Set<String> currentGUIDs = new HashSet<>();
            PermissionInfo[] current = existing.getPermissions();
            if (current != null) {
                for (PermissionInfo p : current) {
                    if (p != null && !SUS.isEmpty(p.getGUID())) {
                        currentGUIDs.add(p.getGUID());
                    }
                }
            }
            boolean drift = !wantedGUIDs.equals(currentGUIDs)
                    || !SharedStringUtil.equals(model.getDescription(), existing.getDescription(), false);
            if (drift) {
                existing.setDescription(model.getDescription());
                existing.setPermissions(wanted);
                dsm.updateRoleInternal(existing);
                report.rolesUpdated++;
            } else {
                report.rolesExisting++;
            }
            ret.put(model, existing);
        }
        return ret;
    }

    private static void seedRoleGroups(ShiroDSDomainSecurityManager dsm, Map<SecurityModel.Role, RoleInfo> roles, Report report) {
        for (SecurityModel.RoleGroup model : SecurityModel.RoleGroup.values()) {
            RoleInfo[] wanted = new RoleInfo[model.getRoles().length];
            for (int i = 0; i < wanted.length; i++) {
                wanted[i] = roles.get(model.getRoles()[i]);
            }
            RoleGroupInfo existing = dsm.lookupRoleGroup(null, model.getName());
            if (existing == null) {
                RoleGroupInfo group = new RoleGroupInfo(wanted);
                group.setName(model.getName());
                group.setDescription(model.getDescription());
                dsm.createRoleGroup(group);
                report.roleGroupsCreated++;
                continue;
            }
            Set<String> wantedGUIDs = new HashSet<>();
            for (RoleInfo r : wanted) {
                wantedGUIDs.add(r.getGUID());
            }
            Set<String> currentGUIDs = new HashSet<>();
            RoleInfo[] current = existing.getRoles();
            if (current != null) {
                for (RoleInfo r : current) {
                    if (r != null && !SUS.isEmpty(r.getGUID())) {
                        currentGUIDs.add(r.getGUID());
                    }
                }
            }
            boolean drift = !wantedGUIDs.equals(currentGUIDs)
                    || !SharedStringUtil.equals(model.getDescription(), existing.getDescription(), false);
            if (drift) {
                existing.setDescription(model.getDescription());
                existing.setRoles(wanted);
                dsm.updateRoleGroup(existing);
                report.roleGroupsUpdated++;
            } else {
                report.roleGroupsExisting++;
            }
        }
    }
}
