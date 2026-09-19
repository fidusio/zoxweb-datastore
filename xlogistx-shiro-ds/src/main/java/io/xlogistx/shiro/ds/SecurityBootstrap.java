package io.xlogistx.shiro.ds;

import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.zoxweb.server.security.SecUtil;
import org.zoxweb.shared.crypto.CIPassword;
import org.zoxweb.shared.crypto.CredentialHasher;
import org.zoxweb.shared.crypto.CryptoConst;
import org.zoxweb.shared.security.RoleGrant;
import org.zoxweb.shared.security.RoleInfo;
import org.zoxweb.shared.security.SubjectIdentifier;
import org.zoxweb.shared.security.model.SecurityModel;
import org.zoxweb.shared.util.BaseSubjectID;
import org.zoxweb.shared.util.SUS;

import java.util.UUID;

/**
 * The one path that creates the super-admin account and hands it the reserved {@code super_admin}
 * role. Not called by the manager itself: an operator runs it through the admin CLI
 * ({@code io.xlogistx.shiro.ds.tools.SecurityAdminTool}), which is the trusted bootstrap path.
 * Idempotent: an existing account is left as it is (password untouched), a missing role grant is
 * added, the catalog is seeded first. The password is hashed with ARGON2 and never logged or kept.
 */
public final class SecurityBootstrap {

    /** What the bootstrap did. */
    public static final class Result {
        public SecurityCatalogSeeder.Report catalog;
        public String principalID;
        public SubjectIdentifier subject;
        public boolean subjectCreated;
        public boolean roleGranted;
        public boolean loginVerified;
        public boolean wildcardVerified;

        @Override
        public String toString() {
            return "super-admin " + principalID + " subject=" + (subject != null ? subject.getGUID() : null)
                    + (subjectCreated ? " (created)" : " (existing)")
                    + " role=" + (roleGranted ? "granted" : "existing")
                    + " login=" + (loginVerified ? "verified" : "not checked")
                    + " wildcard=" + (wildcardVerified ? "verified" : "FAILED")
                    + " catalog{" + catalog + "}";
        }
    }

    private SecurityBootstrap() {
    }

    /**
     * Seeds the catalog and ensures the super-admin account exists and holds the reserved role.
     *
     * @param dsm      the manager (enforcement is expected to be off, or the caller to hold the wildcard)
     * @param password the password for a new account; ignored when the account already exists
     * @return what happened; {@code wildcardVerified} is false only if the realm does not imply
     * an arbitrary permission for the account, which means the guards are misconfigured
     * @throws IllegalArgumentException if the account is missing and no password was given
     */
    public static Result bootstrapSuperAdmin(ShiroDSDomainSecurityManager dsm, String password) {
        SUS.checkIfNulls("manager can't be null", dsm);
        Result ret = new Result();
        ret.principalID = dsm.getSuperAdminPrincipalID();
        ret.catalog = dsm.seedCatalog();

        SubjectIdentifier subject = dsm.lookupSubjectID(ret.principalID);
        if (subject == null) {
            if (SUS.isEmpty(password)) {
                throw new IllegalArgumentException("password required to create " + ret.principalID);
            }
            subject = dsm.createSubjectID(ret.principalID, hash(password), BaseSubjectID.SubjectType.SYSTEM);
            ret.subjectCreated = true;
        }
        ret.subject = subject;

        RoleInfo role = dsm.lookupRole(null, SecurityModel.Role.SUPER_ADMIN.getName());
        if (role == null) {
            throw new IllegalStateException("catalog has no " + SecurityModel.Role.SUPER_ADMIN.getName() + " role after seeding");
        }
        boolean granted = false;
        for (RoleGrant g : dsm.getRoleGrants(subject.getGUID())) {
            if (role.getGUID().equals(g.getRoleGUID())) {
                granted = true;
                break;
            }
        }
        if (!granted) {
            dsm.addRoleGrant(subject, role);
            ret.roleGranted = true;
        }

        if (!SUS.isEmpty(password) && ret.subjectCreated) {
            dsm.login(ret.principalID, password);
            ret.loginVerified = true;
        }
        ret.wildcardVerified = impliesAnything(dsm, subject.getGUID());
        return ret;
    }

    /**
     * Replaces the super-admin password (every old password row is removed).
     *
     * @throws IllegalStateException if the account does not exist
     */
    public static void resetSuperAdminPassword(ShiroDSDomainSecurityManager dsm, String newPassword) {
        SUS.checkIfNulls("manager can't be null", dsm);
        if (SUS.isEmpty(newPassword)) {
            throw new IllegalArgumentException("new password required");
        }
        SubjectIdentifier subject = dsm.lookupSuperAdminSubject();
        if (subject == null) {
            throw new IllegalStateException("super-admin account does not exist: " + dsm.getSuperAdminPrincipalID());
        }
        dsm.updateCredential(subject, hash(newPassword));
    }

    /** True if the realm implies a random, never-granted permission for the subject: only the wildcard does that. */
    static boolean impliesAnything(ShiroDSDomainSecurityManager dsm, String subjectGUID) {
        DSAuthorizingRealm realm = dsm.getRealm();
        realm.evictAuthorization(subjectGUID);
        PrincipalCollection pc = new SimplePrincipalCollection(UUID.fromString(subjectGUID), realm.getName());
        return realm.isPermitted(pc, "bootstrap:probe:" + UUID.randomUUID());
    }

    private static CIPassword hash(String password) {
        CredentialHasher<CIPassword> hasher = SecUtil.lookupCredentialHasher(CryptoConst.HashType.ARGON2.getName());
        return hasher.hash(password);
    }
}
