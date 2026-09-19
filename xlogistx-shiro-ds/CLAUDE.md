# xlogistx-shiro-ds

Second `DomainSecurityManager` implementation (the first is `DomainSecurityManagerDefault` in
zoxweb-core): persistence through **any** `APIDataStore`, authentication / authorization / caching
through **Apache Shiro 1.13**. Package `io.xlogistx.shiro.ds`, JDK 25 (same as h2p-datastore, which
the tests run against). Design page (architecture, decisions, phases):
https://claude.ai/code/artifact/61b80405-b4b9-48f6-a1b1-dd915e119f5e
Diagrams (module dependencies, runtime relationships, entity relationships):
https://claude.ai/code/artifact/c07aba5f-1ad8-4f30-a6a2-67229c4514d9

The manager, realm, flattener, seeder and bootstrap import no store; only `tools.SecurityAdminTool` (and the tests) bind to `H2PDataStore` (h2p-datastore is an optional compile dependency since 2026-09-16).

## Dependencies (pom)

Compile: zoxweb-core, xlogistx-shiro 1.0.0 (tokens incl. `APIKeyAuthenticationToken`,
`CredentialsInfoMatcher`, `DomainAuthenticationInfo`, `DomainPrincipalCollection`,
`ShiroSecurityManager`, `ShiroUtil`),
shiro-core, cache-api, ehcache, slf4j-api, commons-logging, h2p-datastore 1.0.0 (**optional**, for the
admin CLI only). Test: h2 (`${h2.version}` is defined in the parent pom since 2026-09-02), junit-jupiter-params. Version
properties (`xlogistx.version`, `apache.shiro.version`, `junit.version`, ...) come from the
`io.xlogistx:xlogistx-mvn` grandparent, not from this repo.

## Classes

| Class | Role |
|---|---|
| `ShiroDSDomainSecurityManager(APIDataStore[, CacheManager])` | The manager. **Self-managed** mode: builds its own `ShiroSecurityManager` (main-thread block **off**, `MemoryConstrainedCacheManager` unless one is given) with one `DSAuthorizingRealm`. **Attached** mode (`attach(realm, ds)`, `fromGlobal()`, `fromGlobal(ds)`): adopts an INI-built realm, owns no security manager, and routes every Shiro call through `SecurityUtils.getSecurityManager()`. Registers `CIPassword` and `SubjectAPIKey` as credential collections by default. |
| `DSAuthorizingRealm` | `AuthorizingRealm` that reads through the manager. **No-arg constructor for shiro.ini** (defaults: name `shiro-ds`, `CredentialsInfoMatcher`, authn caching off, authz caching on); the manager is bound by the manager itself, by `setDomainSecurityManager`, or lazily on first use from the `APIDataStore` registered in `ResourceManager` under `dataStoreResource` (default `Resource.DATA_STORE` = `"DataStore"`), failing loudly otherwise. Supports `DomainUsernamePasswordToken`, `APIKeyAuthenticationToken` and xlogistx-shiro's `JWTAuthenticationToken`. Authentication caching **off**; authorization cached per **subject GUID**. `evictAuthorization(guid)` / `evictAllAuthorization()`. |
| `io.xlogistx.shiro.authc.CredentialsInfoMatcher` (xlogistx-shiro; `DSCredentialsMatcher` was **merged into it** 2026-09-03) | Passwords (hash via `SecUtil.isPasswordValid`, `autoAuthenticationEnabled` skips the check). Raw API keys: constant-time compare + `isUsable(sak)` (status/expiry). JWTs: HMAC check with the key's secret (`SecUtil.decodeJWT`), `sub` == key ID, `exp`/`nbf` with `setClockSkewMillis` (default 1 min), key scope vs claims, and for `isTimeStampRequired()` keys an `iat` inside `setJWTTimestampWindowMillis` (default 5 min) plus optional `setJWTReplayCache(JWTTokenCache)`. All knobs are bean properties (INI-settable). |
| `io.xlogistx.shiro.authc.APIKeyAuthenticationToken` (**in xlogistx-shiro since 2026-09-03**) | Raw key token; principal = subject GUID once the realm resolved it, `toString` never prints the key. |
| `GrantFlattener` | subject GUID → roles + permission strings: PermissionGrant → its inlined `permission_token`, else the catalog token, **+ `:<resource_guid>` when the grant embeds a `ResourceMap`** (lower-cased, no lookup); RoleGrant → role name + its permission tokens; RoleGroupGrant → each role, **re-read by GUID** so nested permission refs resolve. Missing catalog rows are skipped. **Login scope (2026-09-18):** `flatten(dsm, guid, AppIDDefault scope)` — the realm passes the login's domain+app (`DSAuthorizingRealm.loginScopeOf(principals)`); a null scope selects the **global grants only**, an app scope selects **only the grants whose `app_id` equals that app**; the super-admin's global grants apply in every scope. Strings are always plain tokens (never prefixed). **Drops any wildcard string for a non-super-admin subject** (WARNING logged) — defense in depth behind the manager guards. |
| `SecurityCatalogSeeder` (2026-09-16) | `seed(dsm)` / `dsm.seedCatalog()`: materialises core `SecurityModel.Permission` / `Role` / `RoleGroup` as global rows, idempotent by `(appID=null, name)`, repairs drift (token, description, permission/role sets) in one transaction, never grants. `Report` counts created/updated/existing. `seedCatalog()` enforces `permission:create` + `role:create` even when nothing needs repair. |
| `SecurityBootstrap` (2026-09-16) | `bootstrapSuperAdmin(dsm, password)`: seed → create the super-admin subject (`SubjectType.SYSTEM`, ARGON2) if missing → grant `super_admin` once → verify by login (when created) and by a realm probe of a random permission (`Result.wildcardVerified`). `resetSuperAdminPassword`. The only path that hands out the wildcard. |
| `tools.SecurityAdminTool` (2026-09-16, dotted params since 2026-09-16 evening) | CLI, DMTool-style `key=value` args: `command=seed-catalog\|bootstrap-super-admin\|reset-super-admin-password\|create-subject\|grant-role\|reset-password\|list-catalog`, `db.url=` (→ env `XLOGISTX_SHIRO_DB_URL` → `-Dshiro.ds.db.url`), `db.user/db.password/db.enc-key` (H2 file password), **`store=<vault> store.password=`** (2026-09-17: an io-xlogistx opsec `SecretStore` BCFKS vault, or env `XLOGISTX_SECRET_STORE`; its `db.*` text secrets fill in whatever `db.*` is absent from the command line, command line wins; password prompted once on a console; vault closed before the store opens; missing file / wrong password / no password source → exit 1 with a message), `principal.id=` (the account the command works on; for the two super-admin commands it is that store's super-admin, default `super-admin@xlogistx.io`), `password` (or double console prompt), `role=`. `create-subject` makes the domain admins (`remote-admin`, `local-admin` on offline-H2 devices; `role=domain_admin`, ARGON2) and refuses `role=super_admin` before creating anything; idempotent like bootstrap. **Domain admins are domain-driven: `<domain-app-id>:*` at most, never a bare `*`** (user, 2026-09-16); `isWildcardToken` reserves only a token whose first part is `*`, so `mydomain:*` passes the guards. **App-scoped grants (2026-09-18):** `app.id=<domain>-<app>` (`AppIDDefault.create`; or `domain.id=` + bare `app.id=`) on `create-subject` / `grant-role` scopes the role grant to that app (idempotent per scope; it applies to logins made with that domain+app); `revoke-role principal.id= role= [app.id=]` deletes that grant, `revoke-app principal.id= app.id=` deletes every grant under the app (`revokeAppGrants`), `list-grants principal.id=` prints roles / groups / permissions with `[app …]` and `by <broker>`. `run(out, err, args)` returns 0/1/2 for tests. Needs `h2p-datastore` (now an **optional compile** dependency of this module) and registers BC via `OPSecUtil.singleton()`. |

## Wiring: self-managed vs shiro.ini

| | Self-managed | Attached (shiro.ini) |
|---|---|---|
| Create | `new ShiroDSDomainSecurityManager(ds)` | INI declares `dsRealm = io.xlogistx.shiro.ds.DSAuthorizingRealm`; app registers the store (`ResourceManager.SINGLETON.register(Resource.DATA_STORE, ds)`), sets the INI security manager global, then `ShiroDSDomainSecurityManager.fromGlobal()` (or `fromGlobal(ds)` / `attach(realm, ds)` without ResourceManager) |
| Security manager | own `ShiroSecurityManager` (`getShiroSecurityManager()`) | the INI's; `getShiroSecurityManager()` is `null`, `getSecurityManager()` returns the global one, `isSelfManaged()` false |
| Global install | `installAsGlobal()` = `SecurityUtils.setSecurityManager(own)`; needed before any `ShiroUtil` call | `installAsGlobal()` **throws**; the INI security manager is the one to install |
| Caches | `MemoryConstrainedCacheManager` by default | whatever `securityManager.cacheManager` the INI sets; none → authz re-flattened per call, evictions are no-ops |
| Matcher | manager's own `CredentialsInfoMatcher` | the INI's `dsRealm.credentialsMatcher` when it is a `CredentialsInfoMatcher` (knobs settable in INI: `matcher.clockSkewMillis = ...`), else one is created and set |

Sample: `src/test/resources/shiro-ds.ini`. Other realms can sit beside the DS realm in
`securityManager.realms`; `fromGlobal()` picks the first `DSAuthorizingRealm`. `attach` on a realm
already bound to the same store returns the bound manager; to a different store it throws.

Public extras on the manager beyond the interface: `loginJWT(compactJWT)`, the three bound logins
`loginSubject(principal, password, domainID, appID)` / `loginSubjectApiKey(key, domainID, appID)`
/ `loginSubjectJWT(compactJWT, host)` (full Shiro login: session + `ThreadContext` binding),
`logout()`, `verifyPassword(principal, password)` (boolean, see Login paths), static
`mintJWT(sak, algo, ttlMillis)`, `installAsGlobal()` (`SecurityUtils.setSecurityManager`),
`getShiroSecurityManager()`, `getRealm()`, `getCredentialsMatcher()` (JWT policy knobs),
`setEnforcePermissions(boolean)`, `getSecurityManager()` / `isSelfManaged()`, static `attach` /
`fromGlobal`, and the lookups the realm/flattener use (`lookupSubjectByGUID`,
`lookupSubjectAPIKey(key)` by secret, `lookupSubjectAPIKeyByID(keyID)` by key ID,
`lookupPermissionByGUID`, `lookupRoleByGUID`, `lookupRoleGroupByGUID`).

## Credential kinds

| Kind | Stored as | Login (authc only) | Login (bound Subject) |
|---|---|---|---|
| Principal ID + password | `CIPassword` row per subject | `login(principal, password)` | `loginSubject(principal, password, domainID, appID)` |
| Raw API key | `SubjectAPIKey.api_key` (URL-safe Base64 secret, `FilterType.ENCRYPT`) | `loginApiKey(key)` | `loginSubjectApiKey(key, domainID, appID)` |
| JWT bearer token | Same `SubjectAPIKey` row: `principal_id` = **key ID** (JWT `sub`), `api_key` = HMAC secret, optional `app_id` scope | `loginJWT(compactJWT)` | `loginSubjectJWT(compactJWT, host)` |

JWT model (same as `XXClientAPI` / `CredentialsInfoMatcher` in the xlogistx stack): the client builds
`JWT.createJWT(HS256, sak.getSubjectID(), domainID, appID)` and signs it with
`sak.getAPIKeyAsBytes()`; `mintJWT(sak, algo, ttl)` does exactly that (adds `exp` when `ttl > 0`).
The realm resolves the `SubjectAPIKey` by `sub` → `lookupSubjectAPIKeyByID`, the matcher verifies.
`SubjectAPIKey.getSubjectID()` is an alias of `getPrincipalID()`; `createCredential` stamps a UUIDv7
key ID when it is empty so every stored key can sign tokens. Key IDs are assumed unique per store
(no index enforces it; `lookupSubjectAPIKeyByID` takes the first match).

## Login paths

- `login(principal, password)` / `loginApiKey(key)` → `shiroSecurityManager.authenticate(token)`:
  authenticator only — **no Subject, no session, no thread binding**. Safe for "verify the current
  password" flows (no-sneak `Session.changePassword` calls `login`). Every
  `AuthenticationException` collapses to `SecurityException("Invalid credentials")` /
  `("Invalid key")`; the Shiro subtype is logged at INFO when `log` is enabled.
- `loginSubject(...)` → `new Subject.Builder(sm).buildSubject().login(token)` then
  `ThreadContext.bind`. Returns the Shiro `Subject` for `isPermitted` / `hasRole`.
- `logout()` → `subject.logout()` + `ThreadContext.unbindSubject()` on the calling thread.
- `loginJWT(compactJWT)` → `SecUtil.parseJWT` (no verification) → `JWTAuthenticationToken` →
  authenticator. Realm: `sub` → key → `checkUsable` (status/expiry) → ACTIVE subject; the key's
  `app_id` scope (when set) becomes the `DomainPrincipalCollection` domain/app, and the key ID rides
  along as `getJWSubjectID()`. Matcher: signature, `sub` == key ID, `exp`/`nbf` ± skew, claims must
  equal the key scope (case-insensitive; unscoped key accepts any claims), `iat` window + replay
  cache only when the key has `isTimeStampRequired()`. Any failure → `SecurityException("Invalid
  token")`. Numeric claims are read through the payload's property map because a parsed token holds
  them as `Integer` and the typed getters unbox to `long` (ClassCastException otherwise).
- `loginSubjectApiKey` / `loginSubjectJWT` → same tokens through `Subject.login` + `ThreadContext.bind`
  (shared `bindSubject` helper). A failed bound login leaves nothing bound.
- **xlogistx-shiro `ShiroUtil`** works unchanged once `installAsGlobal()` has run (it goes through
  `SecurityUtils`): `login(domain, realm, user, password)`, `login(AuthenticationToken)` with any of
  the three tokens, `loginSubject(subjectID, credentials, domainID, appID, autoLogin)` and
  `loginBySessionID(id)` (the `DefaultSecurityManager` session store, in-memory). `subjectUserID()`
  returns the subject GUID, `subjectJWTID()` the key ID, `subjectDomainID()` / `subjectAppID()` the
  token's scope. `autoLogin=true` is the stack's trusted-caller path: `CredentialsInfoMatcher` skips
  the password check, but the realm's status gating and the "password credential must exist" rule
  still apply, so a DEACTIVATED subject cannot be auto-logged-in.
- `verifyPassword(principal, password)` → direct check against the stored `CIPassword` with
  `SecUtil.isPasswordValid`, **outside Shiro**: no token, no session, no binding. Returns `false`
  (never throws) for unknown/rejected principal, missing or non-ACTIVE credential, non-ACTIVE
  principal, or a mismatch. The subject may be ACTIVE **or PENDING_RESET_PASSWORD** — this is the
  call a password-reset flow uses to prove knowledge of the current password, since `login` denies
  that status. Failure reason logged at INFO when `log` is enabled.

## Status rules (realm)

`null` counts as ACTIVE — rows created by the default manager were never stamped. The manager stamps
ACTIVE on every principal (`addPrincipalID`) and credential (`createCredential`, replacement path of
`updateCredential`) it creates.

| Object | Allowed | Shiro exception |
|---|---|---|
| Subject (`SecStatus`) | ACTIVE only | `DisabledAccountException` |
| Principal (`SecStatus`) | ACTIVE or null | `LockedAccountException` |
| Password credential (`SecStatus`) | ACTIVE or null | `ExpiredCredentialsException` |
| API key `Const.Status` + `SecStatus` | ACTIVE or null | `DisabledAccountException` |
| API key expiry | 0 or in the future | `ExpiredCredentialsException` |
| JWT (`sub` → key) | key usable as above, subject ACTIVE; signature/claims are the matcher's job | `UnknownAccountException` / `IncorrectCredentialsException` |

Decision 2 of the design page is implemented: PENDING_RESET_PASSWORD is **denied** by `login` /
`loginSubject` / `loginApiKey` (subject must be ACTIVE); `verifyPassword` is the dedicated call
that accepts it. no-sneak's `Session.changePassword` should switch to `verifyPassword` when it
adopts this manager (design phase 4, separate repo).

## Principal IDs

Every principal goes through `SecConst.SubjectIDFilter.SINGLETON.validate` (zoxweb-core,
2026-09-02): trimmed, lower-cased, invisible characters rejected, non-email handles must be ≥ 8
chars (emails bypass the length rule). Writes (`createSubjectID`, `addPrincipalID`) throw
`SecurityException("Invalid principal ID: <filter reason>")`; lookups treat a rejected ID as unknown
(`null` / empty array / login failure) — same as `DomainSecurityManagerDefault.resolvePrincipal`.
`DomainUsernamePasswordToken` lower-cases the username too, and the realm passes the token username
as the primary principal so `CredentialsInfoMatcher`'s principal-equality check holds. Mixed-case rows
created before the filter existed will not match.

## Transactions

`inTransaction(Supplier)`: if `ds.isTransactionActive()` (new `APIDataStore` default `false`;
`H2PDataStore` implements it) the body runs inside the caller's transaction and the caller
commits/aborts; otherwise begin → body → `endTransaction`, or `abortTransaction` on any throwable.
Used by `createSubjectID`, `updateCredential` (both paths), `deletePrincipalID`, `deleteSubjectID`
(now atomic). H2P rejects nested `beginTransaction`, which is why joining matters.

Stores without transactions (mock, or defaults left as no-ops) still work; they lose atomicity and
the row-lock below.

## Behaviour that differs from DomainSecurityManagerDefault

- `updateCredential`: GUID path verifies **both** the given entity and the stored row belong to the
  subject (`SecurityException` otherwise; a missing `subject_guid` on the entity is stamped). No-GUID
  path accepts only `CIPassword`, deletes **every** old password row, inserts the new one. Anything
  else → `IllegalArgumentException` (the default silently ignored it).
- `deletePrincipalID`: inside the transaction, `ds.update(subject)` first (row lock → concurrent
  removals on one subject serialize), then count → delete → recount; `false` when the principal is the
  last one. H2's default `LOCK_TIMEOUT` is 1 s, so the loser of a lock race may get an exception
  rather than `false` (the test tolerates both).
- `deleteSubjectID`: cascades principals, every registered credential collection **plus
  `SubjectAPIKey` always**, all three grant types, then the subject — one transaction.
- `createCredential` on a `SubjectAPIKey` with no key ID (`principalID`) stamps a UUIDv7 one.
- `createSubjectID`: no `synchronized` (unique index on `principal_id` is the guard); subject type
  USER, status ACTIVE, UUIDv7 GUID set before insert; own `SecurityException`s are rethrown, other
  runtime failures wrapped once.
- Cache eviction after mutations: grant add/delete and subject update/delete → that subject's
  authorization entry; permission/role/role-group update/delete and `setDataStore` → whole
  authorization cache.
- Authorization loading is **lazy** by default (Shiro): grants are flattened from the store on the
  first `isPermitted`/`hasRole` and cached per subject GUID. `setEagerAuthorization(true)` (manager
  or realm; INI `dsRealm.eagerAuthorization = true`) loads them inside the login instead: the realm
  overrides `assertCredentialsMatch`, so only a *successful* credential match warms the cache
  (`login`, `loginSubject*`, `ShiroUtil` paths alike). A grant-load failure there is logged and
  swallowed; the login stands and grants load lazily later. Shiro core has no eager flag of its own
  (`getAuthenticationInfo` is final); this is the module's. The realm also implements zoxweb-core's
  `AuthorizationInfoLookup`, so `ShiroUtil.lookupAuthorizationInfo(DSAuthorizingRealm.class, pc)`
  returns the cached-or-loaded info.
- Permission enforcement (`setEnforcePermissions(true)`, default **off**): mutations require the
  thread-bound authenticated Subject to hold the `SecurityModel` token (`PERM_ADD_USER`,
  `PERM_DELETE_SUBJECT`, `PERM_UPDATE_SUBJECT`, `PERM_ADD/UPDATE/DELETE_PERMISSION`,
  `PERM_ADD/UPDATE/DELETE_ROLE` (also role groups), `PERM_ASSIGN/REMOVE_PERMISSION`,
  `PERM_ASSIGN/REMOVE_ROLE`). No subject bound → `AccessException(UNAUTHORIZED)`. A subject acting on
  its **own** principals/credentials is always allowed (`enforceSelfOr`). Off by default because
  `DMTool` and no-sneak bootstrap run with nobody logged in.

## Catalog, super-admin and the reserved wildcard (built 2026-09-16; design page item 20 + issue 1)

Core `SecurityModel` was rebuilt: `Target × Action` enums, `Permission` entries with fixed tokens
(`subject:create`, `permission:assign:permission`, `nventity:read:*`, … — no placeholders, no
instance parts), `Role` now declares `Permission[]` (`SUPER_ADMIN{super_admin_all}`, `DOMAIN_ADMIN`,
`APP_ADMIN`, `APP_SERVICE_PROVIDER`, `APP_USER{}`, `USER{}`), new `RoleGroup` enum
(`domain_admins`, `app_admins`, `app_users`, `service_providers`; none contains `super_admin`),
`isWildcardToken(token)`. The `PERM_*` String constants are unchanged (callers here, opsec, ShiroUtil);
the resource/self/private/public constants and `AppPermission` are `@Deprecated` (dead:
`APIAppManagerProvider` has no production caller). `PermissionToken` enum removed.

**The wildcard `*` (`Permission.SUPER_ADMIN_ALL`, role `super_admin`) is reserved for one account**
— the realm property `superAdminPrincipalID` (default `super-admin@xlogistx.io`, normalized by
`SubjectIDFilter`, INI `dsRealm.superAdminPrincipalID = …`, manager `get/setSuperAdminPrincipalID`,
`isSuperAdminSubject(guid)`, `lookupSuperAdminSubject()`). Guards (all re-read catalog rows by GUID,
never trust the passed object):

| Path | Rule |
|---|---|
| `createPermission` | a wildcard token only as global `super_admin_all`, only while none exists (`IllegalArgumentException`) |
| `updatePermission` / `deletePermission` | reserved row immutable / undeletable; no row may acquire a wildcard token |
| `createRole` / `updateRole` / `deleteRole` | a role embedding the reserved permission only as global `super_admin`, once; reserved role immutable through the public path (`updateRoleInternal` is the seeder's) |
| `createRoleGroup` / `updateRoleGroup` | may never embed `super_admin` |
| `addPermissionGrant` (global) / `addRoleGrant` / `addRoleGroupGrant` | reserved permission / role / group only to the super-admin subject (`AccessException`); scoped and inlined grants already reject `*` (`isInstanceScopable`, `NVEPermissionTokenFilter`) |
| `GrantFlattener` | drops wildcard strings for any other subject (rows smuggled in through the store) |

Bootstrap is **CLI only** (user decision): `SecurityAdminTool command=bootstrap-super-admin db.url=… principal.id=… password=…`
(or console prompt). Nothing creates the account automatically. Enforcement stays off inside the
tool. `deleteSubjectID(superAdmin)` is allowed; recovery = rerun bootstrap. Renaming the property
after bootstrap leaves the old account's `*` rows in place, but the flattener drops them on the next
authorization load (setter evicts all). Tests: `SecurityCatalogDBTest` (14).

## Password reset (built 2026-09-16; issue 3)

**The recovery channel is an email-shaped `PrincipalIdentifier`** (user decision): a subject may
hold several principals; self-service reset needs at least one active principal that passes
`FilterType.EMAIL.isValid`, and the token is meant for every such address. Username-only subjects
are reset by an admin. Core: entity `PasswordResetToken` (table `password_reset_token`:
`principal_id`, `token_hash` = base64url SHA-256 of the clear token, `expiry_ts`, `consumed_ts`,
`status` ACTIVE/DEACTIVATED(consumed)/INACTIVE(superseded, cancelled), `channel` EMAIL/ADMIN,
`broker_guid`; not a `CredentialInfo`), `PasswordResetRequest` (clear token returned once, masked
`toString`), `NoRecoveryChannelException`, `PasswordResetTokenUtil` (32 random bytes, constant-time
compare), `ResourceManager.Resource.DOMAIN_SECURITY_MANAGER`, and six `DomainSecurityManager`
methods (`verifyPassword` promoted to the interface; `DomainSecurityManagerDefault` implements them
unenforced, and its `login` still has no status gate).

| Call | Enforcement | Effect |
|---|---|---|
| `requestPasswordReset(principal)` | none (anonymous) | any principal of the subject; supersedes outstanding tokens, inserts one (EMAIL TTL = `SecStatus.PENDING_RESET_PASSWORD.getValue()` = 2 d), subject → `PENDING_RESET_PASSWORD`, returns token + email principals. Unknown/inactive → generic `SecurityException`; no email principal → `NoRecoveryChannelException` |
| `adminResetPassword(principal)` | `subject:update` (`*` implies) | same, channel ADMIN (TTL 4 h), `broker_guid` = bound subject, works for username-only subjects |
| `completePasswordReset(principal, token, newPassword)` | none (the token is the authorization) | row lock on the subject, outstanding token whose hash matches, `FilterType.PASSWORD` policy (its message is the only non-generic error), replaces every PASSWORD row, marks the token consumed, subject → ACTIVE, evicts authz |
| `cancelPasswordReset(principal)` | self-or-`subject:update` | supersedes tokens, restores ACTIVE |
| `purgeExpiredResetTokens()` | none | deletes every non-outstanding row |

Realm: a `PENDING_RESET_PASSWORD` subject with **no outstanding token** (expired, all cancelled) is
restored to ACTIVE at its next login (`activeSubject`), so an unclaimed request locks an account for
the token lifetime only; `verifyPassword` tolerates PENDING throughout. `setResetTokenTTL(channel,
ms)` / `getResetTokenTTL`. `deleteSubjectID` cascades the token rows. `installAsGlobal()` and
`attach(...)` register the manager under `Resource.DOMAIN_SECURITY_MANAGER` when the slot is empty
— that is how io-xlogistx opsec `services.PasswordReset` (endpoints `POST /opsec/password/reset-request`
[NONE, always 202, mail sent async via `SMTPSender` from the `reset-mailer-config` property and the
`reset-url` template], `/opsec/password/reset-confirm` [NONE, 204 / 400], `/opsec/password/admin-reset`
[ALL + `subject:update`, returns the token]) finds it through the core interface only. Rate limiting
is a deployment concern in front of the two anonymous endpoints. CLI: `SecurityAdminTool
command=reset-password principal.id=<principal>` prints an ADMIN token. no-sneak `Session.changePassword`
now calls `verifyPassword` instead of `login`. Tests: 13 `reset_*` tests here, one round trip in the
h2p default-manager suite, `PasswordResetTokenUtilTest` in core.

## Instance grants and sharing (built 2026-09-15; design page section 12, item 23)

A `PermissionGrant` is **either** catalog-backed (`permission_guid`; `resource_map` optional) **or**
inlined (`permission_token` + mandatory `resource_map`) — never both, never neither. This is a hard
precondition (user, 2026-09-15): `PermissionGrant.validateShape()` (core) runs first in every add
path, before any store access or enforcement. Core side (zoxweb-core, 2026-09-15): `SecurityModel.NVENTITY`,
`NVE_SHARE_ALL`, `toNVEToken(verbs...)`, `isInstanceScopable(token)`, the
`SecurityModel.NVEPermissionTokenFilter` `ValueFilter` wired on `PermissionGrant.permission_token`
(normalizes to lower-case `nventity:<verbs>` with verbs ⊆ {read, update, share, delete}; no create,
no wildcard, no instance part; rejects everything else), accessors renamed
`get/setPermissionToken`, `ResourceMap(NVEntity)` ctor, and four `DomainSecurityManager` methods
(also implemented in `DomainSecurityManagerDefault` without enforcement).

| Call | Shape written | Enforcement (when on) |
|---|---|---|
| `addPermissionGrant(subject, perm)` | global catalog grant | global `permission:assign:permission` |
| `addPermissionGrant(subject, perm, ResourceMap)` | catalog grant scoped to one entity; the catalog token must be 2-part `<ns>:<verbs>` (`isInstanceScopable`) | caller owns the resource, **or** holds `nventity:share:<guid>`, **or** global assign |
| `addPermissionGrant(subject, ResourceMap, token)` | inlined share; token validated by the filter | caller **owns** the resource (a share holder may not inline) |
| `deletePermissionGrant(grant)` | reloads by GUID, deletes grant **then its map row** | global `permission:remove:permission`, **or** caller is the grantor (`broker_guid`), **or** owns the resource |
| `getPermissionGrantsByResource(guid)` | two queries: `resource_map` rows by `resource_guid`, then grants `resource_map IN (…)` | none (read) |
| `deletePermissionGrantsByResource(guid)` | all of the above in one transaction | per grant as for delete |

Common to every add path: the resource must exist (`ds.searchByID(resource_type, resource_guid)`;
unknown class / missing row → `IllegalArgumentException`), `broker_guid` = bound subject GUID or
null when nobody is bound (regardless of enforcement), map row + grant row are written in one
`inTransaction`, the grantee's authorization entry is evicted. Flattened form (see `GrantFlattener`):
`nventity:read,share:<guid>` — one Shiro string per grant; `ShiroSecurityController` checks
`nventity:<verb>:<guid>`, Shiro's `WildcardPermission` handles the comma list. `deleteSubjectID`
keeps grantee scope (grants *received*) and now also removes their map rows; grants the subject
*issued* stay. H2P specifics: `resource_map` is a child table + FK column (`uuid`, indexed) — never
`delete(grant, true)` (it would chase `app_id` too); the H2P formatter now binds a String/NVEntity
criterion on an `ENTITY_REF` column as `uuid` (needed for the `IN` query on PostgreSQL).
Store requirement added: grants must load `resource_map` eagerly (H2P does), otherwise map rows leak.
Not done (still pending): ABAC conditions (A1), `SecurityModel` rework/seeder (item 20).

## What a store must provide (for "works with any APIDataStore")

Equality search on `principal_id` (principals **and** `SubjectAPIKey` key IDs), `subject_guid`,
`name`, `api_key` (both `QueryMatch` ctor forms); `AppIDDefault` persisted as a referenced entity
(`SubjectAPIKey.app_id`) and resolved on read;
`search`/`searchByID` by class-name string as well as by `NVConfigEntity`; insert keeps a pre-set
**UUID-formatted** GUID (`DomainAuthenticationInfo` parses it with `UUID.fromString`); reference
resolution on read (`RoleInfo.getPermissions()` with tokens, `RoleGroupInfo.getRoles()` with GUIDs);
enum + long round-trip (`SecStatus`, `Const.Status`, expiry); `api_key` searchable as written; unique
index on `principal_id`; `fieldNames` projection accepted or ignored. Verified only on H2 and
PostgreSQL through `H2PDataStore`; the Mongo stores are unverified on the GUID and reference points.

## Running the tests

`ShiroDSDomainSecurityManagerDBTest` — 36 tests: the `H2PDomainSecurityManagerDBTest` scenarios
ported, plus `SubjectIDFilter` normalization/rejection, status gating (subject / principal /
credential), credential ownership, unsupported-type rejection, caller-transaction join + rollback,
full password replacement, last-principal guard (sequential + 2-thread), subject-delete cascade incl.
API keys, API-key login with suspend/expire/renew, Shiro `isPermitted`/`hasRole` for permission,
role and role-group grants with eviction on revoke, enforcement on/off, `verifyPassword` (normalization, wrong/null input, PENDING_RESET_PASSWORD
accepted while `login` still denies it, DEACTIVATED subject and inactive credential rejected), and
JWT: HS256/HS512 round trip, forged secret, wrong domain / app / missing scope claims, case-insensitive
scope, unknown key ID, `exp` past, `nbf` future, garbage / empty / null / bad signature segment,
suspended key, deactivated subject, stale `iat` with `TS_REQUIRED`, replay cache refusing a second
presentation; bound logins via JWT and raw key (`isPermitted`, key ID in the principals, clean
unbind on failure); key-ID stamping on insert; every `ShiroUtil` login entry point against the
manager installed as global (password, raw key, JWT, `loginSubject` with and without `autoLogin`,
session resume by ID, logged-out session refused); shiro.ini wiring (INI-built realm + cache manager
+ matcher, store via ResourceManager, `fromGlobal` idempotent, login / ShiroUtil / grant eviction
through the INI realm, `installAsGlobal` refused; unbound realm fails loudly, `attach` binds, second
store rejected; the shipped `shiro-ds.ini` loads and attaches via `fromGlobal(ds)`); eager
authorization (lazy login caches nothing, eager login caches the grants, failed login does not,
`lookupAuthorizationInfo` via realm and `ShiroUtil`). App IDs in tests must pass `AppIDNameFilter`
(`"shirods"`, not `"shiro-ds"`).

Defaults to in-memory H2 (`jdbc:h2:mem:shirods;DB_CLOSE_DELAY=-1;MODE=PostgreSQL`); `-Dds.url`
switches to an H2 file (`;CIPHER=AES` + `-Dds.file_password`) or a PostgreSQL base endpoint
(`-Dds.db`, default `testdb`, created if missing; `-Dds.user` / `-Dds.password`). `@AfterEach` logs
out and aborts any leftover ambient transaction.

Surefire can't fetch its provider offline. Build with
`mvn -o -pl h2p-datastore,xlogistx-shiro-ds -DskipTests install` (h2p first so its
`isTransactionActive` is in the installed jar), then run the `LauncherFactory` main described in
`h2p-datastore/CLAUDE.md` with, on top of the h2p classpath: this module's `target/classes` +
`target/test-classes`, xlogistx-shiro 1.0.0, shiro-core / shiro-lang / shiro-cache /
shiro-crypto-hash / shiro-crypto-cipher / shiro-crypto-core / shiro-config-core /
shiro-config-ogdl / shiro-event 1.13.0, commons-beanutils 1.9.4, commons-logging 1.2,
cache-api 1.1.1, ehcache 3.9.11. Use `-ea:io.xlogistx... -ea:org.zoxweb...` (bare `-ea` also enables
H2's internal assertions and trips one in file-store compaction at close).

## Session log

**2026-09-02** — module created. Manager + realm + matcher + token + flattener written standalone
(not extending the default). `APIDataStore.isTransactionActive()` added to zoxweb-core by the user,
implemented in `H2PDataStore`. Later the same day zoxweb-core added `SecConst.SubjectIDFilter`
(commits "Filter update now implements DataEncoder", "Fixed the SubjectIDFilter" ×2); the manager
switched from its own lower-casing to the filter and gained the normalization test. `h2.version`
moved to the parent pom. Verified: 27/27 on H2 in-memory, 27/27 on PostgreSQL
(lax-2.xlogistx.io / testdb), h2p regression 10/10. Nothing committed.

**2026-09-02 (later)** — `verifyPassword(principal, password)` added (design open decision 2:
`login` keeps denying PENDING_RESET_PASSWORD, the reset flow gets a dedicated check). Test
`verifyPassword_allowsPendingReset_whileLoginDeniesIt`; 28/28 on H2 in-memory and 28/28 on
PostgreSQL (lax-2.xlogistx.io / testdb); h2p regression 10/10 on PostgreSQL. Nothing committed.

**2026-09-02 (JWT)** — API keys can now be presented as JWT bearer tokens: `JWTAuthenticationToken`
supported by the realm (`jwtInfo`, `sub` = key ID via new `lookupSubjectAPIKeyByID`), JWT branch in
`CredentialsInfoMatcher` (signature, scope, `exp`/`nbf`, `iat` window, optional `JWTTokenCache`
replay guard), manager `loginJWT` / `loginSubjectJWT` / `loginSubjectApiKey` / static `mintJWT` /
`getCredentialsMatcher`, key-ID stamping in `createCredential`, `logAuthFailure` now logs the wrapped
cause. Three tests added; 31/31 on H2 and 31/31 on PostgreSQL. Nothing committed.

**2026-09-02 (ShiroUtil)** — verified, no code change needed: all `ShiroUtil` login entry points
work against this realm via `installAsGlobal()`. Test `shiroUtil_loginEntryPoints_workAgainstThisManager`;
32/32 on H2 and PostgreSQL. Nothing committed.

**2026-09-02 (shiro.ini)** — the realm is INI-constructible (no-arg ctor, defaults, lazy store
resolution from `ResourceManager`); the manager gained attached mode (`attach`, `fromGlobal`,
`getSecurityManager`, `isSelfManaged`; `installAsGlobal` throws when attached). Sample
`src/test/resources/shiro-ds.ini`. Three tests added; 35/35 on H2 and PostgreSQL. Nothing committed.

**2026-09-02 (eager authz)** — `eagerAuthorization` on the realm (INI-settable) and the manager;
realm implements `AuthorizationInfoLookup`. One test added; 36/36 on H2 and PostgreSQL. Nothing
committed.

**2026-09-02 (core cleanup step 1)** — zoxweb-core (uncommitted, jar reinstalled) moved
`AuthorizationInfoLookup`, `RealmController`, `RealmControllerHolder` from `shared.security.shiro`
to `shared.security`, renamed `ShiroTokenReplacement` → `SecTokenReplacement`, and pointed
`SecurityModel.toPermission/toRole` at `PermissionInfo`/`RoleInfo` (new
`RoleInfo(name, description, permissions...)` ctor); a second pass dropped the domain/app
parameters from those converters (`Permission.toPermission(NVPair...)`,
`Role.toRole(name, description)`, `SecurityModel.toPermission(name, description, pattern, tokens)`),
since the new catalog entities are not domain/app scoped. io-xlogistx dropped its `sec-model`
module. xlogistx-shiro 1.0.0 was rebuilt against it. Only change here: the realm's
`AuthorizationInfoLookup` import. 36/36 on H2 and PostgreSQL, h2p 10/10.

**2026-09-03 (core cleanup step 3)** — zoxweb-core: `ShiroSessionData` → `shared.security.SecSessionData`;
`APISecurityManager` no longer extends `ShiroRealmStore`/`ShiroRulesManager`; `ZWDataFactory`
lost the Shiro entity entries; `APIAppManager` gained `get/setDomainSecurityManager` (so the app
manager will delegate permissions/roles/grants to a `DomainSecurityManager`, i.e. this manager).
The `shared.security.shiro` package (12 classes) is now referenced by nothing outside itself and
can be deleted (plan step 4). io-xlogistx deleted `APISecurityManagerProvider`, `ShiroBaseRealm`,
`XlogistXShiroRealm`, `ShiroRealmController`, `ShiroXlogistXRealm`, `authz/ShiroAuthorizationInfo`,
`ResourcePrincipalCollection` and two tests; `ShiroUtil.getRealmController` remains on the moved
`RealmController` interface. Both jars reinstalled 2026-09-03; no change needed here. 36/36 on H2
and PostgreSQL, h2p 10/10. Nothing committed anywhere. **xlogistx-ws is out of scope** (user
decision 2026-09-03): its dependence on `APIAppManagerProvider` does not block deleting that class
or the `shared.security.shiro` package. Open: `RealmController` has no implementation left
(implement it on `DSAuthorizingRealm`, or retire `ShiroUtil.getRealmController` + opsec callers).

**2026-09-03 (class relocation)** — core commit "removed dependencies to org.zoxweb.shared.security.shiro"
(859032db) and io-xlogistx commit 9e2c835 landed. The user moved `APIKeyAuthenticationToken` and
`CredentialsInfoMatcher` out of this module into xlogistx-shiro `io.xlogistx.shiro.authc` (sources
identical apart from the package). This module now has three classes (manager, realm,
`GrantFlattener`); INI class name for the matcher is `io.xlogistx.shiro.authc.DSCredentialsMatcher`
(sample ini + test updated). The legacy `shared.security.shiro` package is still shipped in core
2.4.0 (12 classes, unreferenced). 36/36 on H2 and PostgreSQL.

**2026-09-03 (matcher merge)** — `DSCredentialsMatcher` merged into xlogistx-shiro's
`CredentialsInfoMatcher` (io-xlogistx working tree, uncommitted) and deleted. The merged class keeps
the password path (principal equality, `autoAuthenticationEnabled`, `SecUtil.isPasswordValid`), adds
the raw-key branch, and replaces the old JWT branch with the DS rules (null status = active, `sub`
== key ID, `exp`/`nbf` ± skew, `iat` window + replay cache, null-safe scope). Behaviour change for
the older INI/proxy realms on JWT only: null-status keys now accepted, `exp`/`nbf` now enforced,
unscoped keys no longer NPE-to-false. `SecUtil.fromCanonicalID` now throws a checked
`GeneralSecurityException` → rejected with a log line instead of the old `printStackTrace`. INI name:
`matcher = io.xlogistx.shiro.authc.CredentialsInfoMatcher`. 36/36 on H2 and PostgreSQL.

**2026-09-03 (credential-model design)** — decided: JWT `kid` header selects the key row, `sub`
names the principal, no-`kid` fallback keeps `sub` = key ID; a new `SubjectPublicKey` entity
(`CredentialInfo.Type.PUBLIC_KEY`) goes into zoxweb-core. Open: server signing key location (C1),
at-rest protection (C2), SYMMETRIC_KEY entity (C3), multi-key without `kid` (C4). Design page
rewritten as an as-built record: https://claude.ai/code/artifact/61b80405-b4b9-48f6-a1b1-dd915e119f5e
(section 5 = credential model; `SecUtil.decodeJWT` lets the token's `alg` pick the key
interpretation, so the realm must bind the algorithm family to the row's key type).
**Correction (user, 2026-09-03): JWT `sub` = principal ID**, not a key ID —
`JWTPayload.getPrincipalID()`/`getSubjectID()` both read `sub`, and `SubjectAPIKey.principal_id`
means the *owning principal*. This module currently stamps a UUID into `SubjectAPIKey.principal_id`
and looks keys up by it (`lookupSubjectAPIKeyByID`, `mintJWT` sets `sub` to it): that is a misuse
to correct in phase 4 — `kid` = key row GUID, `sub` = principal ID resolved via `lookupPrincipalID`,
compatibility fallback for already-minted tokens until re-issued.
**Key issuance (designed 2026-09-03, on the page):** one flagged principal+password login issues
keys; symmetric = server generates 32 random bytes, stores `SubjectAPIKey` (principal_id = owning
principal, scope from login, named), returns secret once; asymmetric = client sends public key
(PEM / X.509 Base64 / RSA JWK) + proof-of-possession JWT signed with its private key, server
derives RSA/EC+curve from the key, verifies the PoP, stores `SubjectPublicKey`. Guard rails: fresh
password auth only (key-authenticated callers cannot issue keys), one row per device, cap + expiry,
family from the key never from the request. `SecUtil`/`CryptoUtil` already sign/verify both
families; the realm binds `alg` family to the row type before calling `SecUtil.decodeJWT`.
**Field encryption / KeyMaker (designed 2026-09-03, page section 6):** existing chain MK (keystore,
`KeyMakerProvider`) → subject `EncapsulatedKey` (USER_ID, ref = subject GUID) → entity
`EncapsulatedKey` (ref = entity GUID, wrapped under SK) → field `EncryptedData` (AES-256-CBC +
HMAC-SHA256, canonical string). `SecurityController` (io-xlogistx `ShiroSecurityController`) gates
encrypt/decrypt on owner-or-`nventity:<crud>:<guid>` permission of the thread-bound Shiro subject
(UUID principal), unwrapping the *owner's* chain. H2P runs none of it yet. Proposals K1–K9:
canonical string in the varchar column; subject key created in `createSubjectID` and cascaded on
delete (this module); one vocabulary `nventity:...`; entity-level then field-level check, denied
ENCRYPT → field absent, ENCRYPT_MASK → masked; credential rows decrypt under the owner's chain with
no Shiro check (realm runs before a subject exists); bounded key cache + request-scoped unwrapped
keys; no search on encrypted columns (kid first). Phase 5 "at rest"; no-sneak switch becomes phase 6.
Fixes owed: `decryptValues` recursion passes the container not the element; provider prints to
stdout and has an unbounded key map.
**Record + payload redesign (2026-09-03, page section 6):** `EncryptedData` becomes a versioned
JSON *value* (v, alg/kdf, kid, ref = entity GUID + field, iv/len/tag, payload, mask, exp) — never a
row; `EncapsulatedKey` keeps its own GUID, unique (reference_guid, subject_guid), reference fields
authenticated. New records AES-GCM with labelled-HKDF keys; CBC+HMAC read-only for v0. Three
payload locations by declaration, one locator format (`local://`, `gdrive://`, `dropbox://`,
`azure://`, `s3://`): inline for scalars; H2P `sys_file_version` (+ header column) for secure files;
remote providers via new `APIDocumentStore` implementations, ciphertext pushed encrypted, local
record authoritative, chunked (256 KiB) authenticated streaming, ciphertext cacheable, OAuth tokens
are the subject's credentials under the subject's chain.
**Decided 2026-09-03 (page section 6 = spec for the zoxweb-core session):** inline values =
`EncryptedData` JSON record, AES-256-GCM, HKDF-SHA256, no old-format reader (K10), no
`PropertyDAO` base; `EncapsulatedKey` contains (not extends) an `EncryptedData`, own GUID, unique
(reference_guid, subject_guid), reference fields authenticated. File content = **AES Crypt v2
containers via core `AESCrypt`** (one key per file = entity key bytes as password; fresh IVs per
version inside the container; interoperable with aescrypt.com tools); new value class
`EncryptedContentRef` per version (kid, ref, locator, iv1, hmac, lengths) stored on H2P
`sys_file_version` or the local row of a remote version; always verify-then-decrypt (K14); K11–K13
withdrawn. Core work list on the page (EncryptedData, EncapsulatedKey, EncryptedContentRef,
CryptoUtil GCM/HKDF, AESCrypt.verify + IV1/HMAC exposure, KeyMakerProvider fixes, DoNotExpose in
GSONUtil, tests).

**2026-09-04 (core session, page republished elsewhere → version 1788506191-d782):** the
zoxweb-core session **replaced AES Crypt v2 with "VX"**: a chunked AES-256-GCM container written by
core `AESCrypt` (magic `ZAES`, 38-byte header = AAD of every segment: version, cipher, kdf, salt,
7-byte nonce prefix, segment size 64 KB default / 16 MB max; nonce = prefix‖index‖lastFlag; content
key = HKDF-SHA256(key, salt, "ZAES-1 content key")). Each segment is released only after its tag
verifies → **stream-decrypt straight to the caller, no pre-pass** (K14 revised); truncation,
reordering and cross-file substitution all fail. Legacy v1/v2 reader kept (fixed: short reads,
constant-time HMAC, keys zeroed, no debug path). Offline opening with aescrypt.com tools dropped
(it never worked with raw keys). `EncryptedContentRef` now stores the container **header**
(from `AESCrypt.getLastHeader()`) + `cipher_length`/`plain_length`, alg `ZAES1`, instead of
iv1/hmac. JDK providers pinned (~2.5 GB/s enc, 3.0 GB/s dec measured). Work-list item 5 done in
core; decision 15 reversed to VX. Second republish (1788507277-bec8): VX header `kdf` byte now
0 = HKDF-SHA256 for a raw key, 1 = PBKDF2-HMAC-SHA256 for a typed password with the iteration
count in `kdf params` (writer default 600,000) — irrelevant to the datastore path, which always
uses the raw entity key. Third republish (1788507639-964b): `DoNotExpose` enforcement point
deferred — `GSONUtil` stays untouched; serialization vs HTTP response path vs datastore read layer
to be decided after the record types exist (note for this repo: the H2P read layer is a candidate).
Fourth republish (1788508849-b165): core work-list items 1–4 **done** — `EncryptedData` is the
GCM JSON record but **stays a `PropertyDAO`** (reversal: entity fields are simply not part of the
record/AAD; canonical JSON via new `CryptoJSON`; colon parser deleted); `EncapsulatedKey` is on
`PropertyDAO` directly with the wrapped record in a **`wrapped_key` text column** (never a child
table) and `toBindingData()` (GUID, subject GUID, reference GUID/type, lock type) as extra AAD;
`EncryptedContentRef` exists (also a `PropertyDAO`); `CryptoUtil` has GCM `encryptData`/
`decryptEncryptedData`, public `hkdfSHA256`, `createEncryptedKey`/`wrapKey`/`unwrapKey`/
`rekeyEncryptedKey`, MIN_KEY_BYTES 32; `KeyMakerProvider` sets binding fields before wrapping and
unwraps via `CryptoUtil.unwrapKey` (GUID/lock-type stamping still pending step 6). Core working
tree: 10 modified + 5 new files, uncommitted; jar still 2026-09-03. Implications for this repo when
the datastore side starts: `wrapped_key` and the flagged scalar columns are plain text/JSON columns
for H2P; `EncryptedContentRef` being a `PropertyDAO` means it can be either a JSON column on
`sys_file_version` (K11 spirit) or a referenced row — decide with the H2P hooks. Fifth republish (1788510604-c6a6): **record format is not JSON** — the user rejected a JSON
helper in the core session; `CryptoJSON` was removed. `EncryptedData`, the wrapped record inside
`EncapsulatedKey`, and `EncryptedContentRef` are fixed-order `|`-joined strings (binary base64url,
numbers as digits, absent = empty, text attributes must not contain `|`); AAD = the canonical
string without the trailing `|ct`; `toBindingData()` = the five binding fields joined by `|`.
For H2P this means the flagged columns, `wrapped_key`, and the content-ref column are plain
**varchar**, not `jsonb`. Sixth republish (1788511346-7373): **`EncryptedContentRef` was built
and deleted in core** — no caller, and it coupled core to the file container. Core keeps
`AESCrypt` (VX writer/reader, `verify`, `getLastHeader()`); the per-version bookkeeping (kid,
header copy, cipher/plain length, locator, upload/download flow, binding to the file) is now
**this repo's design** — most likely columns on H2P `sys_file_version` plus `FileInfoDAO`'s
existing `resource_locator`/`resource_id`/`remote_file_info_dao` for remote locations. Page
section 6 file part now stops at the container. Seventh republish (1788543480-ee77): **the key
row's GUID is out of the crypto** — `kid` in an inline record is the lookup name (reference GUID +
subject GUID), never a datastore GUID; the wrapped record's `ref` is the reference GUID;
`toBindingData()` = subject GUID | reference GUID | reference type | lock type (four fields). The
store may assign or change the row GUID freely. For H2P/shiro-ds: key rows are found by
(`reference_guid`, `subject_guid`) — the unique-index pair — and nothing depends on their GUID.
Core commit cb341d7a "Updating the Encryption mechanism" (2026-09-04 01:45) landed, with further
uncommitted edits to `CryptoUtil`/`EncapsulatedKey`/`EncryptedData`; jar still 2026-09-03.
Eighth republish (1788586461-db02, 2026-09-04 late): core **committed** 424b4e12 "Code update"
and **reinstalled both jars** (core 22:34, xlogistx-shiro 22:34). Design changes: record fields
renamed (`ct`→`cipher_data`, `len`→`data_length`); **no `kid`/`ref` inside the record** — a sealed
value carries no key pointer, the store finds the key by (subject GUID, reference GUID);
`EncapsulatedKey` **extends `EncryptedData` again** (composition + `wrapped_key` reversed the same
day); new fields `key_guid` (what wrapped this key: parent key, or ML-KEM public-key registry id;
empty under the master key) and `key_size`; binding = `subject_guid|reference_guid|key_guid|key_size`
— `reference_type` and `key_lock_type` are unauthenticated labels; `KeyLockType.USER_ID` renamed
**`SUBJECT_ID`**; **ML-KEM wrapping** (KEM-DEM: `alg` = ML-KEM-512/768/1024, `cipher_data` =
encapsulated key ‖ sealed key, split at `key_size`; private key out of core's scope). Decisions
16–21, K16–K19 recorded. Open in core: the public-key registry `key_guid` points at (close to
`SubjectPublicKey`, but signing and wrapping keys should not share a table). Datastore side (H2P
hooks incl. file bookkeeping, shiro-ds subject keys) still pending. Verified this repo against
the reinstalled jars: build green, shiro-ds 36/36 on H2 and PostgreSQL, h2p 10/10.

**2026-09-05 (design page revision)** — legacy `shared.security.shiro` package deleted from core
(commit cd36d329, 2026-09-04; `APIAppManagerProvider` kept, cleanup step 5 open). **K20 / decision
22:** `EncapsulatedKey` is **one-to-one** with the NVEntity it protects — `reference_guid` is that
entity's GUID (the subject's own GUID for a subject key), exactly one row per entity, found by
`reference_guid` alone, unique index on that column in H2P. Sharing is a permission grant, never a
second key row; an ML-KEM row is the same single row wrapped for a private-key holder, and a device
gets the *subject* key rather than one row per entity (the earlier "one row per recipient" wording
was withdrawn). Decision 23: authorization must support **RBAC + ABAC**. Pending-work matrix added
to the page (section 13, 23 items across zoxweb-core / io-xlogistx / h2p-datastore / shiro-ds /
no-sneak); item 1 (`kid` selects the JWT key) goes first because it removes the last lookup by
secret. `SecurityModel` reviewed: `Permission.USER_READ` maps to the update token, no role-group
permissions, assign/remove missing from the enum, two placeholder enums, `AppPermission` carries
app-specific `order:*` entries, no role→permission composition; no seeder into the manager exists.

**2026-09-07 (authorization model, page section 12)** — three layers over the existing storage,
Shiro the engine for all three: **RBAC** (type-level permission via `RoleGrant` → `RoleInfo` →
`PermissionInfo`), **ACL** (instance-level `PermissionGrant` with `permission_guid` +
`resource_guid`, flattened to `nventity:<verbs>:<guid>`), **ABAC** (conditions evaluated at check
time; deny is the default, no deny rules). Shiro fit: a `ConditionalPermission` in
`AuthorizationInfo.getObjectPermissions()` whose `implies()` matches the wildcard part then
evaluates conditions against a request-side `ResourcePermission`; ownership becomes a default
conditional permission every subject holds (`nventity:*` where `resource.owner == subject.guid`),
so no `self` rows. Worked case "A shares a file with B": one ACL row, grantee in `subject_guid`,
grantor in `broker_guid`, resource in `resource_guid`; revoke = delete the row. **Decision 24:**
instance grants through `PermissionGrant.resource_guid`, catalog rows never contain an instance or
placeholder. **Decision 25:** new **`share`** verb — holding `nventity:share:<guid>` or owning the
entity permits `addPermissionGrant` on it. Gaps in this module today: `addPermissionGrant` sets
only `subject_guid`/`permission_guid` (core interface has no resource overload); `GrantFlattener`
ignores `resource_guid` — a row with the column set flattens to `nventity:read` = read on every
entity, so **do not set the column before the flattener change**; no ownership-or-share check on
assign; the tests work around it with one catalog row per file+action. Pending item 23 (small,
independent): core `addPermissionGrant(grantee, permission, resourceGUID)` + grants-by-resource
lookup for the delete cascade; shiro-ds sets `resource_guid`/`broker_guid` and enforces owner ∨
`nventity:share:<r>` ∨ global assign; flattener emits `token + ":" + resourceGUID`;
`SecurityModel.SHARE` + `NVE_SHARE`. Nothing in Shiro, the realm or the caches changes.
`SecurityModel` bug matrix (10 rows) on the page — only row 4 is live: `AppPermission` rows seeded
by `APIAppManagerProvider.createAppID` keep literal `$$resource_guid$$`/`$$subject_guid$$`
placeholders (only `$$app_id$$` is substituted), so the per-resource and per-order grants on every
app role can never match. Target shape: `Target × Action` enums (`SHARE` added), one `Token` enum,
`Role` with declared permissions, idempotent seeder in shiro-ds. Open: A1 (conditions as
`attribute operator value` rows on the grant entities, AND-ed), A2 (no deny rules), A3 (one catalog
row per verb; comma lists stay legal as Shiro strings).

**2026-09-15 (instance grants + sharing, pending item 23 — built)** — user decisions: edit core
directly; owner-or-share check gated by `enforcePermissions`; accessors renamed
`get/setPermissionToken`; `deleteSubjectID` keeps grantee scope + map cleanup; no schema-migration
concerns (DB recreated); **grant = catalog XOR inlined, hard precondition**. zoxweb-core (uncommitted,
jar reinstalled 12:27 via `mvn -o clean install -Dgpg.skip=true` — plain `mvn clean install` fails
at the GPG sign step from this shell): `SecurityModel` (`NVENTITY`, `NVE_SHARE_ALL`, NVE_* literals
replaced, `toNVEToken`, `isInstanceScopable`, nested `NVEPermissionTokenFilter`), `PermissionGrant`
(filter on `permission_token`, rename, `validateShape()`), `ResourceMap(NVEntity)`,
`DomainSecurityManager` +4 methods, `DomainSecurityManagerDefault` impl + map cleanup;
`PermissionGrantTest` 60/60 (+13, run through the offline launcher — surefire's junit provider is
not cached offline). h2p: `H2PQueryFormatter.normalize` public + ENTITY_REF → uuid binding;
`H2PRegressionTest.testEntityRefCriterionBindsUUID` (24/24). shiro-ds: manager helpers
(`currentSubjectGUID`, `holds`, `enforceGrantOnResource`, `enforceRevoke`, `insertGrant`,
`deleteGrantAndMap`, `loadResource`, `checkCatalogTokenForScope`, `mapGUIDsForResource`), the four
new public methods, `deletePermissionGrant` reload-by-GUID, `GrantFlattener.permissionString`.
13 `share_*` tests: **49/49 on H2 and 49/49 on PostgreSQL** (lax-2.xlogistx.io / testdb, which is the run that proves the uuid binding fix); h2p DSM suite 10/10 on both. Runner
`cp*.txt` bumped to JUnit 6.1.3 (`junit-platform-launcher` 6.1.2 is gone from `.m2`).
Nothing committed.

**2026-09-16 (Stream A: catalog + super-admin, issues 1+2)** — core `SecurityModel` rebuilt
(`Target`/`Action`/`Permission`/`Role` with permissions/`RoleGroup`, `isWildcardToken`;
`PermissionToken` removed, `AppPermission` + resource constants deprecated, `PPEncoder` overload
dropped, `APIAppManagerProvider` renames, `SecurityModelTest` rewritten as JUnit 9/9,
`PermissionGrantTest` 60/60). shiro-ds: realm property `superAdminPrincipalID`, manager guards on
every catalog and grant path, flattener wildcard drop, package-private
`createSubjectID(pid, credential, SubjectType)` / `inTransaction` / `updateRoleInternal`,
`seedCatalog()` (enforces `permission:create` + `role:create`), `SecurityCatalogSeeder`,
`SecurityBootstrap`, `tools.SecurityAdminTool` (h2p-datastore became an optional compile
dependency). `SecurityCatalogDBTest` 13/13 on H2 and PostgreSQL (lax-2/testdb); existing suite
49/49 on both. Encrypted fields (issue 4) deferred by the user. Nothing committed.

**2026-09-16 (Stream B: password reset, issue 3)** — core: `PasswordResetToken`,
`PasswordResetRequest`, `NoRecoveryChannelException`, `PasswordResetTokenUtil` (+ test 4/4),
`ResourceManager.Resource.DOMAIN_SECURITY_MANAGER`, `DomainSecurityManager` +6 methods
(`verifyPassword`, `requestPasswordReset`, `adminResetPassword`, `completePasswordReset`,
`cancelPasswordReset`, `purgeExpiredResetTokens`), `DomainSecurityManagerDefault` implementations +
token cascade; jar reinstalled (`-Dgpg.skip=true`). shiro-ds: `replacePassword` extracted from
`updateCredential`, unenforced internal writers, `issueResetToken`, realm lazy lift of an expired
PENDING lock, `installAsGlobal`/`attach` register the ResourceManager slot, token cascade on subject
delete, `SecurityAdminTool reset-password`. io-xlogistx opsec `services.PasswordReset` (compiles;
not exercised by a test). no-sneak `Session.changePassword` → `verifyPassword`. Existing test
`verifyPassword_allowsPendingReset_whileLoginDeniesIt` now issues a real token (a bare status flip
is lifted at login by design). Suite **62/62 on H2 and PostgreSQL**; `SecurityCatalogDBTest` 13/13
on both; h2p default-manager suite +1 (`passwordReset_roundTrip_throughDefaultManager`). Nothing
committed.

**2026-09-16 (evening: CLI parameters)** — user decisions: no separate setup CLI; `SecurityAdminTool`
keeps the job with **dotted parameters** `db.url`, `db.user`, `db.password`, `db.enc-key` (H2 file
password), `principal.id`, `password`, `role`. `principal.id` is the account every command works
on (replaces `super-admin=` and `subject=`): for `bootstrap-super-admin` / `reset-super-admin-password`
it is that store's super-admin (the serving realm must set the same `superAdminPrincipalID`). New
command **`create-subject`** (`principal.id=`, `password=` or prompt, optional `role=`) creates the
domain admins — `remote-admin`, and `local-admin` for devices running an offline H2 store — with
`role=domain_admin` (ARGON2, idempotent, role granted once). **User clarification the same
evening: these admins are domain-driven, `<domain-app-id>:*` at most, and can never hold a bare
`*` by themselves**; the super-admin remains the single `*` holder per store, and the guards already
admit a domain-prefixed wildcard because `isWildcardToken` reserves only a leading `*`. Open:
`create-subject` grants global catalog roles only (`lookupRole(null, name)`); a per-domain role
or `<domain-app-id>:*` permission row still needs an `app.id`-scoped path.
it refuses `role=super_admin` **before** creating the subject (the first draft created the row and
then failed on the grant guard, leaking the account). `SecurityCatalogDBTest` 14/14 (new
`tool_createSubject_remoteAdminWithDomainAdminRole_neverSuperAdmin`), suite 62/62, both on H2.
Nothing committed.

**2026-09-16 (late evening: live bootstrap)** — `bootstrap-super-admin` run against lax-2/testdb:
account created, `login=verified wildcard=verified`, catalog 24/6/4 already present from the test
runs; rerun without `password=` reported `(existing) … password unchanged`, exit 0; `list-catalog`
also shows the many leftover test rows (`smuggled-*` wildcard permissions and role groups embedding
`super_admin`, written straight to the store by the tests — the manager guards never allowed them
and the flattener drops them). The account holds a throwaway password until the user resets it.

**2026-09-17 (vault wiring)** — `SecurityAdminTool` resolves `db.*` from an opsec `SecretStore`
vault: `store=<file>` (or env `XLOGISTX_SECRET_STORE`) + `store.password=` (or one console prompt);
resolution order for `db.url` is param → vault → `XLOGISTX_SHIRO_DB_URL` → `-Dshiro.ds.db.url`, for
`db.user/db.password/db.enc-key` param → vault. Helpers `loadVault` / `dbSetting` /
`resolveDbUrl(param, vaultEntry, env, property)`; `store.password` hidden from logs; only the
copied `db.*` values outlive the vault handle. Test `tool_run_dbSettingsFromSecretStore`
(missing file, wrong password, no password source, command line beats vault, vault alone);
`SecurityCatalogDBTest` 15/15 on H2. Operator flow smoke-tested through both `main`s
(`SecretStore create/put/list` → `SecurityAdminTool seed-catalog store=…`). Server starters
still resolve nothing from the vault: the realm takes its store from `ResourceManager`, and the
code that registers it lives outside these repos. Nothing committed.

## App-scoped grants and login scope (built 2026-09-18; user decisions 2026-09-17/18)

**A subject's assignment to an app is one or more grants with `app_id` set** (the `AuthzInfo`
field every grant inherits, an `AppIDDefault` = domain + app, persisted by H2P as a referenced
row and resolved on read); removing the assignment is deleting those grants. No membership entity.
The grantor is recorded as `broker_guid` (normally the app's admin) and may revoke what it granted.

**The login decides which grants apply (user, 2026-09-18):** `loginSubject(principal, pw, domainID,
appID)` (and the API-key / JWT logins, whose scope comes from the key) with domain+app loads
**only the grants scoped to that app**; a login without domain/app loads **only the global grants**.
Loaded grants flatten as plain tokens — `domain_admin`, `subject:create` — so every existing
checker (`ShiroSecurityController`'s `nventity:<verb>:<guid>`, `ShiroUtil.isPermitted`) works
unchanged inside an app login. **Exception: the super-admin's global grants (its `*`) apply in
every login.** Realm: `loginScopeOf(principals)` (both `DomainPrincipalCollection` domain and app
set; an invalid pair is logged and treated as global), authorization cache key `<guid>` for a
global login and `<guid>|<domain-app>` for an app login, `evictAuthorization(guid)` clears every
scope of the subject. `appScope(app)` = lower-cased `<domain>-<app>` is a label (cache keys, logs,
CLI), never a token prefix (the prefix design of 2026-09-17 was replaced the next day).

Manager extras (not on the core interface): `addRoleGrant(subject, role, AppIDDefault)`,
`addRoleGroupGrant(subject, group, AppIDDefault)`, `addPermissionGrant(subject, permission, AppIDDefault)`
(null app = the global grant; the `@Override` two-arg forms delegate), `getRoleGrants(guid, app)`,
`revokeAppGrants(guid, app)` (every role / group / permission grant under the app, one transaction,
returns the count). `deleteRoleGrant` / `deleteRoleGroupGrant` reload the row by GUID (a shell with
a GUID suffices; unknown → false).

| Rule | Where |
|---|---|
| Scope needs both domain and app (`IllegalArgumentException`) | `requireApp` |
| Enforcement of an add: caller holds the assign permission **in its current login**, and `scopeAllows(app)`: a global login may grant anywhere, an app login only into that same app, never globally; the super-admin from any login | `enforceScoped`, `scopeAllows` |
| Revoke of a role / group grant: caller is `broker_guid`, or holds `permission:remove:role` with `scopeAllows(grant app)` | `enforceRevokeRole` |
| Revoke of a permission grant: as before (global remove holder, broker, resource owner), the remove holder additionally needs `scopeAllows` | `enforceRevoke` |
| `super_admin`, a group embedding it, and the wildcard permission are **never app-scoped**, not even for the super-admin subject (`AccessSecurityException`) | the three scoped adds |

Consequence: an `app_admin` scoped to app A must **log in with A** to hold `permission:assign:role`;
from that login it assigns roles inside A only, and a bystander logged into A can neither revoke nor
grant. Tests: `appGrant_loginScopeSelectsGrants_andRevokeAppRemovesThem`,
`appGrant_scopedRoleGroup_appliesInThatAppLoginOnly`,
`appGrant_enforcement_appAdminActsInsideItsAppLoginOnly_andBrokerRevokes` (manager suite 65/65);
`appGrant_reservedRoleAndPermission_neverScoped_superAdminAppliesEverywhere` +
`tool_appScopedGrants_createSubject_grantRole_revoke` (`SecurityCatalogDBTest` 17/17); **both suites green on H2 and on PostgreSQL**
(lax-2/testdb, 2026-09-18), which proves the `app_id` reference column on the three grant tables.

**Previously pending (now done):** run
`java -cp "$(cat cp-shiro.txt)" io.xlogistx.shiro.ds.tools.SecurityAdminTool command=bootstrap-super-admin db.url=jdbc:postgresql://lax-2.xlogistx.io:5432/testdb db.user=dbuser db.password=… principal.id=… password=…`
from `.claude/test-runner` (the super-admin password was not supplied yet), expect `wildcard=verified`, rerun without `password=` to confirm idempotency, then `command=list-catalog`.
