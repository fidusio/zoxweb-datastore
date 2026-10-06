# xlogistx-shiro-ds (module removed 2026-10-05; this file was its `CLAUDE.md`)

> **Since 2026-10-05 there is no `xlogistx-shiro-ds` module.** Its tests are in
> `h2p-datastore/src/test/java/io/xlogistx/shiro/ds/test/`, its keystores, the persistent H2 file and
> `shiro-ds.ini` in `h2p-datastore/src/test/resources/` (including `test.store`, which used to be under
> `src/main/resources`), and its documents (`app-model.md`, `datastore-acl.md`, `no-sneak-plan.md`,
> this file) in the `h2p-datastore` directory. Wherever the text below says "this module" or
> `xlogistx-shiro-ds/…`, read `h2p-datastore/…` with those locations; runs start from the
> `h2p-datastore` directory. The session log below is history and keeps the paths of its time.

> **Where the code lives since 2026-10-04.** `ShiroDSDomainSecurityManager`, `DSAuthorizingRealm`,
> `GrantFlattener` and `SecuritySetup` were moved to the io-xlogistx repo, module `shiro` (artifact
> `xlogistx-shiro`), **same package `io.xlogistx.shiro.ds`**. `tools.SecurityAdminTool` followed on 2026-10-05, to the
> io-xlogistx module `opsec` (artifact `xlogistx-opsec`), same package `io.xlogistx.shiro.ds.tools`:
> it needs opsec's `SecretStore`, and `opsec` depends on `shiro`, not the other way round. This
> module now holds no main Java class: only the integration tests (`src/test`), the test keystores
> and `shiro-ds.ini`. Everything below still describes those four classes; read their paths as
> `io-xlogistx/shiro/src/main/java/io/xlogistx/shiro/ds/`.

Second `DomainSecurityManager` implementation (the first is `DomainSecurityManagerDefault` in
zoxweb-core): persistence through **any** `APIDataStore`, authentication / authorization / caching
through **Apache Shiro 1.13**. Package `io.xlogistx.shiro.ds`, JDK 25 (same as h2p-datastore, which
the tests run against). Design page (architecture, decisions, phases):
https://claude.ai/code/artifact/61b80405-b4b9-48f6-a1b1-dd915e119f5e
Diagrams (module dependencies, runtime relationships, entity relationships):
https://claude.ai/code/artifact/c07aba5f-1ad8-4f30-a6a2-67229c4514d9

The manager, realm, flattener and `SecuritySetup` import no store (which is why they could move to xlogistx-shiro on 2026-10-04); the tests, which stay here, bind to `H2PDataStore`. `tools.SecurityAdminTool` (in io-xlogistx `opsec` since 2026-10-05) did too until 2026-10-05; since then it names its datastore creator by a class-name string (`DEFAULT_DS_CREATOR`, or `ds.creator=`), loads it by reflection and works through core's `APIServiceProviderCreator` / `APIDataStore`, so it imports no h2p class (h2p-datastore is still an optional compile dependency of the pom, since 2026-09-16, and must be on the runtime classpath for the default creator).

## Dependencies (pom)

Compile: zoxweb-core, xlogistx-shiro 1.0.0 (tokens,
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
| `DSAuthorizingRealm` | `AuthorizingRealm` that reads through the manager. **No-arg constructor for shiro.ini** (defaults: name `shiro-ds`, `CredentialsInfoMatcher`, authn caching off, authz caching on); the manager is bound by the manager itself, by `setDomainSecurityManager`, or lazily on first use from the `APIDataStore` registered in `ResourceManager` under `dataStoreResource` (default `Resource.DATA_STORE` = `"DataStore"`), failing loudly otherwise. Supports `DomainUsernamePasswordToken` and xlogistx-shiro's `JWTAuthenticationToken` — nothing else (since 2026-10-03 a raw API-key token is not supported; a JWT is accepted only when the key behind its `sub` is a signing key, `SubjectAPIKey.isSigningKey()`). Authentication caching **off**; authorization cached per **subject GUID**. `evictAuthorization(guid)` / `evictAllAuthorization()`. |
| `io.xlogistx.shiro.authc.CredentialsInfoMatcher` (xlogistx-shiro; `DSCredentialsMatcher` was **merged into it** 2026-09-03) | Passwords (hash via `SecUtil.isPasswordValid`, `autoAuthenticationEnabled` skips the check). Raw API keys: **always rejected** since 2026-10-03 (an API key is not a login). JWTs: the key must be a signing key (`isSigningKey()`), then HMAC check with the key's secret (`SecUtil.decodeJWT`), `sub` == key ID, `exp`/`nbf` with `setClockSkewMillis` (default 1 min), key scope vs claims, and for `isTimeStampRequired()` keys an `iat` inside `setJWTTimestampWindowMillis` (default 5 min) plus optional `setJWTReplayCache(JWTTokenCache)`. All knobs are bean properties (INI-settable). |
| `io.xlogistx.shiro.authc.APIKeyAuthenticationToken` (**in xlogistx-shiro since 2026-09-03**) | Raw key token. **Unused since 2026-10-03**: the realm does not support it and the matcher rejects it; the class file is still in xlogistx-shiro. |
| `GrantFlattener` | subject GUID → roles + permission strings: every subject first gets its self permission `resource:S:S:create,read,update,delete,share` (`create` added 2026-10-02: the right to create rows one owns); PermissionGrant → its inlined `permission_token`, else the catalog token, **composed to `resource:<resource guid>:<grantee guid>:<verbs>` when the grant embeds a `ResourceMap`** (`SecurityModel.toResourceToken`; a non-`resource:<verbs>` token is skipped with a WARNING); RoleGrant → role name + its permission tokens; RoleGroupGrant → each role, **re-read by GUID** so nested permission refs resolve. Missing catalog rows are skipped. **Login scope (2026-09-18):** `flatten(dsm, guid, AppIDDefault scope)` — the realm passes the login's domain+app (`DSAuthorizingRealm.loginScopeOf(principals)`); a null scope selects the **global grants only**, an app scope selects **only the grants whose `app_id` equals that app**; the super-admin's global grants apply in every scope. Strings are always plain tokens (never prefixed). **Drops any wildcard string for a non-super-admin subject** (WARNING logged) — defense in depth behind the manager guards. |
| `SecuritySetup` (2026-10-01; merges `SecurityCatalogSeeder` + `SecurityBootstrap` of 2026-09-16, user decision) | The setup of a security store, one class. **Catalog:** `seedCatalog(dsm)` / `dsm.seedCatalog()` creates the common app `xlogistx.com-common` when missing and materialises core `SecurityModel.Permission` / `Role` / `RoleGroup` as **its** rows (`app_id` = the record; app-less rows of an older store are attached to it), idempotent by `(app, name)`, repairs drift (token, description, permission/role sets) in one transaction, never grants; `seedStarterCatalog(dsm, app)` does the same with the non platform-only subset for a new app (see "App model"). `Report` counts created/updated/existing. `dsm.seedCatalog()` enforces `permission:create` + `role:create` even when nothing needs repair. **Super-admin:** `bootstrapSuperAdmin(dsm, password)`: seed → create the super-admin subject (the id the manager was given from the SecretStore's `super-admin-id`; no default, an unset id is an `IllegalStateException`; `SubjectType.SYSTEM`, ARGON2) if missing → grant the `super_admin` role once (a `RoleGrant` with **no domain/app** — the account belongs to no app and its `*` applies in every login; user, 2026-10-01) → verify by login (when created) and by a realm probe of a random permission (`Result.wildcardVerified`). `resetSuperAdminPassword`. The only path that hands out the wildcard. **Common app (2026-10-01):** the bootstrap then ensures the record of the platform's own app `xlogistx.com-common` (`COMMON_DOMAIN_ID` / `COMMON_APP_ID`, `ensureCommonApp(dsm, creatorGUID)`): one `AppIDDefault` row, `name` = `xlogistx.com-common`, `subject_guid` = the super-admin that created it; `Result.app` / `appCreated`. Built on the manager's new `lookupApp(domainID, appID)` (earliest row carrying the pair) and `createApp(domainID, appID)` (owner = bound subject, `app:create` under enforcement, duplicate or invalid pair ⇒ `IllegalArgumentException`). |
| `tools.SecurityAdminTool` (2026-09-16, dotted params since 2026-09-16 evening) | CLI, DMTool-style `key=value` args: `command=seed-catalog\|bootstrap-super-admin\|reset-super-admin-password\|create-subject\|grant-role\|reset-password\|list-catalog`, `db.url=` (→ env `XLOGISTX_SHIRO_DB_URL` → `-Dshiro.ds.db.url`), `db.user/db.password/db.enc-password` (H2 file password; named `db.enc-key` until 2026-10-03), **`store=<vault> store.password=` — REQUIRED since 2026-10-02 (user rule: the prerequisite of every run is to load the keystore with its password and take the db info and the master key from it)**: an io-xlogistx opsec `SecretStore` BCFKS vault, or env `XLOGISTX_SECRET_STORE`; its `master-key` secret key (`MASTER_KEY_ALIAS`) is loaded into `KeyMakerProvider.SINGLETON` and the store is opened with that key maker **and** `ShiroSecurityController` (`openStore(url, user, password, filePassword, keyMaker)`), so encryption at rest and the store's access check are on in the tool; its `db.*` text secrets fill in whatever `db.*` is absent from the command line, command line wins; password prompted once on a console; vault closed before the store opens; no vault / missing file / wrong password / no password source / no `master-key` entry → exit 1 with a message, nothing touched, `principal.id=` (the account the command works on; **not accepted by the two super-admin commands since 2026-10-03**: the super-admin is the vault's `super-admin-id`, read on every run, and a vault without that entry stops the tool), `password` (or double console prompt), `role=`. `create-subject` makes the domain admins (`remote-admin`, `local-admin` on offline-H2 devices; `role=domain_admin`, ARGON2) and refuses `role=super_admin` before creating anything; idempotent like bootstrap. **Domain admins are domain-driven: `<domain-app-id>:*` at most, never a bare `*`** (user, 2026-09-16); `isWildcardToken` reserves only a token whose first part is `*`, so `mydomain:*` passes the guards. **App-scoped grants (2026-09-18):** `app.id=<domain>-<app>` (`AppIDDefault.create`; or `domain.id=` + bare `app.id=`) on `create-subject` / `grant-role` scopes the role grant to that app (idempotent per scope; it applies to logins made with that domain+app); `revoke-role principal.id= role= [app.id=]` deletes that grant, `revoke-app principal.id= app.id=` deletes every grant under the app (`revokeAppGrants`), `list-grants principal.id=` prints roles / groups / permissions with `[app …]` and `by <broker>`. `run(out, err, args)` returns 0/1/2 for tests. Needs `h2p-datastore` (now an **optional compile** dependency of this module) and registers BC via `OPSecUtil.singleton()`. |

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

Public extras on the manager beyond the interface: `loginJWT(compactJWT)`, the two bound logins
`loginSubject(principal, password, domainID, appID)` / `loginSubjectJWT(compactJWT, host)` (full
Shiro login: session + `ThreadContext` binding; `loginSubjectApiKey` was removed 2026-10-03),
`logout()`, `verifyPassword(principal, password)` (boolean, see Login paths), static
`mintJWT(sak, algo, ttlMillis)`, `installAsGlobal()` (`SecurityUtils.setSecurityManager`),
`getShiroSecurityManager()`, `getRealm()`, `getCredentialsMatcher()` (JWT policy knobs),
`setEnforcePermissions(boolean)`, `getSecurityManager()` / `isSelfManaged()`, static `attach` /
`fromGlobal`, and the lookups the realm/flattener use (`lookupSubjectByGUID`,
`lookupSubjectAPIKeyByID(keyID)` by key ID (the lookup by secret was removed 2026-10-03),
`lookupPermissionByGUID`, `lookupRoleByGUID`, `lookupRoleGroupByGUID`).

## Credential kinds

| Kind | Stored as | Login (authc only) | Login (bound Subject) |
|---|---|---|---|
| Principal ID + password | `CIPassword` row per subject | `login(principal, password)` | `loginSubject(principal, password, domainID, appID)` |
| Third-party API key (2026-10-03) | `SubjectAPIKey`, `credential_type` = `API_KEY` or unset; `api_key` sealed (`FilterType.ENCRYPT`) | **never a login**: `loginApiKey` always refuses, `loginSubjectApiKey` / `lookupSubjectAPIKey(secret)` / the realm's raw-key token are removed | — the logged-in owner reads it through the datastore, which serves it in clear |
| JWT bearer token | A **signing key**: `SubjectAPIKey` with `credential_type` = `SYMMETRIC_KEY` (`isSigningKey()`); `principal_id` = **key ID** (JWT `sub`), `api_key` = HMAC secret, optional `app_id` scope. A key of any other purpose is refused by the realm and the matcher | `loginJWT(compactJWT)` | `loginSubjectJWT(compactJWT, host)` |

JWT model (same as `XXClientAPI` / `CredentialsInfoMatcher` in the xlogistx stack): the client builds
`JWT.createJWT(HS256, sak.getSubjectID(), domainID, appID)` and signs it with
`sak.getAPIKeyAsBytes()`; `mintJWT(sak, algo, ttl)` does exactly that (adds `exp` when `ttl > 0`).
The realm resolves the `SubjectAPIKey` by `sub` → `lookupSubjectAPIKeyByID`, the matcher verifies.
`SubjectAPIKey.getSubjectID()` is an alias of `getPrincipalID()`; `createCredential` stamps a UUIDv7
key ID when it is empty so every stored key can sign tokens. Key IDs are assumed unique per store
(no index enforces it; `lookupSubjectAPIKeyByID` takes the first match).

## Login paths

- `login(principal, password)` → `shiroSecurityManager.authenticate(token)`:
  authenticator only — **no Subject, no session, no thread binding**. Safe for "verify the current
  password" flows (no-sneak `Session.changePassword` calls `login`). Every
  `AuthenticationException` collapses to `SecurityException("Invalid credentials")`; the Shiro
  subtype is logged at INFO when `log` is enabled.
- `loginApiKey(key)` → **always throws** `AccessSecurityException("An API key is not a login
  credential")` (2026-10-03). The method remains only because the core interface declares it.
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
- `loginSubjectJWT` → the same token through `Subject.login` + `ThreadContext.bind`
  (shared `bindSubject` helper). A failed bound login leaves nothing bound.
- **xlogistx-shiro `ShiroUtil`** works unchanged once `installAsGlobal()` has run (it goes through
  `SecurityUtils`): `login(domain, realm, user, password)`, `login(AuthenticationToken)` with the
  password or the JWT token (a raw API-key token is refused), `loginSubject(subjectID, credentials, domainID, appID, autoLogin)` and
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
`loginSubject` / the JWT logins (subject must be ACTIVE); `verifyPassword` is the dedicated call
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
- `deleteSubjectID`: cascades principals, the subject key and every entity key of the subject (`encapsulated_key.subject_guid`, when a key table exists), every registered credential collection **plus
  `SubjectAPIKey` always**, all three grant types, then the subject — one transaction.
- `createCredential` on a `SubjectAPIKey` with no key ID (`principalID`) stamps a UUIDv7 one.
- `createSubjectID`: no `synchronized` (unique index on `principal_id` is the guard); **the subject key is mandatory (user rule 2026-10-02)**: the `EncapsulatedKey` wrapped under the `KeyMaker`'s master key, bound to the subject, is inserted in the same transaction as the subject — a store configuration without `KeyMaker`, or a KeyMaker without master key, fails the creation with `AccessSecurityException` (no keyless subject); subject type
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
  `AuthorizationInfoLookup<PrincipalCollection, AuthorizationInfo>` (input type first since
  2026-10-05; the interface extends `DataDecoder`, whose `decode` calls `lookupAuthorizationInfo`),
  so `ShiroUtil.lookupAuthorizationInfo(DSAuthorizingRealm.class, pc)` returns the cached-or-loaded
  info.
- Permission enforcement (`setEnforcePermissions(true)`, default **off**): mutations require the
  thread-bound authenticated Subject to hold the `SecurityModel` token (`PERM_ADD_USER`,
  `PERM_DELETE_SUBJECT`, `PERM_UPDATE_SUBJECT`, `PERM_ADD/UPDATE/DELETE_PERMISSION`,
  `PERM_ADD/UPDATE/DELETE_ROLE` (also role groups), `PERM_ASSIGN/REMOVE_PERMISSION`,
  `PERM_ASSIGN/REMOVE_ROLE`). No subject bound → `AccessException(UNAUTHORIZED)`. A subject acting on
  its **own** principals/credentials is always allowed (`enforceSelfOr`). Off by default because
  `DMTool` and no-sneak bootstrap run with nobody logged in.

## Catalog, super-admin and the reserved wildcard (built 2026-09-16; design page item 20 + issue 1)

Core `SecurityModel` was rebuilt: `Target × Action` enums, `Permission` entries with fixed tokens
(`subject:create`, `permission:assign:permission`, `resource:*:*:read`, … — no placeholders, no
instance parts), `Role` now declares `Permission[]` (`SUPER_ADMIN{super_admin_all}`, `DOMAIN_ADMIN`,
`APP_ADMIN`, `APP_SERVICE_PROVIDER`, `APP_USER{}`, `USER{}`), new `RoleGroup` enum
(`domain_admins`, `app_admins`, `app_users`, `service_providers`; none contains `super_admin`),
`isWildcardToken(token)`. The `PERM_*` String constants are unchanged (callers here, opsec, ShiroUtil);
the resource/self/private/public constants and `AppPermission` are `@Deprecated` (dead:
`APIAppManagerProvider` has no production caller). `PermissionToken` enum removed.

**The wildcard `*` (`Permission.SUPER_ADMIN_ALL`, role `super_admin`) is reserved for one account**
— the realm property `superAdminPrincipalID`. **It has no default and is not hard coded anywhere
(user rule 2026-10-03): the super-admin id lives in the SecretStore, under its reserved
`super-admin-id` entry (`SecretStore.SUPER_ADMIN_ID`, `getSuperAdminID()`), and the start-up code
hands it to `setSuperAdminPrincipalID`.** Until then `getSuperAdminPrincipalID()` is null,
`requireSuperAdminPrincipalID()` throws, `lookupSuperAdminSubject()` is null and
`isSuperAdminSubject(guid)` is false for everybody (so nobody holds `*`). Normalized by
`SubjectIDFilter`; manager `get/setSuperAdminPrincipalID`. Guards (all re-read catalog rows by GUID,
never trust the passed object):

| Path | Rule |
|---|---|
| `createPermission` | a wildcard token only as global `super_admin_all`, only while none exists (`IllegalArgumentException`) |
| `updatePermission` / `deletePermission` | reserved row immutable / undeletable; no row may acquire a wildcard token |
| `createRole` / `updateRole` / `deleteRole` | a role embedding the reserved permission only as global `super_admin`, once; reserved role immutable through the public path (`updateRoleInternal` is the seeder's) |
| `createRoleGroup` / `updateRoleGroup` | may never embed `super_admin` |
| `addPermissionGrant` (global) / `addRoleGrant` / `addRoleGroupGrant` | reserved permission / role / group only to the super-admin subject (`AccessException`); scoped and inlined grants already reject `*` (`isInstanceScopable`, `ResourcePermissionTokenFilter`) |
| `GrantFlattener` | drops wildcard strings for any other subject (rows smuggled in through the store) |

Bootstrap is **CLI only** (user decision): `SecurityAdminTool command=bootstrap-super-admin store=<vault> store.password=… password=…` (the account is the vault's `super-admin-id`; `principal.id=` is refused)
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

## App model (built 2026-10-02; user decisions 2026-10-01/02; plan `app-model.md` in this module)

**Every app is a record and owns its catalog.** `AppIDDefault` is the app record: one row per
domain + app (`lookupApp`, `createApp`), named `<domain>-<app>`, `subject_guid` = its creator. The
platform's own app is **`xlogistx.com-common`** (`COMMON_DOMAIN_ID` / `COMMON_APP_ID` /
`COMMON_SCOPE` on the manager): it owns the full built-in catalog, and **"no domain/app" means the
common app everywhere** — a catalog lookup with `appID = null`, a grant with no app, a login with no
domain/app (`scopeLabel(app)`; cache key `<guid>|<scope>` always). The only unscoped thing left is
the super-admin's `*`, which applies in every login. Every scoped row (catalog rows, grants, API
keys) **references the one record** (`appRecord`): an app that was never created is an
`IllegalArgumentException`, deleting a grant never touches the app row. No database unique index
on (`domain_id`, `app_id`): h2p has no composite-unique declaration; `createApp` checks inside its
transaction and nothing else inserts app rows any more.

**Per-app catalogs, nothing shared** (user, 2026-10-01). `createApp(domain, app, firstManager)`
creates, in one transaction: the record; the app's **starter catalog** — every
`SecurityModel.Role` / `RoleGroup` that is not `isPlatformOnly()` (`super_admin`, `domain_admin`
and the `domain_admins` group exist in the common app only) with exactly the permissions they
declare, as the app's own rows (`SecuritySetup.seedStarterCatalog`); the app's **registrar**; and
the `app_admin` grant of the first manager when named. Rules enforced by the manager:
a catalog row belongs to the given app, else the common app (`recordOrCommon`); creating or
changing one needs the catalog permission **in a login that may act on that app**
(`enforceScoped`: a login into the common app may act anywhere, a login into app A on A only;
`broker_guid` = who created the row); a role carries its own app's permissions only, a group its
own app's roles only (`requireSameAppPermissions` / `requireSameAppRoles`); **a role, group or
permission is grantable inside its own app only** (`requireGrantableIn`; the reserved checks come
first so `super_admin` still fails with "never app-scoped"). `app_admin` gained
`permission:create|update|delete` and `role:create|update|delete` in core so the managers of an app
can shape its catalog; `domain_admin` keeps `app:create`. `deleteApp` removes grants, keys, the
registrar, the catalog rows and the record (never the common app). The flattener: a grant on a
resource (a share, `ResourceMap` set) **follows the data and applies in every login scope**; every
other grant applies in the login of its app.

**Registrar** (user decision 2026-10-02, plan section 3). Per app one `SYSTEM` subject, principal
`registrar.<domain>-<app>` (a handle, no email ⇒ no password-reset channel), whose only credential
is a `SubjectAPIKey` scoped to the app (so its login is always a login into that app) and whose only
grant is the app's own `app_registrar` role = `subject:create`. `createApp` makes it;
`ensureRegistrar(app)` finds or makes it (the bootstrap gives the common app its own). The key is a
**signing key** (`credential_type` = `SYMMETRIC_KEY`, since 2026-10-03). **The secret is not lost
after creation** (corrected 2026-10-03; an earlier version of this file said "never readable
again", which was wrong): the API returns it in `AppCreation.registrarKey` and the CLI prints it
at creation, and it stays in `subject_api_key.api_key` as a sealed record that the master key
opens through the key chain (master key → the registrar's subject key → the key row's own key).
Verified by a run on a scratch H2 file database with a scratch vault (2026-10-03): a fresh JVM
that loads the same vault gets the exact printed secret back from
`dsm.lookupSubjectAPIKeyByID(keyID)`; the registrar, once logged in with a JWT, reads its own row
through the plain datastore and gets the secret in clear; with nobody logged in the plain
datastore returns no row; with a different master key the row is found but the secret does not
come back. What does not exist is a CLI command that prints an existing registrar secret: the
tool only prints it when the key is created or rotated. Its message used to end with "the secret
is not readable back", which was not accurate; since 2026-10-03 it says the secret stays sealed
in the datastore, that the master key opens it, and that the tool has no command to print it again. `rotateRegistrarKey(app)` (an `app_admin` of the app) replaces
the key; the old key then stops working and the new one signs up (covered by the manager suite). **Sign-up =
`registerSubject(principal, password)`**: the caller needs `subject:create`, the app is the
caller's login scope (never a parameter), an unknown principal is created (ARGON2), a known one
must verify its password or the same generic `AccessSecurityException` comes back, and the subject
gets the app's `app_user` — always that role, chosen by the method; nothing else is grantable by
a registrar. **The swap = xlogistx-shiro's `SubjectSwap`** (since 2026-10-03; the manager's own
`runAs(SubjectAPIKey, Callable)` was removed): the application logs the registrar in with
`loginUnboundSubjectJWT(mintJWT(key, …), host)` — a full Shiro login that binds nothing — and runs
the sign-up inside `try (SubjectSwap swap = new SubjectSwap(registrar)) { dsm.registerSubject(…); }`;
`close()` restores the thread's previous subject, success or failure. The caller owns the registrar
subject: one login can serve many sign-ups, or it is logged out after each (both tested). Inside `createSubjectID`
the principal and credential rows are now written unenforced (`subject:create` covers them; a
registrar holds no `subject:update`). The registrar logs in by JWT only, which looks the key up by
its id; there is no raw-key login and no lookup of a key by its secret (both removed 2026-10-03).
**Not built:** the opsec HTTP sign-up endpoint beside `PasswordReset` (reads the registrar key from
the app's vault and calls `loginUnboundSubjectJWT` + `SubjectSwap` + `registerSubject`) — the next step when an application needs it.

**Setup and CLI.** `SecuritySetup.seedCatalog` creates the common app first (owner = the
super-admin when it exists) and seeds its catalog with `app_id` = the record; rows seeded before
the app model (no `app_id`) are **attached to the common app on the next seeding**
(`attachToApp`), so rerunning `bootstrap-super-admin` upgrades a database in place.
`bootstrapSuperAdmin` order: super-admin subject → common app + catalog → `super_admin` grant (no
app) → the common app's registrar (`Result.registrar` / `registrarKey`). `SecurityAdminTool`:
`create-app domain.id= app.id= [principal.id=<first manager>]` (prints the registrar key id and
secret once), `rotate-registrar-key app.id=`, `list-apps`; `create-subject` / `grant-role` /
`revoke-role` look the role up **in the app of `app.id=`** (none = the common app);
`list-catalog` shows the app of every row. Core: `AppIDDefault.create(String)` splits at the
**last** `-` (hyphenated domains), `SecurityModel.Role.APP_REGISTRAR`, `Role.isPlatformOnly()`,
`RoleGroup.isPlatformOnly()`.

Tests: manager suite `app_createGivesStarterCatalogAndRegistrar_deleteRemovesThem`,
`registrar_signUpThroughTheSubjectSwap_createsOrJoins_andNothingElse`,
`appCatalog_isolated_managerWritesItsOwnAppOnly_sharesFollowTheData` (+ the app-scoped grant tests
rewritten to the apps' own roles); catalog suite
`seedCatalog_attachesPreAppModelRowsToTheCommonApp_bootstrapAddsItsRegistrar`, the tool test
rewritten around `create-app`. The test helpers create an app (with its starter set and registrar)
the first time a name is used and delete test apps whole in `@AfterAll`.

## Resource permission model (built 2026-09-29; user decision — supersedes `nventity`)

Access to an NVEntity instance is **a permission, never a `subject_guid` equality test**. One
standardized token, composed only by `SecurityModel.toResourceToken(resourceGUID, subjectGUID, verbs)`:

```
resource:<resource guid | owner guid | *>:<acting subject guid | *>:<verb[,verb]*>
```

- **Self permission**: `GrantFlattener` adds `resource:S:S:read,update,delete,share` for every
  subject S in every login scope (`SecurityModel.RESOURCE_SELF_VERBS`; synthesized, no row, never
  revocable — a locked subject is a status matter). The owner of E (owned by S) therefore passes
  `resource:E.subject_guid:S:<verb>`.
- **Share / scoped grant**: stored as the 2-part token `resource:<verbs>`
  (`SecurityModel.ResourcePermissionTokenFilter`, verbs ⊆ {read, update, share, delete}) plus a
  `ResourceMap`; the flattener emits `resource:<resource guid>:<grantee guid>:<verbs>`. A scoped
  catalog permission must carry a `resource:<verbs>` token too (`checkCatalogTokenForScope`); a
  scoped grant whose token is anything else is skipped with a WARNING.
- **Catalog wildcards**: `nve_all` = `resource:*`, `nve_{create,read,update,delete,share}_all` =
  `resource:*:*:<verb>` (names unchanged so the seeder repaired the rows in place).
- **Checker**: `ShiroUtil.checkResourcePermission(nve, verb)` (io-xlogistx) — owner token first,
  then grant token, else `AccessSecurityException`; returns the **owner's GUID**, the root of the
  entity's key chain. `ShiroSecurityController.checkNVEntityAccess` (OR/AND over CRUD verbs) and
  `isNVEntityAccessible(refID, ownerGUID, crud…)` delegate to it; the manager's own
  `holdsResource(resource, verb)` evaluates the same two tokens against its bound subject.
- **Sharing is the `share` verb**: `enforceGrantOnResource` requires `share` on the resource for
  inlined *and* catalog grants (owner via self permission, or a grantee whose share carries
  `share` — so a share holder may re-share); catalog grants also accept the global assign
  permission. `enforceRevoke`: grantor, `share` holder on the resource, or global remove.
- Consumers: h2p-datastore's encryption at rest asks the controller only (see its CLAUDE.md).
  Tests: `resource_checkResourcePermission_ownerGranteeStranger`, `share_*` (rewritten to the new
  tokens), core `SecurityModelTest.resourceTokenGrammar`.

## Instance grants and sharing (built 2026-09-15; design page section 12, item 23 — tokens migrated to `resource:` 2026-09-29)

A `PermissionGrant` is **either** catalog-backed (`permission_guid`; `resource_map` optional) **or**
inlined (`permission_token` + mandatory `resource_map`) — never both, never neither. This is a hard
precondition (user, 2026-09-15): `PermissionGrant.validateShape()` (core) runs first in every add
path, before any store access or enforcement. Core side (zoxweb-core, 2026-09-15; namespace renamed to `resource` 2026-09-29): `SecurityModel.RESOURCE`,
`NVE_SHARE_ALL`, `toResourceToken(verbs...)`, `isInstanceScopable(token)`, the
`SecurityModel.ResourcePermissionTokenFilter` `ValueFilter` wired on `PermissionGrant.permission_token`
(normalizes to lower-case `resource:<verbs>` with verbs ⊆ {read, update, share, delete}; no create,
no wildcard, no instance part; rejects everything else), accessors renamed
`get/setPermissionToken`, `ResourceMap(NVEntity)` ctor, and four `DomainSecurityManager` methods
(also implemented in `DomainSecurityManagerDefault` without enforcement).

| Call | Shape written | Enforcement (when on) |
|---|---|---|
| `addPermissionGrant(subject, perm)` | global catalog grant | global `permission:assign:permission` |
| `addPermissionGrant(subject, perm, ResourceMap)` | catalog grant scoped to one entity; the catalog token must be `resource:<verbs>` (`isInstanceScopable` + `ResourcePermissionTokenFilter`) | caller holds `share` on the resource (owner via self permission, or a share grantee), **or** global assign |
| `addPermissionGrant(subject, ResourceMap, token)` | inlined share; token validated by the filter | caller holds `share` on the resource (owner, or a grantee whose share carries `share`) |
| `deletePermissionGrant(grant)` | reloads by GUID, deletes grant **then its map row** | global `permission:remove:permission`, **or** caller is the grantor (`broker_guid`), **or** holds `share` on the resource |
| `getPermissionGrantsByResource(guid)` | two queries: `resource_map` rows by `resource_guid`, then grants `resource_map IN (…)` | none (read) |
| `deletePermissionGrantsByResource(guid)` | all of the above in one transaction | per grant as for delete |

Common to every add path: the resource must exist (`ds.searchByID(resource_type, resource_guid)`;
unknown class / missing row → `IllegalArgumentException`), `broker_guid` = bound subject GUID or
null when nobody is bound (regardless of enforcement), map row + grant row are written in one
`inTransaction`, the grantee's authorization entry is evicted. Flattened form (see `GrantFlattener`):
`resource:<resource guid>:<grantee guid>:read,share` — one Shiro string per grant; the checker asks
`resource:<owner>:<caller>:<verb>` then `resource:<guid>:<caller>:<verb>`, Shiro's
`WildcardPermission` handles the comma list. `deleteSubjectID`
keeps grantee scope (grants *received*) and now also removes their map rows; grants the subject
*issued* stay. H2P specifics: `resource_map` is a child table + FK column (`uuid`, indexed) — never
`delete(grant, true)` (it would chase `app_id` too); the H2P formatter now binds a String/NVEntity
criterion on an `ENTITY_REF` column as `uuid` (needed for the `IN` query on PostgreSQL).
Store requirement added: grants must load `resource_map` eagerly (H2P does), otherwise map rows leak.
Not done (still pending): ABAC conditions (A1), `SecurityModel` rework/seeder (item 20).

## What a store must provide (for "works with any APIDataStore")

Equality search on `principal_id` (principals **and** `SubjectAPIKey` key IDs), `subject_guid`,
`name` (both `QueryMatch` ctor forms; `api_key` is never queried by value since 2026-10-03); `AppIDDefault` persisted as a referenced entity
(`SubjectAPIKey.app_id`) and resolved on read;
`search`/`searchByID` by class-name string as well as by `NVConfigEntity`; insert keeps a pre-set
**UUID-formatted** GUID (`DomainAuthenticationInfo` parses it with `UUID.fromString`); reference
resolution on read (`RoleInfo.getPermissions()` with tokens, `RoleGroupInfo.getRoles()` with GUIDs);
enum + long round-trip (`SecStatus`, `Const.Status`, expiry, `CredentialInfo.Type` for `credential_type`); unique
index on `principal_id`; `fieldNames` projection accepted or ignored. Verified only on H2 and
PostgreSQL through `H2PDataStore`; the Mongo stores are unverified on the GUID and reference points.

## Running the tests

`ShiroDSDomainSecurityManagerDBTest` — 72 tests as of 2026-10-03 (the list below was written when
there were 36 and is not complete): the `H2PDomainSecurityManagerDBTest` scenarios
ported, plus `SubjectIDFilter` normalization/rejection, status gating (subject / principal /
credential), credential ownership, unsupported-type rejection, caller-transaction join + rollback,
full password replacement, last-principal guard (sequential + 2-thread), subject-delete cascade incl.
API keys, the third-party API key (sealed at rest, served in clear to its logged-in owner, never a login), Shiro `isPermitted`/`hasRole` for permission,
role and role-group grants with eviction on revoke, enforcement on/off, `verifyPassword` (normalization, wrong/null input, PENDING_RESET_PASSWORD
accepted while `login` still denies it, DEACTIVATED subject and inactive credential rejected), and
JWT: HS256/HS512 round trip, forged secret, wrong domain / app / missing scope claims, case-insensitive
scope, unknown key ID, `exp` past, `nbf` future, garbage / empty / null / bad signature segment,
suspended key, deactivated subject, stale `iat` with `TS_REQUIRED`, replay cache refusing a second
presentation; bound login via JWT (`isPermitted`, key ID in the principals, clean
unbind on failure); key-ID stamping on insert; every `ShiroUtil` login entry point against the
manager installed as global (password, JWT, a raw-key token refused, `loginSubject` with and without `autoLogin`,
session resume by ID, logged-out session refused); shiro.ini wiring (INI-built realm + cache manager
+ matcher, store via ResourceManager, `fromGlobal` idempotent, login / ShiroUtil / grant eviction
through the INI realm, `installAsGlobal` refused; unbound realm fails loudly, `attach` binds, second
store rejected; the shipped `shiro-ds.ini` loads and attaches via `fromGlobal(ds)`); eager
authorization (lazy login caches nothing, eager login caches the grants, failed login does not,
`lookupAuthorizationInfo` via realm and `ShiroUtil`). App IDs in tests must pass `AppIDNameFilter`
(`"shirods"`, not `"shiro-ds"`).

Every run opens a keystore first (`TestVault`): `-Dstore=<vault> -Dstore.password=<pw>` name a
real one, whose `db.*` entries are then the target and whose `master-key` is the master key;
without them a throw-away vault with a fresh master key is used and the suite stays on in-memory
H2 (`jdbc:h2:mem:shirods;DB_CLOSE_DELAY=-1;MODE=PostgreSQL`). `-Dds.url` still overrides the
target: an H2 file (`;CIPHER=AES` + `-Dds.file_password`) or a PostgreSQL base endpoint
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
checker (`ShiroUtil.checkResourcePermission` — `resource:<owner|guid>:<caller>:<verb>`, `ShiroUtil.isPermitted`) works
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

**2026-09-29 (resource permission model + key chain root; plan `spicy-forging-russell.md`)** —
user decisions: access is a permission on the standardized token
`resource:<resource>:<subject>:<verbs>`, every subject implicitly holds
`resource:S:S:read,update,delete,share` (realm-synthesized), stored grants stay 2-part
`resource:<verbs>`, the `nventity` namespace is removed everywhere, `share` is part of the self
permission (a share holder may re-share). Core: `SecurityModel` (`Target.RESOURCE`,
`ResourcePermissionTokenFilter`, `toResourceToken` ×2, `isResourceToken`, `RESOURCE_SELF_VERBS`,
`nve_*` tokens rewritten, `SecurityModelTest` 10/10, `PermissionGrantTest` 60/60),
`EncryptedData.isCanonicalRecord`, `ChainedFilter.validate` record pass-through. io-xlogistx shiro:
`ShiroUtil.checkResourcePermission` (returns owner GUID, String overload, `isResourcePermitted`),
`ShiroSecurityController` delegates `checkNVEntityAccess`/`isNVEntityAccessible`, sets
`data_type`+`mask` before sealing, `decryptValues` recursion fix, null-safe `currentSubjectGUID`.
Here: `GrantFlattener` self permission + composed scoped tokens, `holdsResource` replaces `isOwner`
in grant/revoke enforcement, `checkCatalogTokenForScope` requires `resource:<verbs>`,
`createSubjectKey`/`deleteSubjectKeys` on the subject lifecycle. Tests rewritten to the new tokens
+ `resource_checkResourcePermission_ownerGranteeStranger` + `subjectKey_createdWithSubject_removedWithSubject`:
**67/67 + 17/17 on H2 and on a freshly recreated lax-2 `testdb`** (bootstrapped 24/6/4 in the `resource:` grammar). ENCRYPT* attributes such as `SubjectAPIKey.api_key` are now `bytea` columns in h2p (packed record when the store encrypts, UTF-8 clear text otherwise; the API-key lookup by secret binds UTF-8 bytes and keeps working on non-encrypting stores).
Nothing committed.

**2026-09-30 (controller opens the packed record)** — core `SecurityController`: the pair overload
`decryptValue(ds, container, NVPair, msKey)` became `String decryptValue(ds, container, byte[] value, msKey)`
— `value` is the packed `CipherCodecs` record, the return is the clear text; bytes that are not a
record ⇒ `IllegalArgumentException`, denied ⇒ `AccessSecurityException`. `ShiroSecurityController`
no longer calls `EncryptedData.fromCanonicalID` (its last caller in the active repos); its
`decryptValues` lost the pair branch (a pair cannot hold a packed record, the store opens values
on read) and now only recurses — no caller anywhere, candidate for removal from the interface.
Core `APISecurityManager` (no implementer) still declares the old pair overload. New test
`controller_decryptValue_opensPackedRecord` (owner, read grantee, stranger, non-record bytes):
manager suite 68/68 on H2; catalog 17/17, h2p 26/25/12/7 + 11/8 + DSM 11 on H2. PostgreSQL not rerun.
Nothing committed.

**2026-10-01 (`SecuritySetup`)** — user decision: `SecurityCatalogSeeder` and `SecurityBootstrap`
merged into one class `SecuritySetup` (`seedCatalog(dsm)` replaces `SecurityCatalogSeeder.seed`,
nested `Report` / `Result`, `bootstrapSuperAdmin`, `resetSuperAdminPassword`); the two old files
are deleted, `tools.SecurityAdminTool` stays the command line over it (it alone binds to h2p and
the vault). No behaviour change: `super-admin@xlogistx.io` holds the `super_admin` role through a
`RoleGrant` with no domain/app, which is how the `*` reaches it (user confirmed the `RoleGrant`
stays). Callers updated: manager `seedCatalog()`, the tool, `SecurityCatalogDBTest`. Catalog 17/17
and manager 68/68 on H2; PostgreSQL not rerun. Nothing committed. Decided the same day but NOT
built (planning only, see the plan file `~/.claude/plans/datastore-acl.md`): the datastore ACL, and
the app model (`AppIDDefault` as the one row per domain+app, root app `xlogistx.com-common` owning
the setup roles and permissions, per-app catalogs) — the setup will have to create that app.

**2026-10-01 (common app)** — user decision: the setup creates the app `xlogistx.com-common`
(domain `xlogistx.com`, app `common`), and `AppIDDefault` itself is the app record (one row per
domain + app). Built: manager `lookupApp` / `createApp` (+ package-private overload with an
explicit creator for the setup, where nobody is bound), `SecuritySetup.ensureCommonApp`, called
from `bootstrapSuperAdmin` after the role grant; the super-admin owns the row and still holds no
app-scoped grant. Tests `bootstrap_createsCommonApp_once`,
`createApp_refusesDuplicateAndInvalid_andNeedsAppCreateUnderEnforcement`: catalog 19/19, manager
68/68 on H2; PostgreSQL not rerun. **Not done yet (app model, still to plan):** the seeded
permissions / roles / groups are still app-less rows (`app_id` null) — making them belong to
`xlogistx.com-common` changes every `lookup*(null, name)` caller, the reserved-name guards
(`isReservedName` requires `app_id == null`) and `appIDMatches` (compares the app part only,
ignores the domain); app-scoped grants and API keys still insert their own `AppIDDefault` copy
instead of referencing the record, so no unique index on (`domain_id`, `app_id`) yet and
`createApp` refuses an app that already has such copies; `AppIDDefault.create(String)` splits at
the first `-` (a hyphenated domain mis-splits). Nothing committed.

**2026-10-02 (PostgreSQL run)** — lax-2 `testdb`: catalog 19/19 and manager 68/68 (covers the
`SecuritySetup` merge, the common-app creation and the 2026-09-30 `decryptValue(byte[])` change);
h2p on the same database: `H2PFieldCryptoTest` 11, `H2PSecureFileTest` 8,
`H2PPostgresDataStoreTest` 9, `H2PDomainSecurityManagerDBTest` 11. `testdb` now holds the
`xlogistx.com-common` row in `app_id_default`. Nothing committed.

**2026-10-02 (test leftovers on the shared database)** — the user found
`super-admin-<uuid>@xlogistx.io` in lax-2 `testdb` and no `super-admin@xlogistx.io`:
`SecurityCatalogDBTest` bootstraps with a stand-in super-admin principal (so it never touches the
real account) and left it behind, and that stand-in had created — and owned — the
`xlogistx.com-common` row; the real setup had never been run on that database. Fixed:
`@AfterAll removeStandIns` deletes the stand-in super-admins, the app records the tests create, and
a common-app row owned by a stand-in; a common app created by the real setup is found and left
alone. The leftovers were removed and `bootstrap-super-admin` was run for real:
`super-admin@xlogistx.io` (throwaway password shown in the session — reset it with
`reset-super-admin-password`), `super_admin` role grant with no app, `xlogistx.com-common` owned by
it. Catalog 19/19 on H2 and PostgreSQL afterwards, no `%admin%` principal left but the real one.
**Running the tests is not the setup**: a database gets its super-admin and common app only from
`SecurityAdminTool command=bootstrap-super-admin`.

**2026-10-02 (datastore access control)** — the store now checks every read and write against its
`SecurityController` (h2p-datastore/CLAUDE.md, "Access control"; plan `~/.claude/plans/datastore-acl.md`).
Here: `ShiroDSDomainSecurityManager.ds()` returns a **system view** of the store — a
`java.lang.reflect.Proxy` over `APIDataStore` that runs each call inside the controller's
`runAsSystem` (controller resolved per call; none ⇒ direct call), so the 70-odd call sites are
unchanged. Reasons: the manager reads with nobody logged in (login, realm, grant loading) and writes
rows owned by other subjects (a new subject's record, principals, credentials, key; grants), which
the store's own check would refuse; and the check itself asks Shiro, which loads grants through the
manager — unchecked reads are what stops that from recursing. Who may call what stays the manager's
own `enforce…` rules. `getDataStore()` still hands out the raw store. The self permission gained
`create` (core `RESOURCE_SELF_VERBS`). A subject reading the security tables directly through the
store falls under the general rule: its own `SubjectIdentifier` (it owns itself), nothing of anyone
else, the catalog only with a wildcard. Tests: `datastoreAccessControl_endToEnd_withTheShiroController`
(manager suite 69) and `datastoreAccessControl_superAdminReachesEveryRow_ordinarySubjectOnlyItsOwn`
(catalog suite 20), both with `ShiroSecurityController` set on the store for the test; in-memory H2
only. Not built: the app model and the registrar subject (`~/.claude/plans/app-model.md`). Nothing
committed.

**2026-10-02 (app model built)** — after the user's go on `app-model.md`: everything in the "App
model" section above. Core (installed): `AppIDDefault.create` splits at the last `-`,
`Role.APP_REGISTRAR`, `APP_ADMIN` + catalog create/update/delete rights, `Role.isPlatformOnly`,
`RoleGroup.isPlatformOnly`, `SecurityModelTest` extended. Here: manager (`COMMON_*`,
`scopeLabel`, `appRecord`, `recordOrCommon`, per-app catalog writes with `enforceScoped` +
`broker_guid`, `requireGrantableIn`, `requireSameApp*`, `createApp` with starter set + registrar +
first manager, `createAppRecord`, `deleteApp`, `ensureRegistrar`, `rotateRegistrarKey`,
`registerSubject`, `runAs`, `insertRoleGrant`, unenforced `insertPrincipal` / `insertCredential`
inside subject creation, `setAppOwner`, `attachToApp`, `scopeAllows` on labels, scoped keys
reference the record), `SecuritySetup` (common app first, per-app seeding, repair of app-less
rows, bootstrap order, common registrar), `GrantFlattener.inScope` (null = common, shares
everywhere), `DSAuthorizingRealm` cache key always labelled, `SecurityAdminTool` (`create-app`,
`rotate-registrar-key`, `list-apps`, role lookups per app). Found on the way: `createSubjectID`
re-checked `subject:update` for the principal and credential it writes, so a holder of
`subject:create` alone could never create a subject. Suites on in-memory H2: catalog 21, manager
72; h2p 9/11/8/26/25/12/7 + DSM 11; core SecurityModel 10, PermissionGrant 60, AppIDURI 1.
**PostgreSQL not run.** lax-2 `testdb` (set up this morning) is on the pre-app-model shape:
rerunning `bootstrap-super-admin` attaches its catalog rows to the common app and creates the
common registrar (prints the secret once). Not built: the opsec sign-up endpoint. Nothing
committed.

**2026-10-02 (night: subject key mandatory, keystore prerequisite)** — two user rules, built after
the session restart. (1) *A new `SubjectIdentifier` always gets its subject `EncapsulatedKey` in
tandem, wrapped under the KeyMaker's master key*: `createSubjectKey` no longer returns quietly when
the store has no `KeyMaker`, it throws `AccessSecurityException` and the creation rolls back.
(2) *The prerequisite of any run is to load the keystore with its password and take the db info and
the master key from it*: `SecurityAdminTool` requires `store=` (or `XLOGISTX_SECRET_STORE`), reads
the `db.*` text secrets and the `master-key` secret key (`loadVault` returns a `Vault{db, masterKey}`),
loads the key into `KeyMakerProvider.SINGLETON` and opens the store with that key maker and
`ShiroSecurityController`; `list-apps` reads the store in the system context. Verified on a scratch
H2 file database with a scratch vault: no store → exit 1; vault without `master-key` → exit 1,
no database file created; bootstrap → every subject has one `encapsulated_key` row and
`subject_api_key.api_key` is a 131-byte sealed record that does not contain the printed secret;
rerun idempotent; `list-apps` and `create-subject` work. The user's vault for lax-2 `testdb` is
`src/main/resources/test.store` (entries `db.url`, `db.user`, `db.password`, AES-256 `master-key`);
**the tool was NOT run against lax-2** — the two subjects created there earlier have no subject key
and the registrar key is in clear text. Tests: new `TestVault` (opens a vault — `-Dstore=` /
`-Dstore.password=`, else a throw-away one with a fresh master key — loads the master key, hands
out `secure(cfg)` = controller + key maker, `toolArgs(...)`, `systemView(store)`); both suites now
run on a store with encryption at rest and the access check on, fixtures and raw checks go through
the `sys` system view, tool runs go through `tool(out, err, ...)` which puts the vault first.
In-memory H2: catalog **21/21**, manager **68/72**. The four failures are the raw API-key logins
(`apiKeyLogin_roundTrip_statusAndExpiry`, `jwtAndApiKey_loginSubject_bindsAndAuthorizes`,
`shiroUtil_loginEntryPoints_workAgainstThisManager`, `deleteSubject_cascadesEverything_includingApiKeys`):
`lookupSubjectAPIKey(key)` is an equality query on the `api_key` ENCRYPT column, which an
encrypting store refuses — now that every store encrypts, a raw key cannot be looked up by its
secret. **Open, the user's decision:** drop the raw-key login (JWT only), or make the raw key carry
its key id. PostgreSQL not run. Nothing committed.

**2026-10-02 (night, part 2: no database without the master key; PostgreSQL run)** — third user
rule: *you cannot use the database without a master key*. `H2PDataStore` now refuses to connect
unless its configuration carries a `SecurityController` and a `KeyMaker` with the master key loaded
(h2p-datastore/CLAUDE.md, last session log), so a manager on a store without them cannot even look
a subject up. Here only the tests changed: `subjectKey_createdWithSubject_removedWithSubject`
asserts the refusal of creation **and** lookup with the master key unloaded or the key maker
removed; tool tests on private H2 databases open them with the credentials the vault supplies
(`SecurityAdminTool_openStore`), the vault test's own databases with the H2 defaults
(`catalogSeeded`); the "no db.url anywhere" assertion became "no secret store, no run".
**PostgreSQL, lax-2 `testdb`, a new empty database created by the user, everything through
`src/main/resources/test.store`:** `bootstrap-super-admin` first (0 tables → 13; super-admin and
common registrar each with one `encapsulated_key` row, 3 key rows, `subject_api_key.api_key` a
131-byte sealed record without the secret in it; the super-admin was created with a random
password that was not recorded anywhere — a password is a hash and cannot be read back, so the
account needs `reset-super-admin-password`; the registrar secret was printed in the session
output. That database was dropped and recreated by the user on 2026-10-03). Then catalog **21/21** and manager
**68/72** (the same four raw API-key logins as on H2; still the user's decision). After the runs:
125 subjects, none without a subject key; 15 API keys, all sealed (128–131 bytes); the only
`%admin%` principal is `super-admin@xlogistx.io`; the common app still owned by it. Left behind by
the suites, as designed: the test apps `xlogistx.io-appa…appd`, `-other`, `-shirods`,
`-signup<id>` with their registrars and starter catalogs (150 permissions, 54 roles in total), the
UUID-named test subjects, and the h2p test tables (36 tables in all). Runs:
`-Dstore=<vault> -Dstore.password=<pw>` on the `RunTests` command line; without them the suites use
a throw-away vault and in-memory H2. Nothing committed.

**2026-10-03 (API keys are third-party credentials, not logins)** — user decisions: *API keys can
not be used to log in to xlogistx or no-sneak; they are a subject's credentials for third-party
APIs (Google, ChatGPT, Claude), encrypted at the datastore level; the subject owns the key, must be
logged in, looks it up from the datastore, and the datastore serves it in clear*; *our system logs
in with JWT*. Built ("go 1,2,3"):
1. **Raw-key login removed.** Manager: `loginSubjectApiKey` and `lookupSubjectAPIKey(secret)`
   deleted; `loginApiKey` (still declared by the core interface, now `@Deprecated` there) always
   throws `AccessSecurityException`. Realm: `APIKeyAuthenticationToken` no longer supported,
   `apiKeyInfo` deleted. io-xlogistx `CredentialsInfoMatcher`: the raw-key branch is gone, such a
   token is always rejected (the token class itself is left in place, now unused).
2. **Key purpose.** Core `SubjectAPIKey` gained the persisted attribute `credential_type`
   (`CredentialInfo.Type`): `getCredentialType()` = the stored value, `API_KEY` when unset
   (fail-closed: only a key explicitly made one is a signing key), `setCredentialType` accepts
   `API_KEY` / `SYMMETRIC_KEY` only, `isSigningKey()`. A JWT login needs a signing key: the realm's
   `jwtInfo` and the matcher's JWT branch both refuse anything else, `mintJWT` refuses to sign with
   one, and `newRegistrarKey` creates the registrar key as `SYMMETRIC_KEY`. A key row with no
   stored purpose is treated as a third-party key and refused at JWT login (run-verified: the
   third-party key in the test is stored with no purpose and its forged JWT is refused), so a
   registrar key created before this field needs `rotate-registrar-key`.
3. **Tests.** `apiKeyLogin_roundTrip_statusAndExpiry` →
   `thirdPartyApiKey_sealedInTheStore_servedInClearToItsLoggedInOwner_neverALogin` (sealed raw
   column; the logged-in owner searches the datastore and gets the clear key; a stranger and nobody
   get no row; raw login refused; a JWT forged with the third-party secret and its key id refused);
   `jwtAndApiKey_loginSubject_…` → `jwt_loginSubject_bindsAndAuthorizes`; the ShiroUtil test
   asserts the raw-key token is refused; the delete-cascade test looks the key up by id;
   `newSigningKey` stamps `SYMMETRIC_KEY`.
Jars reinstalled: zoxweb-core (`-DskipTests -Dgpg.skip=true`), xlogistx-shiro
(`-Dmaven.test.skip=true`). **Results: in-memory H2 and PostgreSQL (lax-2 `testdb` through
`test.store`) — catalog 21/21, manager 72/72; h2p on H2 9/11/8/26/25/7/12 + default-manager 11,
on PostgreSQL 9/11/8/9.** After the PostgreSQL run: 231 subjects, none without a subject key; 22
API keys, all sealed. Not touched (other repos), **read in the source only, nothing in no-sneak
was compiled or run**: `Session.java` line 173 calls `domainSecurityManager.loginApiKey(...)`;
there is an `APIKeyRoundTripTest`; `SessionAICredentialSource` and `SubjectPanel` ask for
credentials of `CredentialInfo.Type.API_KEY`. What those do at runtime with this manager was not
tested. **Correction (2026-10-03, read in the no-sneak source):** no-sneak does not use this
manager at all. `Main.createDomainSecManager` and `NoSneakUtil` build core's
`DomainSecurityManagerDefault`, `Session` holds the core `DomainSecurityManager` interface, and
`Main.createDataStore` opens an H2 file through `H2PDSCreator` with no `SecurityController` and
no `KeyMaker`. So its API-key login is the default manager's, not the refusing one here; and once
no-sneak is built against the current h2p-datastore, that store will refuse to connect (no
database without the master key). Nothing committed.

**2026-10-03 (retest on a fresh PostgreSQL database)** — the user recreated lax-2 `testdb` (0
tables) and asked for a retest; no code change. Through `test.store`: `bootstrap-super-admin`
(13 tables; super-admin and common registrar with one subject key each; the registrar key stored
as a sealed `SYMMETRIC_KEY`, 131 bytes), then catalog **21/21**, manager **72/72**, h2p
`H2PPostgresDataStoreTest` 9, field crypto 11, secure file 8, access control 9. Afterwards: 114
subjects, none without a subject key; every API key sealed; the only `%admin%` principal is
`super-admin@xlogistx.io`; the common registrar key is a signing key. **Correction (same day):**
the registrar secret is not lost — it is sealed in `subject_api_key` and the master key in
`test.store` opens it (see "Registrar" above for the run that verifies the mechanism; no read-back
was performed on `testdb` itself). No rotation is needed. The super-admin password is different:
it was a random value that was not recorded, a password is stored as a hash, so that account needs
`reset-super-admin-password`. Test leftovers as before (test apps, UUID
subjects, h2p test tables).

**2026-10-03 (SubjectSwap tested for the registrar sign-up; `runAs` NOT replaced yet)** — the user
pointed at xlogistx-shiro's existing `SubjectSwap` (try-with-resources: binds a logged-in subject,
`close()` restores the thread) and `SubjectTask` (a `Runnable` that runs as a captured subject);
the manager's `runAs` had been written without looking at them. Read in the source and the Shiro
1.13 bytecode: `Subject.execute` (used by `runAs` and by `SubjectTask`) and `SubjectSwap` all go
through `SubjectThreadState.bind()` / `restore()`; `runAs` adds only the registrar's JWT login and
logout around it. User's instructions for the test: realm built in code first, then from
`shiro.ini`; always `test.store` and the key maker with its master key; persistent data, nothing
thrown away — simulate production. New test `SubjectSwapSignUpDBTest` (no production code
changed): refuses to run without `-Dstore`; needs the database set up by the tool first
(`bootstrap-super-admin`, `create-app app.id=xlogistx.io-swapsignup`); reads the registrar's
signing key back from the datastore through the master key; logs the registrar in without binding
it (`new Subject.Builder(m.getSecurityManager()).buildSubject().login(jwt)`); runs one scenario
twice — phase A `new ShiroDSDomainSecurityManager(ds)` (realm built in code), phase B
`shiro-ds.ini` + `fromGlobal(ds)` — under `setEnforcePermissions(true)`. The scenario asserts:
no sign-up with nobody logged in; sign-up inside the swap creates the subject with the app's
`app_user`, grantor = the registrar; nothing bound afterwards on an unbound thread; a subject bound
before is the same instance afterwards; an exception and a refused sign-up inside the swap restore
the thread; the registrar can do nothing else (create app, grant role, delete subject, rotate key
refused); the plain datastore serves the registrar's own record inside the swap and the user's
outside; nested swaps restore in reverse order; one registrar login reused for several sign-ups,
and a fresh login per sign-up; six sign-ups on three pooled threads sharing the registrar subject,
every worker unbound before and after; a swap stored on a `ShiroSession` under `subject-swap` is
closed by the session's `close()` (caller back, then logged out by the session; registrar still
logged in); a logged-out registrar signs nobody up; a fixed account (`swap-resident@example.com`)
is created once and joined on every later sign-up. **Run on lax-2 `testdb` (fresh database made by
the user, set up with the tool through `test.store`): run 1 — phase A and phase B pass; run 2 in a
new JVM on the data of run 1 — both pass, the resident found with the same GUID. Rows afterwards:
60 subjects (super-admin, 2 registrars, 57 signed up), none without a subject key; 57 `app_user`
grants, all brokered by `registrar.xlogistx.io-swapsignup`; one resident; 2 API keys, both sealed
signing keys.** Nothing was deleted. `runAs` was replaced afterwards, see the next entry. The
super-admin of this database has a random password that was not recorded (reset with
`reset-super-admin-password`). Nothing committed.

**2026-10-03 (`runAs` replaced with `SubjectSwap`)** — on the user's word, after the test above
had passed. Manager: `runAs(SubjectAPIKey, Callable)` deleted (with its `Callable` /
`ExecutionException` imports); new public `loginUnboundSubjectJWT(compactJWT, host)` — a full Shiro
login that returns the subject without binding it (`unboundSubject`, which `bindSubject` now
builds on). A sign-up is `loginUnboundSubjectJWT(mintJWT(key, …), host)` + `try (SubjectSwap swap
= new SubjectSwap(registrar)) { registerSubject(…); }`; whether the registrar subject is kept for
further sign-ups or logged out after each is the caller's choice (the user was asked and did not
choose; both are tested). Tests: the manager suite's sign-up test now goes through a test helper
`asRegistrar(key, work)` (unbound login, `SubjectSwap`, logout) in place of `dsm.runAs`;
`SubjectSwapSignUpDBTest` logs the registrar in with the new manager method. **Run on lax-2
`testdb` through `test.store`: `SubjectSwapSignUpDBTest` 2/2 (third run on the persisted data,
resident found), manager suite 72/72, catalog suite 21/21.** In-memory H2 was not run this time
(user rule: these tests use `test.store`). The two suites left their usual test apps and subjects
in `testdb` next to the swap test's data. Nothing committed.

**2026-10-03 (the super-admin id comes from the SecretStore)** — user rule: *from now on all
reference to super-admin comes from the SecretStore, no longer hard coded*. The user added the
reserved entry `super-admin-id` to io-xlogistx opsec `SecretStore` (`SUPER_ADMIN_ID`,
`setSuperAdminID` / `getSuperAdminID` / `removeSuperAdminID`, validated by `SubjectIDFilter`) and
to `test.store` (`super-admin@xlogistx.io`); the opsec jar was reinstalled from the user's working
tree. Here: the constant `DEFAULT_SUPER_ADMIN_PRINCIPAL_ID` is gone and the realm's
`superAdminPrincipalID` starts null; manager `requireSuperAdminPrincipalID()`, null-safe
`isSuperAdminSubject` / `lookupSuperAdminSubject`; `SecuritySetup.bootstrapSuperAdmin` and
`resetSuperAdminPassword` require the id; `SecurityAdminTool.loadVault` reads
`vault.getSuperAdminID()` into `Vault.superAdminID`, refuses a vault without it, sets it on the
manager for every command, and `bootstrap-super-admin` / `reset-super-admin-password` refuse
`principal.id=`. Tests: `TestVault.superAdminID()` (a named vault must hold the entry, a throw-away
vault gets a generated one); the tool tests bootstrap the vault's super-admin instead of a
command-line principal and assert the refusal of `principal.id=` and of a vault without the entry;
`superAdminPrincipalID_hasNoDefault_isNormalizedWhenSet` replaces the test that asserted the
default and an INI-set value (the realm setter is still a bean property, so an INI line could
still set it; nothing in this repo does). No `super-admin@xlogistx.io` literal is left in
`xlogistx-shiro-ds/src`. **Run on lax-2 `testdb` (a new empty database made by the user), through
`test.store`:** the tool refused `principal.id=` on bootstrap; bootstrap created
`super-admin@xlogistx.io` from the vault entry (subject key present, one `super_admin` grant);
`create-app xlogistx.io-swapsignup`; then `SubjectSwapSignUpDBTest` 2/2, catalog 21/21, manager
72/72. Afterwards 144 subjects, none without a subject key; 15 API keys, all sealed. The
super-admin was created with a random password that was not recorded (a hash cannot be read back:
`reset-super-admin-password`). In-memory H2 not run. Nothing committed.

**2026-10-03 (test keystores for H2; `db.enc-key` renamed `db.enc-password`)** — on the user's
instruction two keystores were created under `src/test/resources`, both with the store password the
user gave for testing, `super-admin-id` = `admin.local`, `db.user` = `dbuser`, an AES-256
`master-key` of their own, and a generated `db.password` (kept only inside the keystore):
- `h2mem.store` — `db.url` = `jdbc:h2:mem:shirods-test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL`;
- `h2persist.store` — `db.url` = `jdbc:h2:file:./src/test/resources/h2persist;CIPHER=AES;MODE=PostgreSQL`
  and a generated `db.enc-password`, the H2 file password. The data file is
  `src/test/resources/h2persist.mv.db`, next to the keystore. **The path is relative to the working
  directory, so the tool and the tests must be started from the `xlogistx-shiro-ds` directory**
  (where Maven and the IDE start module tests); started elsewhere, H2 would create a new empty file
  there.
The user named the file-password entry `db.enc-password`; the code read `db.enc-key`, so it was
renamed everywhere it is read: `SecurityAdminTool` (`Param.DB_ENC_PASSWORD`, command-line
`db.enc-password=`), `H2PDumpRestore` (vault entry), `TestVault` / `CryptoTestSupport` callers and
the three shiro-ds test classes. `test.store` has no such entry and is unaffected. Verified with the
tool, run from the module directory: `list-catalog` through `h2mem.store` (exit 0);
`list-catalog` through `h2persist.store` created the data file; a second process opened it
(`list-apps`, exit 0); the same command with `db.enc-password=<wrong>` failed with H2's
"Encryption error in file", exit 2. Both databases are empty: no bootstrap was run on them and no
suite was run through these keystores. Nothing committed.

**2026-10-03 (suites run through `h2mem.store` and `h2persist.store`)** — on the user's
instruction, started from the `xlogistx-shiro-ds` directory. **`h2mem.store`:** catalog 21/21,
manager 72/72; `SubjectSwapSignUpDBTest` did not run — its setup stops with "set the database up
first" because it needs a database prepared by the tool in an earlier process, which an in-memory
database cannot keep. **`h2persist.store`:** tool `bootstrap-super-admin` (super-admin
`admin.local`, from the keystore) and `create-app xlogistx.io-swapsignup`; then
`SubjectSwapSignUpDBTest` 2/2, catalog 21/21, manager 72/72, and the swap test a second time in a
new process 2/2 with the resident found under the same GUID. Rows in the H2 file afterwards
(read-only JDBC with the keystore's credentials): 172 subjects, none without a subject key;
`admin.local`, both registrars and the resident each with one subject key; 15 API keys, all sealed
(128 bytes or more); 57 signed-up principals. The data file grew to about 0.9 MB and H2 wrote
`h2persist.trace.db` beside it. Read in that file: its entries are H2 errors of the form
`Constraint "fk_<table>_app_id" already exists` — the datastore issues its `ADD CONSTRAINT` again
when a new process opens a database that already has the foreign keys, and carries on after the
error. The suites pass regardless. Decided by the user on 2026-10-05: this is the design requirement,
no change (see the 2026-10-05 entry). The super-admin of
this file was created with a random password that was not recorded. Nothing was deleted; nothing
committed.

**2026-10-04 (four manager methods moved to the core interface)** — context: that morning the user
committed io-xlogistx (`1b2c651`), added `isSuperAdminSubject`, `lookupRoleGroupByGUID`,
`lookupRoleByGUID`, `lookupPermissionByGUID` to core's `DomainSecurityManager` (stubs in
`DomainSecurityManagerDefault`) and pointed `GrantFlattener` at that interface. On the user's
instruction the four methods the realm still needed from the concrete class were moved the same
way: `lookupSubjectByGUID(String)`, `lookupSubjectAPIKeyByID(String)`,
`hasOutstandingResetToken(String)`, `restoreActiveAfterExpiredReset(SubjectIdentifier)` are now
declared on the core interface; `ShiroDSDomainSecurityManager` implements them with `@Override`
(the last two went from package-private to public); `DomainSecurityManagerDefault` got stubs in
the style of its other promoted methods (null, null, false, no-op). Every manager method
`DSAuthorizingRealm` calls is now on the core interface; the realm itself still declares its field,
constructor and setter as `ShiroDSDomainSecurityManager` and uses that class's static helpers
(`REALM_NAME`, `appScope`, `scopeLabel`, `attach`) — not changed. Core jar reinstalled
(`-DskipTests -Dgpg.skip=true`). Run through `h2mem.store`: catalog 21/21, manager 72/72 (this is
also the first run since the user's cleanup). PostgreSQL and the persistent H2 file not run.
zoxweb-core and zoxweb-datastore uncommitted.

**2026-10-04 (the realm holds the core `DomainSecurityManager` interface)** — on the user's
instruction. `DSAuthorizingRealm`: field, constructor parameter, `getDomainSecurityManager()`,
`setDomainSecurityManager(...)` and the internal `dsm()` are typed `DomainSecurityManager`; every
instance method it calls is on that interface. What still names the concrete class in the realm,
on purpose: the fallback `ShiroDSDomainSecurityManager.attach(this, ds)` that builds a manager
from the datastore registered in `ResourceManager` when none was set, and the static helpers
`REALM_NAME`, `appScope`, `scopeLabel` (same as `GrantFlattener` after the user's cleanup).
`ShiroDSDomainSecurityManager.attach` / `fromGlobal` now check the realm's manager with
`instanceof` (`shiroManagerOf`) and throw `IllegalStateException` when the realm is bound to
another implementation. Run from the module directory: `h2mem.store` — catalog 21/21, manager
72/72 (including the three INI tests); `h2persist.store` — `SubjectSwapSignUpDBTest` 2/2 on the
persisted data (realm built in code, and realm from `shiro-ds.ini`). PostgreSQL not run. Not
tested: the realm bound to a manager other than `ShiroDSDomainSecurityManager`. Uncommitted.

**2026-10-04 (the four classes moved to io-xlogistx `shiro`)** — user decision after planning:
`DSAuthorizingRealm`, `GrantFlattener`, `ShiroDSDomainSecurityManager` and `SecuritySetup` now
live in `io-xlogistx/shiro/src/main/java/io/xlogistx/shiro/ds/` (package name kept, so
`shiro.ini` files, the tool and the tests are unchanged); the admin tool stays here. `SecuritySetup`
had to go with the manager: each calls the other, through package-private methods. Checked before
the move: the four import only the JDK, Shiro, zoxweb-core and xlogistx-shiro, and a trial compile
at Java 8 level (the io-xlogistx module's level; this module builds at 25) with no h2p / opsec /
H2 / PostgreSQL jars succeeded. Done as a plain move: the files were copied, compared
byte-for-byte, then removed here; no git history carried over; nothing committed in either repo
(io-xlogistx shows the new `shiro/src/main/java/io/xlogistx/shiro/ds/` directory, this repo shows
three tracked files deleted and `SecuritySetup.java` gone). xlogistx-shiro was installed with
`-Dmaven.test.skip=true`; this module was rebuilt with `clean install -DskipTests`, and its
`target/classes` holds only the tool, so the manager can only come from the xlogistx-shiro jar.
The tests cannot move: they need `H2PDataStore`, and io-xlogistx depending on h2p would make the
two repos depend on each other. **Run after the move, from this module's directory:**
`h2mem.store` — catalog 21/21, manager 72/72; `h2persist.store` — swap test 2/2, catalog 21/21,
manager 72/72; `test.store` (PostgreSQL lax-2 `testdb`) — swap test 2/2, catalog 21/21, manager
72/72; `SecurityAdminTool list-apps` on the persistent H2 file works. Build order is unchanged:
zoxweb-core, io-xlogistx, zoxweb-datastore.

**2026-10-04 (`REALM_NAME`, `appScope`, `scopeLabel` moved to `ShiroUtil`)** — on the user's
instruction the three static helpers left `ShiroDSDomainSecurityManager` for
`io.xlogistx.shiro.ShiroUtil` (same names). `scopeLabel` needs the common scope, so
`COMMON_DOMAIN_ID`, `COMMON_APP_ID` and `COMMON_SCOPE` are now defined in `ShiroUtil` as well; the
manager keeps its three `COMMON_*` constants as aliases of those, so `SecuritySetup`, the tool and
the tests that name them are unchanged. Callers redirected: 24 unqualified calls inside the
manager, and the references in `DSAuthorizingRealm` (4), `GrantFlattener` (3), `SecuritySetup`
(3), `SecurityAdminTool` (11) and the three test classes (11). Result: `GrantFlattener` no longer
names the manager class at all, and the only place `DSAuthorizingRealm` names it in code is the
`attach` fallback that builds a manager from the registered datastore. Both modules rebuilt
(xlogistx-shiro installed, this module `clean install`). Run from this module's directory:
`h2mem.store` — catalog 21/21, manager 72/72; `h2persist.store` — swap test 2/2, catalog 21/21,
manager 72/72. Then, on the user's instruction, through `test.store` on PostgreSQL (lax-2
`testdb`): swap test 2/2 (resident found), catalog 21/21, manager 72/72. Nothing committed.

**2026-10-04 (rerun on a new PostgreSQL database)** — the user recreated lax-2 `testdb` (0 tables)
and asked for a rerun; no code change. Through `test.store`, from this module's directory: tool
`bootstrap-super-admin` (super-admin `super-admin@xlogistx.io` from the keystore) and
`create-app xlogistx.io-swapsignup`; `SubjectSwapSignUpDBTest` 2/2 (resident created in the first
phase, found in the second), catalog 21/21, manager 72/72; the swap test again in a new process
2/2 with the resident found under the same GUID. Rows afterwards: 18 tables; 172 subjects, none
without a subject key; the super-admin, both registrars and the resident each with one subject
key; 15 API keys, all sealed (12 signing keys, 3 third-party). The code under test is the state
after the move to io-xlogistx and the helpers in `ShiroUtil`. The super-admin was created with a
random password that was not recorded (`reset-super-admin-password`). Nothing committed.

**2026-10-04 (the super-admin's initial password comes from the keystore)** — user rule: *I added
`super-admin-password` to `test.store`; use it as the init password for the super-admin, later the
password can be changed*. `SecurityAdminTool`: constant `SUPER_ADMIN_PASSWORD =
"super-admin-password"` (an optional text entry; opsec `SecretStore` has no typed accessor for
it, the tool reads it with `get`), `Vault.superAdminPassword`. `bootstrap-super-admin`: when the
vault holds the entry, the account is created with it, the tool prints that the initial password
came from the keystore, and `password=` is refused (exit 1, before anything is written); when the
vault holds none, the old behaviour stays (`password=` or console prompt). It is the initial
password only: a rerun on an existing account leaves the password alone, and
`reset-super-admin-password` still takes its new password from `password=` or the prompt. Tests:
`TestVault.superAdminPassword()` (a throw-away vault gets a generated one; a named vault may have
none); tool tests bootstrap through `toolBootstrap` and log in with `adminPassword()`; the vault
test covers both paths on its own vault (created with `password=` when the entry is absent;
with the entry: `password=` refused, account created with the entry, an existing account
untouched, a reset changes it). Runs, from this module's directory: catalog suite 21/21 on a
throw-away vault (entry present) and 21/21 through `h2mem.store` (entry absent). **PostgreSQL,
lax-2 `testdb` reset by the user (0 tables), through `test.store`:** bootstrap with `password=`
refused and nothing created; bootstrap with no password created `super-admin@xlogistx.io` with
`login=verified`; `create-app`; swap test 2/2, catalog 21/21, manager 72/72; bootstrap rerun
reports the account existing, password unchanged. Rows: 144 subjects, none without a subject key;
the super-admin has one password row and one subject key; 15 API keys, all sealed. `h2mem.store`
and `h2persist.store` hold no `super-admin-password` entry. Nothing committed.

**2026-10-04 (`super-admin-password` mandatory; `SecretStore.StoreParam`)** — user: *the
super-admin-password, the initializer one, make it mandatory, and create an enum `StoreParam` in
`SecretStore` that implements `GetName` and `IsMandatory`*. io-xlogistx opsec `SecretStore`: enum
`StoreParam` — `MASTER_KEY` (`master-key`), `SUPER_ADMIN_ID` (`super-admin-id`),
`SUPER_ADMIN_PASSWORD` (`super-admin-password`) mandatory; `DB_URL`, `DB_USER`, `DB_PASSWORD`,
`DB_ENC_PASSWORD` optional — and `missingMandatory()`, the mandatory entries a store lacks; opsec
jar reinstalled; `SecretStoreTest` 9/9 (new `storeParam_namesAndMandatoryFlags_missingMandatory`).
`db.url` was left optional in the enum because the tool still accepts it from the command line,
an environment variable or a system property (not decided otherwise by the user).
`SecurityAdminTool`: `loadVault` refuses a vault for which `missingMandatory()` is not empty
("holds no <names>: mandatory …"), so a vault without `super-admin-password` stops every command
before the database is touched; `MASTER_KEY_ALIAS` and `SUPER_ADMIN_PASSWORD` take their values
from the enum; `bootstrap-super-admin` always creates the account with the vault's password and
always refuses `password=` (the `password=` / prompt fallback is gone);
`reset-super-admin-password` is unchanged. `TestVault` checks `missingMandatory()` on a named
vault. Tests: tool bootstraps never pass a password; the vault test adds the three mandatory
entries one at a time and asserts the refusal at each step. **Keystores:** `h2mem.store` and
`h2persist.store` had no `super-admin-password` and were refused by the tool after this change; a
generated one was added to each (it exists only in the keystore), and on the persistent H2 file
`admin.local`'s password was set to the keystore's value with `reset-super-admin-password`, so
the two agree. Runs, from this module's directory: throw-away vault — catalog 21/21;
`h2mem.store` — catalog 21/21, manager 72/72; `h2persist.store` — swap 2/2, catalog 21/21, manager
72/72; `test.store` (PostgreSQL) — swap 2/2, catalog 21/21, manager 72/72. Nothing committed.

**2026-10-04 (`db.url` mandatory; the keystore is its only source)** — user: *make db.url
mandatory*, after being told that this also meant dropping the other sources in the admin tool.
opsec `SecretStore.StoreParam.DB_URL` is mandatory (`SecretStoreTest` 9/9, jar reinstalled).
`SecurityAdminTool`: the database is the vault's `db.url` and nothing else — `db.url=` on the
command line is refused ("db.url= is not accepted", exit 1), the environment variable
`XLOGISTX_SHIRO_DB_URL`, the system property `shiro.ds.db.url`, `resolveDbUrl` and the two
constants are removed; a vault without `db.url` is refused by the mandatory check. `db.user`,
`db.password` and `db.enc-password` can still be overridden on the command line (optional
entries, unchanged). Tests: a throw-away `TestVault` now carries a `db.url` (a private in-memory
H2), which is the suites' target when no vault is named; `TestVault.vaultFor(url)` /
`toolArgsFor(url, …)` build a copy of the run's vault with another `db.url`, and the catalog
suite's `tool(out, err, …)` helper turns a `"db.url=<url>"` argument into the vault for that
database instead of passing it on (the 35 tool calls on private databases are otherwise
unchanged); new assertions: `db.url=` on the command line refused and nothing written, a vault
without `db.url` refused. **Not changed:** `H2PDumpRestore` (h2p module) still accepts `--url`
over the vault's `db.url`; the suites still accept `-Dds.url` as a test-harness override. Runs,
from this module's directory: throw-away vault — catalog 21/21, manager 72/72; `h2mem.store` —
21/21, 72/72; `h2persist.store` — swap 2/2, 21/21, 72/72; `test.store` (PostgreSQL) — swap 2/2,
21/21, 72/72; tool with `db.url=` refused, without it `list-apps` works. Nothing committed.

**2026-10-05 (the user's `AuthorizationInfoLookup` change checked; reruns on the three keystores;
duplicate-constraint errors are by design)** — the user changed code and asked for it to be
checked, starting with zoxweb-core. No source file was changed in this session.
*The change (the user's, read in the diff):* core `AuthorizationInfoLookup<I, O>` — the type
parameters are now input then output (they were `<O, I>`) and the interface extends
`DataDecoder<I, O>` with a default `decode` that calls `lookupAuthorizationInfo`; io-xlogistx
`XlogistXIniRealm`, `DSAuthorizingRealm` and the cast in `ShiroUtil.lookupAuthorizationInfo` use
`<PrincipalCollection, AuthorizationInfo>`. No other source in the sibling repos uses the
interface (`ULTIMATE/zoxweb-core` has a copy of its own in another package).
*Run:* offline installs of zoxweb-core, io-xlogistx (7 modules, `-DskipTests`) and
`h2p-datastore` + `xlogistx-shiro-ds` (clean, `-DskipTests`) all succeeded. Maven ran no unit
test (core's pom skips them by default). Suites, from this module's directory: `h2mem.store` —
catalog 21/21, manager 72/72; `test.store` (PostgreSQL, lax-2 `testdb`) — catalog 21/21, manager
72/72, swap 2/2; `h2persist.store` — catalog 21/21, manager 72/72, swap 2/2. The manager suite
includes the `lookupAuthorizationInfo` test through the realm and through `ShiroUtil`. The rows
were not checked after these runs.
*Read, not run:* `DataDecoder` extends `Codec`, whose default `getName()` returns null; the realms
keep their Shiro name because the method inherited from the Shiro base class takes precedence
over an interface default. The `XlogistXIniRealm` lookup path compiled; no test exercised it.
*Duplicate-constraint errors:* in the `h2persist.store` run H2 wrote 26 entries
`Constraint "fk_…" already exists` to `h2persist.trace.db` — nine foreign keys, each once per
suite process (one of them in two of the three), all within the first two seconds of each suite
and none afterwards. Cause, read in `H2PDataStore.ensureTable`: `ALTER TABLE … ADD CONSTRAINT` is
issued for every entity reference without checking for the constraint, once per entity type per
store instance (the `createdTables` guard is an instance field), and `execDDLQuiet` ignores the
duplicate error. So it happens at start-up, on the first use of each type, never per transaction.
On PostgreSQL the same statement is rejected with SQL state `42710` and ignored the same way;
the client writes no trace file and the run logs show nothing; the server log on lax-2 was not
looked at. **User, 2026-10-05: "that was the design requirement which is ok no need to change".**
*Planning only (nothing built, nothing decided):* how `SecurityAdminTool` could stop depending on
`H2PDataStore`. Read: the tool imports `H2PDSCreator`, `H2PDataStore` and `H2PUtil`; its only
h2p-specific code is `openStore`. A `driver` entry in the keystore is **not needed**: the tool and
the tests build the configuration with `H2PDSCreator.toAPIConfigInfo(url, …)`, which sets
`org.postgresql.Driver` for a `jdbc:postgresql` URL and keeps `org.h2.Driver` otherwise (run on
both engines by the suites above); the user confirmed "no need for driver in the keystore".
`H2PDSCreator` implements core's `APIServiceProviderCreator`, and `createAPI` always builds one
class, `H2PDataStore`; the configuration decides the engine. A probe (run in this session, then
deleted; it wrote nothing) opened both databases through the core interfaces only, with the
creator named by a string: PostgreSQL through `test.store` and the encrypted H2 file through
`h2persist.store`; a PostgreSQL URL with `org.h2.Driver` failed with "Driver org.h2.Driver claims
to not accept jdbcUrl". What an interface-only tool would still need (read): the driver choice
from the URL exists only in the static `toAPIConfigInfo`, not in `createEmptyConfigInfo` /
`createAPI` (default `org.h2.Driver`); something has to name the creator class; the tool's
"PostgreSQL URL must name the database" check uses `H2PUtil.parseJdbcURL`. Nothing committed.

**2026-10-05 (`SecurityAdminTool` no longer references h2p classes: creator named by a default
constant)** — user: *specify the class as a default constant inside SecurityAdminTool if no class
name was provided for the H2PDSCreator, that should solve the class dependency*. Only
`tools/SecurityAdminTool.java` was changed.
*Built:* constant `DEFAULT_DS_CREATOR = "io.xlogistx.datastore.h2p.H2PDSCreator"` (a string); new
option `ds.creator=<class>` (`Param.DS_CREATOR`, in the usage text) — its source being the command
line is my choice, the user only said "if no class name was provided"; `loadCreator(className)`
loads the named class, or the default, by reflection as core's `APIServiceProviderCreator` and
refuses with a usage error (exit 1) a class that is not on the classpath, cannot be created or is
not a creator. `openStore(creator, url, user, password, filePassword, keyMaker)` now returns
`APIDataStore<?, ?>`: it takes `creator.createEmptyConfigInfo()`, fills it by property name
(`url`, `user`, `password`, `file_password`, and `driver` = `org.postgresql.Driver` for a
`jdbc:postgresql` URL), sets the `ShiroSecurityController` and the key maker, calls
`creator.createAPI` and checks that the result is an `APIDataStore`. The three h2p imports
(`H2PDSCreator`, `H2PDataStore`, `H2PUtil`) are gone; the file's only mention of h2p is the
constant's value.
*What the tool now does itself, with strings, in place of the h2p helpers it called*
(`H2PDSCreator.toAPIConfigInfo`, `H2PUtil.parseJdbcURL`): reads the subprotocol of the JDBC URL
(`jdbcSubprotocol`), checks that a PostgreSQL URL names its database (`postgresDatabase`), sets the
PostgreSQL driver, and appends `;CIPHER=AES` to an H2 URL that has a file password and no cipher
(`hasCipher`). Behaviour is meant to be the same as before. These property names and engine rules
are h2p's; a different creator class would have to accept the same properties.
*Not changed:* the pom (h2p-datastore stays an optional compile dependency; the tests import its
classes and the default creator must be on the runtime classpath), the tests (their
`SecurityAdminTool_openStore` helper builds its own store with `H2PDSCreator`), h2p-datastore.
No test was added for `ds.creator=`.
*Run, from this module's directory after a clean offline install of the module:* `h2mem.store` —
catalog 21/21, manager 72/72; `h2persist.store` — catalog 21/21, manager 72/72, swap 2/2;
`test.store` (PostgreSQL, lax-2 `testdb`) — catalog 21/21, manager 72/72, swap 2/2. Tool, command
`list-apps` (read-only): default creator through `test.store` exit 0 and through
`h2persist.store` exit 0; `ds.creator=io.xlogistx.datastore.h2p.H2PDSCreator` through `test.store`
exit 0; `ds.creator=no.such.Creator` exit 1 "datastore creator no.such.Creator is not on the
classpath"; `ds.creator=java.lang.String` exit 1 "… is not an
org.zoxweb.shared.api.APIServiceProviderCreator". The rows were not checked after these runs.
Nothing committed.

**2026-10-05 (`SecurityAdminTool` moved to io-xlogistx, module `opsec`)** — the user first asked
for the `shiro` module. That cannot compile (read in the poms): the tool uses opsec's
`SecretStore` and `OPSecUtil`, `opsec` depends on `shiro`, and `shiro` has no dependency on
`opsec`, so the tool there would need a circular dependency. The user then said: *move
SecurityAdminTool to opsec module*. Done as a plain file move, package name unchanged:
`io-xlogistx/opsec/src/main/java/io/xlogistx/shiro/ds/tools/SecurityAdminTool.java`. No line of
the file was changed for the move. This module's `src/main/java` is now empty; `src/main` holds
only `resources/test.store`. The pom of this module and the tests were not changed (the tests use
the tool's public constants and `run`, under the same class name, from the opsec jar).
*Run:* opsec offline install succeeded, the class file is version 52 (Java 8) and is in the
installed `xlogistx-opsec-1.0.0.jar`; the Javadoc step printed its errors in
`DomainIdentityMatcher` / `IdentityStore`, none in the tool, and does not fail the build. Clean
offline install of this module succeeded. Suites from this module's directory: `h2mem.store` —
catalog 21/21, manager 72/72; `h2persist.store` — catalog 21/21, manager 72/72, swap 2/2;
`test.store` (PostgreSQL, lax-2 `testdb`) — catalog 21/21, manager 72/72, swap 2/2. Tool
`list-apps` through `test.store` and through `h2persist.store`: exit 0, and `-verbose:class`
shows the class loaded from the opsec jar. The start command is unchanged
(`java -cp "$(cat cp-shiro.txt)" io.xlogistx.shiro.ds.tools.SecurityAdminTool …`). The opsec
`SecretStoreTest` was not rerun. The rows were not checked after these runs. Nothing committed.

**2026-10-05 (module `xlogistx-shiro-ds` removed; its content is in `h2p-datastore`)** — user:
*remove xlogistx-shiro-ds module and move all its content to h2p-datastore test before deletion*.
Plain file moves, no line of any moved file changed; all 14 files compared byte for byte (SHA-1)
after the move:
- the four test classes, package unchanged, to
  `h2p-datastore/src/test/java/io/xlogistx/shiro/ds/test/`;
- `h2mem.store`, `h2persist.store`, `h2persist.mv.db`, `h2persist.trace.db`, `shiro-ds.ini` and
  `test.store` (it was under `src/main/resources`) to `h2p-datastore/src/test/resources/`;
- this file (it was the module's `CLAUDE.md`) to `h2p-datastore/SHIRO-DS.md`, and `app-model.md`,
  `datastore-acl.md`, `no-sneak-plan.md` to the `h2p-datastore` directory. Putting the documents
  in the module directory and not under `src/test` is my choice.
The parent pom lost its `<module>xlogistx-shiro-ds</module>` line. The `h2p-datastore` pom was not
changed: every dependency the tests need is inherited from the parent pom (read there:
xlogistx-shiro, xlogistx-opsec, shiro-core, cache-api, ehcache, slf4j-api, commons-logging,
junit). `.claude/test-runner/cp-shiro.txt` lost its two `xlogistx-shiro-ds/target` entries, and the
runner's README was updated.
*Run, from the `h2p-datastore` directory after a clean offline install of `h2p-datastore`:*
`h2mem.store` — catalog 21/21, manager 72/72; `h2persist.store` — catalog 21/21, manager 72/72,
swap 2/2 (the relative `db.url` resolves to the moved file, which was rewritten by the run);
`test.store` (PostgreSQL, lax-2 `testdb`) — catalog 21/21, manager 72/72, swap 2/2; tool
`list-apps` through `test.store` and `h2persist.store`, exit 0. The h2p suites of the module
itself were not rerun. The rows were not checked after these runs.
*The directory `zoxweb-datastore/xlogistx-shiro-ds` itself:* after the moves it held only its
`pom.xml` (tracked in git), empty folders and `target/`. My delete command was refused by the
session's permission system and was not retried; the user deleted the directory the same day
(checked afterwards: it no longer exists). Untouched:
the installed artifact `org.zoxweb:xlogistx-shiro-ds:1.0.0` in the local Maven repository (no pom
depends on it), and the IDE files that still name the module (`zoxweb-datastore/.idea`, and a run
configuration in `io-xlogistx/.idea/workspace.xml` that points at the old `test.store` path).
Nothing committed.

**2026-10-05 (`h2persist.store` recreated)** — the user deleted the persistent H2 file
(`h2persist.mv.db`, `h2persist.trace.db`) and then the old `h2persist.store`, and asked for the
keystore to be recreated. New `h2p-datastore/src/test/resources/h2persist.store`, made with the
opsec `SecretStore` command line, same store password as the other test keystores, seven entries
(listed by name after creation): a new `master-key` (AES 256), `super-admin-id` = `admin.local`,
`db.user` = `dbuser`, `db.url` =
`jdbc:h2:file:./src/test/resources/h2persist;CIPHER=AES;MODE=PostgreSQL` (relative: run from the
`h2p-datastore` directory), and newly generated `db.password`, `db.enc-password` and
`super-admin-password`, which exist only inside the keystore. Nothing was run through it: there
is no database file yet, the first run creates it, and it will be empty (no catalog, no
super-admin, no app) until `bootstrap-super-admin` and, for the swap test,
`create-app xlogistx.io-swapsignup` are run through the tool. Nothing committed.

**2026-10-05 (suites run through the new `h2persist.store`)** — on the user's instruction, from
the `h2p-datastore` directory, on the database file the first command created. Set-up through the
tool, as the swap test requires: `bootstrap-super-admin` (exit 0; `admin.local` created with the
keystore's initial password, `login=verified`, `wildcard=verified`, common app created) and
`create-app app.id=xlogistx.io-swapsignup` (exit 0). Then `SubjectSwapSignUpDBTest` 2/2 (resident
created in the first phase, found in the second), `SecurityCatalogDBTest` 21/21,
`ShiroDSDomainSecurityManagerDBTest` 72/72, and the swap test again in a new process 2/2 with the
resident found under the same GUID. `h2persist.trace.db` afterwards holds 30 entries, all
`Constraint "…" already exists` (by design, see above). The registrar secrets the tool printed
were not kept. The rows were not checked after the run. Nothing committed.

**2026-10-05 (no-sneak on the Shiro manager; `DomainSecurityManagerDefault` deleted)** — see
`no-sneak-plan.md` (status block at the top) for the no-sneak replacement, the app id
`xlogistx.com-nosneak` and the three open test cases, and `CLAUDE.md` (session log 2026-10-05) for
the deletion of core's `DomainSecurityManagerDefault` and the port of
`H2PDomainSecurityManagerDBTest` to the Shiro manager. Nothing committed.
