# xlogistx-shiro-ds

Second `DomainSecurityManager` implementation (the first is `DomainSecurityManagerDefault` in
zoxweb-core): persistence through **any** `APIDataStore`, authentication / authorization / caching
through **Apache Shiro 1.13**. Package `io.xlogistx.shiro.ds`, JDK 25 (same as h2p-datastore, which
the tests run against). Design page (architecture, decisions, phases):
https://claude.ai/code/artifact/61b80405-b4b9-48f6-a1b1-dd915e119f5e

Nothing here imports h2p-datastore; only the test binds the manager to `H2PDataStore`.

## Dependencies (pom)

Compile: zoxweb-core, xlogistx-shiro 1.0.0 (tokens incl. `APIKeyAuthenticationToken`,
`CredentialsInfoMatcher`, `DomainAuthenticationInfo`, `DomainPrincipalCollection`,
`ShiroSecurityManager`, `ShiroUtil`),
shiro-core, cache-api, ehcache, slf4j-api, commons-logging. Test: h2p-datastore 1.0.0, h2
(`${h2.version}` is defined in the parent pom since 2026-09-02), junit-jupiter-params. Version
properties (`xlogistx.version`, `apache.shiro.version`, `junit.version`, ...) come from the
`io.xlogistx:xlogistx-mvn` grandparent, not from this repo.

## Classes

| Class | Role |
|---|---|
| `ShiroDSDomainSecurityManager(APIDataStore[, CacheManager])` | The manager. **Self-managed** mode: builds its own `ShiroSecurityManager` (main-thread block **off**, `MemoryConstrainedCacheManager` unless one is given) with one `DSAuthorizingRealm`. **Attached** mode (`attach(realm, ds)`, `fromGlobal()`, `fromGlobal(ds)`): adopts an INI-built realm, owns no security manager, and routes every Shiro call through `SecurityUtils.getSecurityManager()`. Registers `CIPassword` and `SubjectAPIKey` as credential collections by default. |
| `DSAuthorizingRealm` | `AuthorizingRealm` that reads through the manager. **No-arg constructor for shiro.ini** (defaults: name `shiro-ds`, `CredentialsInfoMatcher`, authn caching off, authz caching on); the manager is bound by the manager itself, by `setDomainSecurityManager`, or lazily on first use from the `APIDataStore` registered in `ResourceManager` under `dataStoreResource` (default `Resource.DATA_STORE` = `"DataStore"`), failing loudly otherwise. Supports `DomainUsernamePasswordToken`, `APIKeyAuthenticationToken` and xlogistx-shiro's `JWTAuthenticationToken`. Authentication caching **off**; authorization cached per **subject GUID**. `evictAuthorization(guid)` / `evictAllAuthorization()`. |
| `io.xlogistx.shiro.authc.CredentialsInfoMatcher` (xlogistx-shiro; `DSCredentialsMatcher` was **merged into it** 2026-09-03) | Passwords (hash via `SecUtil.isPasswordValid`, `autoAuthenticationEnabled` skips the check). Raw API keys: constant-time compare + `isUsable(sak)` (status/expiry). JWTs: HMAC check with the key's secret (`SecUtil.decodeJWT`), `sub` == key ID, `exp`/`nbf` with `setClockSkewMillis` (default 1 min), key scope vs claims, and for `isTimeStampRequired()` keys an `iat` inside `setJWTTimestampWindowMillis` (default 5 min) plus optional `setJWTReplayCache(JWTTokenCache)`. All knobs are bean properties (INI-settable). |
| `io.xlogistx.shiro.authc.APIKeyAuthenticationToken` (**in xlogistx-shiro since 2026-09-03**) | Raw key token; principal = subject GUID once the realm resolved it, `toString` never prints the key. |
| `GrantFlattener` | subject GUID → roles + permission strings: PermissionGrant → token; RoleGrant → role name + its permission tokens; RoleGroupGrant → each role, **re-read by GUID** so nested permission refs resolve. Missing catalog rows are skipped. |

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
