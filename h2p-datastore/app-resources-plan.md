# App-owned resources — plan (decided 2026-10-16; NOT built, to be applied on the user's word)

Status: **decided, nothing built.** Six decisions taken by the user on 2026-10-16 (table below),
design and test written out here so a later session can apply them as dictated. No line of code,
no document other than the ones listed under "Docs to update when built" has changed. The plan
file of the session itself was `~/.claude/plans/adaptive-napping-globe.md` (same content).

Repos: io-xlogistx (`shiro`, the manager), zoxweb-datastore (`h2p-datastore`, the test and these
notes). zoxweb-core, the datastore main code, `GrantFlattener`, `SecuritySetup` and
`SecurityAdminTool` are **not** touched by this plan.

## 1. The need (user, 2026-10-16)

Some entity types must belong to an app and not to the subject that creates them. Example: the
app `RubusPI` owns, or has full access to, every `DeviceSession`, while any subject holding the
**create** permission for that class type may create one. The same type may be used in another
app in a different way, so the binding is per app, not per class.

Today nothing of this exists: the store stamps the bound subject as owner
(`ShiroSecurityController.associateResourceToSubject`), and only `registerSubject` uses the
registrar swap, driven by the application with the key from its vault (`app-model.md` section 3).

## 2. Decisions taken by the user (2026-10-16)

| # | Decision |
|---|---|
| 1 | The owning subject of app-owned rows is the app's **registrar** (`registrar.<domain>-<app>`, exists since 2026-10-02). |
| 2 | The creator holds a **CREATE-only** permission for the class type; it keeps nothing on the row. |
| 3 | Creation = `SubjectSwap` to the registrar, insert, swap back to the previous subject. |
| 4 | It lives in a **manager method** (`ShiroDSDomainSecurityManager`); the datastore is untouched. |
| 5 | The declaration "type T is app-owned in app X" is **a permission row only**: `<type>:create` in X's catalog. No new entity type. |
| 6 | The row does **not** record its creator (`subject_guid` = registrar, nothing else). |

Two points settled by Claude, to be vetoed if wrong:
- the token form `<nvce name>:create`, derived from the entity's `NVConfigEntity` name (section 3.1);
- the **new-rows-only** rule: every GUID in the entity graph must be empty (section 3.2, step 4).

## 3. Design

### 3.1 The permission (the binding)

- Token: `SecurityModel.toSecTok(<nvce name>, CREATE)` → e.g. `property_impl:create`,
  `device_session:create`. Row name `<nvce name>_create` (the `subject_create` / `subject:create`
  convention of the built-in catalog). `<nvce name>` = `nve.getNVConfig().getName()`, the
  datastore's type identity (its table name).
- Created in the app's catalog through the existing `createPermission(PermissionInfo)` with
  `app_id` = the app (under enforcement: `permission:create` in a login that may act on the app,
  i.e. an `app_admin` of the app).
- Granted with the existing `addPermissionGrant(subject, permissionInfo, app)` or through a role
  of that app. Holding the token in a login into app X is the "create" right. It gives nothing on
  the rows afterwards (decision 2).

### 3.2 New manager API (`io-xlogistx/shiro/src/main/java/io/xlogistx/shiro/ds/ShiroDSDomainSecurityManager.java`)

1. `public static String appResourceCreateToken(NVConfigEntity type)` — the token of 3.1.
2. `public PermissionInfo bindAppResourceType(AppIDDefault app, NVConfigEntity type)` — finds the
   app's row whose `permission_token` equals the token (scan `lookupAllPermissionsByAppID(scope)`),
   else creates it via `createPermission` (idempotent; enforcement = that of `createPermission`).
   Returns the row.
3. `public PermissionInfo lookupAppResourceType(AppIDDefault app, NVConfigEntity type)` — the row
   or null.
4. `public <V extends NVEntity> V createAppResource(V nve)` — the operation:
   1. `app = loginScope()`: the caller's login scope, never a parameter (as `registerSubject`).
      Null (global login) or the common scope ⇒ `AccessSecurityException` (no registrar there).
   2. `enforce(token)` — a no-op when enforcement is off, the manager's convention.
   3. `lookupAppResourceType(app, type) == null` ⇒ `IllegalArgumentException("<type> is not an app
      resource of <app>")`. This makes decision 5 the real gate, also for a `*` holder.
   4. **New rows only**: `nve.getGUID()` and the GUID of every referenced entity in the graph must
      be empty, else `IllegalArgumentException`. Reason: under the swap the registrar may update
      what it owns; a preset GUID would turn a "create" into an overwrite of a registrar-owned row
      (of any type) by a subject holding only `create`.
   5. `inTransaction`: `registrar = lookupRegistrar(record)` (none ⇒ `IllegalStateException`); its
      active `SYMMETRIC_KEY` `SubjectAPIKey` found by `subject_guid` (the search
      `rotateRegistrarKey` uses; the system view returns the secret in clear, verified 2026-10-03);
      `Subject reg = loginUnboundSubjectJWT(mintJWT(key, null, APP_RESOURCE_JWT_TTL), null)`;
      `try (SubjectSwap swap = new SubjectSwap(reg)) { return getDataStore().insert(nve); }
      finally { reg.logout(); }`.
      - The **plain** store (`getDataStore()`), not the system view `ds()`: the controller stamps
        `subject_guid` = registrar, `checkCreate` passes on the registrar's self permission, the
        entity key is minted under the registrar's subject key. The datastore's ambient
        transaction is thread-local and survives the swap.
      - The swap helper stays **private**: nothing else lets a caller act as the registrar (it
        holds `subject:create`). The app's own full access to its rows is the existing pattern:
        registrar key from the app's vault → `loginUnboundSubjectJWT` → `SubjectSwap` → plain
        datastore (`app-model.md` 3.3).
      - `APP_RESOURCE_JWT_TTL`: short, e.g. 60 s.

Not included (the user may dictate later): a `SecurityAdminTool` command for
`bindAppResourceType`, the opsec HTTP endpoint, a cached registrar `Subject` per app (a login per
call is the first version).

## 4. Test (h2p-datastore `src/test/java/io/xlogistx/shiro/ds/test/ShiroDSDomainSecurityManagerDBTest.java`)

One new test, modelled on `registrar_signUpThroughTheSubjectSwap_createsOrJoins_andNothingElse`
(helpers `asRegistrar`, `loginApp`, `newSubject`, `appRole`, `uniquePrincipal`,
`currentSubjectGUID`), stand-in type `PropertyImpl` (already imported) for `DeviceSession`:

`appResource_createdThroughTheRegistrarSwap_ownedByTheRegistrar_creatorHoldsCreateOnly`
- `createApp(DOMAIN, name)`; `bindAppResourceType(app, NVC_PROPERTY_IMPL)` twice → same row, token
  `property_impl:create`, `app_id` = app; a second app `name + "b"` without the binding.
- Subject S: `addPermissionGrant(S, row, app)`. Enforcement on. `loginApp(S, name)`.
- `createAppResource(p)`: returned row `subject_guid` = registrar GUID; the bound subject is S
  again; `ds.searchByID` as S → nothing; `ds.update` / `ds.delete` as S → `AccessSecurityException`;
  `asRegistrar(key, …)` reads and updates it.
- Refusals: S without the grant; S logged into `name + "b"` (type unbound there, grant absent); S
  logged in globally (no scope); preset GUID; nested entity with a GUID; the common app.
- Enforcement off, nobody bound → still refused (no scope).
- Cleanup: delete the row as the registrar, `deleteApp` both apps.

## 5. Docs to update when built

- `SHIRO-DS.md`: "App model" section — paragraph **App-owned resources** (decisions, token, API,
  new-rows-only rule); the "Public extras" line; session log entry of the build day.
- `app-model.md`: section 7 (added 2026-10-16 as a pointer) gets the as-built notes.
- `io-xlogistx/CLAUDE.md` line 129 (manager entry): add "app-owned resources
  (`createAppResource`, registrar swap)".
- This file: status line → BUILT, with the runs.

## 6. Verification when built

1. io-xlogistx: `mvn -o -pl shiro install -Dmaven.test.skip=true -Dgpg.skip=true` (from the shell).
2. zoxweb-datastore: `mvn -o -pl h2p-datastore -DskipTests test-compile`.
3. Offline runner (`.claude/test-runner`, its README): `ShiroDSDomainSecurityManagerDBTest` through
   `h2mem.store` (expect 78/78), then `h2persist.store` from the `h2p-datastore` directory, and
   PostgreSQL testdb through the user's `D:/dev/data/java/projects/temp/ds/xlogistx.store` only on
   the user's word. The vault passwords are not recorded: ask.
4. `SecurityCatalogDBTest` 21/21 unchanged (the built-in catalog is untouched).
5. Nothing committed.