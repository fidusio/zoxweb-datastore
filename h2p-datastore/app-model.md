# App model — plan (BUILT 2026-10-02 after the user's go; see the status notes below)

Status: sections 3 and 4 are implemented, uncommitted, green on in-memory H2, PostgreSQL not run.
As-built notes: `CLAUDE.md` in this module, section "App model". Deviations: no database unique
index on (`domain_id`, `app_id`) — h2p has no composite-unique declaration, `createApp` checks
inside its transaction and nothing else inserts app rows; the opsec HTTP sign-up endpoint (section
3.3, second bullet) is not built — the manager side is: `registerSubject`, and for the swap
`loginUnboundSubjectJWT` + xlogistx-shiro's `SubjectSwap` (the manager's own `runAs` was removed
2026-10-03).

Written 2026-10-02. Repos: zoxweb-core, io-xlogistx (shiro, opsec), zoxweb-datastore
(xlogistx-shiro-ds). Companion to `datastore-acl.md` (the store-level access check).

## 1. Decisions taken by the user

| # | Decision | Date |
|---|---|---|
| 1 | A domain + app is a real record, created by the super-admin or a domain admin. | 2026-10-01 |
| 2 | The record is `AppIDDefault` itself: one row per domain + app. | 2026-10-01 |
| 3 | Root app: domain `xlogistx.com`, app `common` ⇒ `xlogistx.com-common`. The super-admin creates it at initialization; the setup roles and permissions belong to it. | 2026-10-01 |
| 4 | Each app has its own roles and permissions. One subject, grants per app: a login into app A loads only what was granted in A. | 2026-10-01 |
| 5 | No manager field on the app row. Managers = holders of `app_admin` in that app; `broker_guid` on a permission / role row = who created it; the app row's `subject_guid` = who created the app. | 2026-10-01 |
| 6 | `super-admin@xlogistx.io` is the super-admin: the `super_admin` role through a `RoleGrant`, whose permission is `*`; no domain/app; applies in every login. | 2026-10-01 |
| 7 | Seeder + bootstrap are one class, `SecuritySetup`, and it creates `xlogistx.com-common`. | 2026-10-01 |
| 8 | First-time creation of a subject by an app = a **subject swap**: the app runs the creation as a subject of that app holding the right. A dedicated **registrar subject** per app, not the human app-admin. | 2026-10-02 |

Standing assumptions (stated to the user, not objected to):
- A new app starts with a starter set of its own roles and permissions.
- A login with no domain/app, for anyone but the super-admin, is a login into `xlogistx.com-common`.

## 2. Built so far (uncommitted, green on H2; setup run for real on lax-2 `testdb`)

- `SecuritySetup` (merge of `SecurityCatalogSeeder` + `SecurityBootstrap`).
- Manager `lookupApp(domainID, appID)` / `createApp(domainID, appID)` (owner = bound subject,
  `app:create` under enforcement, duplicate or invalid pair refused).
- `SecuritySetup.ensureCommonApp`, called from `bootstrapSuperAdmin`: the `xlogistx.com-common`
  row, owned by the super-admin.

## 3. The registrar subject (decision 8)

### 3.1 What it is

One service subject per app whose only job is the first-time creation of a subject for that app
(sign-up). The sign-up code swaps to it for the creation, then swaps back.

| Property | Value |
|---|---|
| Subject type | `SubjectType.SYSTEM` |
| Principal | one per app, derived from the app id; **not email-shaped**, so it has no password-reset channel |
| Credential | a `SubjectAPIKey` scoped to the app (`app_id` = the app record), stored as a **signing key** (`credential_type` = `SYMMETRIC_KEY`, 2026-10-03). No password. The registrar logs in only with a JWT signed by it, so its login is always a login into that app. |
| Grant | the app's own `app_registrar` role, scoped to the app; `broker_guid` = the creator of the app |
| Created | by `createApp`, in the same transaction as the app row and its starter set |
| Secret | returned by `createApp` and printed by the CLI at creation; meant to be kept in the app's `SecretStore` vault, never in code or config files. It also stays in the datastore as a sealed record that the master key opens (verified by a run 2026-10-03, see CLAUDE.md "Registrar"); the CLI has no command that prints it again |

### 3.2 What it may do

- `app_registrar` carries **`subject:create` only**.
- It does **not** hold `permission:assign:role`. With that permission a compromised sign-up path
  could hand out `app_admin`.
- The role assignment a sign-up needs is done by a dedicated manager method instead:

`registerSubject(principalID, credential)` on the manager:
1. caller must be logged into an app and hold `subject:create` there;
2. the app is taken from the caller's login scope — there is no app parameter, so a registrar can
   never register into another app;
3. unknown principal ⇒ create the subject (as `createSubjectID` does today);
4. known principal ⇒ no second subject: the supplied password must verify (`verifyPassword`),
   otherwise the generic failure — the answer never reveals whether the account exists;
5. grant that app's `app_user` role to the subject, `broker_guid` = the registrar. Always that one
   role, chosen by the method, not by the caller.

### 3.3 The swap

- A helper builds a separate Shiro `Subject`, logs it in with the registrar key, runs the
  registration inside `Subject.execute(...)` (bound for that block only, the thread's previous
  state is restored afterwards), and logs it out in `finally`.
- Entry point for applications: an opsec service beside `PasswordReset` — an anonymous sign-up
  endpoint that reads the registrar key from the vault and performs the swap. Rate limiting in
  front of it is a deployment concern, as for the reset endpoints.
- At the datastore nothing special happens: the manager writes the new subject's rows in the
  system context (see `datastore-acl.md`); the swap satisfies the manager's rule about who may
  create a subject. The registrar gets no permission on the new subject's data.

### 3.4 Lifecycle

- Key rotation: an `app_admin` of the app replaces the registrar's key (the new secret is returned by the call and printed by the CLI).
- Turning sign-up off for an app: deactivate the registrar subject or its key.
- Deleting an app removes its registrar.
- `xlogistx.com-common` gets a registrar like any other app, created by `SecuritySetup`.

## 4. The rest of the app model, in build order

1. **One app row, referenced.** App-scoped grants and API keys reference the `AppIDDefault`
   record instead of inserting their own copy; an app that was never created is an error; deleting
   a grant never deletes the app row. Then a unique index on (`domain_id`, `app_id`).
2. **Catalog ownership.** The seeded permissions / roles / role groups get `app_id` =
   `xlogistx.com-common`. Touches: the seeder's lookups (`(appID = null, name)` today), every
   `lookup*(null, name)` caller, `isReservedName` (requires `app_id == null` today),
   `appIDMatches` (compares the app part only — must compare the domain too).
3. **App creation with a starter set.** `createApp` also creates the app's own `app_admin`,
   `app_user`, `app_service_provider`, `user`, `app_registrar` roles with their permissions, the
   registrar subject (section 3), and grants the app's `app_admin` to the first manager named by
   the creator. `super_admin` and `domain_admin` exist only in `xlogistx.com-common`.
4. **Per-app catalog writes.** Creating or changing a permission / role of app A requires being a
   manager of A, logged into A; `broker_guid` is stamped. Roles of A are grantable only inside A.
5. **Login with no domain/app** = login into `xlogistx.com-common` (flattener scope, cache key,
   `scopeAllows`). The super-admin's `*` keeps applying everywhere.
6. **Registration** (section 3): `app_registrar` role in core `SecurityModel.Role`,
   `registerSubject`, the swap helper, the opsec sign-up service.
7. **CLI**: `create-app` (prints the registrar key id and secret when it creates them), `rotate-registrar-key`, `list-apps`.
8. `AppIDDefault.create(String)` splits at the **last** `-` (app names are letters and digits only,
   domains may contain hyphens).

## 5. Points I settled myself — say no to any of them

1. The registrar holds `subject:create` only; the role assignment is inside `registerSubject`.
2. Its credential is an app-scoped signing key (a `SubjectAPIKey` of type `SYMMETRIC_KEY`), no password.
3. Its principal is not email-shaped.
4. A known principal signing up in another app proves its password and gets that app's role; no
   second subject.
5. Every app has a registrar, the common app included.

## 6. Tests to write (in-memory H2; PostgreSQL only when asked)

- `createApp` produces the row, the starter set, the registrar and its key; the stored column
  holds a sealed record, not the secret. (The secret itself is readable through the datastore by
  the system context and by the logged-in registrar, because the master key opens it.)
- Registrar login is scoped to its app; it cannot create an app, assign a role, update or delete a
  subject, or register into another app.
- `registerSubject`: new principal; known principal with the right and with the wrong password
  (same generic failure as an unknown one); always `app_user` of the caller's app; broker = registrar.
- The swap helper restores the thread's previous subject on success and on failure.
- Rotation invalidates the old key; a deactivated registrar stops sign-up.
- Tests remove what they create on a shared database (see the shiro-ds notes, 2026-10-02).
