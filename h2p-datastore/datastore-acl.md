# Datastore ACL — plan (BUILT 2026-10-02 after the user's go)

Status: sections 3 to 5 are implemented, uncommitted, green on in-memory H2; PostgreSQL not run
(the user keeps `testdb` free of test data). As-built notes: h2p-datastore/CLAUDE.md, section
"Access control". Section 8 is still open.

Written 2026-10-01. Repos: zoxweb-core, io-xlogistx (shiro), zoxweb-datastore (h2p-datastore,
xlogistx-shiro-ds). Target store: h2p only (Derby retiring, Mongo on standby).

## 1. The rule

A subject using the datastore can only reach the NVEntities it holds the proper permission on —
**read, update, delete, share** — whether the data is encrypted or not.

- The permission is the existing resource token `resource:<resource>:<subject>:<verbs>`
  (`SecurityModel.toResourceToken`). The owner passes through its self permission
  `resource:S:S:…`; anyone else through a share grant `resource:<entity guid>:<grantee>:<verbs>`;
  the super-admin through `*`.
- The store never compares `subject_guid` itself. It asks the `SecurityController`.
- Today the store asks only when it must decrypt a value or open an encrypted file. Plaintext rows
  from `search` / `searchByID` and every `update` / `delete` are not checked at all. This plan
  closes that.

## 2. Decisions already taken (not re-opened)

| # | Decision |
|---|---|
| 1 | Full CRUD is checked at the store, not only reads. |
| 2 | Denied read ⇒ the row is dropped silently (fewer search results, empty `searchByID`, a denied referenced entity is null/absent). Denied write/delete ⇒ `AccessSecurityException`. |
| 3 | Code that runs with nobody logged in (login, realm grant loading, key-row lookups, bootstrap, admin tool, dump/restore) runs in a **system context**: a thread-local entered with `runAsSystem(...)`, exposed on core `SecurityController`. Not a daemon subject holding `*`, not a per-type exemption. |
| 4 | Controller configured + nobody authenticated + not system context ⇒ reads return nothing, writes throw. |
| 5 | Activation = a `SecurityController` on the store's `APIConfigInfo`. ~~No controller ⇒ unchecked, as today (dev/tests/CLI). The `KeyMaker` only governs encryption.~~ **Superseded 2026-10-02 (user rule: no database without the master key):** a store without a controller, without a `KeyMaker`, or without a loaded master key refuses to connect, so there is no unchecked store. |
| 6 | A row without `subject_guid` has no owner: only `*`, `resource:*:*:<verb>`, an explicit share on that row, or the system context reach it. |
| 7 | Read choke point = `H2PDataStore.buildEntity`; the check runs before any decryption; the owner verdict is memoized per owner within one call. |
| — | Super-admin: one permission `*`, everything, in every login (user, 2026-10-01). |

## 3. What each operation does once a controller is configured

"Caller" = the bound, authenticated subject. "System" = inside `runAsSystem`.

| Operation | Check | Denied |
|---|---|---|
| `search`, `userSearch`, `searchByID`, `userSearchByID`, `lookupByReferenceID`, `nextBatch` | `read` on every row built, including referenced entities and collection members | row absent; reference null; collection member missing |
| `batchSearch` (guid report) | `read` per matching row | guid left out of the report |
| `countMatch` | `read` per matching row | not counted |
| `insert` of a new row | owner stamped = caller; `create` on that owner (see 5.3) | `AccessSecurityException` |
| `insert` of an existing GUID, `update`, `patch` | `update` against the **stored** owner | `AccessSecurityException` |
| `delete(nve, withReference)` | `delete` against the stored owner; each cascaded child checked too | parent: exception; child: kept, cascade continues (same as a shared child today) |
| `delete(nvce, criteria)` | per matching row | rows the caller cannot read are skipped; a readable row it cannot delete ⇒ exception before anything is deleted |
| `createFile` / `updateFile` | as insert/update on the `FileInfo` row | exception |
| `readFile`, `fileVersions` | `read` on the file (plaintext versions too, not only encrypted ones) | nothing written, `null` / empty |
| `deleteFile`, `rollbackFile` | `delete` / `update` on the file | exception |
| `dump`, `dumpZip`, `restore` | system context required | exception (a silently partial dump is worse than a refusal) |

`share` is not a store operation. Sharing is creating a grant through the security manager, which
already requires `share` on the resource; the grant is what the store then honours on read/update/
delete.

Not covered (not NVEntity rows with an owner): `DynamicEnumMap`, sequences. Left unchecked;
listed in section 8.

## 4. Changes by repository

### 4.1 zoxweb-core

- `SecurityController`: two **default** methods, fail-closed so existing implementers keep compiling:
  `<V> V runAsSystem(Supplier<V>)` (default: just runs the supplier, no elevation) and
  `boolean isSystemContext()` (default `false`).
- `SecurityModel.RESOURCE_SELF_VERBS`: add `create` (see 5.3). `ResourcePermissionTokenFilter`
  (stored share tokens) is unchanged — a share never carries `create`.
- Tests: `SecurityModelTest` self-verb assertion.

### 4.2 io-xlogistx (shiro)

- `ShiroUtil.runAsSystem(Supplier)` / `isSystemContext()`: re-entrant thread-local depth counter,
  always restored in `finally`.
- `ShiroUtil.checkResourcePermission(resourceGUID, ownerGUID, verb)`:
  - system context ⇒ allowed;
  - `ownerGUID == null` ⇒ skip the owner token, evaluate the grant token only (decision 6) — today
    it throws `NullPointerException`, which locks even the super-admin out of ownerless rows.
- `ShiroSecurityController`: implements the two new methods by delegating to `ShiroUtil`;
  `isNVEntityAccessible(ref, owner, crud…)` accepts a null owner; the key-chain reads
  (`KeyMakerProvider.getKey` after the controller's own access check, in `encryptValue` and the
  three `decryptValue` overloads) run inside `runAsSystem` — a grantee opening a shared value
  walks the **owner's** key rows, which the grantee may not read on its own.
- The type exemptions in the controller (`MessageTemplate`, `APICredentialsDAO`, `APITokenDAO`)
  are left as they are in this pass (they belong to the deferred retire questions).

### 4.3 h2p-datastore

Read path:
- A per-call read context replaces the bare `cache` map threaded through `select` / `buildEntity` /
  `innerSearchByIDs`: entity cache + owner-verdict memo + "ACL active" flag + system flag.
- `select`: always includes `subject_guid` in the column list when a projection is given.
- `buildEntity`: right after `guid` and `subject_guid` are read from the row — verdict from the
  memo (owner token), else the grant token on the row's GUID. Denied ⇒ return null, record the
  GUID as denied in the cache (so it is not re-queried), nothing decrypted, no reference resolved.
  The entity is registered in the cache only after it is allowed.
- Callers of `buildEntity` skip nulls (`select` loop, `ENTITY_REF` resolution, collection fill).
- `batchSearch` and `countMatch`: select `guid, subject_guid` and filter/count through the same check.

Write path:
- `innerUpdate` / `patch` / `innerInsert`: the `existsByGuid` probe becomes "read the stored
  `subject_guid`" (same single round trip). Existing row ⇒ `update` against the stored owner; an
  in-memory `subject_guid` that differs from the stored one is refused outside the system context
  (no ownership change through an update).
- New row: null owner ⇒ stamped with the caller, also when the GUID was preset (today the
  controller's association only stamps when the GUID is null, which lets a preset-GUID row be
  written ownerless). Then `create` on that owner.
- `insertChildren`: a referenced entity that already exists and that the caller may not update is
  **referenced, not rewritten**, provided the caller may read it; otherwise exception. Today a
  parent write silently rewrites every referenced row.
- `deleteByGuid` and `delete(nvce, criteria)`: per the table in section 3.
- Files: `fileAccess` applies whenever a controller is configured, independent of encryption.
- `H2PFieldCrypto.ensureEntityKey` / `entityKey`: key-row lookups and the key-row insert run in the
  controller's system context.
- `H2PDumpRestore`: refuses to run with a controller configured unless in system context.

Tests (new class `H2PAccessControlTest`, plus additions to the two crypto suites), on the
Shiro-free `TestSecurityController` extended with the system context:
owner / share grantee per verb / stranger / unauthenticated / system, for: search, searchByID,
projection, batch report, count, referenced entity owned by someone else, collection with mixed
owners, insert (stamping, preset GUID, foreign owner), update with a forged in-memory owner,
patch, delete with cascade into a foreign child, criteria delete, plaintext file read/update/
delete, dump refusal, ownerless row, no controller ⇒ ~~unchanged behaviour~~ the database is refused (since 2026-10-02).

### 4.4 xlogistx-shiro-ds

- The manager's `ds()` returns a system-context view of the store (a `java.lang.reflect.Proxy`
  over `APIDataStore` that wraps each call in the controller's `runAsSystem`) when the store has a
  controller; the 71 call sites stay as they are. The realm, flattener, seeder and bootstrap all
  read through the manager, so they are covered.
- The manager's own `enforce…` rules remain the access policy for security rows (subjects,
  principals, credentials, grants, catalog). A subject reading those tables **directly** through
  the store falls under the general rule (owner or grant), nothing special.
- Invariant this protects: the ACL check calls `isPermitted`, which loads the caller's grants
  through the manager; without the system view that load would itself be ACL-checked and recurse.
- Tests: a store with `ShiroSecurityController` configured — login, grant loading and eviction
  still work; a logged-in subject reads its own rows and a shared row, not a stranger's; the
  manager works with nobody logged in.

## 5. Points I settled myself — say no to any of them

1. **Stored owner, not the caller's object.** Update/delete are checked against the
   `subject_guid` read from the database. Otherwise a caller passes the check by writing its own
   GUID into the object it sends.
2. **No ownership change through update** outside the system context.
3. **Create as a permission.** Creating a row owned by O requires `resource:O:<caller>:create`.
   The self permission gains `create`, so a subject creates its own rows, an admin holding
   `resource:*:*:create` creates on behalf of others, and nobody plants rows in another subject's
   name. Keeps the "never an equality test" rule.
4. **Criteria delete**: invisible rows skipped, visible-but-not-deletable rows abort the call.
5. **Dump/restore need the system context** when a controller is configured.
6. **Every row is judged on its own** (its own GUID and owner), referenced entities included. A
   share on a parent does not reach its children — this is how encrypted values already behave.
   Whether a shared parent should carry its children is the deferred child-rows question.

## 6. Known costs and limits

- One `isPermitted` per distinct owner per call, plus one per row not owned by a permitted owner.
  In-memory against the caller's cached permissions; no extra database round trip.
- `MAX_SELECT_RESULTS` applies before the filter, so a capped search can return fewer rows than
  the cap while more permitted rows exist. `batchSearch` / `nextBatch` is exact.
- No SQL push-down of the filter in this pass (the store cannot know the caller's shares without
  the controller). Possible later: the controller hands the store a visibility hint.

## 7. Order of work and verification

1. zoxweb-core (interface defaults, self verbs) → install with `-Dgpg.skip=true`.
2. io-xlogistx shiro (`ShiroUtil`, `ShiroSecurityController`) → install.
3. h2p-datastore (read path, write path, files, dump, tests).
4. xlogistx-shiro-ds (system view, tests).
5. Full regression on H2 with the offline runner: h2p 26/25/12/7 + 11/8 + new ACL class, DSM 11,
   shiro-ds 68/17, core `SecurityModelTest` / `PermissionGrantTest`. Then the same on PostgreSQL
   lax-2 `testdb` (also owed from 2026-09-30).

Nothing is active in an application until it sets a `SecurityController` on its store's
`APIConfigInfo`; no production code does today (only the h2p crypto tests). no-sneak logs in with
`login(...)`, which binds no subject — it must move to `loginSubject(...)` before it turns the
controller on. That switch is outside this plan.

## 8. Deferred (user, 2026-10-01: "don't worry about 1–6 for now")

- The six catalog questions: `APIAppManagerProvider` family, `AIProviderConfig` / `AISkill`
  ownership, unused document types, child rows, config-file classes, direct catalog reads.
- `DynamicEnumMap` and sequences under the ACL.
- The **app model** has its own plan: `app-model.md` (app record, `xlogistx.com-common`, per-app
  roles and permissions, the registrar subject used for first-time subject creation).

How a subject is created under this ACL (user question, 2026-10-02): never by a direct store
insert. The security manager writes the new subject's rows in the system context; who may call it
is the manager's own rule (`subject:create`). The first subject comes from the command-line setup;
later ones from a logged-in admin, or from an app's registrar subject through a subject swap
(`app-model.md` section 3).
