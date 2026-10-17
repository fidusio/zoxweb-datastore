# Sharing gaps — plan (decided 2026-10-06; the user said: implement it after the restart)

Status: **BUILT 2026-10-06** (session after the restart; nothing committed). Decisions taken by the
user at the start of that session, which narrow section 4: **catalog-scoped grants stay as they
are** — the eight rules, the uniqueness check, the change method and the cascade apply to **inlined
shares only**; the structural/caller split (first choice below) and the inlined-only cascade were
confirmed with a yes. What was built, the runs (77/77 on the three keystores, companion suites,
testdb rows checked) and the manager internals are recorded in `SHIRO-DS.md` ("Instance grants and
sharing" + session log 2026-10-06). The text below is the plan as dictated before the restart.

Readable version of the whole discussion (how sharing and the key chain work today, sections 1-8;
this plan, section 9): https://claude.ai/artifact/NsmuaUENPrr6MSzkZMzZH5 (private artifact page).

## 1. Direction

Keep today's model: **one `encapsulated_key` row per entity**, a grantee reads a shared resource
through the owner's key chain in the system context (`ShiroSecurityController.dataKey` →
`KeyMakerProvider.getKey(ds, msKey, ownerGUID, entityGUID)`). The change is in the **permission
layer only**: the share rows and the manager's rules. Nothing in the key maker, the controller,
`H2PFieldCrypto` or the datastore changes.

An alternative, "a key row per share" (a second key row for the grantee, Alpha's key sealed under
B's subject key), was fully discussed and **dropped**: a read costs the same either way, a share
would cost an unwrap + wrap + row, and the key maker / controller / key cache would all change.
It stays possible later for public-key (ML-KEM) sharing, because the master key can create those
rows at any time. The 2026-09-05 decision stands: sharing is never a second key row.

## 2. Rules decided by the user

A owns Alpha. A share = `PermissionGrant` with a `ResourceMap` (inlined `resource:<verbs>` token,
or a catalog permission scoped to the resource), `subject_guid` = grantee, `broker_guid` = who
issued it.

1. **One share per grantee per resource.** B has at most one share on Alpha.
2. **The owner changes a share in place** (for example `read` → `read,update`, or back). The
   grantee's cached permissions are evicted.
3. **An existing share is changed by the owner only.** If B already has a share on Alpha and C (a
   `share` holder) shares Alpha with B, C is refused. (Option 1; "C may add `share` to the existing
   share" was not taken.)
4. **Only a holder of `share` may share** (as today: `enforceGrantOnResource`).
5. **A sharer who is not the owner may give only `read` or `read,share`.** `update` and `delete`
   come only from the owner.
6. **Revoking a share revokes what it handed out**: the shares the grantee issued on that resource
   (`broker_guid` = grantee), recursively, in one transaction.
7. **Taking `share` away does the same**: when a change leaves the share without `share`, the
   shares that grantee issued on the resource are revoked, recursively. The grantee keeps its
   reduced share.
8. **No key is touched** on share, change or revoke.

Worked examples (A owner; A→B `read,update,share`; B→C `read,share`; C→D `read`):
- B→C `read,update` refused (B is not the owner).
- C→B anything: refused (B already has a share; only A changes it).
- A changes B to `read`: B's share updated in place; C's and D's shares revoked (B lost `share`).
- A revokes B: B's, then C's, then D's shares deleted, one transaction.

## 3. Where the code is

io-xlogistx repo, module `shiro` (artifact `xlogistx-shiro`, **Java 8 source level**), package
`io.xlogistx.shiro.ds`, file `shiro/src/main/java/io/xlogistx/shiro/ds/ShiroDSDomainSecurityManager.java`
(line numbers as read on 2026-10-06):
- `addPermissionGrant(subject, permissionInfo, ResourceMap)` ~2392 and
  `addPermissionGrant(subject, ResourceMap, token)` ~2408 — both call `loadResource`,
  `enforceGrantOnResource(res, inlined)` ~1602, then `insertGrant` ~2483 (sets `broker_guid` =
  bound subject, inserts in `inTransaction`, evicts the grantee). **No uniqueness check today.**
- `deletePermissionGrant` ~2426 → `enforceRevoke` ~1625 → `deleteGrantAndMap` ~2491
  (`deleteGrantRows`: grant row, then its map row).
- `getPermissionGrantsByResource(resourceGUID)` ~2453 (map rows by `resource_guid`, then grants
  `resource_map IN (...)`), `deletePermissionGrantsByResource` ~2465.
- `holdsResource(resource, verb)` ~1580: owner token then grant token against the bound subject.
- `currentSubjectGUID()`, `enforcePermissions` flag (default **off**; every `enforce…` is a no-op
  when off), `realm.evictAuthorization(guid)`.
- `GrantFlattener` (same package) composes `resource:<resource guid>:<grantee>:<verbs>` from the
  stored `resource:<verbs>` + `ResourceMap`; a grant with a `ResourceMap` applies in every login scope.
- zoxweb-core: `PermissionGrant` (`validateShape`, `permission_token` with
  `SecurityModel.ResourcePermissionTokenFilter` — verbs ⊆ {read, update, share, delete}),
  `ResourceMap`, `SecurityModel.toResourceToken`, `SecurityModel.SHARE`, interface
  `DomainSecurityManager` (declares the three `addPermissionGrant` forms, `deletePermissionGrant`,
  `getPermissionGrantsByResource`, `deletePermissionGrantsByResource`).
- Tests live in **this repo**: `h2p-datastore/src/test/java/io/xlogistx/shiro/ds/test/ShiroDSDomainSecurityManagerDBTest.java`
  (72 tests; the `share_*` tests cover today's behaviour), helpers `TestVault`, `SecurityCatalogDBTest`.

## 4. What to build

1. **Uniqueness** — before inserting a share (both `addPermissionGrant` forms with a `ResourceMap`):
   look for an existing grant with the same `subject_guid` and a map on the same `resource_guid`
   (reuse `mapGUIDsForResource` / `getPermissionGrantsByResource`, filter by grantee); if one
   exists → refuse (`IllegalArgumentException` or `AccessSecurityException`, message naming the
   existing share). Inside the transaction (h2p has no composite unique declaration).
2. **Change share** — a new manager method, e.g.
   `PermissionGrant updatePermissionGrant(PermissionGrant grant, String permissionToken)` (inlined
   shares; for a catalog-scoped share, replace the `permission_guid` the same way or refuse —
   see choices): reload by GUID, enforcement = caller must hold the owner token on the resource
   (`resource:<owner>:<caller>:share` through `holdsResource` with the owner check only — not a
   grantee's `share`), validate the token with the filter, update the row, evict the grantee; if the
   new token has no `share` → cascade (item 4) for that grantee.
3. **Verb limit for non-owners** — in `enforceGrantOnResource` (or beside it): when the caller is
   not the owner (owner token fails, grant token passes), the requested verbs must be ⊆ {read, share};
   else `AccessSecurityException`. For a catalog-scoped grant the token is the catalog row's
   `resource:<verbs>`.
4. **Cascade** — `revokeIssuedShares(resourceGUID, brokerGUID)`: every grant on the resource whose
   `broker_guid` = brokerGUID → for each: recurse with that grant's `subject_guid` as broker, then
   `deleteGrantRows`; collect the grantees and evict each. Called from `deletePermissionGrant`,
   `deletePermissionGrantsByResource` (already deletes all, so only eviction matters) and the
   change method when `share` is removed. All inside the one `inTransaction`.
5. **Core interface** — if the change method must be reachable through `DomainSecurityManager`
   (callers hold the core interface), add it there with a default that throws
   `UnsupportedOperationException`, like the other promoted methods; rebuild core with
   `-DskipTests -Dgpg.skip=true` and install.
6. **Docs** — `h2p-datastore/SHIRO-DS.md` (section "Instance grants and sharing" + a session-log
   entry), this file's status line, and section 9 of the artifact page (read it with the Artifact
   tool by URL, then republish).

### Choices taken on my own (state them; withdraw on a no)
- Uniqueness (1) and the cascade (4) are **structural** and apply whether or not
  `enforcePermissions` is on; the caller-based rules (3, owner-only change, `share` holder) follow
  the existing pattern and apply **under enforcement only** (with enforcement off nobody is bound
  and `broker_guid` is null, as today).
- The change method takes an inlined token; a catalog-scoped share is changed by giving another
  catalog permission of the same `resource:<verbs>` shape (or refused if that turns out awkward).
- Uniqueness is per (grantee, resource) across both share shapes (inlined and catalog-scoped).

## 5. Tests (user rules: production-like, through a keystore, nothing throw-away)

Add to `ShiroDSDomainSecurityManagerDBTest` (run with `setEnforcePermissions(true)` and logged-in
subjects, like the existing `share_*` tests):
- second share to the same grantee refused; owner changes verbs in place, grantee's permissions
  follow (isPermitted before/after); non-owner sharer: `read,share` ok, `read,update` refused,
  nothing without `share`; C→B refused when B's share exists; revoke A→B cascades to C and D (rows
  gone, `isPermitted` false for all three, one transaction); A reduces B to `read` → C and D gone,
  B still reads; no `encapsulated_key` row changes across all of it (count before/after).
- Clean up what the test creates on the shared database (the suite already deletes its test apps
  and subjects in `@AfterAll`).

Run order and commands (`.claude/test-runner/README.md`):
1. zoxweb-core (only if the interface changes): `mvn -o -DskipTests -Dgpg.skip=true install`.
2. io-xlogistx `shiro`: `mvn -o -pl shiro -Dmaven.test.skip=true install` (its `ShiroMetaModelTest`
   does not compile; that is known).
3. this repo: `mvn -o -pl h2p-datastore -DskipTests install`, then from the `h2p-datastore`
   directory: `java -ea:io.xlogistx... -ea:org.zoxweb... -cp "$(cat ../.claude/test-runner/cp-shiro.txt);../.claude/test-runner" -Dstore=<vault> -Dstore.password=<pw> RunTests io.xlogistx.shiro.ds.test.ShiroDSDomainSecurityManagerDBTest`
   with `src/test/resources/h2mem.store`, then `h2persist.store`, then `test.store` (PostgreSQL
   lax-2 `testdb`). **The vault password is not recorded anywhere: ask the user for it.**
4. Also rerun `SecurityCatalogDBTest` and `SubjectSwapSignUpDBTest` (the last needs the database
   set up by the tool first — see SHIRO-DS.md). Report pass counts per keystore; check the rows
   after the PostgreSQL run (no leftover shares from the new tests).

## 6. State of the working trees at the restart (2026-10-06)

- zoxweb-datastore: uncommitted, unrelated to this plan — `README.TXT` entry added to the zip
  dump (`H2PDumpRestore`, `H2PDataStore` Javadoc, `H2PDumpRestoreTest`, `CLAUDE.md`;
  `H2PDumpRestoreTest` 12/12 green on in-memory H2), plus the earlier untracked test keystores.
  Never commit without the user's explicit request.
- io-xlogistx and zoxweb-core: whatever the user's own uncommitted work is; do not touch it.
