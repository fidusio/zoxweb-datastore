# xlogistx-datastore — Claude Working Notes

Scope: this module only. `mongo-sync` is **reference-only and must not be modified**.

## What has been done

### Port of `SyncMongoDS` → `XlogistxMongoDataStore`

`mongo-sync` is not a compile dependency of this module, so the eight-file Mongo datastore was duplicated verbatim into `io.xlogistx.datastore` with mechanical package/class rewrites only. Behavioral fixes to the port are tracked in the next section; the verbatim port itself added no behavior changes.

Files created under `src/main/java/io/xlogistx/datastore/`:

| New file | Ported from (mongo-sync) |
|---|---|
| `XlogistxMongoDataStore.java` | `SyncMongoDS.java` (replaced the 11-line stub) |
| `XlogistxMongoDSCreator.java` | `SyncMongoDSCreator.java` |
| `XlogistxMongoExceptionHandler.java` | `SyncMongoExceptionHandler.java` |
| `XlogistxMongoMetaManager.java` | `SyncMongoMetaManager.java` |
| `XlogistxMongoDBObjectMeta.java` | `SyncMongoDBObjectMeta.java` |
| `XlogistxMongoUtil.java` | `MongoUtil.java` |
| `MongoQueryFormatter.java` | `MongoQueryFormatter.java` |
| `UpdateFilterClass.java` | `UpdateFilterClass.java` |

> **Note:** `MongoUtil` was subsequently renamed to `XlogistxMongoUtil`. Older rows below that say "MongoUtil" refer to this file.

Mechanical rewrites applied in each file:

- `package org.zoxweb.server.ds.mongo.sync;` → `package io.xlogistx.datastore;`
- Class renames: `SyncMongoDS` → `XlogistxMongoDataStore`, `SyncMongoDSCreator` → `XlogistxMongoDSCreator`, `SyncMongoMetaManager` → `XlogistxMongoMetaManager`, `SyncMongoExceptionHandler` → `XlogistxMongoExceptionHandler`, `SyncMongoDBObjectMeta` → `XlogistxMongoDBObjectMeta`
- `MongoUtil`, `MongoQueryFormatter`, `UpdateFilterClass` — package change only; `MongoUtil.DataDeserializer` parameter type rebound from `SyncMongoDS` to `XlogistxMongoDataStore`
- `XlogistxMongoDSCreator.API_NAME` changed from `"MongoDBSync"` to `"XlogistxMongoDS"` (and its description) so both providers can coexist on the classpath
- `@author mzebib` → `@author javaconsigliere@gmail.com` in `XlogistxMongoDataStore.java:67` (the only `@author` tag in the module)

No pom.xml changes. `mongodb-driver-sync` was already declared; everything else comes transitively from the parent `zoxweb-datastore` POM. **Do not add `mongo-sync` as a dependency.**

Verified: `mvn -pl xlogistx-datastore -DskipTests compile` → `BUILD SUCCESS`.

### Review findings — all fixed

The post-port review surfaced 37 issues. All have been addressed in the `xlogistx-datastore` module; `mongo-sync` retains its original (unfixed) copies.

| # | Category | Location | Fix applied |
|---|---|---|---|
| 1 | Critical | `XlogistxMongoDataStore.batchSearch` / `nextBatch` | `batchSearch` now stores UUIDs decoded from `_id` (was storing guid strings but casting to `List<UUID>` on read) |
| 2 | Critical | `XlogistxMongoDataStore.lookupByReferenceID(Document)` | Reads UUID from `ReservedID.REFERENCE_ID.getValue()` (`_id`), not `MetaToken.REFERENCE_ID.getName()` via `getObjectId` |
| 3 | Critical | `MongoQueryFormatter.map()` | `new ObjectId((String) val)` → `IDGs.UUIDV4.decode((String) val)` for ref-id query values (later migrated to `UUIDV7` — see below) |
| 4 | Critical | `deleteAll` + `patch()` | Both now use `nvce.toCanonicalID()` for collection / lookup, matching the rest of the codebase |
| 5 | Critical | `delete(NVConfigEntity, QueryMarker...)` | Rejects empty criteria; directs callers to `deleteAll` explicitly |
| 6 | Critical | `delete(V, boolean)` | Filters by `_id` (decoded from `getReferenceID()`), not by the unindexed `guid` field. Uses `toCanonicalID()` for collection |
| 7 | Critical | `localNextSequenceValue` | Replaced read-modify-write with `findOneAndUpdate` + `$inc`; atomic across JVMs/replicas. Default-increment path still needs one read to resolve the default |
| 8 | Critical | `MongoUtil.init()` | Removed the dead duplicate `String[].class` deserializer (second registration was always the effective one) |
| 9 | Critical | `MongoUtil.init()` | Removed `Date[].class` from the `NVLongList` lambda — Date arrays come back as `List<Date>`, not `List<Long>` |
| 10 | Critical | `MongoUtil.init()` | `doc.getBoolean(key)` → `doc.getBoolean(key, false)` for `_is_fixed` markers (fixes NPE on absent key) |
| 11 | Critical | `XlogistxMongoDataStore.close()` | Now also nulls `mongoDB` and `gridFSDB` so reconnect works |
| 12 | Robustness | `batchSearch` | Null-guarded `getSecurityController()` — if absent, the access check is skipped rather than NPE-ing |
| 13 | Robustness | `toCanonicalID()` | Returns `XlogistxMongoDSCreator.API_NAME + ":" + name` instead of `null` |
| 14 | Robustness | `isProviderActive()` / `isBusy()` | Reflect actual state (`mongoClient != null && mongoDB != null`, and the `updateLock` lock state respectively) |
| 15 | Perf | `updateMappedValue` | Routed from the N+1 `lookupByReferenceIDs(List<Document>)` to the batched `lookupByReferenceIDsMaybe(...)` and then deleted the N+1 method |
| 16 | Robustness | `patch()` | Uses `Filters.eq("_id", refIdUUID)` as the update filter — no longer passes the entire original document |
| 18 | Robustness | `ping()` | `APIException` now carries the underlying cause via `initCause` |
| 19 | Robustness | `fromDB` | `printStackTrace` → `log.getLogger().log(WARNING, ..., e)` for `ClassNotFoundException` |
| 20 | Robustness | `MongoUtil.bsonNVEGUID` | `UUID.fromString(...)` → `IDGs.UUIDV4.decode(...)` to match the rest of the codebase (later migrated to `UUIDV7` — see below) |
| 21 | Robustness | `createFile()` | Metadata update failure rolls back the GridFS upload via `bucket.delete(...)`; rollback errors are logged |
| 22 | Perf | `XlogistxMongoMetaManager` | Backed by `ConcurrentHashMap`; reads (`isIndexed`, etc.) are lock-free |
| 23 | Perf | `XlogistxMongoMetaManager.addCollectionInfo` | No longer holds a class-wide lock during `createIndex` I/O; uses `putIfAbsent` semantics |
| 24 | Perf | `XlogistxMongoMetaManager.addNVConfigEntity` | Caches resolved `NVConfigEntity` per canonical id — eliminates the per-write Mongo round trip |
| 25 | Perf | `UpdateFilterClass.isValid(Class)` | Fast-path: classes not in the blacklist never call `SecurityUtils.getSubject()`. Shiro is only consulted when overriding an actual blacklist hit |
| 26 | Robustness | `XlogistxMongoDataStore` | Added `operationTimeoutMS` (default 30s) applied via `withTimeout(findIterable)` on `lookupByReferenceID`, `lookupByReferenceIDs`, `userSearch`, `batchSearch`, `getAllDynamicEnumMap` |
| 27 | Perf | `userSearch`, `batchSearch` | Added `maxSearchResults` cap (default 10k) via `.limit(...)`; warning logged when hit |
| 28 | Hygiene | throughout | `printStackTrace` in hot paths replaced with `log.getLogger().log(WARNING, ..., e)` where relevant |
| 29 | Perf | `connect()` | Dynamic enum maps are now loaded lazily on first use, not eagerly during first connect |
| 30 | Correctness | `MongoQueryFormatter.formatQuery` | Rewritten with SQL-style precedence — AND binds tighter than OR. `A OR B AND C` is now `A OR (B AND C)`. Old pairwise behavior removed |
| 31 | Design | `XlogistxMongoMetaManager` | `SINGLETON` removed; each `XlogistxMongoDataStore` owns its own manager via `getMetaManager()`. Multiple datastores per JVM are now supported |
| 32 | Design | `UpdateFilterClass` | Blacklist is now a mutable `ConcurrentHashMap`-backed set with `addExcluded` / `removeExcluded` / `getExcluded`. Default entries preserved |
| 33 | Hygiene | `XlogistxMongoDataStore.java`, `MongoUtil.java` | ~560 lines of commented-out code removed across the module (main file shrank 2792 → 2421 lines; MongoUtil 388 → 276 lines) |
| 34 | Hygiene | `XlogistxMongoDataStore.java:2571` (pre-strip) | `new Long(...)` → `Long.valueOf(...)` |
| 35 | Hygiene | `XlogistxMongoDataStore.java:2641` (pre-strip) | `clazz.newInstance()` → `clazz.getDeclaredConstructor().newInstance()` |
| 36 | Correctness | `getAllDynamicEnumMap` | Non-null `domainID` / `userID` now add `domain_id` / `subject_guid` filters to the query |

#### Deferred (explicitly left as-is at user's request)

- **37** `MongoUtil.idAsGUID(NVEntity)` reads `getReferenceID()` despite its name. Works because `insert()` sets guid == referenceID. Revisit only if they diverge.

### NV type coverage — full support for all `org.zoxweb.shared` types

An audit of all NVEntity classes and NVBase types in `org.zoxweb.shared.util` and `org.zoxweb.shared.data` revealed 5 gaps in serialization/deserialization. All have been fixed.

| # | Category | Location | Fix applied |
|---|---|---|---|
| 38 | Critical | `insert()`, `patch()` | **NVGenericMapList** — serialization body was empty (silent data loss). Now serializes as `List<Document>` via `serNVGenericMap()` for each entry |
| 39 | Critical | `MongoUtil.init()` | **NVGenericMapList** — no deserialization handler existed. Added `NVGenericMapList.class` deserializer that reads `List<Document>` and converts each via `fromNVGenericMap()` |
| 40 | Moderate | `serNVEntity()` | Missing `NVStringList`, `NVStringSet`, `NVGenericMap`, `NVGenericMapList` branches. These types fell through to the default path, which would serialize `NVGenericMap` as a raw Java map instead of a proper BSON Document. Added explicit branches matching `insert()` and `patch()` |
| 41 | Moderate | `fromNVGenericMap()` | `List<Document>` values (NVGenericMapList entries nested inside NVGenericMap) were silently dropped during deserialization. Added `value instanceof List` handling before the `Document` check |
| 42 | Moderate | `serNVGenericMap()` | NVGenericMapList entries within an NVGenericMap were silently dropped during serialization. Added `gnv instanceof NVGenericMapList` branch that serializes each map via `serNVGenericMap()` |

#### NV types now fully covered

**Scalar:** NVPair, NVInt, NVLong, NVDouble, NVFloat, NVBoolean, NVNumber, NVBigDecimal, NVBlob, NVEnum, NamedValue

**List/Set:** NVStringList, NVStringSet, NVIntList, NVLongList, NVDoubleList, NVFloatList, NVBigDecimalList, NVEnumList, NVPairList, NVGetNameValueList, NVPairGetNameMap

**Entity references:** NVEntityReference, NVEntityReferenceList, NVEntityGetNameMap, NVEntityReferenceIDMap

**Generic containers:** NVGenericMap, NVGenericMapList

**All 48 DAO classes** (UserInfoDAO, AddressDAO, PropertyDAO, CreditCardDAO, Range, etc.) work through the generic NVConfigEntity reflection mechanism — no per-DAO handling needed.

#### Test coverage added

Three new test methods in `XlogistxMongoDataStoreTest.java`:

| Test | What it covers |
|---|---|
| `testNVGenericMapList()` | NVGenericMapList insert + read-back round-trip inside PropertyDAO's NVGenericMap properties |
| `testNVGenericMapListUpdate()` | NVGenericMapList mutation followed by `update()`, verifying the patch path |
| `testNVGenericMapDiverseTypes()` | NVGenericMap with mixed types: String, Int, Long, Double, Float, nested NVGenericMap, NVGenericMapList, NVStringList — full round-trip |

### ID generator migration (UUIDV4 → UUIDV7)

Project-wide swap of `IDGs.UUIDV4` → `IDGs.UUIDV7` across all modules (8 files, 50 call sites). In this module: `XlogistxMongoDataStore.java` (19), `MongoUtil.java` (3), `MongoQueryFormatter.java` (2).

Both generators implement the same `IDGenerator<String, UUID>` contract (`encode`, `decode`, `isValid`, `genID`), so call sites are unchanged.

Implications:
- New `_id` / `referenceID` / `guid` values are now **time-ordered** (UUID v7 embeds a millisecond timestamp). This improves Mongo `_id` B-tree index locality and makes range scans by insertion time meaningful.
- Existing v4 records decode correctly — both formats are standard 128-bit UUIDs.
- `IDGs.UUIDV7.isValid(refID)` validates v7 specifically; legacy v4 strings written before the migration may need a transitional check if strict validation is enforced. Verify behavior of the shared `IDGs.UUIDV7.isValid(...)` implementation before relying on it for mixed v4/v7 data.

### Configuration change

`XlogistxMongoDSCreator.MongoParam.DB_URI` was removed. Connection is now configured via `DB_NAME` + `HOST` + `PORT` components, with `dataStoreURI()` building the full URI string. Test database changed from `xlogistx_ds_test` to `DB_TEST`.

### GUID canonicalization + hygiene pass (2026-06-30)

`referenceID` is deprecated in favor of `GUID`. This pass made `GUID` the single source of truth for the persisted `_id` and cleaned up leftover debug/logging.

| # | Category | Location | Fix applied |
|---|---|---|---|
| 43 | Critical | `insert()` | `_id` now decoded from `nve.getGUID()` (always populated) instead of the possibly-null `referenceID`. Previously an entity with a null `referenceID` got a Mongo-assigned `ObjectId` `_id` while GridFS keyed off `GUID` → `lookupByReferenceID` could never find it |
| 44 | Correctness | `serNVEntity()`, `patch()`, `delete(V, boolean)` | All ID derivation (`_id` append, update filter, delete filter, patch reload) switched from `referenceID` to `GUID` for consistency with the new invariant |
| 45 | Robustness | `updateDynamicEnumMap()` | Update filter changed from passing the entire original document to `Filters.eq(name, ...)` — DEMs are uniquely keyed by name |
| 46 | Hygiene | `insert()` | Removed a debug `System.out.println(doc)` that printed every inserted document to stdout |
| 47 | Robustness | `fromNVGenericMap()` | `ClassNotFoundException` on an embedded class now logs and `continue`s instead of falling through to an NPE on `subClass.isEnum()` |
| 48 | Hygiene | throughout | Remaining `printStackTrace()` calls replaced with `log ... WARNING` (`toNVPair`, `fromNVGenericMap`, `userSearchByID`, `nextBatch`, `newConnection`, `userSearch(className)`, `XlogistxMongoDSCreator`). `newConnection` / `userSearch` now also chain the cause via `initCause` |
| 49 | Hygiene | `XlogistxMongoDSCreator` | Deprecated `Class.forName(...).newInstance()` → `getDeclaredConstructor().newInstance()`; added a `LogWrapper` |
| 50 | Hygiene | `XlogistxMongoDataStore` | Removed the dead `getDBAddress()` method (connection goes through the URI string) and its now-unused `ServerAddress` / `UnknownHostException` imports |
| 51 | Critical | `insertDynamicEnumMap()` | Now writes `_id` as a native UUID (reusing the DEM's GUID or generating one via `genNativeID()`). The old code wrote to a `"guid"` field keyed off `referenceID`, leaving `_id` as a Mongo `ObjectId` — so the immediately-following `getRefIDAsUUID(doc)` (which reads `_id` as a `UUID`) threw for every fresh DEM |

### Reserved-ID UUID storage invariant (2026-07-02)

Invariant: **every `ReservedID` field and every `isTypeReferenceID` attribute is persisted in Mongo as a native UUID and surfaced on the java side as its `IDGs.UUIDV7` string encoding.** Audit found this was not implemented — zoxweb-core's `ReferenceIDDAO` NVConfigs (`NVC_SUBJECT_GUID`, `NVC_GUID`) are NOT flagged `isRefID`, so `subject_guid`/`broker_guid` were silently stored as raw Strings, and truly-flagged ref-ID fields were written under `"_" + name` but read from `name` (data loss on read).

| # | Category | Location | Fix applied |
|---|---|---|---|
| 52 | Critical | `XlogistxMongoUtil.ReservedID` | Added `isUUIDField(NVConfig)` — true when the attribute is a ReservedID member OR `isTypeReferenceID`. Membership in the enum is what makes reserved names UUID-stored (their zoxweb-core NVConfigs are not ref-ID-flagged) |
| 53 | Critical | `insert()`, `serNVEntity()`, `patch()` | Write gate widened from `nvc.isTypeReferenceID()` to `ReservedID.isUUIDField(nvc)` — `subject_guid`/`broker_guid`/etc. now persist as native UUIDs |
| 54 | Critical | `updateMappedValue()` | New branch for non-reserved ref-ID fields: reads via `ReservedID.map()` (the `"_"`-prefixed key the writers use) and re-encodes UUID→String. Previously these fields were never read back |
| 55 | Correctness | `MongoQueryFormatter.map()` | String query values decode to UUID for any `isUUIDField` attribute (was: only `isTypeReferenceID`); dotted-name decode extended from `== ReservedID.GUID` to any ReservedID leaf |

The read path tolerates legacy String-stored values (`instanceof UUID` / `instanceof String` branches), so pre-fix documents still load. **Caveat: pre-fix documents with String `subject_guid` will NOT match post-fix UUID queries** — needs a one-off migration if querying legacy data by reserved IDs.

`ReservedID` was also extended with `PERMISSION_GUID`, `ROLE_GROUP_GUID`, `ROLE_GUID` (and `BROCKER_GUID` typo fixed to `BROKER_GUID`); `isUUIDField` covers new members automatically.

Test: `testReservedIDUUIDInvariant()` in `XlogistxMongoDataStoreTest` — asserts the raw Mongo document stores `_id` and `subject_guid` as native `java.util.UUID`, the entity round-trip returns the UUIDV7 string, and a String `subject_guid` query criterion matches.

### Reference sub-document guid read fix (2026-07-02)

Reference *sub-documents* (`serNVEntityReference`, `serNVPair` DEM branch) store the target id under the `"guid"` key, but three readers extracted it via `ReservedID.GUID.getValue()` (`"_id"`) — a port artifact of GUID's remap from `"guid"` to `"_id"`, which is only correct for top-level documents. Result: `NVEntityReferenceList`/`GetNameMap`/`ReferenceIDMap` read back empty (silently), and non-embedded `NVEntityReference` / NVGenericMap-embedded entities / DEM value filters NPE'd on read.

| # | Category | Location | Fix applied |
|---|---|---|---|
| 56 | Critical | `lookupByReferenceIDsMaybe`, `lookupByReferenceID(Document)`, `getValueFilter` | Read the ref UUID from `MetaToken.GUID.getName()` (`"guid"`) instead of `ReservedID.GUID.getValue()` (`"_id"`). No data migration needed — writers always emitted `"guid"` |

**Rule of thumb:** `ReservedID.GUID.getValue()` / `getRefIDAsUUID()` are for **top-level** documents only; reference **sub-documents** key the target id under `MetaToken.GUID.getName()`.

Test: `testEntityReferenceRoundTrip()` in `XlogistxMongoDataStoreTest` — `DSConst.ComplexTypes` with a non-embedded `NVEntityReference` (single, via `lookupByReferenceID(Document)`) and an `NVEntityReferenceList` of 3 (batched, via `lookupByReferenceIDsMaybe`); asserts every reference resolves with matching GUIDs after read-back.

### Running tests in this environment

`mvn test` silently skips: surefire can't download its JUnit provider (PKIX/TLS failure to Maven Central; local repo is `D:/dev/data/java/.m2/repository`). Workaround: compile with `mvn -o test-compile`, then run via `junit-platform-launcher-6.0.1` (present in the local repo) with the classpath from `mvn -o dependency:build-classpath`. Tests need the live replica set `mongodb://localhost:27017/?replicaSet=rs0`.

### Verification

`mvn -pl xlogistx-datastore -DskipTests compile` → `BUILD SUCCESS` at every checkpoint through the fix sequence.

### Line counts (current)

| File | Lines |
|---|---:|
| `XlogistxMongoDataStore.java` | ~2450 |
| `XlogistxMongoUtil.java` | 300 |
| `XlogistxMongoMetaManager.java` | 264 |
| `XlogistxMongoDSCreator.java` | 139 |
| `XlogistxMongoExceptionHandler.java` | 133 |
| `MongoQueryFormatter.java` | 133 |
| `UpdateFilterClass.java` | 106 |
| `XlogistxMongoDBObjectMeta.java` | 40 |
| **Total (main)** | **3558** |
| `XlogistxMongoDataStoreTest.java` | 330 |

### MongoDB transaction support

`APIDataStore` gained two `default` hooks — `<T> T beginTransaction()` and `void endTransaction()` — implemented in `XlogistxMongoDataStore`.

Design (matches the "ambient session" approach):
- A `ThreadLocal<ClientSession> txSession` binds the active transaction to the calling thread.
- `beginTransaction()` starts a `ClientSession` + `startTransaction()`, stores it, and returns the session as `T`. Rejects nesting (`IllegalStateException` if one is already active).
- `endTransaction()` commits (aborts + rethrows on commit failure); always closes the session and clears the ThreadLocal.
- `abortTransaction()` (module-local — the interface has no rollback) rolls back. Usage: `begin` → work in `try` → `endTransaction()` to commit / `abortTransaction()` in `catch`.
- Every data op routes through null-safe helpers (`sInsertOne`, `sUpdateOne`, `sDeleteOne`, `sDeleteMany`, `sCountDocuments`, `sFind`): **session active → op joins the txn; session null → normal auto-committed op (no behavior change).** Never pass a null session to a session overload — always branch.

Intentionally **outside** any transaction:
- **Schema/metadata** — `XlogistxMongoMetaManager` (collection registration, `createIndex`, `nv_config_entities`) is left non-transactional. Avoids illegal in-transaction DDL and is idempotent. First-ever use of an entity type still triggers index creation, so **pre-warm each type outside a txn** (insert one instance) before relying on transactions for it.
- **`LongSequence`** (`localNextSequenceValue` / `findOneAndUpdate`) — stays outside the ambient txn so a rollback doesn't un-consume a sequence value.
- **GridFS** (`createFile`/`readFile`/`deleteFile`) — out of scope for v1; still non-transactional.

Caveats:
- Requires a **replica set / mongos** — a standalone `mongod` cannot run transactions (commit throws). Enable it via the connection URL: `XlogistxMongoDSCreator` now preserves the URL query string into a new `MongoParam.OPTIONS` and `dataStoreURI()` re-applies it (merged with the forced `uuidRepresentation=standard`). Tests use `?replicaSet=rs0`. Previously the query was silently dropped because `dataStoreURI()` rebuilt the URI from `HOST`/`PORT`/`DB_NAME` only.
- `dataCacheMonitor` CRUD notifications currently fire before commit; on rollback the cache can be stale (buffering-until-commit is a possible follow-up).
- A `ClientSession` is not thread-safe — do not cross threads inside a transaction (note `lookupProperty(ASYNC_CREATE)` returns TRUE).

Caller-driven usage: `begin` … work … `end` (commit) or `abort` (rollback), on the **same thread**, always paired (`try`/`finally`). The datastore's CRUD methods never begin/end a transaction themselves — they just join the ambient one if present. `endTransaction()` commits once and, on commit failure, aborts and rethrows (no retry — atomic all-or-nothing by design).

#### Transaction test coverage

Tests require a replica set; the test URLs use `?replicaSet=rs0`.

| Test | File | What it covers |
|---|---|---|
| `testTransactionCommit()` | `XlogistxMongoDataStoreTest` | Pre-warm → `begin` → two inserts → read-your-own-writes visible inside the txn → `endTransaction()` (commit) → both rows persist |
| `testTransactionRollback()` | `XlogistxMongoDataStoreTest` | `begin` → two inserts (visible inside) → `abortTransaction()` → neither row persists |
| `createSubject_partialFailure_rollsBackAtomically()` | `DomainSecurityManagerDBTest` | `createSubjectID` inserts subject + principal, then `createCredential` throws (non-`NVEntity` credential); asserts the whole unit of work rolled back — no subject, principal, or credential survives |

`DomainSecurityManagerDBTest` also covers subject create/lookup, login success/failure, credential lookup, and permission/role/role-group grant round-trips (mirrors the mock-backed `DomainSecurityManagerDefaultTest`, but against live Mongo). `roleGroupGrant_roundTripsThroughDataStore` additionally exercises the reference sub-document resolution path (fix #56): `RoleGroupInfo.roles` is a GET_NAME_MAP of non-embedded `RoleInfo` references resolved via `lookupByReferenceIDsMaybe` on read-back. All tests use unique UUID-suffixed principals/names and delete nothing, so they are safe to re-run against a persistent DB.

## Ground rules for future sessions on this module

1. `mongo-sync` is immutable — never edit anything under the `mongo-sync/` directory.
2. Do not add `mongo-sync` as a Maven dependency of `xlogistx-datastore`. Fixes belong in the ported copies.
3. Preserve the package-private visibility of `XlogistxMongoDataStore` methods that `XlogistxMongoUtil` calls (`fromDB`, `toNVPair`, `fromNVGenericMap`, `getAPIConfigInfo`) — they all live in the same package.
4. Compile check: `mvn -pl xlogistx-datastore -DskipTests compile` from the repo root.
5. When evaluating new `//`-comment lines, prefer Javadoc `/** */` for documentation so the dead-code-strip heuristic can stay simple.
6. `XlogistxMongoMetaManager` is per-instance; use `datastore.getMetaManager()` rather than any global state.
7. `operationTimeoutMS` and `maxSearchResults` on `XlogistxMongoDataStore` are tunable at runtime via setters — surface them via config if callers need non-default values.
8. When adding support for a new NV type, update **all five serialization paths**: `insert()`, `serNVEntity()`, `patch()`, `serNVGenericMap()` (for NVGenericMap-embedded values), and the deserializer in `XlogistxMongoUtil.init()` / `fromNVGenericMap()`.
9. **`_id` is keyed off `GUID`, not the deprecated `referenceID`.** All ID-bearing paths (`insert`, `serNVEntity`, `patch`, `delete`, GridFS helpers) decode `_id` from `nve.getGUID()`. `referenceID` is legacy — do not reintroduce it into the persisted `_id` / lookup logic.
10. **Transactions route through the ambient `ThreadLocal<ClientSession>` — never call the Mongo driver's `insertOne`/`find`/etc. directly.** Use the `s*` helpers (`sInsertOne`, `sFind`, …) so new call sites join an active transaction; a direct driver call silently runs outside the txn. Schema/metadata (`XlogistxMongoMetaManager` DDL, `nv_config_entities`), `LongSequence` (`findOneAndUpdate`), and GridFS are intentionally non-transactional — keep them that way. No commit retry (atomic all-or-nothing).
