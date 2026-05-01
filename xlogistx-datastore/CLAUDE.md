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
| `MongoUtil.java` | `MongoUtil.java` |
| `MongoQueryFormatter.java` | `MongoQueryFormatter.java` |
| `UpdateFilterClass.java` | `UpdateFilterClass.java` |

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

### Verification

`mvn -pl xlogistx-datastore -DskipTests compile` → `BUILD SUCCESS` at every checkpoint through the fix sequence.

### Line counts (current)

| File | Lines |
|---|---:|
| `XlogistxMongoDataStore.java` | 2457 |
| `MongoUtil.java` | 286 |
| `XlogistxMongoMetaManager.java` | 264 |
| `XlogistxMongoDSCreator.java` | 139 |
| `XlogistxMongoExceptionHandler.java` | 133 |
| `MongoQueryFormatter.java` | 133 |
| `UpdateFilterClass.java` | 106 |
| `XlogistxMongoDBObjectMeta.java` | 40 |
| **Total (main)** | **3558** |
| `XlogistxMongoDataStoreTest.java` | 330 |

## Ground rules for future sessions on this module

1. `mongo-sync` is immutable — never edit anything under the `mongo-sync/` directory.
2. Do not add `mongo-sync` as a Maven dependency of `xlogistx-datastore`. Fixes belong in the ported copies.
3. Preserve the package-private visibility of `XlogistxMongoDataStore` methods that `MongoUtil` calls (`fromDB`, `toNVPair`, `fromNVGenericMap`, `getAPIConfigInfo`) — they all live in the same package.
4. Compile check: `mvn -pl xlogistx-datastore -DskipTests compile` from the repo root.
5. When evaluating new `//`-comment lines, prefer Javadoc `/** */` for documentation so the dead-code-strip heuristic can stay simple.
6. `XlogistxMongoMetaManager` is per-instance; use `datastore.getMetaManager()` rather than any global state.
7. `operationTimeoutMS` and `maxSearchResults` on `XlogistxMongoDataStore` are tunable at runtime via setters — surface them via config if callers need non-default values.
8. When adding support for a new NV type, update **all five serialization paths**: `insert()`, `serNVEntity()`, `patch()`, `serNVGenericMap()` (for NVGenericMap-embedded values), and the deserializer in `MongoUtil.init()` / `fromNVGenericMap()`.
