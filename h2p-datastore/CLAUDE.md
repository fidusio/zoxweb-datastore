# h2p-datastore — Claude Working Notes

> **Since 2026-10-05 this module also holds what was the `xlogistx-shiro-ds` module** (removed on
> the user's instruction): the security integration tests in
> `src/test/java/io/xlogistx/shiro/ds/test/` (`SecurityCatalogDBTest`,
> `ShiroDSDomainSecurityManagerDBTest`, `SubjectSwapSignUpDBTest`, `TestVault`), the test keystores
> `test.store` / `h2mem.store` / `h2persist.store`, the persistent encrypted H2 file and
> `shiro-ds.ini` in `src/test/resources/`, and the documents `SHIRO-DS.md` (that module's notes and
> session log — read it for anything about the Shiro manager, realm, admin tool or these tests),
> `app-model.md`, `datastore-acl.md` and `no-sneak-plan.md` beside this file. The classes under test
> live in io-xlogistx (`shiro` and `opsec` modules). Older text that names
> `xlogistx-shiro-ds/src/main/resources/test.store` means `h2p-datastore/src/test/resources/test.store`.

Scope: this module only (`io.xlogistx.datastore.h2p`). It is a **normalized, relational**
implementation of zoxweb's `APIDataStore<Connection, Connection>` that runs on **both** H2 (in
PostgreSQL compatibility mode) **and** a native PostgreSQL server — the same code, swapping only the
JDBC driver + URL.

## Files

| File | Role |
|---|---|
| `H2PDataStore.java` | The datastore — DDL, CRUD, search, references, transactions, sequences, DEM, **versioned file storage** (implements `APIDataStore` **and** `APIDocumentStore`, like `XlogistxMongoDataStore`) |
| `H2PDSCreator.java` | Factory + `H2PParam` config enum + URL/DSType resolution |
| `H2PUtil.java` | Attribute classification (`AttrKind`) + column-type mapping + identifier quoting + `parseJdbcURL` |
| `H2PQueryFormatter.java` | `QueryMarker` → `WHERE` clause + parameter binding (`QueryMatch` incl. `LIKE`/`NOT_LIKE`, `QueryMatchIn`, `QueryGroup` parens; unknown markers REJECTED) |
| `H2PExceptionHandler.java` | SQLState → `APIException` mapping |
| `H2PMetaManager.java` | Per-instance table registry (case-insensitive; backs `getStoreTables()`; cleared on reconfigure) |
| `H2PDialect.java` | **Dialect codec** for schemaless columns (H2 `varchar` vs Postgres `jsonb`) |
| `H2PDumpRestore.java` | **JSON dump/restore engine** (JSONL) behind `H2PDataStore.dump(...)`/`restore(...)` — see the dedicated section |
| `H2PFieldCrypto.java` | **Encryption at rest** (package-private): ENCRYPT* field records via the `SecurityController`, AESCrypt VX file content, entity-key lifecycle on the `KeyMaker` chain, raw mode for dumps — see the dedicated section |

## Storage model (fully normalized — no binary blobs)

One table per `NVConfigEntity` type (table name = `nvce.getName()`). Every row has
`guid uuid PRIMARY KEY` (UUID v7). Each attribute maps by kind via `H2PUtil.classify(NVConfig)`:

| `AttrKind` | Storage |
|---|---|
| `SCALAR` | typed column — `varchar`/`integer`/`bigint`/`real`/`double precision`/`boolean`; **any attribute whose name ends in `guid`** (`H2PUtil.GUID_SUFFIX`, case-insensitive; user rule 2026-09-15, replaces the old reserved-name set) or flagged reference-id → `uuid` (values must be UUID strings, empty binds NULL); `Enum` → `varchar` (name); `Date` → `bigint`; `NVNumber` → `varchar` with a `int:/long:/float:/double:/bigdec:` type tag |
| `BLOB` | `bytea` column (`byte[]` field data) |
| `ENTITY_REF` (single `NVEntityReference`) | `uuid` column **+ `FOREIGN KEY` → child type's table** |
| `ENTITY_COLLECTION` (`NVEntityReferenceList`/`GetNameMap`/`ReferenceIDMap`) | **join table** `<table>__<attr>(parent_guid, child_guid, ord)` with FK constraints + `ON DELETE CASCADE` |
| `SCHEMALESS` (`NVGenericMap`, `NamedValue`, `NVStringList`, `NVIntList`, `NVEnumList`, …) | a JSON column — **`varchar` on H2, native `jsonb` on Postgres** (see dialect below) |

Referenced entities are stored as **their own rows** and resolved on read (`insert` is post-order:
children first so FK targets exist; read resolves single refs via `searchByID` and collections via
the join table). Referential integrity is DB-enforced. There is **no binary serialization** of
entities.

**`sys_meta_catalog`** (`table_name` PK → `class_type`) is the persistent type catalog: upserted
(best-effort, own auto-commit connection, guarded by the `catalogSynced` set) whenever `ensureTable`
creates a table or `tableExists` first confirms one. `discoverStoreTypes()` returns the session
registry ∪ catalog rows resolved via `resolveNVCE` — this is what lets a **fresh JVM** enumerate
every stored type (the whole-store `dump` depends on it; `H2PMetaManager` only knows types touched
this session). Databases created before the catalog existed only have rows for types touched since.

`ensureTable` also emits `CREATE INDEX IF NOT EXISTS` (portable to both engines) via `createIndex`:
join tables get `(parent_guid, ord)` + `(child_guid)`, `ENTITY_REF` columns get one, and non-unique
uuid scalars (every `*guid` attribute, reference ids) get one. **A FOREIGN KEY indexes only the referenced
side** — on PostgreSQL *and* H2 the referencing column needs its own index or every collection read
and cascade delete is a full scan.

**Identifier policy** (two tiers):
- **User-controlled names** (entity type + attribute names) are **validated, never mangled**:
  `H2PUtil.checkNameForSQL` (called once per type from `attrInfos`, the choke point of every
  read/write path) requires printable **ASCII, ≤ 63 bytes** (PostgreSQL's identifier limit — the
  lowest mainstream engine, and the only one that silently *truncates* instead of erroring) and
  rejects violations with an actionable `IllegalArgumentException` — the caller fixes the name at
  its source. ASCII also guarantees chars == UTF-8 bytes, so identifier math stays exact.
  (A matching check in zoxweb-core's `NVConfigPortable.setName` was tried and reverted — it broke
  too many existing name usages — so this gate is the sole enforcement; do not remove it.)
- **Composed identifiers** the datastore builds itself (join tables `<table>__<attr>`, FK constraint
  and index names) go through `H2PUtil.sqlName`, which keeps names ≤ 63 bytes by truncating +
  suffixing a CRC32 of the full name — deterministic and collision-resistant where server-side
  truncation isn't (the caller can't shorten these, so hashing is correct here).

### Schema evolution (additive sync + type gate)

The first touch of a type (write path `ensureTable`, or read path `tableExists` on first sight of a
pre-existing table — both funnel into `ensureTable`, guarded by `createdTables` + `ddlLock`, so it
runs **once per type per store instance** — `createdTables` is an instance field, read
2026-10-05; reconfigure clears the guard) makes ONE
`INFORMATION_SCHEMA.COLUMNS` probe (`readColumnTypes`) and branches:

- **Table absent** → normal `CREATE TABLE` (column types via `columnDDLType`, the single mapping
  create and sync share).
- **Table pre-existing** → `syncExistingTable`:
  - **Added attribute** → `ALTER TABLE ADD COLUMN IF NOT EXISTS` (nullable ⇒ old rows read the
    attribute at its default — document-store missing-key semantics; new rows round-trip). A unique
    scalar gets a best-effort UNIQUE constraint (duplicate pre-existing data logs a warning). Added
    collections / FK refs / indexes are covered by the idempotent FK/join/index DDL that follows.
  - **Deleted attribute** → its column / join table stays as dead weight, ignored by reads and
    writes. Never dropped automatically.
  - **Changed column type** → `APIException` "SCHEMA TYPE MISMATCH" (never auto-`ALTER`ed — that is
    data-destructive). Sanctioned migration: dump with the old entity classes, restore with the new
    (`H2PDumpRestore`), or revert the attribute type. A same-named table without a `guid uuid`
    column is likewise rejected (not ours).
- Type comparison goes through `H2PUtil.normalizeSqlType`, which unifies engine spellings
  (H2 `CHARACTER VARYING`/`BINARY VARYING` vs PG `character varying`/`bytea`, `int4/int8`,
  `float4/float8`, `bool`, `text`≡`varchar`, `json`≡`jsonb`).
- After the sync the caches are updated as one unit: `metaManager.register`, `createdTables`,
  `sys_meta_catalog` (`class_type` upserts to the latest class). A mismatch throws BEFORE any cache
  is touched, so the type stays unregistered and rejects again on the next touch.
- Regression: `H2PRegressionTest.testSchemaEvolution` (V1 → V2 adds a column across store
  reopens on one DB; a String→Long change is rejected).
- **"Constraint already exists" at start-up is by design (user, 2026-10-05: "that was the design
  requirement which is ok no need to change").** `ensureTable` issues `ALTER TABLE … ADD CONSTRAINT
  fk_…` for every entity reference without checking for the constraint, and `execDDLQuiet` ignores
  the duplicate error (H2 `90045`, PostgreSQL `42710`). On a database that already has the foreign
  keys, each new store instance therefore gets one rejected statement per foreign key, on the
  first use of the type and never again (not per transaction). H2 writes each one to its
  `<db>.trace.db` file; with PostgreSQL the client shows nothing. Seen in the run of 2026-10-05 on
  the encrypted H2 file of `xlogistx-shiro-ds` (nine foreign keys, once per test process, in the
  first two seconds of each); the PostgreSQL server log was not looked at. Do not report these as
  a defect.

### Schemaless JSON

Produced uniformly by `GSONUtil.toJSONDefault(nvb)` / `fromJSONDefault(json, targetClass)` — the
NV-aware `NVGenericMapSerDeserializer` emits clean plain JSON (e.g. `{"user":"mario"}`) and serializes
enums-inside-maps by name (no Gson reflection crash). Special cases kept in `H2PDataStore`:
`encodeSchemaless`/`decodeSchemaless`:
- **top-level `NVEnumList`** → stored as a JSON array of enum names, rebuilt via the enum class from
  `NVConfig.getMetaTypeBase()` (`GSONUtil.toJSONDefault(NVEnumList)` fails on Gson enum reflection).
- **`NamedValue`** → its inner `properties` map name is restored on read (JSON doesn't encode a nested
  map's own name) so the value re-serializes cleanly.

Fidelity is at the **JSON level** (re-serializing a read-back value yields the same JSON). JSON can't
distinguish `long` from `int` or `NVGenericMapList` from `NVPairList`, so the schemaless tests assert
JSON-stability, not exact NV subtypes.

## Dual-target dialect (H2 vs native PostgreSQL)

The datastore resolves its engine once, at creation, and holds it:
- `H2PDataStore.currentDSType` (`APIDataStore.DSType`) — set in `setAPIConfigInfo` via
  `H2PDSCreator.resolveDSType(config)`; returned by the overridden `getDSType()`.
- `H2PDataStore.dialect` (`H2PDialect`) — `H2PDialect.forDSType(currentDSType)`.

**Auto-detection** (`H2PDSCreator.resolveDSType` / `H2PParam.isPostgres`): a `jdbc:postgresql` URL or
the `org.postgresql.Driver` driver ⇒ `POSTGRES`; a `jdbc:h2` URL or the H2 driver ⇒ `H2`.

The dialect governs **only schemaless columns** — everything else (uuid, bytea, typed scalars, FK +
join tables) is identical on both engines:

| | H2 | PostgreSQL |
|---|---|---|
| schemaless column DDL | `varchar` | `jsonb` |
| write bind | `setString(json)` | `PGobject(type="jsonb", value=json)` via `setObject` |
| read normalize | column is `String` | column is `org.postgresql.util.PGobject` → `.getValue()` |

`PGobject` is imported only in `H2PDialect` (postgres is a compile dependency). Because jsonb
normalizes key order/whitespace, schemaless round-trips on Postgres are asserted by **semantic value**,
not raw-JSON-string equality.

### URL building (`H2PParam.dataStoreURI`)
- A full `url` param wins verbatim (either engine).
- Postgres (by driver/url): `jdbc:postgresql://host:port/db[?raw-options]` — **no** H2-only settings
  (`MODE`/`CIPHER`/`IF_EXISTS`/`AUTO_SERVER`/`DB_CLOSE_DELAY`).
- H2 (default): `jdbc:h2:mem|file|tcp:…` per `TYPE`, with `;MODE=PostgreSQL` + optional settings.
  Both `mem` and `file` append `;DB_CLOSE_DELAY=-1` so the DB stays open across connections for the
  JVM lifetime — without it, `file` mode (connection-per-op) closes + reopens the DB file every op.
- `dataStorePassword`: for an **encrypted** H2 DB (`CIPHER` set) H2 wants both secrets in one
  space-separated value, so it returns `filePwd + " " + pwd` (`FILE_PASSWORD` = file-encryption
  password, `PASSWORD` = user password). Without `CIPHER` (not encrypted) it returns the plain user
  password — a stray `FILE_PASSWORD` is ignored. Postgres always returns the plain password.

## Configuration

`H2PParam` (in `H2PDSCreator`): `DRIVER`, `URL`, `TYPE` (mem/file/tcp), `HOST`, `PORT`, `PATH`,
`DB_NAME`, `USER`, `PASSWORD`, `MODE` (H2 SQL compat, default `PostgreSQL`), `CIPHER`,
`FILE_PASSWORD`, `IF_EXISTS`, `AUTO_SERVER`, `OPTIONS`, `POOL_MAX_SIZE`/`POOL_MIN_IDLE` (HikariCP,
both engines), `MAX_SELECT_RESULTS` (opt-in SELECT cap), `ORPHAN_CLEANUP` (opt-in update-time
detached-child deletion), `FILE_VERSIONS_MAX` (opt-in per-file version-retention cap).

```java
H2PDSCreator creator = new H2PDSCreator();

// H2 (in-memory, PostgreSQL dialect)
APIConfigInfo h2 = creator.toAPIConfigInfo("jdbc:h2:mem:mydb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");

// Native PostgreSQL — a jdbc:postgresql URL auto-selects org.postgresql.Driver (both toAPIConfigInfo overloads)
APIConfigInfo pg = creator.toAPIConfigInfo("jdbc:postgresql://host:5432/db", "user", "pass");

H2PDataStore ds = new H2PDataStore();
ds.setAPIConfigInfo(pg);            // getDSType() -> POSTGRES, schemaless columns -> jsonb
```

The single-URL factories (`toAPIConfigInfo(url)` / `toAPIConfigInfo(url,user,pwd)`) support **both**
engines: a `jdbc:postgresql` URL auto-sets `DRIVER=org.postgresql.Driver`; any other URL keeps the
default H2 driver. When building a config by components instead (no `URL`), set `DRIVER` yourself for
Postgres. Detection is **parser-backed** (`isPostgres`/`resolveDSType` → `H2PUtil.parseJdbcURL`,
matching the `subprotocol`, not a prefix substring), with a driver-class fallback for the no-URL path.
The Hikari pool connects with whatever `DRIVER` is set — so the driver must match the URL.

### JDBC URL parser
`H2PUtil.parseJdbcURL(String) -> NVGenericMap` is the structured parser the creator uses internally
(instead of ad-hoc `startsWith`/`contains`). It returns `url` + `subprotocol` always, plus (when
present) `type` (H2 mem/file/tcp), `host`, `port` (NVInt), `database`, `path`, and a nested `params`
map of the `;`- or `?&`-delimited settings (keys verbatim, e.g. `CIPHER`, `MODE`, `DB_CLOSE_DELAY`).
Throws `IllegalArgumentException` for null / non-`jdbc:` input. Key names are the `H2PUtil.JDBC_*`
constants.

`H2PUtil.defaultH2JdbcURL(location, dbName)` builds the default **encrypted** H2 file-DB URL —
`jdbc:h2:file:<location>/<dbName>;MODE=PostgreSQL;CIPHER=AES` (the `DEFAULT_H2_URL` template). Both
args are trimmed; `location` must be an existing directory (else `IllegalArgumentException`), and
null/blank args throw `NullPointerException`. Because the URL carries `;CIPHER=AES`, opening it needs a
file password (e.g. `toAPIConfigInfo(url, user, password, filePassword)`).

### Encrypted H2 (CIPHER)
The **cipher is not a secret** and lives in the URL (`;CIPHER=AES`); only the passwords are supplied
separately (typically from a different source — GUI/web/CLI). Use
`toAPIConfigInfo(url, user, password, filePassword)` — it sets `USER`/`PASSWORD`/`FILE_PASSWORD` and
leaves the cipher in the URL. **A supplied `filePassword` implies encryption:** if the H2 URL has no
cipher, the factory appends H2's default `;CIPHER=AES` automatically (otherwise `dataStorePassword`
would silently drop the file password — H2 needs a cipher to treat the file as encrypted).
`H2PParam.isEncrypted` (via `hasCipher`) treats the DB as encrypted when `CIPHER` is a param **or** a
parsed URL setting, and `dataStorePassword` then emits H2's `"<filePwd> <userPwd>"` form (plain user
password when not encrypted; always plain for Postgres). Example:
```java
APIConfigInfo enc = creator.toAPIConfigInfo(
    "jdbc:h2:file:./data/secure;CIPHER=AES", "sa", "userPass", "encPass");
// dataStorePassword() -> "encPass userPass"
// (same result even if ";CIPHER=AES" is omitted from the URL — a file password auto-adds it)
```

## Connections / pooling
- **Both engines are pooled via HikariCP** — `H2PDataStore.newConnection()` always returns
  `pool().getConnection()`. The `HikariDataSource` is built lazily from the resolved
  URL/user/password/driver (`dataStorePassword` handles the encrypted-H2 `"<filePwd> <userPwd>"`
  form), sized by `POOL_MAX_SIZE` (default 10) / `POOL_MIN_IDLE` (default 2), closed by the
  datastore's `close()` and **retired + rebuilt by `setAPIConfigInfo`** on reconfigure (a new config
  may point at a different database/credentials).
- A pooled `connection.close()` returns the connection to the pool, so `acquire()`, the per-op
  `close(...)`, `execDDL` (its own connection), and the ThreadLocal transaction machinery are all
  engine-agnostic. A failed pool bootstrap (bad URL/credentials/file password) surfaces as an
  `APIException` with the SQL cause attached. For H2 `mem`/`file`, keep `DB_CLOSE_DELAY=-1` in the
  URL (the factory adds it) — DB lifetime must not depend on the pool's idle churn.
- The datastore extends **`APIServiceProviderBase<Connection, Connection>`** (same lifecycle plumbing
  as the Mongo stores): config/exception-handler storage, `touch()`-driven
  `lastTimeAccessed()`/`inactivityDuration()` (touched in `acquire()`), `pendingCalls`-based
  `isBusy()`, and `lookupProperty` answering `APIProperty.ASYNC_CREATE`/`RETRY_DELAY`.

## Read/write path costs (things already fixed — don't regress them)
- **Collection reads are batched.** `buildEntity` fetches a whole entity collection with one
  `IN (…)` query and re-orders in memory by the join table's `ord`. Measured on 100 entities × a
  3-element collection: **404 → 204 statements**. (Single `ENTITY_REF`s are still one query each —
  batching those across rows would need `select()` restructured; open opportunity.)
- **`childNVCE(ai)` memoizes on `AttrInfo`.** It looks up a *Java class name* while `H2PMetaManager`
  is keyed by *meta-type name* (`address_dao`), so the registry can never hit — unmemoized this cost
  a `Class.forName` + reflective `newInstance()` per reference attribute per row. `resolveNVCE` also
  memoizes by the name it was given (`nvceByTypeName`).
- **`tableExists` caches positives** into `createdTables`. A JVM reading a pre-existing DB never runs
  `ensureTable`, so without this every select paid an `INFORMATION_SCHEMA` round trip.
  `setAPIConfigInfo` clears `createdTables` **and** `metaManager` (a new config may point at a
  different database, and `getStoreTables()` must not report the old one's tables).
- **Per-type INSERT/UPDATE SQL is cached** (`insertSQLCache`/`updateSQLCache`); `syncJoins` prepares
  once and uses `addBatch`/`executeBatch`; `materialize` resolves column labels once per result set;
  `AttrInfo.lowerName` is precomputed; `delete(nve, true)` recurses through `innerDelete(con, …)` so
  the cascade doesn't re-`acquire()` a connection per referenced entity.

Note in-memory H2 will *not* show these as wall-clock wins — a query there costs microseconds.
Benchmark statement **counts** (H2 `SET QUERY_STATISTICS TRUE` + `INFORMATION_SCHEMA.QUERY_STATISTICS`),
not elapsed ms; the payoff is on Postgres round trips and H2 `file` mode.

- **Cyclic entity graphs are supported.** Writes carry a per-operation `WriteCtx`: a `seen` set stops
  the child recursion, and an FK column / join row pointing at an ancestor still being inserted is
  bound NULL and patched by `applyFixups` once the whole graph is on disk (H2 has no deferrable
  constraints). Reads thread a per-call `Map<String,NVEntity>` cache through
  `select`/`buildEntity`/`innerSearchByIDs`: an entity registers itself **before** resolving its
  references, so a cycle resolves to the same instance instead of recursing forever — and repeated
  child fetches within one call are deduplicated for free.
- **`userSearchByID` scopes by subject**: `guid IN (…) AND subject_guid = ?` — it is NOT a plain
  `searchByID` (regression: `H2PRegressionTest.testUserSearchByIDScoping`).
- **`patch()` is a real partial update** (mirrors `SyncMongoDS.patch` semantics): `nvConfigNames` +
  `includeParam=true` = exact set of attributes written; `includeParam=false` = attributes excluded;
  empty = full update. `updateTS` touches timestamps, `sync` serializes on the instance lock,
  `updateRefOnly` binds existing child GUIDs without writing the child rows. Null/empty GUID →
  insert; unknown GUID → `APIException` ("Can not patch a missing object").
- **`fieldNames` projection is implemented** in `search`/`userSearch`: SELECT covers `guid` + the
  named columns only, and only named entity collections are resolved; null/empty = all fields
  (contract). Non-projected attributes stay at their defaults on the returned entity.

- **`MAX_SELECT_RESULTS`** (`H2PParam`, opt-in): when set > 0, open-ended **predicate searches**
  (`search`/`userSearch`) are capped with `LIMIT n` — a safety valve against unbounded search
  materialization (off by default: full results). Guid-list reads are **never** capped (their size is
  already bounded by the id list): `searchByID`, entity-collection resolution in `buildEntity`,
  `nextBatch` pages and the dump always return complete results — and `batchSearch`'s guid report is
  likewise **never** capped: `batchSearch`/`nextBatch` IS the datastore-agnostic user-space pagination
  mechanism (complete guid-only report; the caller pages actual data via `nextBatch` at its own size).
  A cap on any of these would silently drop
  collection children and a follow-up `update()` would persist the loss via the join-table resync
  (regression: `H2PRegressionTest.testMaxSelectResultsValve`). The cap is the `capResults` flag on
  `select()`; only `innerSearch` passes true.
  `batchSearch` orders its ID report by `guid` (UUID v7 is time-ordered) so `nextBatch` pages are
  deterministic.
- **`delete(nve, withReference=true)` cascades from DB state, not the in-memory object**
  (`deleteByGuid`/`collectDbChildren`): the stored row's FK columns + join-table rows decide the
  children, so a shell entity (GUID only, children not loaded) cascades exactly like a fully loaded
  one; a `visited` set guards cyclic chains. A child still referenced elsewhere (**shared**) raises
  an FK violation, is **kept**, and the cascade continues (`deleteChildSafely` — SAVEPOINT-wrapped
  inside a transaction because PostgreSQL aborts the whole tx on any failed statement).
- **`ORPHAN_CLEANUP`** (`H2PParam`, opt-in `"true"`): `update()` deletes child rows it just detached
  (replaced single refs, children removed from collections) unless they are shared. Default **off**:
  detached children remain as rows and their lifecycle belongs to the caller.

SecurityController integration: done (encryption at rest 2026-09-29, access control 2026-10-02).
Still open: `search` materializes all matches (use `batchSearch`/`nextBatch` for large sets — that IS the
pagination mechanism); real `discover()`/`search(String...)` implementations over `file_info`
for the document store (currently null stubs, parity with the Mongo stores);
`isProviderActive()` returns "driver ever loaded", not health (`ping()` is the health check).

## Performance roadmap (agreed plan — NOT yet implemented)

All remaining wins are round-trip eliminations: they show on live PostgreSQL and H2 file/tcp, not
on in-memory H2 (benchmark statement counts, not wall clock). Ranked, per the 2026-09-01 planning
discussion with the user:

**Tier 1 (do first, one pass):**
1. **Batch reference resolution across rows** — `select()`/`buildEntity` two-phase: materialize all
   rows, then per collection attribute ONE `parent_guid IN (…)` join query for all parents, and per
   child type ONE `guid IN (…)` fetch for all single-ref children (the per-call GUID cache already
   dedups). Search of N entities with refs goes O(N)→O(1) queries per reference attribute; the
   documented 100×3-collection case drops ~204 → ~4 statements.
2. **Drop the `existsByGuid` probe** on insert/update — DEM pattern instead: `update()` = UPDATE
   first (0 rows → insert); `insert()` = INSERT first (23505, SAVEPOINT-wrapped on PG → update).
   Saves 1 round trip per entity per write, per node on graph writes.
3. **pgjdbc URL options** (config only, Postgres path): `reWriteBatchedInserts=true` (multi-row
   INSERTs for `syncJoins`/restore batches) and `prepareThreshold=1` (server-side plans for the hot
   cached per-type statements).

**Tier 2:** multi-row batching in restore's per-transaction loop; diff join rows on update instead
of delete+reinsert (`syncJoins(deleteFirst=true)` rewrites unchanged collections today); optional
LIMIT/OFFSET paging on `search`.

**Tier 3 (only on demonstrated need):** `fetchSize` streaming instead of `materialize()`'s row-map
copy (memory); opt-in secondary indexes on frequently-queried scalar columns (arbitrary-field
searches are full scans today — this also completes the query-pushdown win from the
`QueryGroup`/`IN`/`LIKE` vocabulary); NO entity cache (invalidation risk > win at this scale).

## Cross-repo coupling + scope (2026-09-01)

- The query vocabulary lives in **zoxweb-core** (local tree: `D:\dev\data\java\projects\zoxweb-core`):
  `shared/db/QueryGroup.java`, `shared/db/QueryMatchIn.java` (added by Claude, drafted for the user),
  and `LIKE`/`NOT_LIKE` appended (APPEND-ONLY — ordinals persisted) to `Const.RelationalOperator`.
  The installed 2.4.0 jar in `D:/dev/data/java/.m2/repository` contains them. h2p's formatter and
  tests depend on this — if zoxweb-core is rebuilt from an older tree (e.g. a checkout where these
  additions were never committed), `H2PQueryFormatter` won't compile. When committing, land the
  zoxweb-core additions before (or with) the h2p changes.
- zoxweb-core also aliases `get/setReferenceID` → `get/setGUID` (default methods on `ReferenceID`)
  and dropped `NVC_REFERENCE_ID` from `ReferenceIDDAO`'s meta; a validating `NVConfigPortable.setName`
  was tried and REVERTED (broke existing name usages) — the h2p identifier gate is the sole
  name enforcement, do not remove it.
- **Scope rule (user decision): Derby (`zoxweb-jdbc`) is being retired and the Mongo stores are on
  standby** — new features (query markers included) land in h2p ONLY; do not update the other
  stores' formatters (their silently-skip-unknown-marker behavior is accepted as-is). The Mongo
  stores remain the behavioral reference for parity (SyncMongoDS for SecurityController semantics).

## Encryption at rest — fields and files (built 2026-09-29; `H2PFieldCrypto`)

Authority for the record format: zoxweb-core `META-ENCRYPTED-DATA.md`. User decisions 2026-09-28/29.

**Activation.** `H2PDataStore.isEncryptionActive()` = the `APIConfigInfo` carries **both** a
`SecurityController` and a `KeyMaker`. **Since 2026-10-02 that is the only configuration that
reaches the database**: `newConnection()` → `requireMasterKey()` refuses to connect when the
controller, the key maker or the master key is missing (`AccessSecurityException`). The older
behaviour described in this section for "neither" (plaintext) and "exactly one"
(`requireConsistent` throwing on the first sealed write) can no longer occur through a
connection; `requireConsistent` is still in the code.

**Key chain** = `KeyMaker` (`KeyMakerProvider.SINGLETON` in practice): master key → **subject key**
(`EncapsulatedKey`, created *with the subject* by `ShiroDSDomainSecurityManager.createSubjectID`
when a KeyMaker is configured, removed by `deleteSubjectID`; **this store never mints it** — a
missing one is a loud `AccessSecurityException("No key for <subject>")`) → **entity key** (one per
entity, minted here on the first sealed write: `km.lookupEncapsulatedKey(ds, guid)` else
`km.createNVEntityKey(ds, nve, km.getKey(ds, null, subjectGUID))`, which inserts the row through
this store) → the sealed values. Entity deletion (`deleteByGuid`, incl. cascades, and
`delete(nvce, criteria)`) removes `encapsulated_key` rows with `reference_guid = guid`. The key
maker's lookup cache keeps the wrapped row until JVM restart — harmless.

**Access is a permission, never a `subject_guid` equality test** (`SecurityModel.RESOURCE`
grammar `resource:<resource guid>:<acting subject>:<verbs>`; every subject implicitly holds
`resource:S:S:read,update,delete,share`). This store asks the controller only:
`encryptValue`/`decryptValue` do their own owner-or-grant check and walk the *owner's* chain;
files go through `isNVEntityAccessible(fileGuid, ownerGuid, crud)` (`H2PFieldCrypto.accessAllowed`).

**Storage form: the packed binary record, never canonical text** (user, 2026-09-29 — the `|`-joined
canonical form is slated for removal; META-ENCRYPTED-DATA §5 names `CipherCodecs` as the storage form
for a datastore column). An ENCRYPT* attribute is therefore a **`bytea` column, always**
(`H2PUtil.scalarColumnType`; the schema never depends on a store instance's configuration): the
`CipherCodecs.EDEncoder` bytes of the record when the store encrypts, the clear text as UTF-8 bytes
when it does not. The two are told apart by layout (`H2PFieldCrypto.isPackedRecord`: version byte
`0x02` first, then a decodable layout — clear text never starts with a control byte). Where only text
fits (a pair inside the JSON column, the dump) the carrier is base64url of the packed bytes
(`packedText`) — base64 of the column form, not a second record grammar.

| | Fields (`FilterType.ENCRYPT` / `ENCRYPT_MASK` attributes) | Files (`sys_file_version`) |
|---|---|---|
| Declaration | `AttrInfo.encrypted/masked` from the attribute's filter (`ChainedFilter.isFilterSupported`); must be a **String SCALAR, not `*guid`** (else `IllegalArgumentException` at first touch); column type `bytea`. Also ENCRYPT* `NVPair`s inside schemaless containers. | every file when active; column `enc SMALLINT` (0 plaintext, 1 VX), added with `ALTER TABLE … ADD COLUMN IF NOT EXISTS` on pre-existing tables |
| Write | `bindColumn`: null/`""` ⇒ NULL; encrypting store: masked value equal to the stored record's mask ⇒ the stored bytes kept (`WriteCtx.storedRecords`, preloaded by `prepareCrypto` on update/patch), else `sc.encryptValue(...)` ⇒ `EDEncoder` bytes; non-encrypting store ⇒ UTF-8 bytes of the clear text. Schemaless: a JSON copy is sealed (`encodeSchemaless`), pairs carry `packedText` under the bare marker; the caller's object keeps its plaintext. | `createFile`/`updateFile`: `associate` → UPDATE access when the file exists → `insert(info)` → entity key → `AESCrypt.encryptBuffer(entityKey, plain)` (VX container, HKDF, 64 KiB segments) → version row with `enc=1`, `length` = **plaintext** length |
| Read | `buildEntity` first decides whether the row may be read at all (see "Access control" — a denied row is not returned), then defers encrypted scalars/containers until `guid`+`subject_guid` are set, then `decryptScalar`/`decryptSchemaless`: not a packed record ⇒ UTF-8 clear text set as-is; record + inactive store ⇒ attribute left **null** (one WARN per type); record + active ⇒ `EDDecoder` → `sc.decryptValue(ds, nve, nvb, record, null)`; a denied *decrypt* ⇒ ENCRYPT null, ENCRYPT_MASK shows the record's mask (pair filter swapped to the bare marker per instance) — unreachable for a subject without `read`, which no longer gets the row. | `writeVersionTo`: `enc=0` copied as-is (legacy); `enc=1` ⇒ READ access (owner resolved from `file_info.subject_guid` when the map is a shell) — **denied ⇒ nothing written, `readFile` returns `null`**; allowed ⇒ `AESCrypt.decrypt` segment by segment; tag failure ⇒ `APIException("… tampered with or key mismatch")` |
| Delete | key rows go with the entity (above) | `deleteFile` needs DELETE access; FK cascade + key row removal |
| Query | `H2PQueryFormatter.bindWhere(..., encryptionActive, ...)` **rejects value-bound criteria** on ENCRYPT* attributes when active (`IllegalArgumentException`); `IS [NOT] NULL` allowed. (There is no non-encrypting store any more since 2026-10-02, and shiro-ds's lookup of an API key by its secret was removed 2026-10-03: nothing queries an ENCRYPT* attribute by value.) | `fileVersions` adds `encrypted` |
| Dump/restore | dumps run in **raw mode** (`H2PFieldCrypto.setRaw`): encrypted attributes are left out of the entity JSON and their stored bytes ride **beside the entity line** as base64 in the envelope's `enc` map (`readEncryptedColumns`); restore inserts the entity then writes those bytes straight into the columns (`writeEncryptedColumns`) — nothing ever passes through the entity's filters or the JSON codec, core stays untouched. Readable again **only under the same master key** with the `encapsulated_key` rows restored (ordinary entity rows; dump them). | `file_version` records carry `enc` (absent ⇒ 0); bytes verbatim in JSONL and zip |

Migration: a pre-existing `varchar` column for an ENCRYPT attribute trips the schema type gate
(`SCHEMA TYPE MISMATCH`) — the sanctioned path (dump with the old classes, restore into the new
schema) or, on a disposable DB, drop the table. lax-2 `testdb` was recreated from scratch 2026-09-29.

The general read/update/delete check on every entity, plaintext included, was added 2026-10-02 —
see "Access control" below. Tests: `H2PFieldCryptoTest` (11), `H2PSecureFileTest`
(8) with the Shiro-free `TestSecurityController` (self permission + grant tokens) and
`CryptoTestSupport` (`-Dh2p.pg.url` switch as the PG suite).

## Access control — every read and write (built 2026-10-02)

User rule 2026-09-30: *a subject using the datastore can only reach the NVEntities it holds the
proper permission on — read, update, delete, share — whether the data is encrypted or not.* Plan:
`~/.claude/plans/datastore-acl.md`.

**Activation** = a `SecurityController` on the `APIConfigInfo` (`isAccessControlActive()`); the
`KeyMaker` only governs encryption. **Since 2026-10-02 (night) there is no unchecked store:** the
database is never used without the master key — `H2PDataStore.newConnection()` calls
`requireMasterKey()` first, and a configuration without `SecurityController`, without `KeyMaker`,
or whose key maker has no master key loaded is refused with `AccessSecurityException` ("Database
access refused: …") before any connection is taken. Every store that connects therefore has the
access check **and** encryption at rest on; the "controller only" and "neither" configurations
described further down no longer reach the database.

**The verdict** is always the controller's: `SecurityController.isNVEntityAccessible(guid, owner, crud)`
(`H2PFieldCrypto.accessAllowed`, reached through `H2PDataStore.permitted`). The store passes the row's
GUID and its **stored** `subject_guid` and never compares owners itself. With the Shiro controller
that is `ShiroUtil.checkResourcePermission`: owner token `resource:<owner>:<caller>:<verb>` (the self
permission, which now includes `create`), else grant token `resource:<guid>:<caller>:<verb>`; `*` and
`resource:*:*:<verb>` imply both. A row with a null `subject_guid` has no owner token — a grant or a
wildcard only.

| Operation | Check | Denied |
|---|---|---|
| `search`, `userSearch`, `searchByID`, `userSearchByID`, `lookupByReferenceID`, `nextBatch` | `read` per row built — referenced entities and collection members included, each judged on its own GUID/owner | row absent, reference null, member missing |
| `batchSearch`, `countMatch` | `read` per matching row (`guid, subject_guid` selected) | not in the report / not counted |
| `insert` of a new row, `patch` with no GUID | owner stamped = bound subject when null (also with a preset GUID), then `create` on that owner | `AccessSecurityException` |
| `insert` of an existing GUID, `update`, `patch` | `update` against the stored owner; an object without owner keeps the stored one, a different owner is refused | `AccessSecurityException` |
| referenced entity of a row being saved | an existing child the caller may not `update` is **linked, not rewritten** if it may `read` it | exception when it may not even read it |
| `delete(nve, withReference)` | `delete` against the stored owner; cascaded children too | parent: exception; child: kept, cascade continues |
| `delete(nvce, criteria)` | per matching row | unreadable rows skipped; a readable, undeletable row aborts before anything is deleted |
| `readFile`, `fileVersions` | `read` on the file — plaintext versions too | nothing written / `null` / empty list |
| `createFile`/`updateFile`, `rollbackFile`, `deleteFile` | `update` / `update` / `delete` on the file, stored owner | exception |
| `dump*`, `restore` | system context required | `AccessSecurityException` (a partial dump is worse than a refusal) |

Not checked (not owned entity rows): `DynamicEnumMap`, sequences.

**System context.** `SecurityController.runAsSystem(Supplier)` / `isSystemContext()` (core default
methods, fail-closed; Shiro: a re-entrant per-thread counter in `ShiroUtil`). Inside it every check
passes: it is for login, grant loading, key-chain walks, setup, dump/restore — never an application
request. Users: the security manager (shiro-ds `ShiroDSDomainSecurityManager.ds()` is a
`java.lang.reflect.Proxy` running each store call in it — the check asks Shiro, Shiro loads grants
through the manager, so those reads must not be checked or the check recurses), the controller's own
key-chain reads, and `H2PFieldCrypto.ensureEntityKey` / `entityKey` (the key rows are the owner's; a
grantee writing or reading a sealed value walks them).

**Code map.** `ReadCtx` (per-call entity cache + owner-verdict memo, replaces the bare cache map
through `select` / `buildEntity` / `innerSearchByIDs`; `buildEntity` returns null for a denied row,
remembered as a null cache entry) · `storedRow` (the write path's single probe: existence + stored
owner; replaces `existsByGuid`) · `upsert` → `insertRow` / `updateRow` (replace `innerInsert` /
`innerUpdate`) · `checkCreate` / `checkWrite` · `deleteCheckedByCriteria` · `fileAccess` (stored
owner always) · `requireSystemContext`.

**Consequences worth knowing.**
- A subject without `read` gets no row, so it never sees an `ENCRYPT_MASK` mask either; the mask is
  what a value written back is recognized by, and what a denied *decrypt* would still show.
- `MAX_SELECT_RESULTS` caps before the filter: a capped search can return fewer rows than the cap
  while more readable rows exist. `batchSearch` / `nextBatch` is exact.
- No SQL push-down of the filter: one in-memory `isPermitted` per distinct owner per call, plus one
  per row of an owner the caller may not read.
- A share on a parent does not reach its children (each row is judged on its own) — the deferred
  child-rows question.
- A store with a controller and **no** key maker does not connect at all (since 2026-10-02;
  before that it refused file writes and ENCRYPT* values and stored plaintext entities).
- The subject must be logged in **and thread-bound** through Shiro (`loginSubject`,
  `loginSubjectJWT`, `ShiroUtil.login`); `dsm.login(...)` only verifies a credential, and
  `loginApiKey(...)` always refuses since 2026-10-03.

Tests: `H2PAccessControlTest` (9, controller-only store, Shiro-free `TestSecurityController`),
`H2PFieldCryptoTest` / `H2PSecureFileTest` adapted, shiro-ds
`datastoreAccessControl_endToEnd_withTheShiroController` and
`datastoreAccessControl_superAdminReachesEveryRow_ordinarySubjectOnlyItsOwn`.

## File storage (APIDocumentStore) — versioned

`H2PDataStore` implements `APIDocumentStore<Connection, Connection>` alongside `APIDataStore`
(same both-interfaces pattern as `XlogistxMongoDataStore`). Designed for files **1 KB – a few MB**
(whole content is materialized in memory per op — no chunking/streaming).

- **Metadata** is a regular `FileInfo` row (existing normalized CRUD, table `file_info`; the class
  was `FileInfoDAO` until 2026-09-28); `FileInfo` itself implements `APIFileInfoMap`. **Content**
  is versioned in `sys_file_version` — one `bytea` row per version, `PRIMARY KEY (file_guid,
  version)`, `enc SMALLINT` (0 plaintext / 1 AESCrypt VX, see "Encryption at rest"), FK →
  `file_info(guid) ON DELETE CASCADE`. `sys_file_head(file_guid PK, current_version)` points
  at the current version. Identical SQL on H2 and PostgreSQL — no dialect divergence.
- **`createFile`/`updateFile`** store the stream as the file's next version (`MAX(version)+1`,
  monotonic, never reused — also not after a rollback) and repoint the head. Concurrent updates
  are **last-write-wins with full history**: the version INSERT retries on 23505
  (SAVEPOINT-wrapped inside a transaction — PostgreSQL aborts the tx on any failed statement).
  Metadata + version + head move **atomically**: the op joins the ambient transaction if one is
  active, else it runs its own local transaction (no orphaned metadata/content — the SQL
  equivalent of `XlogistxMongoDataStore.createFile`'s GridFS rollback).
- **`readFile(map, os, …)`** streams the head version; **`readFile(map, version, os, …)`** a
  specific one (both return `null` and write nothing when an encrypted version is denied to the
  bound subject); **`fileVersions(map)`** lists them newest-first (`version`/`length`/`created_ts`/
  `current`/`encrypted` per `NVGenericMap`); **`rollbackFile(map, version)`** repoints the head (no content
  copy, no history rewrite) and restores the metadata `length`. The three version methods are
  `default` methods on `APIDocumentStore` (zoxweb-core) throwing `UnsupportedOperationException` —
  the Mongo stores don't override them.
- **`deleteFile`** deletes the metadata row; FK cascade removes every version + the head row.
- **`FILE_VERSIONS_MAX`** (`H2PParam`, opt-in > 0): after each create/update, versions older than
  the newest n are pruned — **never the head-pointed version**. Default: unlimited.
- `discover()`/`createFolder()`/`search(String...)` return `null` — parity with both Mongo stores;
  folders are just `FULL_PATH_NAME` strings on the DAO.
- File tables are created out-of-band via `execDDL` (`ensureFileTables`, guarded by a volatile
  flag reset in `setAPIConfigInfo`). With a `SecurityController` + `KeyMaker` configured every
  version is an AESCrypt VX container under the file's entity key (see "Encryption at rest");
  otherwise content is stored as-is with `enc = 0`.

## Transactions / sequences / DEM
- Transactions: ambient `ThreadLocal<Connection>` (`autoCommit=false`), `begin/end/abort`. Data ops
  route through `acquire()`; **schema DDL runs out-of-band** on its own connection (`execDDL`) — on H2
  because DDL implicitly commits; on Postgres it's harmless (and still correct).
- Sequences: table-based `sys_long_sequence` (portable — no native `SEQUENCE`). **Sequence ops are
  non-transactional and atomic**: they always run on a dedicated auto-commit connection (never the
  ambient tx — a rollback must not undo an increment, and an uncommitted tx row lock must not block
  other callers), and `incrementSequence` does `SELECT … FOR UPDATE` + `UPDATE` in one short DB txn —
  safe across threads, pooled connections and JVMs (the old JVM `ReentrantLock` wasn't). The
  `createSequence` seed INSERT swallows a 23505 race loss.
- DEM: portable UPDATE-then-INSERT upsert (no H2 `MERGE` / no Postgres `ON CONFLICT`); a 23505 on the
  INSERT (concurrent creator won) retries the UPDATE once.

## JSON dump / restore (JSONL, engine-portable)

Public API on `H2PDataStore` (engine in package-private `H2PDumpRestore`):
- `long dump(NVConfigEntity, OutputStream)` — one type as JSONL; returns rows written.
- `String dumpToJSON(NVConfigEntity)` — one type as a JSON array string (small tables; materializes).
- `NVGenericMap dump(OutputStream[, includeFiles], NVConfigEntity... types)` — whole store: header
  line, every entity of every type, DEM, sequences and (default on) versioned file content
  (`sys_file_version`/`sys_file_head`, content base64). Empty `types` ⇒ discovery via
  `discoverStoreTypes()` (catalog ∪ session registry) — pass explicit types for pre-catalog DBs.
  Returns per-kind counts (`types` nested map — read with `getNV("types")` — `dem`, `sequences`,
  `file_versions`, `file_heads`, `cycles_skipped`).
- `NVGenericMap dumpZip(OutputStream[, includeFiles], NVConfigEntity... types)` — same dump as a
  **zip archive**: entry `dump.jsonl` first (its `file_version` records carry an
  `entry:"files/<file_guid>/<version>"` pointer instead of inline base64), then one raw
  deflate-compressed content entry per stored version. The right form when file content dominates
  (no base64 ~33% inflation). Stream is `finish()`ed, not closed.
- `NVGenericMap restore(InputStream, RestoreMode)` — **auto-detects the container** (`PK` magic ⇒
  zip, else plain JSONL). `MERGE` (guid-keyed upsert, idempotent, sequences raise-only) or
  `WIPE_AND_LOAD` (clears every discoverable entity table — join tables first, FK columns nulled —
  file content, DEM and sequences, then loads). Accepts a header-less per-type dump too. Zip
  restore is sequential: `dump.jsonl` loads normally while entry-referencing `file_version` records
  park as pending metadata, then each content entry completes one version (one version's bytes in
  memory); versions still pending at end-of-archive ⇒ `APIException` (truncated zip). A JSONL
  stream with `entry` records but no surrounding zip fails with a clear "restore from zip" error.

Positioning: **migration/interchange** (H2 ↔ PostgreSQL, cross-store — the JSON is self-describing
via `class_type`), not same-engine backup (use H2 `SCRIPT TO` / `pg_dump` for that).

**CLI**: `H2PDumpRestore.main` — `java -cp <module+deps+entity classes> io.xlogistx.datastore.h2p.H2PDumpRestore
dump|restore --url <jdbc-url> [--user/--password/--file-password] --out|--in <file>
[--types c1,c2] [--no-files] [--format zip|jsonl] [--mode merge|wipe]`. A `.zip` `--out` extension
selects the zip container; restore auto-detects. Prints the stats JSON; exit 0/1 (usage)/2 (failed).
The entity classes named by the dump's `class_type` must be on the CLI classpath.

Format: one `{"k":<kind>,"v":{...}}` envelope per line; kinds `header` (`format:"h2p-json-dump"`,
`version:1`), `entity` (`GSONUtil.toJSON(nve, printClassType=true)`), `dem`, `seq`, `file_version`,
`file_head`. Streamed both ways: dump pages via `batchSearch`/`nextBatch` (`MAX_SELECT_RESULTS` never affects it —
the valve caps only predicate searches, not guid-list fetches);
restore loads entity lines in per-batch transactions (256/tx; a failure aborts the open batch and
rethrows with the line number — completed batches stay committed, `MERGE` re-runs converge).

Semantics to keep in mind:
- `GSONUtil` **inlines** referenced entities: every entity line carries its whole subtree, so restore
  has no ordering constraints and shared children (dumped redundantly) dedup by GUID through
  `insert`'s upsert. The flip side: a **cyclic** graph is not JSON-representable — cyclic entities
  are detected up front (`H2PDumpRestore.hasCycle`, identity-based DFS; the read path materializes
  cycles as the same instance) and **skipped**, counted in `cycles_skipped`.
- Restore needs the entity classes (`class_type` → `Class.forName`) on the restoring JVM's classpath.
- Non-entity records restore outside the batch transaction (sequences are non-transactional by
  contract; file-content SQL runs on its own connection and preserves version numbers — `createFile`
  would renumber). Sequence restore is raise-only under `MERGE` so issued values are never re-issued.
- File dump lines hold one version's bytes in memory at a time (same bound as the file API); the
  `FileInfo` metadata type is force-included whenever `includeFiles` is on.

## PostgreSQL-portability rules (keep it dual-target)
1. Use only types valid on both: `uuid`, `bytea`, `varchar`, `integer`, `bigint`, `real`,
   `double precision`, `boolean`, and `jsonb` (Postgres) / `varchar` (H2) **only via `H2PDialect`**.
2. No H2-only or Postgres-only SQL in the shared paths (no `MERGE`, no `ON CONFLICT`, no `SEQUENCE`).
3. Any new dialect divergence goes through `H2PDialect` keyed on `currentDSType` — never inline
   `if (postgres)` in the datastore.
4. `INFORMATION_SCHEMA.TABLES` checks filter `TABLE_TYPE='BASE TABLE'` and scope to
   `TABLE_SCHEMA = CURRENT_SCHEMA` (works on both engines) — a same-named table in another schema
   must not count as ours.
5. UUID via `setObject(uuid)` / `getObject(col, UUID.class)`; bytea via `setBytes`/`getBytes` — both
   pgjdbc-native.

Criteria (`H2PQueryFormatter`) — supported markers: `QueryMatch` with every `RelationalOperator`
including `LIKE`/`NOT_LIKE` (appended to the enum in zoxweb-core — APPEND-ONLY, ordinals are
persisted); `QueryMatchIn` → `[NOT] IN (…)` (empty value list renders as a constant: matches
nothing, or everything when negated); `QueryGroup.OPEN/CLOSE` → explicit parentheses (balance
validated — without grouping, `a AND b OR c` silently parses as `(a AND b) OR c`);
`Const.LogicalOperator`. Any other `QueryMarker` is REJECTED with `IllegalArgumentException` —
silently skipping an unknown criterion would widen the result set on version skew. Typing: a
null-valued `=`/`!=` `QueryMatch` renders as `IS NULL`/`IS NOT NULL` (a bound null parameter can
never match, and pgjdbc rejects untyped nulls); a criterion on a single entity-reference column
(`AttrKind.ENTITY_REF`, stored as `uuid`) binds a GUID String **or the child entity itself** as
`UUID` (`H2PQueryFormatter.normalize`, public; added 2026-09-15 for `permission_grant.resource_map
IN (…)` — H2 in PG mode coerced the varchar silently, native PostgreSQL does not; regression
`H2PRegressionTest.testEntityRefCriterionBindsUUID`); `Date` values bind as epoch millis (columns are
`bigint`); values against `Number`-typed (NVNumber) attributes bind through `H2PUtil.encodeNumber`
— equality only. Regression: `testGroupedCriteria`/`testInCriteria`/`testLikeCriteria`/
`testMalformedCriteriaRejected`.

## Running the tests

Tests run via the JUnit Platform launcher (surefire can't fetch its provider offline in this env).
Compile with `mvn -o -pl h2p-datastore -DskipTests test-compile`, then run a small
`LauncherFactory`-based main selecting package `io.xlogistx.datastore.h2p.test`, with `-ea`.
Local repo is `D:/dev/data/java/.m2/repository` (see `~/.m2/settings.xml`); the classpath =
`target/classes` + `target/test-classes` + zoxweb-core 2.4.0, common-datastore 1.0.0, uuid-creator,
gson, HikariCP, slf4j-api, h2, postgresql, the junit-jupiter/junit-platform 6.1.2 jars + opentest4j —
**plus**, because `H2PDataStoreTest.setup` calls `OPSecUtil.singleton()`: xlogistx-opsec,
xlogistx-core, sshd-common/core/scp/sftp 2.16.0 and bouncycastle bcprov/bcpkix/bctls/bcutil-jdk18on
(bcutil is required for the live-PG SSL handshake once OPSecUtil registers the BC JSSE provider).
(The dependency:build-classpath plugin is not cached offline — list the jars manually.)

- `H2PDataStoreTest` — full suite on in-memory H2 (`MODE=PostgreSQL`). All green. Includes
  `testEncryptedH2FileRoundTrip` — a temp **file** DB with `;CIPHER=AES` (secrets passed via the 4-arg
  `toAPIConfigInfo`): asserts `dataStorePassword` → `"<filePwd> <userPwd>"`, CRUD over the encrypted
  file, persistence across a fresh-store reopen, and rejection of a wrong file password. It omits
  `DB_CLOSE_DELAY=-1` on purpose so the DB closes between stores and the password is re-validated.
  Also `testParseJdbcURL` (H2 mem/file/tcp/bare + Postgres host/opts/multi-host/db-only + guards) and
  `testDefaultH2JdbcURL` (composed URL, parse-back, trimming, null/non-directory guards).
- `H2PRegressionTest` — regression suite for the analysis fixes (in-memory H2): cyclic pair +
  self-reference insert/read, 4-thread sequence uniqueness, sequence-inside-transaction no-block +
  rollback-survival, `userSearchByID` scoping, `IS [NOT] NULL` criteria, concurrent DEM upsert,
  `patch` include/exclude/missing-object modes, `fieldNames` projection, `sqlName` identifier
  hashing (composed names) + rejection of >63-byte / non-ASCII entity-type names, the
  `MAX_SELECT_RESULTS` valve, the
  `APIServiceProviderBase` lifecycle (touch/lookupProperty/isBusy), shell-entity cascade delete,
  shared-child keep-on-delete, and `ORPHAN_CLEANUP` on/off behavior.
- `H2PDumpRestoreTest` — JSON dump/restore (in-memory H2, per-test stores on unique URLs): per-type
  and whole-store round trips into a fresh store (entities compared by re-serialized JSON, DEM by
  both stores' read-back, sequence continuity, 2-version file with rolled-back head), shared-child
  GUID dedup, cycle skip policy (`cycles_skipped`), dump completeness under `MAX_SELECT_RESULTS`,
  MERGE vs WIPE_AND_LOAD, cold-start discovery through `sys_meta_catalog`, foreign-format rejection,
  and the
  zip container (layout + auto-detected round trip incl. rolled-back head, missing-content-entry
  failure, external-`entry` JSONL rejected without its zip).
- `H2PFileStoreTest` — versioned file storage (in-memory H2): 1 KB + ~3 MB round-trips, version
  bumping + specific-version reads, head-pointer rollback (monotonic numbering afterwards),
  4-thread concurrent updates (unique versions, head = highest), `FILE_VERSIONS_MAX` pruning,
  cascade delete, and ambient-transaction commit/abort participation.
- `H2PPostgresDataStoreTest` — **live PostgreSQL**; auto-skipped unless configured. `h2p.pg.url` is the
  **base endpoint** (no db); the test connects to the `postgres` maintenance db, **creates the target
  database if missing** (default `testpostgres`, override `-Dh2p.pg.db`), then runs the same scenarios
  (jsonb NVGenericMap/NamedValue, bytea, FK references, transactions, versioned file storage) and
  asserts `getDSType()==POSTGRES`. Also runs the dump/restore **headline scenario**: a whole-store
  JSONL dump from an in-memory H2 store restored into live PG (`MERGE` — the shared test DB is never
  wiped) covering entities/DEM/sequence/2-version file, then a per-type dump back off PG into a
  fresh H2 store (H2 → PG → H2 loop):
  ```
  -Dh2p.pg.url=jdbc:postgresql://host:5432 -Dh2p.pg.user=… -Dh2p.pg.password=…
  ```
- `H2PDomainSecurityManagerDBTest` — `DomainSecurityManager` integration (subjects/credentials/
  permissions/roles/role-groups), **engine-agnostic via one JDBC URL**. Auto-skipped unless `-Dds.url`
  is set; the setup parses it with `H2PUtil.parseJdbcURL` and branches: **H2** (mem/file, cipher in the
  URL) uses the 4-arg factory; **Postgres** auto-creates the target db (like above). Standard `ds.*`
  **system properties** for both engines:
  ```
  -Dds.url=jdbc:h2:mem:dsm;DB_CLOSE_DELAY=-1;MODE=PostgreSQL
  -Dds.url=jdbc:h2:file:./data/dsm;CIPHER=AES;MODE=PostgreSQL -Dds.file_password=encPass -Dds.user=sa -Dds.password=userPass
  -Dds.url=jdbc:postgresql://host:5432 -Dds.user=… -Dds.password=…   # -Dds.db optional
  ```

## Session log — 2026-09-15

- **`*guid` ⇒ `uuid` rule** (user): `H2PUtil.isUUIDField` now matches any non-entity attribute whose
  name ends in `guid` (case-insensitive) or is ref-id flagged; the fixed reserved-name set is gone.
  Newly uuid-typed: `resource_guid` (`resource_map`), `reference_guid`/`key_guid`
  (`encapsulated_key`), `app_guid` (`subject_preference`). `permission_grant` + `resource_map` were
  dropped and recreated on lax-2 testdb (the only pre-rule varchar `*guid` column there).
- **Entity-reference criteria bind as uuid**: `H2PQueryFormatter.normalize` (now public) decodes a
  GUID String or the child `NVEntity` for `AttrKind.ENTITY_REF` columns — needed by shiro-ds's
  `permission_grant."resource_map" IN (…)` on PostgreSQL.
- Tests: `H2PRegressionTest` +2 (`testEntityRefCriterionBindsUUID`, `testGuidSuffixedAttributesAreUUIDColumns`),
  25/25; `H2PDataStoreTest` 26/26; `H2PDumpRestoreTest` 12/12; DSM suite 10/10 on H2 and PG;
  shiro-ds 49/49 on H2 and PG. Runner cp files moved to JUnit 6.1.3.

## Session log — 2026-09-02

- **`isTransactionActive()`** implemented on `H2PDataStore` (true while the thread-local ambient
  connection from `beginTransaction()` is bound). Backs the new `APIDataStore` default (false) added
  in zoxweb-core; `xlogistx-shiro-ds`'s manager uses it to join a caller's transaction instead of
  nesting (which `beginTransaction()` rejects). `H2PDomainSecurityManagerDBTest` class Javadoc
  rewritten (it described the retired Mongo path). Both suites re-verified: 10/10 H2, 10/10 PG.

## Session log — 2026-09-01 (all tested: 68/68 local H2; PG suite 73/73 verified live on lax-2.xlogistx.io)

1. **`MAX_SELECT_RESULTS` scoped to predicate searches only** (`capResults` flag on `select()`);
   guid-list reads, `nextBatch`, collection resolution, `batchSearch` report and dump are NEVER
   capped (dump's clamp workaround removed).
2. **Identifier gate** — `H2PUtil.checkNameForSQL` (ASCII, ≤63 bytes) called once per type from
   `attrInfos`; rejects loudly. Sole enforcement (core-side setName validation was reverted).
3. **`reference_id` machinery removed** (`META_INSERT_EXCLUSION`, `excludeMeta`); `AttrKind.EXCLUDED`
   is now only the null-NVConfig guard.
4. **Schema evolution** — additive sync + type gate in `ensureTable`/`syncExistingTable`
   (see dedicated section); verified on live PG including a real `ALTER TABLE ADD COLUMN`.
5. **Query vocabulary** — `QueryGroup`/`QueryMatchIn`/`LIKE`/`NOT_LIKE` (zoxweb-core) wired into
   `H2PQueryFormatter` with fail-loud on unknown markers.
6. **`decodeSchemaless` null-guard** (meta/class drift skips the column with a warning, no NPE).
7. Stale HikariCP pom comment fixed (the pool serves BOTH engines — H2 included).

Next agreed work item: **performance Tier 1** (section above). The user provides SecurityController
integration separately.

## Session log — 2026-09-29 (encryption at rest + resource permission model)

Built per plan `spicy-forging-russell.md` (design page section 6 superseded by the "Encryption at
rest" section above). zoxweb-core (installed, `-Dgpg.skip=true`): `SecurityModel` — `nventity`
namespace **removed**, `Target.RESOURCE`, `ResourcePermissionTokenFilter` (stored form
`resource:<verbs>`), `toResourceToken(res, subject, verbs)` composes the standardized 4-part token,
`isResourceToken`, `RESOURCE_SELF_VERBS`, catalog `nve_*` rows now `resource:*` / `resource:*:*:<verb>`;
(a first cut stored the canonical text form and taught `ChainedFilter` to pass it through — **reverted
the same day**: the record's canonical text is slated for removal, fields now hold `CipherCodecs`
packed bytes in `bytea` columns and dumps move them beside the entity line, core untouched).
io-xlogistx shiro (installed): `ShiroUtil.checkResourcePermission`
returns the owner GUID, String overload + `isResourcePermitted`; `ShiroSecurityController`
delegates both access checks to it, sets `data_type`/`mask` before sealing, `decryptValues`
recursion fix, null-safe `currentSubjectGUID`. shiro-ds: flattener synthesizes the self permission
and composes scoped grants, `share` replaces the owner equality in grant/revoke enforcement,
subject key created/removed with the subject; 67 + 17 tests. h2p: `H2PFieldCrypto`, hooks in
`H2PDataStore`/`H2PQueryFormatter`/`H2PDumpRestore`, `sys_file_version.enc`; new tests 11 + 8,
existing 26/25/12/7 + DSM 11 unchanged. Deferred: general ACL on plaintext entities; composite
`<kid>.<secret>` API-key login (equality lookup on `api_key` is refused only when a store encrypts);
`DoNotExpose` enforcement point.

**PostgreSQL DDL inside an ambient transaction (fixed the same day).** Bootstrapping a **fresh**
lax-2 `testdb` deadlocked: the seeder runs inside `beginTransaction()`, `ensureTable` ran its DDL on
an out-of-band connection, and the join table's `CREATE TABLE … FOREIGN KEY … REFERENCES role_info`
waited on the relation lock the idle transaction held on `role_info` (it had just created/written
it) — forever, invisible to the lock manager (`pg_stat_activity`: one session `idle in
transaction`, one `active` on `Lock/relation`). Never seen before because every table already
existed. Fix: `execDDL` (and the `sys_meta_catalog` upsert in `registerInCatalog`) run **on the
transaction connection, under a SAVEPOINT, when the engine is PostgreSQL and a transaction is
bound** — PostgreSQL DDL is transactional, so a table created inside a rolled-back transaction
simply disappears and is recreated on the next touch; a failed quiet DDL (duplicate constraint)
rolls back to the savepoint instead of aborting the transaction. H2 keeps the out-of-band
connection (its DDL auto-commits and would end the ambient transaction). Verified by
`bootstrap-super-admin` on a freshly created database (29 tables, 24/6/4 catalog rows) and the
full PG suites afterwards. `readColumnTypes` still probes on its own connection; the
`createdTables` guard keeps a type created inside a transaction from being re-probed in that JVM.

## Session log — 2026-10-02 (datastore access control)

Built per `~/.claude/plans/datastore-acl.md` after the user's go. zoxweb-core (installed,
`-Dgpg.skip=true`; the working tree also held the user's own in-progress `AccountID` removal, which
went into the jar with it): `SecurityController.runAsSystem` / `isSystemContext` as fail-closed
default methods, `RESOURCE_SELF_VERBS` = `create,read,update,delete,share`. io-xlogistx shiro
(installed with `-Dmaven.test.skip=true` — `ShiroMetaModelTest` no longer compiles since the core
rename `setAppIdDAO` → `setAppID`, not touched): `ShiroUtil.runAsSystem` / `isSystemContext`,
`checkResourcePermission` passes in the system context and accepts a null owner (it threw
`NullPointerException`, locking even `*` out of ownerless rows), `ShiroSecurityController` implements
the two methods, accepts a null owner in `isNVEntityAccessible`, and walks the key chain in the system
context. h2p: everything in the "Access control" section; fixed on the way — `fileAccess` trusted the
`subject_guid` of the caller's `FileInfo` (a shell stamped with the caller's own GUID passed as the
owner), a preset-GUID insert could leave a row ownerless, and an update through an object without
`subject_guid` nulled the stored owner (now kept, under access control). shiro-ds: the manager's
system view of the store. Suites on in-memory H2: access control 9, field crypto 11, secure file 8,
store 26, regression 25, dump/restore 12, file store 7, default-manager 11; shiro-ds catalog 20,
manager 69; core `SecurityModelTest` 10, `PermissionGrantTest` 60. **PostgreSQL not run**: the user
keeps lax-2 `testdb` free of test data (2026-10-02). Nothing committed.

## Ground rules for future sessions
0. **Every attribute named `*guid` is a `uuid` column** (user rule 2026-09-15, `H2PUtil.isUUIDField`):
   never store a non-UUID string in one; a pre-rule table with a varchar `*guid` column trips the
   schema type gate — drop and recreate it (the DB is not in production). Regression:
   `H2PRegressionTest.testGuidSuffixedAttributesAreUUIDColumns`.
1. Keep the SQL PostgreSQL-portable; route every dialect difference through `H2PDialect`.
2. `currentDSType` is resolved once in `setAPIConfigInfo` — don't re-detect per call.
3. When adding an NV type, update `H2PUtil.classify` + `scalarColumnType` and the five paths in
   `H2PDataStore`: DDL (`ensureTable`), write (`bindColumn`), read (`buildEntity`/`setScalar`/
   `decodeSchemaless`), and — for entity refs — `insertChildren`/`syncJoins`/join resolution.
4. Referenced entities are separate rows with FKs; never re-introduce inline/binary embedding.
5. `guid` (UUID v7) is the single row identity. `referenceID` no longer exists as an attribute:
   zoxweb-core's `ReferenceID` interface aliases `get/setReferenceID` to `get/setGUID` (default
   methods) and `NVC_REFERENCE_ID` is gone from `ReferenceIDDAO`'s meta — so `lookupByReferenceID`/
   `isValidReferenceID` operate on GUIDs, and no exclusion set is needed (`AttrKind.EXCLUDED`
   remains only as the defensive classification for a null NVConfig).

## Session log — 2026-10-02 night (no database without the master key)

User rule: *you cannot use the database without a master key*; and *the prerequisite of any run is
to load the keystore with its password and take the db info and the master key from it*.

- `H2PDataStore.requireMasterKey()` — called at the top of `newConnection()` (every read, write,
  DDL, transaction, sequence, dump, restore goes through it): the `APIConfigInfo` must carry a
  `SecurityController` and a `KeyMaker` whose `getMasterKey()` returns a key, else
  `AccessSecurityException`. `close()` and `setAPIConfigInfo` are not gated.
- `H2PDumpRestore.main`: `--store <vault>` is required (`--store-password`, or a console prompt;
  `--controller <class>`, default `io.xlogistx.shiro.mgt.ShiroSecurityController`, instantiated by
  name because this module has no controller of its own). `loadVault` reads the opsec `SecretStore`
  file with the plain JCA API (BCFKS on BC; a text secret is a `PBEKey` password entry): the
  `master-key` secret key goes into `KeyMakerProvider.SINGLETON`, the `db.*` entries are the
  connection unless `--url/--user/--password/--file-password` override them. The operation runs
  inside `controller.runAsSystem`. Checked on scratch H2 files: no `--store` → usage error; dump →
  zip; restore into a second database; `SecurityAdminTool list-apps` / bootstrap on the copy.
- Tests. `CryptoTestSupport.loadVault()` opens a vault (`-Dstore=` + `-Dstore.password=`, else a
  throw-away one with a fresh master key), loads the master key, `secure(cfg)` = stub controller +
  key maker, `db(name)` / `vaultPostgresURL()`; `config()` and the two live suites take the
  PostgreSQL target from the vault when it names one (`-Dh2p.pg.*` / `-Dds.*` still override).
  New JUnit extension `SystemContext` runs a whole class in the stub controller's system context:
  the mechanics suites (`H2PDataStoreTest`, `H2PRegressionTest`, `H2PFileStoreTest`,
  `H2PDumpRestoreTest`, `H2PPostgresDataStoreTest`, `H2PDomainSecurityManagerDBTest`) carry it and
  open every store through `secure(...)`. File content is always sealed now, so a file needs an
  owner with a subject key: `H2PFileStoreTest` binds one per class, the dump and PostgreSQL file
  tests bind one for the file writes and unbind before the dump/restore. The three "half / no
  configuration" tests became refusal tests (`incompleteConfiguration_databaseRefused` ×2,
  `noController_noDatabase`); `H2PAccessControlTest` runs on a controller + key maker store.
  `testFullStoreRoundTrip` restores 11 entities (the 9 + the owner's subject key + the file's key).
- Owner stamping during a restore — **corrected 2026-10-03 after tracing the code**. What was
  observed (run): in `testFullStoreRoundTrip`, rows that had no owner in the source came back with
  the bound test subject as owner when the restore ran while that subject was bound. Cause
  (read in the source): `insertRow` calls `controller.associateNVEntityToSubjectGUID(nve, null)`
  for every new row, also in the system context; the test stub `TestSecurityController` stamps any
  entity without `subject_guid` when a subject is bound. The production `ShiroSecurityController`
  stamps only an entity that has **no GUID**, and a restored row always carries its GUID, so by
  that code it is not stamped. That last point was read, not run with the Shiro controller. The
  earlier wording here presented it as a datastore behaviour; it is a property of the test stub.
- Results. In-memory H2: access control 9, field crypto 11, secure file 8, data store 26,
  regression 25, file store 7, dump/restore 12, default-manager 11 — all green. **PostgreSQL
  (lax-2 `testdb`, through `xlogistx-shiro-ds/src/main/resources/test.store`):**
  `H2PPostgresDataStoreTest` 9, field crypto 11, secure file 8, access control 9 — all green.
  `H2PDomainSecurityManagerDBTest` was not run on PostgreSQL: core's `DomainSecurityManagerDefault`
  creates subjects without a subject key (read in the source: its `createSubjectID` inserts the
  subject and the class never references `EncapsulatedKey` or `KeyMaker`; the user has since said
  that class is being replaced by `ShiroDSDomainSecurityManager`). Nothing committed.

## Session log — 2026-10-05 (`DomainSecurityManagerDefault` deleted by the user)

The user deleted core's `DomainSecurityManagerDefault` and its `DomainSecurityManagerDefaultTest`
("fix the code"). `H2PDomainSecurityManagerDBTest` now builds `ShiroDSDomainSecurityManager(ds)`
and seeds the catalog in its set-up (the Shiro manager's catalog rows belong to the common app,
which must exist); the Mongo `DomainSecurityManagerDBTest` in `xlogistx-datastore` had already had
its construction commented out by the user (it skips without a Mongo server). While rebuilding,
the parent `pom.xml` turned out to have its `<version>1.0.0</version>` line replaced by a stray
`N` (modified at 15:50, not by my earlier edit, which only removed the module line); restored.
Run: zoxweb-core, io-xlogistx and the whole zoxweb-datastore reactor install offline;
`H2PDomainSecurityManagerDBTest` 11/11 through `h2mem.store`; `SecurityCatalogDBTest` 21/21 and
`ShiroDSDomainSecurityManagerDBTest` 72/72 through `h2mem.store`; no-sneak-core and no-sneak-app
compile against the new core jar. Nothing committed.
