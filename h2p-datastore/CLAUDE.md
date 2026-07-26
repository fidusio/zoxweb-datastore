# h2p-datastore — Claude Working Notes

Scope: this module only (`io.xlogistx.datastore.h2p`). It is a **normalized, relational**
implementation of zoxweb's `APIDataStore<Connection, Connection>` that runs on **both** H2 (in
PostgreSQL compatibility mode) **and** a native PostgreSQL server — the same code, swapping only the
JDBC driver + URL.

## Files

| File | Role |
|---|---|
| `H2PDataStore.java` | The datastore — DDL, CRUD, search, references, transactions, sequences, DEM |
| `H2PDSCreator.java` | Factory + `H2PParam` config enum + URL/DSType resolution |
| `H2PUtil.java` | Attribute classification (`AttrKind`) + column-type mapping + identifier quoting + `parseJdbcURL` |
| `H2PQueryFormatter.java` | `QueryMarker` → `WHERE` clause + parameter binding |
| `H2PExceptionHandler.java` | SQLState → `APIException` mapping |
| `H2PMetaManager.java` | Per-instance table registry (case-insensitive; backs `getStoreTables()`; cleared on reconfigure) |
| `H2PDialect.java` | **Dialect codec** for schemaless columns (H2 `varchar` vs Postgres `jsonb`) |

## Storage model (fully normalized — no binary blobs)

One table per `NVConfigEntity` type (table name = `nvce.getName()`). Every row has
`guid uuid PRIMARY KEY` (UUID v7). Each attribute maps by kind via `H2PUtil.classify(NVConfig)`:

| `AttrKind` | Storage |
|---|---|
| `SCALAR` | typed column — `varchar`/`integer`/`bigint`/`real`/`double precision`/`boolean`; reserved/reference-id → `uuid`; `Enum` → `varchar` (name); `Date` → `bigint`; `NVNumber` → `varchar` with a `int:/long:/float:/double:/bigdec:` type tag |
| `BLOB` | `bytea` column (`byte[]` field data) |
| `ENTITY_REF` (single `NVEntityReference`) | `uuid` column **+ `FOREIGN KEY` → child type's table** |
| `ENTITY_COLLECTION` (`NVEntityReferenceList`/`GetNameMap`/`ReferenceIDMap`) | **join table** `<table>__<attr>(parent_guid, child_guid, ord)` with FK constraints + `ON DELETE CASCADE` |
| `SCHEMALESS` (`NVGenericMap`, `NamedValue`, `NVStringList`, `NVIntList`, `NVEnumList`, …) | a JSON column — **`varchar` on H2, native `jsonb` on Postgres** (see dialect below) |

Referenced entities are stored as **their own rows** and resolved on read (`insert` is post-order:
children first so FK targets exist; read resolves single refs via `searchByID` and collections via
the join table). Referential integrity is DB-enforced. There is **no binary serialization** of
entities.

`ensureTable` also emits `CREATE INDEX IF NOT EXISTS` (portable to both engines) via `createIndex`:
join tables get `(parent_guid, ord)` + `(child_guid)`, `ENTITY_REF` columns get one, and non-unique
uuid scalars (`subject_guid`, reference ids) get one. **A FOREIGN KEY indexes only the referenced
side** — on PostgreSQL *and* H2 the referencing column needs its own index or every collection read
and cascade delete is a full scan. All composed identifiers (table names, join tables, FK
constraint and index names) go through `H2PUtil.sqlName`, which keeps names ≤ 63 bytes
(PostgreSQL's limit) by truncating + suffixing a CRC32 of the full name — deterministic and
collision-resistant where server-side truncation isn't. Attribute/column names are used as-is.

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
  (`MODE`/`CIPHER`/`IFEXISTS`/`AUTO_SERVER`/`DB_CLOSE_DELAY`).
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
`FILE_PASSWORD`, `IFEXISTS`, `AUTO_SERVER`, `OPTIONS`, `POOL_MAX_SIZE`/`POOL_MIN_IDLE` (HikariCP,
both engines), `MAX_SELECT_RESULTS` (opt-in SELECT cap), `ORPHAN_CLEANUP` (opt-in update-time
detached-child deletion).

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

- **`MAX_SELECT_RESULTS`** (`H2PParam`, opt-in): when set > 0 every entity SELECT is capped with
  `LIMIT n` — a safety valve against unbounded search materialization (off by default: full results).
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

Still open: no true API-level pagination (`search` still materializes all matches unless the valve is
set); `insert`/`update` each do an `existsByGuid` probe first; SecurityController integration (see
dedicated section below — work in progress).

## SecurityController integration — WORK IN PROGRESS (not yet supported)

**Current state:** the `SecurityController` from `APIConfigInfo` is used in exactly one place —
`associateNVEntityToSubjectGUID(nve, null)` on the insert/patch path (subject association only).

**Not implemented yet** (the reference behavior is `SyncMongoDS`):
- **Field encryption at rest** — `encryptValue(...)` on every write and `decryptValue(...)` on every
  read. Consequence today: entities whose fields are marked for encryption are stored in
  **plaintext** by this datastore. Do not point a store at data that relies on controller-managed
  field encryption.
- **Read ACL enforcement** — `isNVEntityAccessible(guid, subjectGUID, CRUD.READ)` in
  `search`/`batchSearch`/`userSearch`. Today only `userSearch`/`userSearchByID` scope by
  `subject_guid`; there is no per-entity accessibility check.

**Status:** actively being worked on. Until it lands, treat a configured `SecurityController` as
subject-association only; encryption/ACL semantics here are NOT equivalent to the Mongo stores.
When implementing, thread the controller through `bindColumn`/`setScalar` and the schemaless
encode/decode, add the accessibility check to the read paths, and mirror `SyncMongoDS` semantics
(SyncMongoDS.java:248, 605, 1708, 2617).

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

Criteria typing (`H2PQueryFormatter`): a null-valued `=`/`!=` `QueryMatch` renders as
`IS NULL`/`IS NOT NULL` (a bound null parameter can never match, and pgjdbc rejects untyped nulls);
`Date` values bind as epoch millis (columns are `bigint`); values against `Number`-typed (NVNumber)
attributes bind through `H2PUtil.encodeNumber` — equality only, range comparison on the tagged
varchar encoding is not possible.

## Running the tests

Tests run via the JUnit Platform launcher (surefire can't fetch its provider offline in this env).
Compile with `mvn -o -pl h2p-datastore -DskipTests test-compile`, then run the launcher with the
module's runtime classpath (`mvn -o -pl h2p-datastore dependency:build-classpath` + the
`junit-platform-*` jars) selecting package `io.xlogistx.datastore.h2p.test`, with `-ea`.

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
  hashing + a >63-char entity-type round trip, the `MAX_SELECT_RESULTS` valve, the
  `APIServiceProviderBase` lifecycle (touch/lookupProperty/isBusy), shell-entity cascade delete,
  shared-child keep-on-delete, and `ORPHAN_CLEANUP` on/off behavior.
- `H2PPostgresDataStoreTest` — **live PostgreSQL**; auto-skipped unless configured. `h2p.pg.url` is the
  **base endpoint** (no db); the test connects to the `postgres` maintenance db, **creates the target
  database if missing** (default `testpostgres`, override `-Dh2p.pg.db`), then runs the same scenarios
  (jsonb NVGenericMap/NamedValue, bytea, FK references, transactions) and asserts `getDSType()==POSTGRES`:
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

## Ground rules for future sessions
1. Keep the SQL PostgreSQL-portable; route every dialect difference through `H2PDialect`.
2. `currentDSType` is resolved once in `setAPIConfigInfo` — don't re-detect per call.
3. When adding an NV type, update `H2PUtil.classify` + `scalarColumnType` and the five paths in
   `H2PDataStore`: DDL (`ensureTable`), write (`bindColumn`), read (`buildEntity`/`setScalar`/
   `decodeSchemaless`), and — for entity refs — `insertChildren`/`syncJoins`/join resolution.
4. Referenced entities are separate rows with FKs; never re-introduce inline/binary embedding.
5. `guid` (UUID v7) is the single row identity; `referenceID` is legacy.
