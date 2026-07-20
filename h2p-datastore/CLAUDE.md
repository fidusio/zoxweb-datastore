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
| `H2PUtil.java` | Attribute classification (`AttrKind`) + column-type mapping + identifier quoting |
| `H2PQueryFormatter.java` | `QueryMarker` → `WHERE` clause + parameter binding |
| `H2PExceptionHandler.java` | SQLState → `APIException` mapping |
| `H2PMetaManager.java` | Per-instance table registry |
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
`FILE_PASSWORD`, `IFEXISTS`, `AUTO_SERVER`, `OPTIONS`, `POOL_MAX_SIZE`/`POOL_MIN_IDLE` (HikariCP, Postgres only).

```java
H2PDSCreator creator = new H2PDSCreator();

// H2 (in-memory, PostgreSQL dialect)
APIConfigInfo h2 = creator.toAPIConfigInfo("jdbc:h2:mem:mydb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");

// Native PostgreSQL — set the driver, pass a jdbc:postgresql URL (or host/port/db components)
APIConfigInfo pg = creator.toAPIConfigInfo("jdbc:postgresql://host:5432/db", "user", "pass");
pg.getProperties().build(H2PDSCreator.H2PParam.DRIVER.getName(), "org.postgresql.Driver");

H2PDataStore ds = new H2PDataStore();
ds.setAPIConfigInfo(pg);            // getDSType() -> POSTGRES, schemaless columns -> jsonb
```

## Connections / pooling
- **Native PostgreSQL is pooled via HikariCP** — `H2PDataStore.newConnection()` returns
  `pool().getConnection()` when `currentDSType == POSTGRES`. The `HikariDataSource` is built lazily
  from the resolved URL/user/password/driver, sized by `POOL_MAX_SIZE` (default 10) / `POOL_MIN_IDLE`
  (default 2), and closed by the datastore's `close()`.
- **H2 is not pooled** — it opens a fresh `DriverManager` connection per op (in-mem is ~free).
- A pooled `connection.close()` returns the connection to the pool, so `acquire()`, the per-op
  `close(...)`, `execDDL` (its own connection), and the ThreadLocal transaction machinery are all
  unchanged — only the physical connection *source* differs between engines. HikariCP is a compile
  dependency (`com.zaxxer:HikariCP:5.1.0`), only exercised on the Postgres path.

## Transactions / sequences / DEM
- Transactions: ambient `ThreadLocal<Connection>` (`autoCommit=false`), `begin/end/abort`. Data ops
  route through `acquire()`; **schema DDL runs out-of-band** on its own connection (`execDDL`) — on H2
  because DDL implicitly commits; on Postgres it's harmless (and still correct).
- Sequences: table-based `sys_long_sequence` (portable — no native `SEQUENCE`).
- DEM: portable UPDATE-then-INSERT upsert (no H2 `MERGE` / no Postgres `ON CONFLICT`).

## PostgreSQL-portability rules (keep it dual-target)
1. Use only types valid on both: `uuid`, `bytea`, `varchar`, `integer`, `bigint`, `real`,
   `double precision`, `boolean`, and `jsonb` (Postgres) / `varchar` (H2) **only via `H2PDialect`**.
2. No H2-only or Postgres-only SQL in the shared paths (no `MERGE`, no `ON CONFLICT`, no `SEQUENCE`).
3. Any new dialect divergence goes through `H2PDialect` keyed on `currentDSType` — never inline
   `if (postgres)` in the datastore.
4. `INFORMATION_SCHEMA.TABLES` checks filter `TABLE_TYPE='BASE TABLE'` and exclude
   `pg_catalog`/`information_schema` (Postgres has many schemas/views).
5. UUID via `setObject(uuid)` / `getObject(col, UUID.class)`; bytea via `setBytes`/`getBytes` — both
   pgjdbc-native.

Known low-priority gap: an equality `QueryMatch` with a **null** value binds an untyped null
(`col = ?`), which pgjdbc may reject; such criteria are semantically dead. Not exercised by tests.

## Running the tests

Tests run via the JUnit Platform launcher (surefire can't fetch its provider offline in this env).
Compile with `mvn -o -pl h2p-datastore -DskipTests test-compile`, then run the launcher with the
module's runtime classpath (`mvn -o -pl h2p-datastore dependency:build-classpath` + the
`junit-platform-*` jars) selecting package `io.xlogistx.datastore.h2p.test`, with `-ea`.

- `H2PDataStoreTest` — full suite on in-memory H2 (`MODE=PostgreSQL`). All green.
- `H2PPostgresDataStoreTest` — **live PostgreSQL**; auto-skipped unless configured. `h2p.pg.url` is the
  **base endpoint** (no db); the test connects to the `postgres` maintenance db, **creates the target
  database if missing** (default `testpostgres`, override `-Dh2p.pg.db`), then runs the same scenarios
  (jsonb NVGenericMap/NamedValue, bytea, FK references, transactions) and asserts `getDSType()==POSTGRES`:
  ```
  -Dh2p.pg.url=jdbc:postgresql://host:5432 -Dh2p.pg.user=… -Dh2p.pg.password=…
  ```

## Ground rules for future sessions
1. Keep the SQL PostgreSQL-portable; route every dialect difference through `H2PDialect`.
2. `currentDSType` is resolved once in `setAPIConfigInfo` — don't re-detect per call.
3. When adding an NV type, update `H2PUtil.classify` + `scalarColumnType` and the five paths in
   `H2PDataStore`: DDL (`ensureTable`), write (`bindColumn`), read (`buildEntity`/`setScalar`/
   `decodeSchemaless`), and — for entity refs — `insertChildren`/`syncJoins`/join resolution.
4. Referenced entities are separate rows with FKs; never re-introduce inline/binary embedding.
5. `guid` (UUID v7) is the single row identity; `referenceID` is legacy.
