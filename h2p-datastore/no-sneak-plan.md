# Plan: move no-sneak to the subject login (Shiro manager, keystore, SubjectSwap)

Status: **plan only, 2026-10-03. Nothing in no-sneak has been changed, compiled or run.**

> **Update 2026-10-05 (user: "replace it in no-sneak with ShiroDSDomainSecurityManager", then
> "the appid for nosneak is xlogistx.com-nosneak").** Built, uncommitted, in the no-sneak repo:
> `Main.createDomainSecManager` and `NoSneakUtil` build `new ShiroDSDomainSecurityManager(store)`;
> `Session` has `NO_SNEAK_DOMAIN_ID` = `xlogistx.com`, `NO_SNEAK_APP_ID` = `nosneak` and scopes
> the keys it issues to that app (it was `xlogistx.io-nosneak`); the 8 `no-sneak-app` session tests
> build their manager through a new `TestSecurity` helper (throw-away `SecretStore` master key in
> the `KeyMakerProvider`, `MockAPIDataStore` with that key maker on its configuration, catalog
> seeded, app `xlogistx.com-nosneak` created); the four cases that logged in with an API key now
> assert the refusal (API keys never log in); `addRejectsBlank` accepts the Shiro manager's
> "Invalid principal ID" wording. `no-sneak-core` and `no-sneak-app` compile. Tests (standalone
> runner, offline): Register 3/3, Address 8/8, AssistantStorage 9/9, ChangePassword 6/6,
> Identifier 6/6, Profile 4/4, SessionAICredentialSource 7/7, APIKey 18/21. **Open:** the 3
> failing cases (`createStoresDomainAndAppIDWhenBothProvided`, `createNormalizesDomainAndAppIDCase`,
> `externalKeyStoresMetadataAndMarksExternal`) store a third-party key with the vendor's domain and
> app (`example.com-myapp123`) in the key's app field, which the Shiro manager treats as an
> app-model scope that must exist - decision pending (keep the vendor in the key's properties, or
> something else). Still true: the running app opens its store with no controller and no key maker
> (D1) and nothing creates the app `xlogistx.com-nosneak` in a real store yet (D4); the plan below
> is otherwise unchanged.

## 1. Goal

no-sneak logs a person in as a Shiro subject through `ShiroDSDomainSecurityManager`, on a store
that is opened with a keystore (master key in the KeyMaker, security controller), and every
datastore operation runs as that subject. API keys stop being a login and become what they now are
everywhere else: the subject's sealed credentials for third-party APIs.

## 2. What no-sneak does today (read in the source)

| Area | Today | File |
|---|---|---|
| Manager | `new DomainSecurityManagerDefault()` (core's older manager, being replaced) | `no-sneak-app/.../Main.java` `createDomainSecManager`; also `no-sneak-core/.../NoSneakUtil.java` (with a Mongo store) |
| Type held | the core `DomainSecurityManager` interface | `no-sneak-app/.../ui/utility/Session.java` |
| Store | H2 file through `H2PDSCreator`, no `SecurityController`, no `KeyMaker` | `Main.createDataStore` |
| First-run setup | panel asks for location, database user, database password, encryption password | `DataStoreSetupPanel` |
| Password login | `domainSecurityManager.login(principal, password)` — verifies only, binds nothing | `Session.loginUsernamePassword` |
| API-key login | `domainSecurityManager.loginApiKey(key)` | `Session.loginAPIKey`, `LoginPanel` |
| Sign-up | `createSubjectID(principal, BCrypt hash)` with nobody logged in | `Session.registerUsernamePassword` |
| Own data | `Session` calls the datastore directly for `ReportContent` and `ProbeContent` | `Session.saveScanResult`, `getAllProbes`, … |
| Third-party keys | stored as `SubjectAPIKey`, read back as `CredentialInfo.Type.API_KEY` for the AI assistant | `Session.storeAPIKey`, `SessionAICredentialSource`, `SubjectPanel` |
| Threads | Swing. UI work goes through `BackgroundTask.run/runCatching` (io-xlogistx `gui-audio`), which runs it on a `SwingWorker` thread; the login itself runs there too | `LoginPanel`, `ScanPanel` |
| Tests | `no-sneak-app` round-trip tests build `DomainSecurityManagerDefault` on `MockAPIDataStore` | `RegisterRoundTripTest`, `APIKeyRoundTripTest`, … |
| Dependencies | `h2p-datastore`, `xlogistx-shiro`, `xlogistx-opsec` are there; `xlogistx-shiro-ds` is not | `no-sneak-app/pom.xml` |
| Working tree | uncommitted changes of the user in `pom.xml`, `no-sneak-app/pom.xml`, `Session.java` (1 line), `ai-assistant` | `git status` |

Consequence already in force: as soon as no-sneak is built against the current `h2p-datastore`, its
store refuses to connect (no database without the master key). That is a reading of
`H2PDataStore.requireMasterKey`, not a run of no-sneak.

## 3. What has to change

1. **Store** — opened with the keystore: `KeyMakerProvider` loaded with the master key,
   `ShiroSecurityController` and the key maker on the `APIConfigInfo`.
2. **Manager** — `ShiroDSDomainSecurityManager` instead of `DomainSecurityManagerDefault`.
3. **Login** — the subject login. Because the login runs on a `SwingWorker` thread and later work
   runs on other worker threads, `Session` must not rely on a thread-bound login: it logs in
   **unbound**, keeps the Shiro `Subject`, and wraps each of its own operations in a
   `SubjectSwap`. `Session` is the single place every panel goes through, so the swap lives
   there and `BackgroundTask` (a shared library class) is not touched.
4. **API keys** — the API-key login goes away; third-party keys stay.
5. **Tests** — move off `DomainSecurityManagerDefault` + mock store.

Missing piece on the manager side: there is an unbound login for a JWT
(`loginUnboundSubjectJWT`) but none for a password. A `loginUnboundSubject(principal, password,
domainID, appID)` is needed (small: the bound `loginSubject` already builds on the same private
method).

## 4. Decisions needed from the user before any code

| # | Question | Why it matters |
|---|---|---|
| D1 | **Keystore on a desktop install.** Created at first-run setup next to the database, holding a generated master key and the `db.*` settings? Protected by which password — the existing "Encryption Password" field, or a new one? | Every start of no-sneak must load it before the store opens. |
| D2 | **Is no-sneak an app in the app model** (for example `xlogistx.io-nosneak`, with its registrar and its `app_user` role, logins scoped to it), or do its users live in the common app with no scope? | Decides how sign-up works (D3) and what `domainID` / `appID` the login passes. |
| D3 | **Sign-up.** Today anyone at the login screen creates an account directly. Options: keep a direct `createSubjectID` (manager enforcement off, as the admin tool runs), or the registrar path (`loginUnboundSubjectJWT` + `SubjectSwap` + `registerSubject`), which needs D2 = an app. | The registrar path is what was just built and tested; the direct path is what no-sneak does now. |
| D4 | **Bootstrap of a local store.** A new local database needs the catalog and the common app; does it also get a super-admin, and/or the `local-admin` account the user described on 2026-09-16 for offline H2 stores? Created by the setup screen or by the admin tool? | Nothing can be granted before the catalog exists. |
| D5 | **Keys generated inside no-sneak** (`Session.generateAPIKey`, the non-external branch of `storeAPIKey`, `rotateAPIKey`). They existed for the API-key login. Drop them, or turn them into signing keys (`SYMMETRIC_KEY`) for a JWT login? | The user said the system logs in with JWT; a desktop login screen has no obvious JWT field. |
| D6 | **Existing no-sneak databases.** Subjects there have no subject key, keys are stored in clear, there is no catalog. Start fresh, or write a migration? | A migration is a project of its own. |
| D7 | **Password hash.** no-sneak registers with BCrypt; `registerSubject` uses ARGON2. Keep BCrypt for existing flows or move to ARGON2? | Both verify; it is a consistency choice. |
| D8 | **Test target.** The rule for the shiro-ds tests is `test.store` + persistent PostgreSQL. no-sneak runs on a local H2 file: a test keystore of its own with a persistent H2 file, or `test.store` and PostgreSQL? | Sets up phase 1. |
| D9 | **`no-sneak-core` `NoSneakUtil`** (Mongo store + `DomainSecurityManagerDefault`): in scope, or left for later since Mongo is on standby? | Scope. |

## 5. Phases — test first, then replace (same order as SubjectSwap)

**Phase 0 — decisions D1–D9.**

**Phase 1 — proof test, no production code in no-sneak changed.** A new test in `no-sneak-app`
(dependency `xlogistx-shiro-ds` added for it) that does, on the target chosen in D8, what the app
will do:
1. open the store through the keystore; build `ShiroDSDomainSecurityManager`;
2. sign a user up the way D3 says; log in unbound; keep the `Subject`;
3. on worker threads from a pool (as `SwingWorker` does), run inside `SubjectSwap`: save and read a
   `ReportContent` and a `ProbeContent`, store a third-party key and read it back in clear, change
   the password, add and remove an identifier;
4. a second user: sees none of the first user's rows or keys;
5. after logout the kept subject can do nothing; no worker thread is left with a subject bound;
6. rerun on the persisted data.
Realm built in code first, then from `shiro.ini`, if the user wants both again.

**Phase 2 — manager method.** `loginUnboundSubject(principal, password, domainID, appID)` in
`ShiroDSDomainSecurityManager`, with a test in the manager suite.

**Phase 3 — startup.** `Main.createDataStore` / `createDomainSecManager` and
`DataStoreSetupPanel`: keystore creation and loading (D1), store opened with controller + key
maker, Shiro manager, local bootstrap (D4).

**Phase 4 — `Session`.** Holds the Shiro `Subject`; `loginUsernamePassword` = unbound login;
`logout` = `subject.logout()`; each operation that touches the manager or the datastore runs in a
`SubjectSwap`; `loginAPIKey` removed; sign-up per D3; internal keys per D5. `Session`'s field
becomes the Shiro manager type (the subject login is not on the core interface).

**Phase 5 — UI.** `LoginPanel`: the API-key login control removed (or replaced per D5). The
API-key panel in `SubjectPanel` keeps working as the third-party key list.

**Phase 6 — tests.** The `no-sneak-app` round-trip tests move to the new setup;
`APIKeyRoundTripTest` loses its login cases and keeps the third-party key cases;
`DataStoreSetupFlowTest` covers the keystore creation.

**Phase 7 — run and verify.** All suites; then the rows: every subject has a subject key, every
key is sealed, each user's content carries its owner.

## 6. Risks (from reading, none of them run)

- **Pooled worker threads.** `SwingWorker` reuses threads. A bound login would leave a subject on
  a pool thread; the unbound login + `SubjectSwap` inside `Session` avoids that. A thread started
  while a swap is active inherits the subject (Shiro's thread context is inheritable).
- **Owner stamping.** With the access check on, a row `Session` inserts without an owner gets the
  bound subject as owner, and `ds.userSearch(subjectGUID, …)` already filters by it; rows written
  by an older no-sneak have whatever owner they had (D6).
- **Queries by an encrypted value** are refused by the store. no-sneak must not look a key up by
  its secret; the code that does so today is the API-key login, which goes away.
- **The AI assistant** reads the third-party key through `Session` on its own executor threads:
  those reads must go through the swap too.
- **The user's uncommitted work** in the no-sneak tree is left alone; changes are made on top of it.

## 7. Out of scope unless the user says otherwise

`no-sneak-net`, `ai-model`, scan logic, the Mongo path in `no-sneak-core` (D9), a data migration
(D6), committing anything.
