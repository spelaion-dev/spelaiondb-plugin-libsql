# SpelaionDB's libSQL plugin — rules for agents

libSQL for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver, and a
small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Hrana over HTTP (SpelaionDB's ADR-058):
  - `http.rs` opens its own connections with `hyper`: to `connect_addr` when it is given (the
    local end of SpelaionDB's tunnel or proxy), never through a system proxy, with TLS verified
    against `host` (`tls.rs`, libpq's modes, as in the ClickHouse plugin). The token travels
    in the `Authorization` header, never in the address, and is taken out of every error.
  - `hrana.rs`: the stream. Its baton is kept, so a stream is the session and a transaction
    spans statements. A stream the server let go (expired, restarted) loses the session
    (ADR-028): nothing is sent on it again and nothing is re-run. Values arrive with their
    SQLite storage class.
  - `lib.rs`: each connection's requests go one at a time through an actor (a tokio task).
    Results are typed by `spelaion-sqlite-schema`'s `ResultBuilder` and the explorer is read
    with its `catalog`, exactly as SpelaionDB's SQLite driver does: never a copy of either here.
    No cancel (closing a request would not stop the statement on the server, and would leave
    the stream in doubt), no `insert_rows`.
- `src/bin/spelaiondb-plugin-libsql.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/libsql.rs`: SQLite's type zoo (the snapshot of SpelaionDB's SQLite driver, which must
  stay identical), errors, a transaction across requests, bound values, batches, the explorer,
  the token and TLS checked against the host name, against real servers.
  `tests/conformance.rs`: SpelaionDB's conformance set against this program, and `plugin.toml`
  checked against what the program says of itself.
- `testing/` and `ci/services.sh`: the test servers, libSQL's own image (`sqld` 0.24.33):
  without authentication on 18091, checking tokens on 18092 (`testing/gen-keys.sh` makes the
  Ed25519 key and signs a token with `openssl`), and behind a TLS proxy on 18493 with a
  certificate for `libsql.spelaion.test`.
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent and introspects.
So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's repository, "What a
plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`.
- **The token arrives in `connect` and is written nowhere**: not to a log, an error, a file,
  the environment or a request's address.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a session is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, opens no tunnel, and
  reads no proxy from the environment.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk`, `spelaion-sqlite-schema` and, for the tests,
`spelaion-plugin-host` are git dependencies on one tag of SpelaionDB's repository
(`plugin-sdk-vX.Y.Z`). A change to SQLite's typing or catalog is made there first, in the
library and SQLite's driver together, then reaches this plugin by moving the tag.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit). **Nothing is signed and no secret lives here**:
a release reaches users only when SpelaionDB's repository pins its hashes in a pull request of
its own (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against real servers (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_LIBSQL=127.0.0.1:18091 SPELAION_TEST_LIBSQL_AUTH=127.0.0.1:18092 SPELAION_TEST_LIBSQL_TLS=127.0.0.1:18493 cargo test`.
- `cargo deny check` if dependencies change.
- If something could not be tested, say so.
