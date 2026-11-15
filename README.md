# spelaiondb-plugin-libsql

libSQL for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop database
client: its server `sqld`, and Turso, which hosts it. The engine's driver, served to SpelaionDB
as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for libSQL (its ADR-058). It is built here, in its own repository,
and SpelaionDB pins by hash the exact release it was tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engine:** `libsql`, over Hrana over HTTP (version 3), with `hyper` and rustls. The token
  is the connection's password. Results are typed, and the explorer is read, by SpelaionDB's
  own SQLite library (`spelaion-sqlite-schema`), so libSQL shows exactly what SQLite shows.
- **What it does not do yet:** cancel a statement, read-only connections and agents (libSQL
  has no session barrier: a read-only token is the server's to enforce), bulk imports.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (libSQL's own image in Docker: without authentication, with tokens,
and behind TLS, with their keys). Then

```bash
SPELAION_TEST_LIBSQL=127.0.0.1:18091 SPELAION_TEST_LIBSQL_AUTH=127.0.0.1:18092 SPELAION_TEST_LIBSQL_TLS=127.0.0.1:18493 cargo test
```

runs SQLite's type zoo, the token, TLS and the protocol's conformance set against this
plugin's own program. Without the variables the tests that need a server are skipped, which
does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_LIBSQL=/path/to/target/debug/spelaiondb-plugin-libsql`, in SpelaionDB's
repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
