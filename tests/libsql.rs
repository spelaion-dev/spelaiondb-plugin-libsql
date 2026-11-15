//! Tests against a real libSQL server (`ghcr.io/tursodatabase/libsql-server`).
//!
//! They need `SPELAION_TEST_LIBSQL=host:port` (no authentication), and for the token and TLS
//! `SPELAION_TEST_LIBSQL_AUTH=host:port` (tokens checked with `testing/keys/jwt.pub`) and
//! `SPELAION_TEST_LIBSQL_TLS=host:port` (HTTPS with a certificate for `libsql.spelaion.test`
//! signed by `testing/keys/ca.crt`). Locally: `ci/services.sh` and
//! `SPELAION_TEST_LIBSQL=127.0.0.1:18091 SPELAION_TEST_LIBSQL_AUTH=127.0.0.1:18092 SPELAION_TEST_LIBSQL_TLS=127.0.0.1:18493 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, Secret, TlsMode, TransactionState, Value,
};
use spelaion_driver_libsql::LibsqlDriver;

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("port")))
}

fn params(host: String, port: u16) -> ConnectParams {
    ConnectParams {
        host,
        port,
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn connect() -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_LIBSQL")?;
    Some(
        LibsqlDriver
            .connect(&params(host, port))
            .await
            .expect("connects"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn exec(conn: &dyn Connection, sql: &str) {
    run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
}

/// Header (name, logical type, native type) and the rows as text.
async fn table(
    conn: &dyn Connection,
    sql: &str,
) -> (Vec<(String, LogicalType, String)>, Vec<Vec<String>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut header = Vec::new();
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(schema) => {
                header = schema
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.logical, c.native_type.clone()))
                    .collect();
            }
            ExecEvent::Batch(batch) => {
                for r in 0..batch.num_rows() {
                    rows.push(
                        header
                            .iter()
                            .enumerate()
                            .map(|(i, (_, logical, _))| {
                                Value::from_array(batch.column(i).as_ref(), r, *logical)
                                    .unwrap()
                                    .to_string()
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (header, rows)
}

/// A table name of this test run, so tests sharing the server do not meet.
fn name(base: &str) -> String {
    format!("{base}_{}", std::process::id())
}

/// SpelaionDB's SQLite type zoo (`spelaion-driver-sqlite`'s `tests/sqlite.rs`), through
/// libSQL: the same table gives exactly the same types and cells, since both are typed by
/// `spelaion-sqlite-schema` (ADR-058).
#[tokio::test]
async fn sqlites_type_zoo_types_alike() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let zoo = name("zoo");
    exec(conn, &format!("drop table if exists {zoo}")).await;
    exec(
        conn,
        &format!(
            "create table {zoo} (
            i integer, big bigint, r real, d double precision, n numeric, dec decimal(10, 2),
            t text, v varchar(10), b blob, flag boolean, day date, untyped
         )"
        ),
    )
    .await;
    exec(
        conn,
        &format!(
            "insert into {zoo} values (1, 9223372036854775807, 1.5, 2.0, 7, 12.5, 'text ñ €', 'v', x'deadbeef', 1, '2026-01-31', null)"
        ),
    )
    .await;
    exec(
        conn,
        &format!("insert into {zoo} values (null, null, null, null, null, null, null, null, null, null, null, 'x')"),
    )
    .await;
    let (header, rows) = table(conn, &format!("select * from {zoo}")).await;
    let mut out = String::new();
    for (i, (name, logical, native)) in header.iter().enumerate() {
        out.push_str(&format!(
            "{name:<8} {:<8} {native:<18} {:<26} {}\n",
            logical.as_str(),
            rows[0][i],
            rows[1][i]
        ));
    }
    // The snapshot of SQLite's own test, as it is in SpelaionDB's repository.
    let expected = "\
i        int      INTEGER            1                          NULL
big      int      bigint             9223372036854775807        NULL
r        float    REAL               1.5                        NULL
d        float    double precision   2                          NULL
n        int      numeric            7                          NULL
dec      float    decimal(10, 2)     12.5                       NULL
t        text     TEXT               text ñ €                   NULL
v        text     varchar(10)        v                          NULL
b        bytes    BLOB               \\xdeadbeef                 NULL
flag     int      boolean            1                          NULL
day      text     date               2026-01-31                 NULL
untyped  text     TEXT               NULL                       x
";
    assert_eq!(out, expected);
    exec(conn, &format!("drop table {zoo}")).await;
}

#[tokio::test]
async fn expressions_without_a_declared_type() {
    let Some(conn) = connect().await else { return };
    let (header, rows) = table(
        conn.as_ref(),
        "select 1 + 1 as n, 1.0 / 4 as r, 'a' || 'b' as t, x'01' as b, null as z",
    )
    .await;
    let types: Vec<_> = header
        .iter()
        .map(|(n, l, native)| (n.as_str(), *l, native.as_str()))
        .collect();
    assert_eq!(
        types,
        [
            ("n", LogicalType::Int, "INTEGER"),
            ("r", LogicalType::Float, "REAL"),
            ("t", LogicalType::Text, "TEXT"),
            ("b", LogicalType::Bytes, "BLOB"),
            ("z", LogicalType::Bytes, "NULL"),
        ]
    );
    assert_eq!(rows[0], ["2", "0.25", "ab", "\\x01", "NULL"]);
}

#[tokio::test]
async fn errors_are_the_servers_and_the_session_survives() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    match run(conn, "select * from a_table_that_is_not_there").await {
        Err(DriverError::Database { message, .. }) => {
            assert!(message.contains("a_table_that_is_not_there"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    let (_, rows) = table(conn, "select 'still here'").await;
    assert_eq!(rows[0][0], "still here");
    assert!(!conn.is_closed());
}

/// A stream is the session: a transaction spans requests, and the state comes from the
/// server (ADR-031).
#[tokio::test]
async fn a_transaction_spans_requests_on_the_stream() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let t = name("tx");
    exec(conn, &format!("drop table if exists {t}")).await;
    exec(conn, &format!("create table {t} (id integer primary key)")).await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    exec(conn, "begin").await;
    let events = run(conn, &format!("insert into {t} values (1), (2)"))
        .await
        .unwrap();
    assert!(matches!(
        events.last(),
        Some(ExecEvent::Done {
            rows_affected: Some(2)
        })
    ));
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Open
    );
    exec(conn, "rollback").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    let (_, rows) = table(conn, &format!("select count(*) from {t}")).await;
    assert_eq!(rows[0][0], "0");
    exec(conn, &format!("drop table {t}")).await;
}

#[tokio::test]
async fn bound_values_and_batches() {
    let Some(conn) = connect().await else { return };
    let events: Vec<ExecEvent> = conn
        .execute_bound(
            "select ? || '-' || ?",
            vec!["it's".into(), "') or 1=1 --".into()],
            &ExecOptions::default(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let text = events.iter().find_map(|e| match e {
        ExecEvent::Batch(b) => Value::from_array(b.column(0).as_ref(), 0, LogicalType::Text),
        _ => None,
    });
    assert_eq!(text.unwrap().to_string(), "it's-') or 1=1 --");

    let events: Vec<ExecEvent> = conn
        .execute(
            "with recursive n(i) as (select 1 union all select i + 1 from n where i < 2500) select i from n",
            &ExecOptions {
                batch_size: 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let sizes: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(sizes, [1000, 1000, 500]);
}

#[tokio::test]
async fn the_explorer_reads_sqlites_catalog() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    let (parent, child, view) = (name("parent"), name("child"), name("view"));
    exec(conn, &format!("drop view if exists {view}")).await;
    exec(conn, &format!("drop table if exists {child}")).await;
    exec(conn, &format!("drop table if exists {parent}")).await;
    exec(
        conn,
        &format!("create table {parent} (id integer primary key, code text not null unique)"),
    )
    .await;
    exec(
        conn,
        &format!("create table {child} (id integer primary key, parent_id integer references {parent} (id))"),
    )
    .await;
    exec(
        conn,
        &format!("create view {view} as select id from {parent}"),
    )
    .await;

    let schemas = conn.list_schemas().await.unwrap();
    assert!(schemas.iter().any(|s| s.name == "main"), "{schemas:?}");
    let objects = conn.list_objects("main").await.unwrap();
    let kind = |n: &str| objects.iter().find(|o| o.name == n).map(|o| o.kind);
    assert_eq!(kind(&parent), Some(ObjectKind::Table));
    assert_eq!(kind(&view), Some(ObjectKind::View));

    let table = conn.describe_table("main", &parent).await.unwrap();
    assert_eq!(table.primary_key, ["id"]);
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "code" && !c.nullable)
    );
    let child_info = conn.describe_table("main", &child).await.unwrap();
    assert_eq!(child_info.foreign_keys[0].referenced_table, parent);
    assert_eq!(child_info.foreign_keys[0].referenced_columns, ["id"]);
    let referencing = conn.list_referencing_keys("main", &parent).await.unwrap();
    assert_eq!(referencing[0].table, child);
    let columns = conn.list_columns("main").await.unwrap();
    assert!(
        columns
            .iter()
            .any(|c| c.table == child && c.name == "parent_id")
    );
    let definition = conn.relation_definition("main", &view).await.unwrap();
    assert!(
        definition.to_lowercase().contains("create view"),
        "{definition}"
    );

    exec(conn, &format!("drop view {view}")).await;
    exec(conn, &format!("drop table {child}")).await;
    exec(conn, &format!("drop table {parent}")).await;
}

fn keys() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys")
}

/// The token is the connection's password: without it the server refuses, with it the
/// session works, and no error ever repeats it.
#[tokio::test]
async fn the_token_is_the_password_and_never_shown() {
    let Some((host, port)) = addr("SPELAION_TEST_LIBSQL_AUTH") else {
        return;
    };
    let token = std::fs::read_to_string(keys().join("token.jwt")).expect("testing/gen-keys.sh");
    let refused = LibsqlDriver
        .connect(&params(host.clone(), port))
        .await
        .err()
        .expect("no token, no session");
    assert!(refused.to_string().contains("401"), "{refused}");

    let mut wrong = params(host.clone(), port);
    wrong.password = Some(Secret::new(format!("{token}x")));
    let refused = LibsqlDriver
        .connect(&wrong)
        .await
        .err()
        .expect("a bad token");
    assert!(!refused.to_string().contains(&token), "{refused}");

    let mut good = params(host, port);
    good.password = Some(Secret::new(token.trim()));
    let conn = LibsqlDriver.connect(&good).await.expect("with the token");
    let (_, rows) = table(conn.as_ref(), "select 'signed in'").await;
    assert_eq!(rows[0][0], "signed in");
}

/// TLS to 127.0.0.1, the address a tunnel gives, with the certificate checked against the
/// connection's host name: that name works, any other is refused.
#[tokio::test]
async fn tls_is_verified_against_the_host_name_not_the_address() {
    let Some((host, port)) = addr("SPELAION_TEST_LIBSQL_TLS") else {
        return;
    };
    let tls = |name: &str| ConnectParams {
        host: name.into(),
        port,
        connect_addr: Some(format!("{host}:{port}").parse().unwrap()),
        tls: TlsMode::VerifyFull,
        ca_certificate: Some(keys().join("ca.crt")),
        ..Default::default()
    };
    let conn = LibsqlDriver
        .connect(&tls("libsql.spelaion.test"))
        .await
        .expect("verify-full against the name");
    let (_, rows) = table(conn.as_ref(), "select 'over tls'").await;
    assert_eq!(rows[0][0], "over tls");
    let refused = LibsqlDriver
        .connect(&tls("another.spelaion.test"))
        .await
        .err()
        .expect("another name is refused");
    assert!(refused.to_string().contains("TLS"), "{refused}");
}
