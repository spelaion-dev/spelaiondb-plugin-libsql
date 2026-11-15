//! libSQL (`sqld`, and Turso, which hosts it) for SpelaionDB (ADR-058 there): SQLite's SQL and
//! types behind a network service, reached with Hrana over HTTP.
//!
//! - `http.rs` opens the plugin's own connections to `connect_addr` when it is given (the local
//!   end of SpelaionDB's tunnel or proxy), never through a system proxy, with TLS verified
//!   against `host` (`tls.rs`, libpq's modes). The token is the connection's password.
//! - `hrana.rs` keeps the stream (its baton): a stream is the session, so a transaction can span
//!   statements; a stream the server let go loses the session (ADR-028) and nothing is re-run.
//! - Results are typed and the explorer is read exactly as SpelaionDB's SQLite driver does,
//!   with the same library (`spelaion-sqlite-schema`): the same types, the same catalog.
//!
//! Each connection's requests go one at a time through an actor (a tokio task). A statement
//! cannot be cancelled: closing the request would not stop it on the server, and would leave
//! the stream in doubt.

mod hrana;
mod http;
mod tls;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, LikeFilter, ObjectInfo, ReferencingKey, Result, SchemaColumn,
    SchemaInfo, SchemaObject, TableInfo, TransactionState,
};
use spelaion_sqlite_schema::typing::{is_dml, locate_name};
use spelaion_sqlite_schema::{ResultBuilder, ResultColumn, Row, catalog};
use tokio::sync::{mpsc, oneshot};

use crate::hrana::Stream;

/// The libSQL driver. `host`, `port` and TLS reach the server; the password is its token
/// (empty for a server without authentication). `database` is not used: a libSQL server serves
/// one database per address.
#[derive(Debug, Default, Clone)]
pub struct LibsqlDriver;

#[async_trait]
impl Driver for LibsqlDriver {
    fn engine(&self) -> &str {
        "libsql"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if let Some(key) = params.options.keys().next() {
            return Err(DriverError::Connection(format!(
                "{key} is not a setting of a libSQL connection"
            )));
        }
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(tls::ClientIdentity { cert, key }),
            _ => None,
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let endpoint = Arc::new(http::Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            params.password.clone(),
        ));
        let mut stream = Stream::new(http::Client::new(endpoint.clone()));
        // The first request checks the server, the TLS and the token, within the connect
        // timeout.
        match tokio::time::timeout(params.connect_timeout(), stream.execute("select 1", &[])).await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(scrubbed(e, &endpoint)),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        }
        let closed = Arc::new(AtomicBool::new(false));
        let (commands, inbox) = mpsc::unbounded_channel();
        tokio::spawn(actor(stream, endpoint, closed.clone(), inbox));
        Ok(Box::new(LibsqlConnection {
            commands,
            closed,
            cancel: Arc::new(NoCancel),
            capabilities: Capabilities {
                transactions: true,
                cancel: false,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: true,
            },
        }))
    }
}

/// The token taken out of an error's text.
fn scrubbed(e: DriverError, endpoint: &http::Endpoint) -> DriverError {
    match e {
        DriverError::Connection(m) => DriverError::Connection(endpoint.scrub(&m)),
        DriverError::Other(m) => DriverError::Other(endpoint.scrub(&m)),
        DriverError::Database {
            code,
            message,
            detail,
            hint,
            position,
        } => DriverError::Database {
            code,
            message: endpoint.scrub(&message),
            detail,
            hint,
            position,
        },
        other => other,
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Execute {
        sql: String,
        params: Vec<String>,
        batch_size: usize,
        events: mpsc::Sender<Result<ExecEvent>>,
    },
    Version(Reply<String>),
    Schemas(Reply<Vec<SchemaInfo>>),
    Objects(String, Reply<Vec<ObjectInfo>>),
    SchemaObjects(String, Reply<Vec<SchemaObject>>),
    Definition(Box<SchemaObject>, Reply<String>),
    RelationDefinition(String, String, Reply<String>),
    Describe(String, String, Reply<TableInfo>),
    Referencing(String, String, Reply<Vec<ReferencingKey>>),
    Columns(String, Reply<Vec<SchemaColumn>>),
    TransactionState(Reply<TransactionState>),
}

/// The connection's loop: one request at a time on the stream. Ends when the connection is
/// dropped, closing the stream on the server.
async fn actor(
    mut stream: Stream,
    endpoint: Arc<http::Endpoint>,
    closed: Arc<AtomicBool>,
    mut inbox: mpsc::UnboundedReceiver<Command>,
) {
    while let Some(command) = inbox.recv().await {
        match command {
            Command::Execute {
                sql,
                params,
                batch_size,
                events,
            } => {
                if let Err(e) = execute(&mut stream, &sql, &params, batch_size, &events).await {
                    let _ = events.send(Err(scrubbed(e, &endpoint))).await;
                }
            }
            Command::Version(reply) => {
                let result = query(&mut stream, "select sqlite_version()")
                    .await
                    .map(|rows| format!("libSQL (SQLite {})", catalog::names(&rows).concat()));
                let _ = reply.send(result);
            }
            Command::Schemas(reply) => {
                let result = query(&mut stream, &catalog::schemas_sql(&LikeFilter::default()))
                    .await
                    .map(|rows| catalog::schemas(&rows));
                let _ = reply.send(result);
            }
            Command::Objects(schema, reply) => {
                let _ = reply.send(list_objects(&mut stream, &schema).await);
            }
            Command::SchemaObjects(schema, reply) => {
                let result = query(
                    &mut stream,
                    &catalog::schema_objects_sql(&schema, &LikeFilter::default()),
                )
                .await
                .map(|rows| catalog::schema_objects(&schema, &rows));
                let _ = reply.send(result);
            }
            Command::Definition(object, reply) => {
                let result = match catalog::definition_sql(&object) {
                    Ok(sql) => match query(&mut stream, &sql).await {
                        Ok(rows) => catalog::definition(&object, &rows),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                };
                let _ = reply.send(result);
            }
            Command::RelationDefinition(schema, name, reply) => {
                let result = query(
                    &mut stream,
                    &catalog::relation_definition_sql(&schema, &name),
                )
                .await
                .and_then(|rows| catalog::relation_definition(&schema, &name, &rows));
                let _ = reply.send(result);
            }
            Command::Describe(schema, name, reply) => {
                let _ = reply.send(describe_table(&mut stream, &schema, &name).await);
            }
            Command::Referencing(schema, name, reply) => {
                let result = async {
                    let rows =
                        query(&mut stream, &catalog::referencing_keys_sql(&schema, &name)).await?;
                    let primary_key = catalog::names(
                        &query(&mut stream, &catalog::primary_key_sql(&schema, &name)).await?,
                    );
                    Ok(catalog::referencing_keys(&schema, &rows, &primary_key))
                }
                .await;
                let _ = reply.send(result);
            }
            Command::Columns(schema, reply) => {
                let result = query(&mut stream, &catalog::schema_columns_sql(&schema))
                    .await
                    .map(|rows| catalog::schema_columns(&rows));
                let _ = reply.send(result);
            }
            Command::TransactionState(reply) => {
                let result = stream.is_autocommit().await.map(|autocommit| {
                    if autocommit {
                        TransactionState::Idle
                    } else {
                        TransactionState::Open
                    }
                });
                let _ = reply.send(result);
            }
        }
        if stream.is_lost() {
            closed.store(true, Ordering::SeqCst);
        }
    }
    stream.close().await;
}

/// The rows of a catalog query.
async fn query(stream: &mut Stream, sql: &str) -> Result<Vec<Row>> {
    Ok(stream.execute(sql, &[]).await?.rows)
}

/// One statement: its columns and rows, typed as SpelaionDB's SQLite driver types them, in
/// batches of `batch_size`; or the rows it changed.
async fn execute(
    stream: &mut Stream,
    sql: &str,
    params: &[String],
    batch_size: usize,
    events: &mpsc::Sender<Result<ExecEvent>>,
) -> Result<()> {
    let result = stream
        .execute(sql, params)
        .await
        .map_err(|e| locate_name(e, sql))?;
    if result.columns.is_empty() {
        let _ = events
            .send(Ok(ExecEvent::Done {
                rows_affected: is_dml(sql).then_some(result.affected),
            }))
            .await;
        return Ok(());
    }
    let columns = result
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            declared: c.decltype.clone(),
            origin: None,
        })
        .collect();
    let batch_size = batch_size.max(1);
    let first = result.rows.len().min(batch_size);
    let mut builder = ResultBuilder::new(columns, &result.rows[..first])?;
    if events
        .send(Ok(ExecEvent::Columns(builder.schema())))
        .await
        .is_err()
    {
        return Ok(());
    }
    for row in &result.rows[first..] {
        if builder.pending() >= batch_size
            && events
                .send(Ok(ExecEvent::Batch(builder.take_batch()?)))
                .await
                .is_err()
        {
            return Ok(());
        }
        builder.push_row(row)?;
    }
    if builder.pending() > 0
        && events
            .send(Ok(ExecEvent::Batch(builder.take_batch()?)))
            .await
            .is_err()
    {
        return Ok(());
    }
    let _ = events
        .send(Ok(ExecEvent::Done {
            rows_affected: None,
        }))
        .await;
    Ok(())
}

async fn list_objects(stream: &mut Stream, schema: &str) -> Result<Vec<ObjectInfo>> {
    let rows = query(
        stream,
        &catalog::objects_sql(schema, &LikeFilter::default()),
    )
    .await?;
    Ok(catalog::objects(schema, &rows))
}

/// A table as the explorer shows it, from the same catalog queries as SQLite's driver.
async fn describe_table(stream: &mut Stream, schema: &str, name: &str) -> Result<TableInfo> {
    let object = list_objects(stream, schema)
        .await?
        .into_iter()
        .find(|o| o.name == name)
        .ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    let columns = query(stream, &catalog::columns_sql(schema, name)).await?;
    let indexes = query(stream, &catalog::indexes_sql(schema, name)).await?;
    let foreign_keys = query(stream, &catalog::foreign_keys_sql(schema, name)).await?;
    let unique = query(stream, &catalog::unique_columns_sql(schema, name)).await?;
    let (mut table, to_primary_keys) =
        catalog::table(object, &columns, &indexes, &foreign_keys, &unique);
    for i in to_primary_keys {
        let referenced = table.foreign_keys[i].referenced_table.clone();
        table.foreign_keys[i].referenced_columns =
            catalog::names(&query(stream, &catalog::primary_key_sql(schema, &referenced)).await?);
    }
    Ok(table)
}

struct LibsqlConnection {
    commands: mpsc::UnboundedSender<Command>,
    closed: Arc<AtomicBool>,
    cancel: Arc<NoCancel>,
    capabilities: Capabilities,
}

fn gone() -> DriverError {
    DriverError::Connection("the libSQL connection is closed".into())
}

impl LibsqlConnection {
    async fn ask<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(make(tx)).map_err(|_| gone())?;
        rx.await.map_err(|_| gone())?
    }

    fn run(&self, sql: &str, params: Vec<String>, opts: &ExecOptions) -> Result<EventStream> {
        let (events, rx) = mpsc::channel(4);
        self.commands
            .send(Command::Execute {
                sql: sql.to_string(),
                params,
                batch_size: opts.batch_size.max(1),
                events,
            })
            .map_err(|_| gone())?;
        Ok(Box::pin(futures_util::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|event| (event, rx)) },
        )))
    }
}

#[async_trait]
impl Connection for LibsqlConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        self.run(sql, Vec::new(), opts)
    }

    async fn execute_bound(
        &self,
        sql: &str,
        params: Vec<String>,
        opts: &ExecOptions,
    ) -> Result<EventStream> {
        self.run(sql, params, opts)
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        self.cancel.clone()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn server_version(&self) -> Result<String> {
        self.ask(Command::Version).await
    }

    async fn transaction_state(&self) -> Result<TransactionState> {
        self.ask(Command::TransactionState).await
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.ask(Command::Schemas).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::Objects(schema, reply)).await
    }

    async fn list_schema_objects(&self, schema: &str) -> Result<Vec<SchemaObject>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::SchemaObjects(schema, reply))
            .await
    }

    async fn object_definition(&self, object: &SchemaObject) -> Result<String> {
        let object = Box::new(object.clone());
        self.ask(|reply| Command::Definition(object, reply)).await
    }

    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        let (schema, name) = (schema.to_string(), name.to_string());
        self.ask(|reply| Command::RelationDefinition(schema, name, reply))
            .await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let (schema, name) = (schema.to_string(), name.to_string());
        self.ask(|reply| Command::Describe(schema, name, reply))
            .await
    }

    async fn list_referencing_keys(&self, schema: &str, name: &str) -> Result<Vec<ReferencingKey>> {
        let (schema, name) = (schema.to_string(), name.to_string());
        self.ask(|reply| Command::Referencing(schema, name, reply))
            .await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let schema = schema.to_string();
        self.ask(|reply| Command::Columns(schema, reply)).await
    }
}

/// libSQL statements cannot be cancelled (`Capabilities::cancel` is false).
struct NoCancel;

#[async_trait]
impl CancelHandle for NoCancel {
    async fn cancel(&self) -> Result<()> {
        Err(DriverError::Other(
            "a libSQL statement cannot be cancelled".into(),
        ))
    }
}
