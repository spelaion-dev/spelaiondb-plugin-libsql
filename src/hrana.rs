//! Hrana over HTTP (version 3), the protocol of libSQL's server: a pipeline of requests in one
//! `POST /v3/pipeline`, answered with one result each. The `baton` of each answer keeps the
//! stream open for the next request: a stream is the session (ADR-058). Once a stream is gone
//! (it expired, or the server restarted), its session is lost and never replaced (ADR-028).
//!
//! Every value of a row comes with its SQLite storage class, which is what SpelaionDB's SQLite
//! typing (`spelaion-sqlite-schema`) is built on.

use serde_json::{Value as Json, json};
use spelaion_driver_api::{DriverError, Result};
use spelaion_sqlite_schema::SqlValue;

use crate::http::Client;

/// One column of a result: its name and its declared type, when it comes from a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub decltype: Option<String>,
}

/// The result of one statement.
#[derive(Debug, Default)]
pub struct StatementResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<SqlValue>>,
    pub affected: u64,
}

/// A stream: the session's requests go through it, one at a time.
pub struct Stream {
    client: Client,
    baton: Option<String>,
    /// Set once the server said the stream is gone: nothing more is sent on it.
    lost: bool,
}

impl Stream {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            baton: None,
            lost: false,
        }
    }

    pub fn is_lost(&self) -> bool {
        self.lost
    }

    /// Sends `requests` on the stream and returns their results. A failure that leaves the
    /// stream in doubt (the request lost on the way, the stream expired) loses the session.
    async fn pipeline(&mut self, requests: Vec<Json>) -> Result<Vec<Json>> {
        if self.lost {
            return Err(gone());
        }
        let body = json!({ "baton": self.baton, "requests": requests }).to_string();
        let answer = match self.client.post("/v3/pipeline", body).await {
            Ok(answer) => answer,
            Err(DriverError::Other(text)) if text.starts_with("hrana:") => {
                self.lost = true;
                return Err(stream_error(&text["hrana:".len()..]));
            }
            Err(e) => {
                if matches!(e, DriverError::Connection(_)) {
                    self.lost = true;
                }
                return Err(e);
            }
        };
        let answer: Json = serde_json::from_slice(&answer)
            .map_err(|e| DriverError::Other(format!("the server's answer is not JSON: {e}")))?;
        self.baton = answer["baton"].as_str().map(str::to_string);
        if self.baton.is_none() {
            // The server closed the stream: a transaction cannot span it any more.
            self.lost = true;
        }
        Ok(answer["results"].as_array().cloned().unwrap_or_default())
    }

    /// Runs one statement with its positional arguments bound as text.
    pub async fn execute(&mut self, sql: &str, args: &[String]) -> Result<StatementResult> {
        let args: Vec<Json> = args
            .iter()
            .map(|a| json!({ "type": "text", "value": a }))
            .collect();
        let request = json!({
            "type": "execute",
            "stmt": { "sql": sql, "args": args, "want_rows": true },
        });
        let mut results = self.pipeline(vec![request]).await?;
        let result = results.pop().ok_or_else(|| {
            DriverError::Other("the server answered no result for the statement".into())
        })?;
        statement_result(&result)
    }

    /// Whether the stream is outside a transaction (SQLite's autocommit).
    pub async fn is_autocommit(&mut self) -> Result<bool> {
        let mut results = self
            .pipeline(vec![json!({ "type": "get_autocommit" })])
            .await?;
        let result = results
            .pop()
            .ok_or_else(|| DriverError::Other("the server answered nothing".into()))?;
        check_ok(&result)?;
        result["response"]["is_autocommit"]
            .as_bool()
            .ok_or_else(|| {
                DriverError::Other("the server did not say whether a transaction is open".into())
            })
    }

    /// Closes the stream on the server, if it is open. Best effort: a stream left open expires.
    pub async fn close(&mut self) {
        if self.baton.is_some() && !self.lost {
            let _ = self.pipeline(vec![json!({ "type": "close" })]).await;
        }
        self.lost = true;
    }
}

fn gone() -> DriverError {
    DriverError::Connection(
        "the libSQL stream is gone (it expired or the server restarted): open the connection again"
            .into(),
    )
}

/// An error about the stream itself (an expired or unknown baton): the session is lost.
fn stream_error(text: &str) -> DriverError {
    let parsed: Json = serde_json::from_str(text).unwrap_or(Json::Null);
    let message = parsed["message"]
        .as_str()
        .or_else(|| parsed["error"].as_str())
        .unwrap_or(text);
    DriverError::Connection(format!("the libSQL stream is gone: {message}"))
}

fn check_ok(result: &Json) -> Result<()> {
    match result["type"].as_str() {
        Some("ok") => Ok(()),
        Some("error") => {
            let error = &result["error"];
            Err(DriverError::Database {
                code: error["code"].as_str().map(str::to_string),
                message: error["message"]
                    .as_str()
                    .unwrap_or("the statement failed")
                    .to_string(),
                detail: None,
                hint: None,
                position: None,
            })
        }
        other => Err(DriverError::Other(format!(
            "the server answered a result of type {other:?}"
        ))),
    }
}

/// One `execute` result as columns and rows of storage-class values.
fn statement_result(result: &Json) -> Result<StatementResult> {
    check_ok(result)?;
    let body = &result["response"]["result"];
    let columns = body["cols"]
        .as_array()
        .map(|cols| {
            cols.iter()
                .map(|c| Column {
                    name: c["name"].as_str().unwrap_or_default().to_string(),
                    decltype: c["decltype"].as_str().map(str::to_string),
                })
                .collect()
        })
        .unwrap_or_default();
    let mut rows = Vec::new();
    for row in body["rows"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let cells = row.as_array().map(Vec::as_slice).unwrap_or_default();
        rows.push(cells.iter().map(value).collect::<Result<Vec<_>>>()?);
    }
    Ok(StatementResult {
        columns,
        rows,
        affected: body["affected_row_count"].as_u64().unwrap_or(0),
    })
}

/// A Hrana value as SQLite stores it.
pub(crate) fn value(v: &Json) -> Result<SqlValue> {
    let bad = |what: &str| DriverError::Decode {
        type_name: v["type"].as_str().unwrap_or("?").to_string(),
        message: format!("{what}: {v}"),
    };
    Ok(match v["type"].as_str() {
        Some("null") => SqlValue::Null,
        // Integers travel as strings: a JSON number would lose precision past 2^53.
        Some("integer") => SqlValue::Integer(
            v["value"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["value"].as_i64())
                .ok_or_else(|| bad("not an integer"))?,
        ),
        Some("float") => SqlValue::Real(v["value"].as_f64().ok_or_else(|| bad("not a number"))?),
        Some("text") => SqlValue::Text(
            v["value"]
                .as_str()
                .ok_or_else(|| bad("not text"))?
                .to_string(),
        ),
        Some("blob") => SqlValue::Blob(
            base64_decode(v["base64"].as_str().ok_or_else(|| bad("no base64"))?)
                .ok_or_else(|| bad("not base64"))?,
        ),
        _ => return Err(bad("unknown value type")),
    })
}

/// Standard base64, with or without its `=` padding (the server leaves it out).
pub(crate) fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0;
    for c in text.trim_end_matches('=').bytes() {
        let n = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        };
        buffer = (buffer << 6) | u32::from(n);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_with_and_without_padding() {
        assert_eq!(base64_decode("/w"), Some(vec![0xff]));
        assert_eq!(base64_decode("/w=="), Some(vec![0xff]));
        assert_eq!(base64_decode("aGVsbG8"), Some(b"hello".to_vec()));
        assert_eq!(base64_decode(""), Some(vec![]));
        assert_eq!(base64_decode("a b"), None);
    }

    #[test]
    fn values_keep_their_storage_class() {
        assert_eq!(
            value(&json!({"type": "integer", "value": "9223372036854775807"})).unwrap(),
            SqlValue::Integer(i64::MAX)
        );
        assert_eq!(
            value(&json!({"type": "float", "value": 1.5})).unwrap(),
            SqlValue::Real(1.5)
        );
        assert_eq!(
            value(&json!({"type": "blob", "base64": "/w"})).unwrap(),
            SqlValue::Blob(vec![0xff])
        );
        assert_eq!(value(&json!({"type": "null"})).unwrap(), SqlValue::Null);
        assert!(value(&json!({"type": "integer", "value": "x"})).is_err());
    }

    #[test]
    fn a_statement_error_is_the_servers() {
        let error = statement_result(&json!({
            "type": "error",
            "error": {"message": "no such table: t", "code": "SQLITE_ERROR"}
        }))
        .unwrap_err();
        match error {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("SQLITE_ERROR"));
                assert_eq!(message, "no such table: t");
            }
            other => panic!("{other:?}"),
        }
    }
}
