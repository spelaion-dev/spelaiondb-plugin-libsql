//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042): it starts, speaks the protocol's version, opens sessions, returns results in the
//! order the contract says, fails without hanging, obeys a cancel while a statement runs and
//! ends when its input closes. What the engine's types look like is `tests/libsql.rs`. It
//! needs `SPELAION_TEST_LIBSQL` (`ci/services.sh`); without it the test is skipped, saying so.

use spelaion_driver_api::ConnectParams;
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-libsql");

#[tokio::test]
async fn the_plugin_conforms() {
    let Ok(addr) = std::env::var("SPELAION_TEST_LIBSQL") else {
        eprintln!("SPELAION_TEST_LIBSQL not set: test skipped");
        return;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let target = Target {
        program: PROGRAM.into(),
        engine: "libsql".into(),
        params: ConnectParams {
            host: host.into(),
            port: port.parse().expect("port"),
            tls: spelaion_driver_api::TlsMode::Disable,
            ..Default::default()
        },
        one_row: "select 1".into(),
        refused: "select * from a_table_that_is_not_there".into(),
        // libSQL cannot cancel a statement (closing the request would not stop it).
        slow: None,
    };
    assert_conforms(&target).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    // Three numbers, as SpelaionDB's `plugins.toml` wants.
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
