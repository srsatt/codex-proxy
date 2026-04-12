/// Binary entry point. Delegates all logic to [`codex_proxy::run`] and exits
/// with a non-zero status code on failure so that shell scripts and process
/// supervisors can detect errors reliably.
#[tokio::main]
async fn main() {
    if let Err(error) = codex_proxy::run().await {
        eprintln!("codex-proxy failed: {error:#}");
        std::process::exit(1);
    }
}
