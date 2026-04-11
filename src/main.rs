#[tokio::main]
async fn main() {
    if let Err(error) = codex_proxy::run().await {
        eprintln!("codex-proxy failed: {error:#}");
        std::process::exit(1);
    }
}
