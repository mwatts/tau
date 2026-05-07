use tau_agent_web::routes;

use clap::Parser;
use std::sync::Arc;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "tau-web", about = "Web UI server for tau agent")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    #[arg(long, default_value = "8080")]
    port: u16,
    /// Path to SQLite database for durable streams. If omitted, streams are disabled.
    #[arg(long)]
    streams_db: Option<String>,
}

fn generate_token() -> String {
    use rand::RngExt;
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
}

fn write_token(token: &str) -> std::io::Result<()> {
    let dir = tau_agent_base::paths::config_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("web-token");
    std::fs::write(&path, token)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let token = generate_token();

    if let Err(e) = write_token(&token) {
        eprintln!("warning: could not write auth token: {}", e);
    }

    let streams = if let Some(ref path) = args.streams_db {
        match tau_streams::SqliteStore::open(path) {
            Ok(store) => {
                let hub = Arc::new(tau_streams::LiveDeliveryHub::default());
                let ds = Arc::new(tau_streams::DurableStream::new(store, hub));
                eprintln!("streams db: {}", path);
                Some(ds)
            }
            Err(e) => {
                eprintln!("warning: could not open streams db '{}': {}", path, e);
                None
            }
        }
    } else {
        None
    };

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port)
        .parse()
        .expect("invalid bind address");

    let app = routes::build_router(token.clone(), streams);

    eprintln!("tau web UI: http://{}:{}", args.bind, args.port);
    eprintln!("auth token: {}", token);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}
