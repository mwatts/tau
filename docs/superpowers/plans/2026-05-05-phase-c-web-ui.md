# Phase C: Web UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional browser-based client started via `tau serve --web`. Vanilla HTML+JS+CSS SPA that connects to the daemon via WebSocket, bridged through an axum web server.

**Architecture:** New `tau-agent-web` crate. axum serves static assets (embedded via `rust-embed`) and provides a WebSocket endpoint. Each browser WebSocket gets its own `tokio::net::UnixStream` to the daemon. Messages are JSON lines of the existing `Request`/`Response` protocol with a multiplexing wrapper. The daemon requires zero modifications.

**Tech Stack:** Rust (axum, tokio, tower-http, rust-embed), vanilla HTML+JS+CSS, marked.js for markdown

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `crates/tau-agent-web/Cargo.toml` | Crate manifest |
| Create | `crates/tau-agent-web/src/main.rs` | CLI args, config, startup, auth token |
| Create | `crates/tau-agent-web/src/ws_bridge.rs` | WebSocket ↔ Unix socket bridge |
| Create | `crates/tau-agent-web/src/routes.rs` | HTTP routes (static files, health) |
| Create | `crates/tau-agent-web/assets/index.html` | SPA shell |
| Create | `crates/tau-agent-web/assets/app.js` | Router, WebSocket manager |
| Create | `crates/tau-agent-web/assets/style.css` | Dark theme styling |
| Modify | `crates/tau-agent/src/main.rs` | Add `tau serve --web` command |

---

### Task 1: Create tau-agent-web crate scaffold

**Files:**
- Create: `crates/tau-agent-web/Cargo.toml`
- Create: `crates/tau-agent-web/src/main.rs`
- Create: `crates/tau-agent-web/src/routes.rs`

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "tau-agent-web"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true
homepage.workspace = true
keywords.workspace = true
categories.workspace = true
description = "Web UI server for tau agent"

[[bin]]
name = "tau-web"
path = "src/main.rs"

[dependencies]
tau-agent-base.workspace = true
serde_json.workspace = true
serde.workspace = true
axum = { version = "0.8", features = ["ws"] }
tokio = { version = "1", features = ["full"] }
tower-http = { version = "0.6", features = ["cors"] }
rust-embed = { version = "8", features = ["axum"] }
clap = { version = "4", features = ["derive"] }
rand = "0.9"
hex = "0.4"
mime_guess = "2"
```

- [ ] **Step 2: Create main.rs**

```rust
mod routes;
mod ws_bridge;

use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser)]
#[command(name = "tau-web", about = "Web UI server for tau agent")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    #[arg(long, default_value = "8080")]
    port: u16,
}

fn generate_token() -> String {
    use rand::Rng;
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

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port)
        .parse()
        .expect("invalid bind address");

    let app = routes::build_router(token.clone());

    eprintln!("tau web UI: http://{}:{}", args.bind, args.port);
    eprintln!("auth token: {}", token);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}
```

- [ ] **Step 3: Create routes.rs**

```rust
use axum::{Router, extract::Query, extract::State, extract::WebSocketUpgrade, response::IntoResponse};
use rust_embed::Embed;
use std::sync::Arc;

#[derive(Embed)]
#[folder = "assets/"]
struct Assets;

#[derive(Clone)]
pub struct AppState {
    pub token: String,
}

#[derive(serde::Deserialize)]
pub struct TokenQuery {
    token: Option<String>,
}

pub fn build_router(token: String) -> Router {
    let state = Arc::new(AppState { token });
    Router::new()
        .route("/health", axum::routing::get(health))
        .route("/ws", axum::routing::get(ws_handler))
        .fallback(static_handler)
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<TokenQuery>,
) -> impl IntoResponse {
    if query.token.as_deref() != Some(&state.token) {
        return (axum::http::StatusCode::UNAUTHORIZED, "invalid token").into_response();
    }
    ws.on_upgrade(|socket| crate::ws_bridge::handle_ws(socket)).into_response()
}

async fn static_handler(uri: axum::http::Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(axum::http::header::CONTENT_TYPE, mime.as_ref())],
                file.data,
            )
                .into_response()
        }
        None => {
            // SPA fallback: serve index.html for non-file paths
            match Assets::get("index.html") {
                Some(file) => (
                    [(axum::http::header::CONTENT_TYPE, "text/html")],
                    file.data,
                )
                    .into_response(),
                None => (axum::http::StatusCode::NOT_FOUND, "not found").into_response(),
            }
        }
    }
}
```

- [ ] **Step 4: Create placeholder ws_bridge.rs**

```rust
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};

pub async fn handle_ws(mut socket: WebSocket) {
    // Send a hello message
    let hello = serde_json::json!({"event": "connected"});
    let _ = socket.send(Message::Text(hello.to_string().into())).await;

    // Echo loop placeholder — will be replaced with daemon bridge
    while let Some(Ok(msg)) = socket.next().await {
        if let Message::Text(text) = msg {
            let echo = serde_json::json!({"echo": text.as_str()});
            if socket.send(Message::Text(echo.to_string().into())).await.is_err() {
                break;
            }
        }
    }
}
```

- [ ] **Step 5: Create minimal assets**

Create `crates/tau-agent-web/assets/index.html`:
```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tau</title>
<link rel="stylesheet" href="/style.css">
</head>
<body>
<div id="app">
  <nav id="sidebar">
    <h2>tau</h2>
    <div id="session-list"></div>
  </nav>
  <main id="main">
    <div id="chat-view"></div>
  </main>
</div>
<script src="/app.js"></script>
</body>
</html>
```

Create `crates/tau-agent-web/assets/style.css`:
```css
* { margin: 0; padding: 0; box-sizing: border-box; }
:root {
  --bg: #1a1b26; --bg-alt: #24283b; --fg: #c0caf5;
  --accent: #7aa2f7; --border: #3b4261; --error: #f7768e;
  --success: #9ece6a; --warning: #e0af68;
}
body { background: var(--bg); color: var(--fg); font-family: 'SF Mono', 'Fira Code', monospace; font-size: 14px; }
#app { display: flex; height: 100vh; }
#sidebar { width: 260px; background: var(--bg-alt); border-right: 1px solid var(--border); padding: 16px; overflow-y: auto; }
#sidebar h2 { color: var(--accent); margin-bottom: 16px; font-size: 18px; }
#main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
#chat-view { flex: 1; overflow-y: auto; padding: 16px; }
.session-item { padding: 8px 12px; border-radius: 6px; cursor: pointer; margin-bottom: 4px; }
.session-item:hover { background: var(--bg); }
.session-item.active { background: var(--accent); color: var(--bg); }
.message { margin-bottom: 16px; padding: 12px; border-radius: 8px; }
.message.user { background: var(--bg-alt); border: 1px solid var(--border); }
.message.assistant { background: transparent; }
.input-area { padding: 16px; border-top: 1px solid var(--border); }
.input-area textarea { width: 100%; background: var(--bg-alt); color: var(--fg); border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-family: inherit; font-size: 14px; resize: none; }
.input-area textarea:focus { outline: none; border-color: var(--accent); }
.status { font-size: 12px; color: var(--border); }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
.badge.running { background: var(--accent); color: var(--bg); }
.badge.idle { background: var(--border); }
.badge.error { background: var(--error); color: var(--bg); }
```

Create `crates/tau-agent-web/assets/app.js`:
```javascript
(function() {
  'use strict';
  const state = { ws: null, sessions: [], activeSession: null };

  function connect() {
    const token = new URLSearchParams(window.location.search).get('token') || '';
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/ws?token=${token}`);
    ws.onopen = () => { console.log('connected'); requestSessions(); };
    ws.onmessage = (e) => handleMessage(JSON.parse(e.data));
    ws.onclose = () => { console.log('disconnected'); setTimeout(connect, 3000); };
    ws.onerror = (e) => console.error('ws error', e);
    state.ws = ws;
  }

  function send(requestId, request) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify({ request_id: requestId, request: request }));
    }
  }

  function requestSessions() {
    send('list-sessions', { ListSessions: { all: false } });
  }

  function handleMessage(msg) {
    console.log('recv', msg);
    if (msg.event === 'connected') return;
    // Handle session list, stream events, etc.
    if (msg.echo) {
      appendChat('system', msg.echo);
    }
  }

  function appendChat(role, text) {
    const view = document.getElementById('chat-view');
    const div = document.createElement('div');
    div.className = `message ${role}`;
    div.textContent = text;
    view.appendChild(div);
    view.scrollTop = view.scrollHeight;
  }

  document.addEventListener('DOMContentLoaded', connect);
})();
```

- [ ] **Step 6: Add `futures-util` dependency**

The ws_bridge uses `futures_util`. Add to Cargo.toml dependencies:
```toml
futures-util = "0.3"
```

- [ ] **Step 7: Verify**

Run: `cargo check -p tau-agent-web`

- [ ] **Step 8: Commit**

```bash
git add crates/tau-agent-web/
git commit -S -m "feat(web): scaffold tau-agent-web crate with axum, static assets, and WebSocket stub"
```

---

### Task 2: WebSocket ↔ Unix socket bridge

**Files:**
- Modify: `crates/tau-agent-web/src/ws_bridge.rs`

- [ ] **Step 1: Replace placeholder with daemon bridge**

Replace the entire `ws_bridge.rs` with a real bridge that:
1. Connects to the daemon via `tokio::net::UnixStream`
2. Forwards WebSocket messages as JSON-line `Request`s to the daemon
3. Streams daemon `Response`s back as WebSocket messages
4. Wraps messages with `request_id` correlation

```rust
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

pub async fn handle_ws(socket: WebSocket) {
    let socket_path = tau_agent_base::paths::socket_path();
    let daemon = match UnixStream::connect(&socket_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to connect to daemon: {}", e);
            return;
        }
    };

    let (daemon_read, mut daemon_write) = daemon.into_split();
    let (mut ws_sink, mut ws_stream) = socket.split();

    // Daemon → Browser: read JSON lines from daemon, forward as WS messages
    let daemon_to_ws = {
        let mut reader = BufReader::new(daemon_read);
        let mut ws_sink_clone = ws_sink;
        async move {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        if ws_sink_clone.send(Message::Text(trimmed.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    };

    // Browser → Daemon: read WS messages, extract request, forward as JSON lines
    let ws_to_daemon = async move {
        while let Some(Ok(msg)) = ws_stream.next().await {
            match msg {
                Message::Text(text) => {
                    // Parse the wrapper: { "request_id": "...", "request": {...} }
                    if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
                        if let Some(request) = wrapper.get("request") {
                            let mut line = request.to_string();
                            line.push('\n');
                            if daemon_write.write_all(line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    };

    // Run both directions concurrently; when either ends, both stop
    tokio::select! {
        _ = daemon_to_ws => {}
        _ = ws_to_daemon => {}
    }
}
```

Wait — the above has a problem. `ws_sink` is moved into `daemon_to_ws`, but we need it in a `tokio::select!`. We need to restructure. The correct pattern uses `socket.split()` and `tokio::select!` properly.

Actually the problem is different: `ws_sink` is moved into `daemon_to_ws` closure, but `ws_sink_clone` isn't a real clone (WebSocket sinks don't implement Clone). We need to use a different approach.

Use this corrected version:

```rust
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

pub async fn handle_ws(socket: WebSocket) {
    let socket_path = tau_agent_base::paths::socket_path();
    let daemon = match UnixStream::connect(&socket_path).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to connect to daemon: {}", e);
            return;
        }
    };

    let (daemon_read, mut daemon_write) = daemon.into_split();
    let (mut ws_sink, mut ws_stream) = socket.split();
    let mut reader = BufReader::new(daemon_read);

    let mut line = String::new();
    loop {
        tokio::select! {
            // Daemon → Browser
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break,
                    Ok(_) => {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() {
                            if ws_sink.send(Message::Text(trimmed.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        line.clear();
                    }
                    Err(_) => break,
                }
            }
            // Browser → Daemon
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
                            if let Some(request) = wrapper.get("request") {
                                let mut req_line = request.to_string();
                                req_line.push('\n');
                                if daemon_write.write_all(req_line.as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}
```

- [ ] **Step 2: Verify**

Run: `cargo check -p tau-agent-web`

- [ ] **Step 3: Commit**

```bash
git add crates/tau-agent-web/src/ws_bridge.rs
git commit -S -m "feat(web): implement WebSocket to Unix socket bridge"
```

---

### Task 3: Add `tau serve --web` CLI integration

**Files:**
- Modify: `crates/tau-agent/src/main.rs`

- [ ] **Step 1: Add Commands::Serve variant**

In the `Commands` enum, after the `Supervise` variant and before `McpServer`, add:

```rust
/// Start the web UI server
#[command(alias = "web")]
Serve {
    /// Bind address
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    /// Port
    #[arg(long, default_value = "8080")]
    port: u16,
},
```

- [ ] **Step 2: Add dispatch arm**

In the main match, add:

```rust
Commands::Serve { bind, port } => {
    let status = std::process::Command::new("tau-web")
        .arg("--bind").arg(&bind)
        .arg("--port").arg(port.to_string())
        .status()
        .map_err(|e| tau_agent_lib::Error::Io(
            format!("failed to start tau-web: {} (is tau-agent-web installed?)", e)
        ))?;
    if !status.success() {
        eprintln!("tau-web exited with: {}", status);
    }
}
```

This delegates to the `tau-web` binary. Alternatively, we could use the library directly, but keeping it as a subprocess is simpler and matches the spec's "separate process" design.

- [ ] **Step 3: Verify**

Run: `cargo check -p tau-agent`

- [ ] **Step 4: Commit**

```bash
git add crates/tau-agent/src/main.rs
git commit -S -m "feat(web): add tau serve CLI command"
```

---

### Task 4: Full verification

- [ ] **Step 1:** `cargo build --workspace`
- [ ] **Step 2:** `cargo test --workspace`
- [ ] **Step 3:** `cargo clippy -p tau-agent-web -p tau-agent -- -D warnings`
