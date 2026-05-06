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
