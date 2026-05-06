use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;

pub async fn handle_ws(mut socket: WebSocket) {
    let hello = serde_json::json!({"event": "connected"});
    let _ = socket
        .send(Message::Text(hello.to_string().into()))
        .await;

    while let Some(Ok(msg)) = socket.next().await {
        if let Message::Text(text) = msg {
            let echo = serde_json::json!({"echo": text.as_str()});
            if socket
                .send(Message::Text(echo.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}
