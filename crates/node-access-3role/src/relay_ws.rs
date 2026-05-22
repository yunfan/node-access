use crate::mux::WsStream;
use anyhow::{Context, Result};
use tokio_tungstenite::connect_async;

pub async fn connect_control(relay_base: &str, server_id: &str) -> Result<WsStream> {
    connect(relay_base, server_id, "server", None).await
}

pub async fn connect_server_data(
    relay_base: &str,
    server_id: &str,
    connection_id: &str,
) -> Result<WsStream> {
    connect(relay_base, server_id, "server", Some(connection_id)).await
}

pub async fn connect_client_data(
    relay_base: &str,
    server_id: &str,
    connection_id: Option<&str>,
) -> Result<WsStream> {
    connect(relay_base, server_id, "client", connection_id).await
}

async fn connect(
    relay_base: &str,
    server_id: &str,
    role: &str,
    connection_id: Option<&str>,
) -> Result<WsStream> {
    let url = build_ws_url(relay_base, server_id, role, connection_id);
    let (ws, _) = connect_async(&url)
        .await
        .with_context(|| format!("failed to connect relay websocket: {url}"))?;
    Ok(ws)
}

pub fn build_ws_url(
    relay_base: &str,
    server_id: &str,
    role: &str,
    connection_id: Option<&str>,
) -> String {
    let mut base = relay_base.trim_end_matches('/').to_string();
    if let Some(rest) = base.strip_prefix("http://") {
        base = format!("ws://{rest}");
    } else if let Some(rest) = base.strip_prefix("https://") {
        base = format!("wss://{rest}");
    }
    if !base.ends_with("/ws") {
        base.push_str("/ws");
    }

    let mut url = format!(
        "{base}?serverId={}&role={role}&v=2",
        urlencoding::encode(server_id)
    );
    if let Some(connection_id) = connection_id {
        url.push_str("&connectionId=");
        url.push_str(&urlencoding::encode(connection_id));
    }
    url
}
