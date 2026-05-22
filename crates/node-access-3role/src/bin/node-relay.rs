use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use node_access_common::crypto::FrameCrypto;
use node_access_common::legacy_cli::normalize_legacy_args;
use node_access_common::mapping::auth_matches;
use node_access_common::mux::{
    ControlMessage, MuxConnection, MuxHandle, OpenRequest, VirtualStream,
};
use node_access_common::relay_ws::{connect_control, connect_server_data};
use node_access_common::socks::serve_socks5;
use node_access_common::tunnel::pipe;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "node-relay")]
#[command(about = "Relay-side node for relay-backed private network access")]
struct Args {
    #[arg(
        long,
        alias = "hub",
        default_value = "wss://relay.example.com",
        env = "NODE_RELAY_URL"
    )]
    relay: String,

    #[arg(
        long,
        alias = "server-id",
        default_value = "default",
        env = "NODE_RELAY_SERVER_ID"
    )]
    id: String,

    #[arg(long, env = "NODE_RELAY_SECRET")]
    secret: Option<String>,

    #[arg(long, default_value = ":8686")]
    local: String,

    #[arg(long, default_value = ":9090")]
    tunnel: String,

    #[arg(long)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RelayControlEvent {
    #[serde(rename = "connected")]
    Connected {
        #[serde(rename = "connectionId")]
        connection_id: String,
    },
    #[serde(rename = "disconnected")]
    Disconnected {
        #[serde(rename = "connectionId")]
        connection_id: String,
    },
    #[serde(rename = "sync")]
    Sync {
        #[serde(rename = "connectionIds")]
        connection_ids: Vec<String>,
    },
    #[serde(rename = "pong")]
    Pong {},
}

#[derive(Clone)]
struct ProviderEndpoint {
    connection_id: String,
    mux: MuxHandle,
    auth: Option<String>,
}

#[derive(Default)]
struct RelayState {
    active_connections: HashSet<String>,
    providers: HashMap<String, ProviderEndpoint>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse_from(normalize_legacy_args(&[
        "hub",
        "local",
        "tunnel",
        "id",
        "url",
        "secret",
        "relay",
        "server-id",
    ]));
    let state = Arc::new(Mutex::new(RelayState::default()));

    loop {
        if let Err(error) = run_control(&args, Arc::clone(&state)).await {
            warn!(error = %format!("{error:#}"), "control channel disconnected");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_control(args: &Args, state: Arc<Mutex<RelayState>>) -> Result<()> {
    info!(
        relay = %args.relay,
        server_id = %args.id,
        local = %args.local,
        tunnel = %args.tunnel,
        url = args.url.as_deref().unwrap_or(""),
        "connecting relay control channel"
    );
    let ws = connect_control(&args.relay, &args.id).await?;
    let (mut sink, mut read) = ws.split();

    let ping_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if sink
                .send(Message::Text(r#"{"type":"ping"}"#.to_string()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(message) = read.next().await {
        let message = message?;
        let Message::Text(text) = message else {
            continue;
        };
        let event: RelayControlEvent = serde_json::from_str(&text)
            .with_context(|| format!("invalid control event: {text}"))?;
        match event {
            RelayControlEvent::Connected { connection_id } => {
                start_connection(args, Arc::clone(&state), connection_id).await;
            }
            RelayControlEvent::Sync { connection_ids } => {
                for connection_id in connection_ids {
                    start_connection(args, Arc::clone(&state), connection_id).await;
                }
            }
            RelayControlEvent::Disconnected { connection_id } => {
                info!(connection_id, "access node disconnected");
            }
            RelayControlEvent::Pong { .. } => {}
        }
    }

    ping_task.abort();
    Ok(())
}

async fn start_connection(args: &Args, state: Arc<Mutex<RelayState>>, connection_id: String) {
    {
        let mut state_guard = state.lock().await;
        if !state_guard.active_connections.insert(connection_id.clone()) {
            return;
        }
    }

    let relay = args.relay.clone();
    let server_id = args.id.clone();
    let secret = args.secret.clone();
    tokio::spawn(async move {
        info!(connection_id, "opening data channel");
        if let Err(error) = run_data_connection(
            relay,
            server_id,
            secret,
            connection_id.clone(),
            Arc::clone(&state),
        )
        .await
        {
            warn!(%error, connection_id, "data channel ended");
        }
        let mut state_guard = state.lock().await;
        state_guard.active_connections.remove(&connection_id);
        state_guard
            .providers
            .retain(|_, provider| provider.connection_id != connection_id);
    });
}

async fn run_data_connection(
    relay: String,
    server_id: String,
    secret: Option<String>,
    connection_id: String,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    let ws = connect_server_data(&relay, &server_id, &connection_id).await?;
    let crypto = secret
        .as_deref()
        .map(|secret| FrameCrypto::from_secret(secret, 1, 0));
    let MuxConnection {
        handle,
        mut incoming,
        mut control,
        mut closed,
    } = MuxConnection::start(ws, 2, crypto);

    loop {
        tokio::select! {
            Some(incoming_stream) = incoming.recv() => {
                let request = incoming_stream.request.clone();
                match request {
                    OpenRequest::Socks => {
                        tokio::spawn(async move {
                            if let Err(error) = serve_socks5(incoming_stream.into_stream()).await {
                                warn!(%error, "SOCKS5 stream failed");
                            }
                        });
                    }
                    OpenRequest::Visit { name, auth } => {
                        let visitor_stream = incoming_stream.into_stream();
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            if let Err(error) = handle_visit(state, name.clone(), auth, visitor_stream).await {
                                warn!(%error, name, "visitor stream failed");
                            }
                        });
                    }
                    OpenRequest::ProviderConnect { .. } => {
                        warn!(?request, "unexpected provider-connect stream on relay");
                    }
                }
            }
            Some(message) = control.recv() => {
                match message {
                    ControlMessage::Bind { name, auth } => {
                        handle_bind(&state, &handle, &connection_id, name, auth).await;
                    }
                    other => warn!(?other, connection_id, "unexpected control message from access node"),
                }
            }
            _ = closed.recv() => break,
        }
    }
    Ok(())
}

async fn handle_bind(
    state: &Arc<Mutex<RelayState>>,
    handle: &MuxHandle,
    connection_id: &str,
    name: String,
    auth: Option<String>,
) {
    let mut state_guard = state.lock().await;
    if let Some(existing) = state_guard.providers.get(&name) {
        if existing.connection_id != connection_id {
            let _ = handle.send_control(ControlMessage::BindAck {
                name,
                ok: false,
                error: Some("provider name is already registered".to_string()),
            });
            return;
        }
    }

    state_guard.providers.insert(
        name.clone(),
        ProviderEndpoint {
            connection_id: connection_id.to_string(),
            mux: handle.clone(),
            auth,
        },
    );
    let _ = handle.send_control(ControlMessage::BindAck {
        name: name.clone(),
        ok: true,
        error: None,
    });
    info!(provider = name, connection_id, "provider registered");
}

async fn handle_visit(
    state: Arc<Mutex<RelayState>>,
    name: String,
    auth: Option<String>,
    visitor_stream: VirtualStream,
) -> Result<()> {
    let provider = {
        let state_guard = state.lock().await;
        state_guard.providers.get(&name).cloned()
    };
    let Some(provider) = provider else {
        return Err(anyhow::anyhow!("provider not found"));
    };
    if !auth_matches(provider.auth.as_deref(), auth.as_deref()) {
        return Err(anyhow::anyhow!("visitor auth rejected"));
    }
    let provider_stream = provider
        .mux
        .open_stream(OpenRequest::ProviderConnect { name: name.clone() })
        .await?;
    pipe(visitor_stream, provider_stream).await?;
    Ok(())
}
