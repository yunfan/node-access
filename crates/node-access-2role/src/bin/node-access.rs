use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use node_access_common::crypto::FrameCrypto;
use node_access_common::legacy_cli::normalize_legacy_args;
use node_access_common::mapping::{
    auth_matches, parse_tcp_mapping, parse_visitor_mapping, TcpMapping, VisitorMapping,
};
use node_access_common::mux::{
    ControlMessage, MuxConnection, MuxHandle, OpenRequest, VirtualStream,
};
use node_access_common::relay_ws::{
    build_ws_url, connect_client_data, connect_control, connect_server_data,
};
use node_access_common::socks::serve_socks5;
use node_access_common::tunnel::pipe;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "node-access")]
#[command(about = "All-in-one node-access for relay-backed private network access")]
struct Args {
    #[arg(
        long,
        alias = "hub",
        default_value = "wss://relay.example.com",
        env = "NODE_RELAY_URL"
    )]
    relay: String,

    #[arg(long, env = "NODE_RELAY_SECRET")]
    secret: Option<String>,

    #[arg(long, default_value = "anonymous")]
    name: String,

    #[arg(long)]
    list: bool,

    #[arg(long = "provider", value_parser = parse_tcp_mapping)]
    providers: Vec<TcpMapping>,

    #[arg(long = "visitor", value_parser = parse_visitor_mapping)]
    visitors: Vec<VisitorMapping>,

    #[arg(long, hide = true)]
    connection_id: Option<String>,
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
struct ServeState {
    active_connections: HashSet<String>,
    local_providers: HashMap<String, TcpMapping>,
    providers: HashMap<String, ProviderEndpoint>,
}

#[derive(Clone)]
struct RuntimeConfig {
    relay: String,
    secret: Option<String>,
    name: String,
    providers: Vec<TcpMapping>,
    visitors: Vec<VisitorMapping>,
    connection_id: Option<String>,
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
        "relay",
        "secret",
        "name",
        "list",
        "provider",
        "visitor",
        "connection-id",
    ]));

    if args.list {
        print_listing(&args);
        return Ok(());
    }

    let config = RuntimeConfig {
        relay: args.relay,
        secret: args.secret,
        name: args.name,
        providers: args.providers,
        visitors: args.visitors,
        connection_id: args.connection_id,
    };

    run_forever(config).await
}

fn print_listing(args: &Args) {
    println!("\nConfigured node-access:");
    println!("--------------------------------------------------");
    println!(
        "Self node: {} | Relay: {}",
        args.name,
        build_ws_url(&args.relay, &args.name, "server", None)
    );
    for visitor in &args.visitors {
        println!(
            "Visitor: {}:{} -> {}",
            visitor.node, visitor.service, visitor.bind_addr
        );
    }
    println!("--------------------------------------------------");
    println!("relay does not provide directory listing.");
}

async fn run_forever(config: RuntimeConfig) -> Result<()> {
    let local_providers = config
        .providers
        .iter()
        .map(|provider| (provider.name.clone(), provider.clone()))
        .collect();
    let state = Arc::new(Mutex::new(ServeState {
        active_connections: HashSet::new(),
        local_providers,
        providers: HashMap::new(),
    }));
    let serve_config = config.clone();
    let serve_state = Arc::clone(&state);
    let serve_task = tokio::spawn(async move {
        run_control_forever(serve_config, serve_state).await;
    });

    let mut visitor_tasks: Vec<JoinHandle<()>> = Vec::new();
    for visitor in &config.visitors {
        visitor_tasks.push(spawn_remote_visitor(config.clone(), visitor.clone()));
    }

    let _ = serve_task.await;
    for task in visitor_tasks {
        task.abort();
    }
    Ok(())
}

async fn run_control_forever(config: RuntimeConfig, state: Arc<Mutex<ServeState>>) {
    loop {
        if let Err(error) = run_control(&config, Arc::clone(&state)).await {
            warn!(%error, "node control channel disconnected");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_control(config: &RuntimeConfig, state: Arc<Mutex<ServeState>>) -> Result<()> {
    info!(
        relay = %config.relay,
        node = %config.name,
        "serving node-access node"
    );
    let ws = connect_control(&config.relay, &config.name).await?;
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
                start_served_connection(config, Arc::clone(&state), connection_id).await;
            }
            RelayControlEvent::Sync { connection_ids } => {
                for connection_id in connection_ids {
                    start_served_connection(config, Arc::clone(&state), connection_id).await;
                }
            }
            RelayControlEvent::Disconnected { connection_id } => {
                info!(connection_id, "peer node-access disconnected");
            }
            RelayControlEvent::Pong { .. } => {}
        }
    }

    ping_task.abort();
    Ok(())
}

async fn start_served_connection(
    config: &RuntimeConfig,
    state: Arc<Mutex<ServeState>>,
    connection_id: String,
) {
    {
        let mut state_guard = state.lock().await;
        if !state_guard.active_connections.insert(connection_id.clone()) {
            return;
        }
    }

    let relay = config.relay.clone();
    let server_id = config.name.clone();
    let secret = config.secret.clone();
    tokio::spawn(async move {
        info!(connection_id, "opening served data channel");
        if let Err(error) = run_served_data_connection(
            relay,
            server_id,
            secret,
            connection_id.clone(),
            Arc::clone(&state),
        )
        .await
        {
            warn!(%error, connection_id, "served data channel ended");
        }
        let mut state_guard = state.lock().await;
        state_guard.active_connections.remove(&connection_id);
        state_guard
            .providers
            .retain(|_, provider| provider.connection_id != connection_id);
    });
}

async fn run_served_data_connection(
    relay: String,
    server_id: String,
    secret: Option<String>,
    connection_id: String,
    state: Arc<Mutex<ServeState>>,
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
                        warn!(?request, "unexpected provider-connect stream on served node");
                    }
                }
            }
            Some(message) = control.recv() => {
                match message {
                    ControlMessage::Bind { name, auth } => {
                        handle_bind(&state, &handle, &connection_id, name, auth).await;
                    }
                    other => warn!(?other, connection_id, "unexpected control message from peer node-access"),
                }
            }
            _ = closed.recv() => break,
        }
    }
    Ok(())
}

async fn handle_bind(
    state: &Arc<Mutex<ServeState>>,
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
    state: Arc<Mutex<ServeState>>,
    name: String,
    auth: Option<String>,
    visitor_stream: VirtualStream,
) -> Result<()> {
    let (local_provider, provider) = {
        let state_guard = state.lock().await;
        (
            state_guard.local_providers.get(&name).cloned(),
            state_guard.providers.get(&name).cloned(),
        )
    };
    if let Some(local_provider) = local_provider {
        if !auth_matches(local_provider.auth.as_deref(), auth.as_deref()) {
            return Err(anyhow!("visitor auth rejected"));
        }
        let local = TcpStream::connect(&local_provider.addr)
            .await
            .with_context(|| format!("failed to connect local provider {}", local_provider.addr))?;
        pipe(visitor_stream, local).await?;
        return Ok(());
    }

    let Some(provider) = provider else {
        return Err(anyhow!("provider not found"));
    };
    if !auth_matches(provider.auth.as_deref(), auth.as_deref()) {
        return Err(anyhow!("visitor auth rejected"));
    }
    let provider_stream = provider
        .mux
        .open_stream(OpenRequest::ProviderConnect { name: name.clone() })
        .await?;
    pipe(visitor_stream, provider_stream).await?;
    Ok(())
}

fn spawn_remote_visitor(config: RuntimeConfig, visitor: VisitorMapping) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(error) = run_remote_visitor_once(&config, &visitor).await {
                warn!(
                    %error,
                    node = %visitor.node,
                    service = %visitor.service,
                    "visitor remote connection ended"
                );
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}

async fn run_remote_visitor_once(config: &RuntimeConfig, visitor: &VisitorMapping) -> Result<()> {
    let connection_id = config
        .connection_id
        .clone()
        .unwrap_or_else(|| format!("access-{}-to-{}", config.name, visitor.node));
    info!(
        relay = %config.relay,
        target_node = %visitor.node,
        connection_id,
        "connecting visitor to remote node"
    );
    let ws = connect_client_data(&config.relay, &visitor.node, Some(&connection_id)).await?;
    let crypto = config
        .secret
        .as_deref()
        .map(|secret| FrameCrypto::from_secret(secret, 0, 1));
    let MuxConnection {
        handle,
        mut incoming,
        mut control,
        mut closed,
    } = MuxConnection::start(ws, 1, crypto);

    for provider in &config.providers {
        handle.send_control(ControlMessage::Bind {
            name: provider.name.clone(),
            auth: provider.auth.clone(),
        })?;
    }

    let providers: HashMap<String, String> = config
        .providers
        .iter()
        .map(|mapping| (mapping.name.clone(), mapping.addr.clone()))
        .collect();
    let listener_task = spawn_visitor_listener(visitor.clone(), handle.clone());

    loop {
        tokio::select! {
            Some(incoming_stream) = incoming.recv() => {
                let request = incoming_stream.request.clone();
                match request {
                    OpenRequest::ProviderConnect { name } => {
                        let Some(target) = providers.get(&name).cloned() else {
                            warn!(name, "provider target is not configured locally");
                            continue;
                        };
                        let stream = incoming_stream.into_stream();
                        tokio::spawn(async move {
                            match TcpStream::connect(&target).await {
                                Ok(local) => {
                                    if let Err(error) = pipe(stream, local).await {
                                        warn!(%error, name, target, "provider stream ended with error");
                                    }
                                }
                                Err(error) => warn!(%error, name, target, "failed to dial provider target"),
                            }
                        });
                    }
                    other => warn!(?other, "unexpected incoming stream on visitor connection"),
                }
            }
            Some(message) = control.recv() => {
                match message {
                    ControlMessage::BindAck { name, ok, error } => {
                        if ok {
                            info!(name, "provider registered on remote node");
                        } else {
                            warn!(name, error = error.unwrap_or_else(|| "unknown error".to_string()), "provider registration rejected");
                        }
                    }
                    ControlMessage::Info { message } => info!(message),
                    ControlMessage::Bind { .. } => {}
                }
            }
            _ = closed.recv() => break,
        }
    }

    listener_task.abort();
    Err(anyhow!("relay mux closed"))
}

fn spawn_visitor_listener(visitor: VisitorMapping, handle: MuxHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&visitor.bind_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(
                    %error,
                    node = %visitor.node,
                    service = %visitor.service,
                    bind_addr = %visitor.bind_addr,
                    "failed to bind visitor listener"
                );
                return;
            }
        };
        info!(
            node = %visitor.node,
            service = %visitor.service,
            bind_addr = %visitor.bind_addr,
            "visitor listener ready"
        );
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, bind_addr = %visitor.bind_addr, "visitor accept failed");
                    break;
                }
            };
            let handle = handle.clone();
            let service = visitor.service.clone();
            let auth = visitor.auth.clone();
            tokio::spawn(async move {
                match handle
                    .open_stream(OpenRequest::Visit {
                        name: service.clone(),
                        auth,
                    })
                    .await
                {
                    Ok(stream) => {
                        if let Err(error) = pipe(client, stream).await {
                            warn!(%error, %peer, service, "visitor tunnel ended with error");
                        }
                    }
                    Err(error) => warn!(%error, %peer, service, "failed to open visitor tunnel"),
                }
            });
        }
    })
}
