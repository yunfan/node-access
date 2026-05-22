use anyhow::{anyhow, Result};
use clap::Parser;
use node_access_common::crypto::FrameCrypto;
use node_access_common::legacy_cli::normalize_legacy_args;
use node_access_common::mapping::{parse_tcp_mapping, TcpMapping};
use node_access_common::mux::{ControlMessage, MuxConnection, MuxHandle, OpenRequest};
use node_access_common::relay_ws::connect_client_data;
use node_access_common::tunnel::pipe;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "node-access")]
#[command(about = "Local access node for relay-backed private network access")]
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
    node: String,

    #[arg(long, env = "NODE_RELAY_SECRET")]
    secret: Option<String>,

    #[arg(long, default_value = "anonymous")]
    name: String,

    #[arg(long, default_value = "127.0.0.1:1080")]
    socks5: String,

    #[arg(long)]
    list: bool,

    #[arg(long = "no-socks5", hide = true)]
    no_socks5: bool,

    #[arg(long = "provider", value_parser = parse_tcp_mapping)]
    providers: Vec<TcpMapping>,

    #[arg(long = "visitor", value_parser = parse_tcp_mapping)]
    visitors: Vec<TcpMapping>,

    #[arg(long, hide = true)]
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
        "socks5",
        "secret",
        "node",
        "name",
        "list",
        "provider",
        "visitor",
        "no-socks5",
        "connection-id",
        "relay",
        "server-id",
    ]));
    if args.list {
        println!("\nAvailable Relay Nodes:");
        println!("--------------------------------------------------");
        println!(
            "[1] ID: {} | Relay: {}",
            args.node,
            node_access_common::relay_ws::build_ws_url(&args.relay, &args.node, "client", None)
        );
        println!("--------------------------------------------------");
        println!("relay does not provide directory listing; pass -node <ID> to connect.");
        return Ok(());
    }
    loop {
        if let Err(error) = run_once(&args).await {
            warn!(%error, "node-access disconnected");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_once(args: &Args) -> Result<()> {
    let connection_id = args
        .connection_id
        .clone()
        .unwrap_or_else(|| format!("access-{}", args.name));
    info!(
        relay = %args.relay,
        server_id = %args.node,
        connection_id,
        "connecting to relay"
    );
    let ws = connect_client_data(&args.relay, &args.node, Some(&connection_id)).await?;
    let crypto = args
        .secret
        .as_deref()
        .map(|secret| FrameCrypto::from_secret(secret, 0, 1));
    let MuxConnection {
        handle,
        mut incoming,
        mut control,
        mut closed,
    } = MuxConnection::start(ws, 1, crypto);

    for provider in &args.providers {
        handle.send_control(ControlMessage::Bind {
            name: provider.name.clone(),
            auth: provider.auth.clone(),
        })?;
    }

    let providers: HashMap<String, String> = args
        .providers
        .iter()
        .map(|mapping| (mapping.name.clone(), mapping.addr.clone()))
        .collect();
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    if !args.no_socks5 && !args.socks5.is_empty() {
        tasks.push(spawn_socks_listener(args.socks5.clone(), handle.clone()));
    }
    for visitor in &args.visitors {
        tasks.push(spawn_visitor_listener(
            visitor.name.clone(),
            visitor.addr.clone(),
            visitor.auth.clone(),
            handle.clone(),
        ));
    }

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
                    other => warn!(?other, "unexpected incoming stream on access node"),
                }
            }
            Some(message) = control.recv() => {
                match message {
                    ControlMessage::BindAck { name, ok, error } => {
                        if ok {
                            info!(name, "provider registered");
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

    for task in tasks {
        task.abort();
    }
    Err(anyhow!("relay mux closed"))
}

fn spawn_socks_listener(bind_addr: String, handle: MuxHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(%error, bind_addr, "failed to bind socks5 listener");
                return;
            }
        };
        info!(bind_addr, "SOCKS5 listener ready");
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, bind_addr, "SOCKS5 accept failed");
                    break;
                }
            };
            let handle = handle.clone();
            tokio::spawn(async move {
                match handle.open_stream(OpenRequest::Socks).await {
                    Ok(stream) => {
                        if let Err(error) = pipe(client, stream).await {
                            warn!(%error, %peer, "SOCKS5 tunnel ended with error");
                        }
                    }
                    Err(error) => warn!(%error, %peer, "failed to open SOCKS5 tunnel"),
                }
            });
        }
    })
}

fn spawn_visitor_listener(
    name: String,
    bind_addr: String,
    auth: Option<String>,
    handle: MuxHandle,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                error!(%error, name, bind_addr, "failed to bind visitor listener");
                return;
            }
        };
        info!(name, bind_addr, "visitor listener ready");
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, name, bind_addr, "visitor accept failed");
                    break;
                }
            };
            let handle = handle.clone();
            let name = name.clone();
            let auth = auth.clone();
            tokio::spawn(async move {
                match handle
                    .open_stream(OpenRequest::Visit {
                        name: name.clone(),
                        auth,
                    })
                    .await
                {
                    Ok(stream) => {
                        if let Err(error) = pipe(client, stream).await {
                            warn!(%error, %peer, name, "visitor tunnel ended with error");
                        }
                    }
                    Err(error) => warn!(%error, %peer, name, "failed to open visitor tunnel"),
                }
            });
        }
    })
}
