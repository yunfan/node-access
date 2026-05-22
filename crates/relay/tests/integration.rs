use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use relay::{app, AppState, RelayLimitsConfig, RelayTimingConfig};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

async fn spawn_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    spawn_test_server_with_state(AppState::default()).await
}

async fn spawn_test_server_with_state(
    state: AppState,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    (addr, handle)
}

fn http_base(addr: SocketAddr) -> String {
    format!("http://{}:{}", addr.ip(), addr.port())
}

fn ws_base(addr: SocketAddr) -> String {
    format!("ws://{}:{}", addr.ip(), addr.port())
}

async fn next_text(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> String {
    let message = timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match message {
        Message::Text(text) => text,
        other => panic!("expected text message, got {other:?}"),
    }
}

#[tokio::test]
async fn req_001_health_returns_ok() {
    let (addr, handle) = spawn_test_server().await;
    let response = reqwest::get(format!("{}/health", http_base(addr)))
        .await
        .unwrap();
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    handle.abort();
}

#[tokio::test]
async fn req_003_invalid_version_returns_400() {
    let (addr, handle) = spawn_test_server().await;
    let response = reqwest::get(format!(
        "{}/ws?serverId=s1&role=server&v=3",
        http_base(addr)
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), 400);
    handle.abort();
}

#[tokio::test]
async fn req_002_ws_requires_upgrade() {
    let (addr, handle) = spawn_test_server().await;
    let response = reqwest::get(format!(
        "{}/ws?serverId=s1&role=server&v=2",
        http_base(addr)
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), 426);
    handle.abort();
}

#[tokio::test]
async fn req_002_unknown_path_returns_404() {
    let (addr, handle) = spawn_test_server().await;
    let response = reqwest::get(format!("{}/nope", http_base(addr)))
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    handle.abort();
}

#[tokio::test]
async fn req_003_missing_version_defaults_to_v1() {
    let (addr, handle) = spawn_test_server().await;
    let (mut server, _) = connect_async(format!("{}/ws?serverId=s0&role=server", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!("{}/ws?serverId=s0&role=client", ws_base(addr)))
        .await
        .unwrap();

    client.send(Message::Text("fallback".into())).await.unwrap();
    assert_eq!(next_text(&mut server).await, "fallback");
    handle.abort();
}

#[tokio::test]
async fn req_008_control_receives_sync_and_connected() {
    let (addr, handle) = spawn_test_server().await;
    let (mut control, _) =
        connect_async(format!("{}/ws?serverId=s1&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();

    let sync = next_text(&mut control).await;
    assert!(sync.contains("\"type\":\"sync\""));

    let (_client, _) = connect_async(format!(
        "{}/ws?serverId=s1&role=client&v=2&connectionId=conn-a",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let connected = next_text(&mut control).await;
    assert!(connected.contains("\"type\":\"connected\""));
    assert!(connected.contains("conn-a"));
    handle.abort();
}

#[tokio::test]
async fn req_010_pending_frames_flush_when_server_data_arrives() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s2&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();

    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s2&role=client&v=2&connectionId=conn-b",
        ws_base(addr)
    ))
    .await
    .unwrap();
    client.send(Message::Text("early".into())).await.unwrap();

    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s2&role=server&v=2&connectionId=conn-b",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let flushed = next_text(&mut server_data).await;
    assert_eq!(flushed, "early");
    handle.abort();
}

#[tokio::test]
async fn req_009_v2_forwards_client_frames_to_server_data() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s4&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s4&role=server&v=2&connectionId=conn-d",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s4&role=client&v=2&connectionId=conn-d",
        ws_base(addr)
    ))
    .await
    .unwrap();

    client
        .send(Message::Text("hello-server".into()))
        .await
        .unwrap();

    let forwarded = next_text(&mut server_data).await;
    assert_eq!(forwarded, "hello-server");
    handle.abort();
}

#[tokio::test]
async fn req_009_v2_forwards_server_data_frames_to_clients() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s5&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s5&role=client&v=2&connectionId=conn-e",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s5&role=server&v=2&connectionId=conn-e",
        ws_base(addr)
    ))
    .await
    .unwrap();

    server_data
        .send(Message::Text("hello-client".into()))
        .await
        .unwrap();

    let forwarded = next_text(&mut client).await;
    assert_eq!(forwarded, "hello-client");
    handle.abort();
}

#[tokio::test]
async fn req_009_v2_fans_out_server_data_to_multiple_clients() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s7&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client_a, _) = connect_async(format!(
        "{}/ws?serverId=s7&role=client&v=2&connectionId=conn-f",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut client_b, _) = connect_async(format!(
        "{}/ws?serverId=s7&role=client&v=2&connectionId=conn-f",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s7&role=server&v=2&connectionId=conn-f",
        ws_base(addr)
    ))
    .await
    .unwrap();

    server_data
        .send(Message::Text("fanout".into()))
        .await
        .unwrap();

    assert_eq!(next_text(&mut client_a).await, "fanout");
    assert_eq!(next_text(&mut client_b).await, "fanout");
    handle.abort();
}

#[tokio::test]
async fn req_009_v2_forwards_binary_frames() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s8&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s8&role=client&v=2&connectionId=conn-g",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s8&role=server&v=2&connectionId=conn-g",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let payload = vec![1_u8, 2, 3, 4, 5];
    client
        .send(Message::Binary(payload.clone().into()))
        .await
        .unwrap();

    let forwarded = timeout(Duration::from_secs(5), server_data.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match forwarded {
        Message::Binary(bytes) => assert_eq!(bytes, payload),
        other => panic!("expected binary message, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_007_replacing_server_data_closes_previous_socket() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s9&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut original, _) = connect_async(format!(
        "{}/ws?serverId=s9&role=server&v=2&connectionId=conn-h",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (_replacement, _) = connect_async(format!(
        "{}/ws?serverId=s9&role=server&v=2&connectionId=conn-h",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let closed = timeout(Duration::from_secs(5), original.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_008_control_receives_disconnected_when_last_client_leaves() {
    let (addr, handle) = spawn_test_server().await;
    let (mut control, _) =
        connect_async(format!("{}/ws?serverId=s10&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();
    let _initial_sync = next_text(&mut control).await;

    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s10&role=client&v=2&connectionId=conn-i",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let connected = next_text(&mut control).await;
    assert!(connected.contains("\"type\":\"connected\""));

    client.close(None).await.unwrap();

    let disconnected = next_text(&mut control).await;
    assert!(disconnected.contains("\"type\":\"disconnected\""));
    assert!(disconnected.contains("conn-i"));
    handle.abort();
}

#[tokio::test]
async fn req_006_client_without_connection_id_gets_routed_after_sync() {
    let (addr, handle) = spawn_test_server().await;
    let (mut control, _) =
        connect_async(format!("{}/ws?serverId=s11&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();
    let _initial_sync = next_text(&mut control).await;

    let (mut client, _) =
        connect_async(format!("{}/ws?serverId=s11&role=client&v=2", ws_base(addr)))
            .await
            .unwrap();
    let connected = next_text(&mut control).await;
    let value: serde_json::Value = serde_json::from_str(&connected).unwrap();
    let generated = value["connectionId"].as_str().unwrap().to_string();
    assert!(generated.starts_with("conn_"));

    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s11&role=server&v=2&connectionId={generated}",
        ws_base(addr)
    ))
    .await
    .unwrap();

    client.send(Message::Text("auto-id".into())).await.unwrap();
    assert_eq!(next_text(&mut server_data).await, "auto-id");
    handle.abort();
}

#[tokio::test]
async fn req_005_v1_forwards_bidirectionally() {
    let (addr, handle) = spawn_test_server().await;
    let (mut server, _) =
        connect_async(format!("{}/ws?serverId=s6&role=server&v=1", ws_base(addr)))
            .await
            .unwrap();
    let (mut client, _) =
        connect_async(format!("{}/ws?serverId=s6&role=client&v=1", ws_base(addr)))
            .await
            .unwrap();

    client
        .send(Message::Text("to-server".into()))
        .await
        .unwrap();
    let to_server = next_text(&mut server).await;
    assert_eq!(to_server, "to-server");

    server
        .send(Message::Text("to-client".into()))
        .await
        .unwrap();
    let to_client = next_text(&mut client).await;
    assert_eq!(to_client, "to-client");
    handle.abort();
}

#[tokio::test]
async fn req_005_replacing_v1_server_closes_previous_socket() {
    let (addr, handle) = spawn_test_server().await;
    let (mut original, _) =
        connect_async(format!("{}/ws?serverId=s12&role=server&v=1", ws_base(addr)))
            .await
            .unwrap();
    let (_replacement, _) =
        connect_async(format!("{}/ws?serverId=s12&role=server&v=1", ws_base(addr)))
            .await
            .unwrap();

    let closed = timeout(Duration::from_secs(5), original.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_007_replacing_control_closes_previous_socket() {
    let (addr, handle) = spawn_test_server().await;
    let (mut original, _) =
        connect_async(format!("{}/ws?serverId=s13&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();
    let _initial_sync = next_text(&mut original).await;
    let (_replacement, _) =
        connect_async(format!("{}/ws?serverId=s13&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();

    let closed = timeout(Duration::from_secs(5), original.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_011_last_client_disconnect_closes_server_data() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s14&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s14&role=client&v=2&connectionId=conn-j",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s14&role=server&v=2&connectionId=conn-j",
        ws_base(addr)
    ))
    .await
    .unwrap();

    client.close(None).await.unwrap();

    let closed = timeout(Duration::from_secs(5), server_data.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1001),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_010_pending_buffer_keeps_latest_200_frames() {
    let (addr, handle) = spawn_test_server().await;
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s15&role=client&v=2&connectionId=conn-k",
        ws_base(addr)
    ))
    .await
    .unwrap();

    for i in 0..205 {
        client
            .send(Message::Text(format!("msg-{i}").into()))
            .await
            .unwrap();
    }
    sleep(Duration::from_millis(50)).await;

    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s15&role=server&v=2&connectionId=conn-k",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let mut received = Vec::new();
    for _ in 0..200 {
        received.push(next_text(&mut server_data).await);
    }
    assert_eq!(received.first().unwrap(), "msg-5");
    assert_eq!(received.last().unwrap(), "msg-204");
    handle.abort();
}

#[tokio::test]
async fn req_012_control_gets_sync_nudge_then_forced_close_when_unresponsive() {
    let timings = RelayTimingConfig {
        initial_nudge_delay: Duration::from_millis(20),
        second_nudge_delay: Duration::from_millis(20),
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(timings, RelayLimitsConfig::default())).await;
    let (mut control, _) =
        connect_async(format!("{}/ws?serverId=s16&role=server&v=2", ws_base(addr)))
            .await
            .unwrap();
    let _initial_sync = next_text(&mut control).await;

    let (_client, _) = connect_async(format!(
        "{}/ws?serverId=s16&role=client&v=2&connectionId=conn-l",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let connected = next_text(&mut control).await;
    assert!(connected.contains("\"type\":\"connected\""));
    let sync_nudge = next_text(&mut control).await;
    assert!(sync_nudge.contains("\"type\":\"sync\""));

    let closed = timeout(Duration::from_secs(5), control.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1011),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_015_rejects_oversized_frames() {
    let limits = RelayLimitsConfig {
        max_frame_bytes: 8,
        ..RelayLimitsConfig::default()
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(RelayTimingConfig::default(), limits)).await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s17&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s17&role=client&v=2&connectionId=conn-m",
        ws_base(addr)
    ))
    .await
    .unwrap();

    client
        .send(Message::Text("0123456789".into()))
        .await
        .unwrap();

    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap();
    match closed {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 1009),
        Some(Err(_)) => {}
        other => panic!("expected close or protocol error, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_015_rejects_too_many_clients_per_connection() {
    let limits = RelayLimitsConfig {
        max_clients_per_connection: 1,
        ..RelayLimitsConfig::default()
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(RelayTimingConfig::default(), limits)).await;
    let (_first, _) = connect_async(format!(
        "{}/ws?serverId=s18&role=client&v=2&connectionId=conn-n",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut second, _) = connect_async(format!(
        "{}/ws?serverId=s18&role=client&v=2&connectionId=conn-n",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let closed = timeout(Duration::from_secs(5), second.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_015_rejects_too_many_sockets_per_session() {
    let limits = RelayLimitsConfig {
        max_sockets_per_session: 2,
        ..RelayLimitsConfig::default()
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(RelayTimingConfig::default(), limits)).await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s19&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (_client, _) = connect_async(format!(
        "{}/ws?serverId=s19&role=client&v=2&connectionId=conn-o",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut extra, _) = connect_async(format!(
        "{}/ws?serverId=s19&role=server&v=2&connectionId=conn-o",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let closed = timeout(Duration::from_secs(5), extra.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_017_closes_idle_socket() {
    let limits = RelayLimitsConfig {
        idle_timeout: Duration::from_millis(30),
        ..RelayLimitsConfig::default()
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(RelayTimingConfig::default(), limits)).await;
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s20&role=client&v=2&connectionId=conn-p",
        ws_base(addr)
    ))
    .await
    .unwrap();

    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap();
    match closed {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 1001),
        Some(Err(_)) => {}
        other => panic!("expected close or protocol error, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_018_rate_limits_inbound_messages_per_socket() {
    let limits = RelayLimitsConfig {
        max_messages_per_window: 2,
        rate_limit_window: Duration::from_secs(60),
        ..RelayLimitsConfig::default()
    };
    let (addr, handle) =
        spawn_test_server_with_state(AppState::new(RelayTimingConfig::default(), limits)).await;
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s21&role=client&v=2&connectionId=conn-q",
        ws_base(addr)
    ))
    .await
    .unwrap();

    client.send(Message::Text("m1".into())).await.unwrap();
    client.send(Message::Text("m2".into())).await.unwrap();
    client.send(Message::Text("m3".into())).await.unwrap();

    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap();
    match closed {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 1008),
        Some(Err(_)) => {}
        other => panic!("expected close or protocol error, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn req_011_server_disconnect_closes_clients() {
    let (addr, handle) = spawn_test_server().await;
    let (_control, _) = connect_async(format!("{}/ws?serverId=s3&role=server&v=2", ws_base(addr)))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!(
        "{}/ws?serverId=s3&role=client&v=2&connectionId=conn-c",
        ws_base(addr)
    ))
    .await
    .unwrap();
    let (mut server_data, _) = connect_async(format!(
        "{}/ws?serverId=s3&role=server&v=2&connectionId=conn-c",
        ws_base(addr)
    ))
    .await
    .unwrap();

    server_data.close(None).await.unwrap();

    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match closed {
        Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1012),
        other => panic!("expected close frame, got {other:?}"),
    }
    handle.abort();
}
