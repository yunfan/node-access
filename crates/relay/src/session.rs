use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tracing::{info, warn};
use uuid::Uuid;

use crate::protocol::{ConnectionRole, RelayVersion};

const PENDING_FRAME_LIMIT: usize = 200;
const INITIAL_NUDGE_DELAY: Duration = Duration::from_secs(10);
const SECOND_NUDGE_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct RelayTimingConfig {
    pub initial_nudge_delay: Duration,
    pub second_nudge_delay: Duration,
}

impl Default for RelayTimingConfig {
    fn default() -> Self {
        Self {
            initial_nudge_delay: INITIAL_NUDGE_DELAY,
            second_nudge_delay: SECOND_NUDGE_DELAY,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelayLimitsConfig {
    pub max_frame_bytes: usize,
    pub max_clients_per_connection: usize,
    pub max_sockets_per_session: usize,
    pub max_outbound_queue_messages: usize,
    pub idle_timeout: Duration,
    pub max_messages_per_window: usize,
    pub rate_limit_window: Duration,
}

impl Default for RelayLimitsConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 64 * 1024,
            max_clients_per_connection: 8,
            max_sockets_per_session: 64,
            max_outbound_queue_messages: 256,
            idle_timeout: Duration::from_secs(120),
            max_messages_per_window: 240,
            rate_limit_window: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub server_id: String,
    pub version: RelayVersion,
}

#[derive(Debug, Clone)]
pub enum OutboundMessage {
    Text(String),
    Binary(Bytes),
    Pong(Bytes),
    Close { code: u16, reason: String },
}

#[derive(Debug, Clone)]
pub enum RelayFrame {
    Text(String),
    Binary(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(Uuid);

impl PeerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone)]
pub enum PeerKind {
    V1Server,
    V1Client,
    V2ServerControl,
    V2ServerData { connection_id: String },
    V2Client { connection_id: String },
}

#[derive(Debug, Clone)]
pub struct PeerMeta {
    pub peer_id: PeerId,
    pub server_id: String,
    pub role: ConnectionRole,
    pub version: RelayVersion,
    pub connection_id: Option<String>,
    pub kind: PeerKind,
}

#[derive(Debug, Clone)]
pub struct PeerHandle {
    pub meta: PeerMeta,
    pub tx: mpsc::Sender<OutboundMessage>,
}

#[derive(Default)]
struct SessionState {
    v1_server: Option<PeerHandle>,
    v1_client: Option<PeerHandle>,
    server_control: Option<PeerHandle>,
    server_data: HashMap<String, PeerHandle>,
    clients: HashMap<String, HashMap<PeerId, PeerHandle>>,
    pending: HashMap<String, VecDeque<RelayFrame>>,
}

#[derive(Default)]
pub struct SessionRegistry {
    timings: RelayTimingConfig,
    limits: RelayLimitsConfig,
    sessions: Mutex<HashMap<SessionKey, Arc<RelaySession>>>,
}

impl SessionRegistry {
    pub fn new(timings: RelayTimingConfig, limits: RelayLimitsConfig) -> Self {
        Self {
            timings,
            limits,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_create(&self, key: SessionKey) -> Arc<RelaySession> {
        let mut sessions = self.sessions.lock().await;
        let timings = self.timings.clone();
        let limits = self.limits.clone();
        sessions
            .entry(key.clone())
            .or_insert_with(|| Arc::new(RelaySession::new(key, timings, limits)))
            .clone()
    }
}

pub struct RelaySession {
    key: SessionKey,
    timings: RelayTimingConfig,
    limits: RelayLimitsConfig,
    state: Mutex<SessionState>,
}

impl RelaySession {
    pub fn new(key: SessionKey, timings: RelayTimingConfig, limits: RelayLimitsConfig) -> Self {
        Self {
            key,
            timings,
            limits,
            state: Mutex::new(SessionState::default()),
        }
    }

    pub async fn register(self: &Arc<Self>, handle: PeerHandle) -> Result<(), &'static str> {
        let mut state = self.state.lock().await;
        if count_live_sockets(&state) >= self.limits.max_sockets_per_session {
            return Err("Session socket limit exceeded");
        }
        match &handle.meta.kind {
            PeerKind::V1Server => {
                if let Some(existing) = state.v1_server.replace(handle.clone()) {
                    close_handle(&existing, 1008, "Replaced by new connection");
                }
            }
            PeerKind::V1Client => {
                if let Some(existing) = state.v1_client.replace(handle.clone()) {
                    close_handle(&existing, 1008, "Replaced by new connection");
                }
            }
            PeerKind::V2ServerControl => {
                if let Some(existing) = state.server_control.replace(handle.clone()) {
                    close_handle(&existing, 1008, "Replaced by new connection");
                }
                let connection_ids = collect_client_connection_ids(&state);
                info!(
                    server_id = %self.key.server_id,
                    version = %self.key.version.as_str(),
                    connection_ids = ?connection_ids,
                    "控制通道发送初始同步"
                );
                let sync = json!({ "type": "sync", "connectionIds": connection_ids }).to_string();
                send_or_close(&handle, OutboundMessage::Text(sync));
            }
            PeerKind::V2ServerData { connection_id } => {
                if let Some(existing) = state
                    .server_data
                    .insert(connection_id.clone(), handle.clone())
                {
                    close_handle(&existing, 1008, "Replaced by new connection");
                }
                info!(
                    server_id = %self.key.server_id,
                    version = %self.key.version.as_str(),
                    connection_id = %connection_id,
                    pending_frames = state.pending.get(connection_id).map(VecDeque::len).unwrap_or(0),
                    "服务端数据通道已注册"
                );
                flush_pending_locked(&mut state, connection_id, &handle);
            }
            PeerKind::V2Client { connection_id } => {
                let clients = state.clients.entry(connection_id.clone()).or_default();
                if clients.len() >= self.limits.max_clients_per_connection {
                    return Err("Client connection limit exceeded");
                }
                clients.insert(handle.meta.peer_id, handle.clone());
                info!(
                    server_id = %self.key.server_id,
                    version = %self.key.version.as_str(),
                    connection_id = %connection_id,
                    client_sockets = clients.len(),
                    "客户端数据通道已注册"
                );
                notify_control_locked(
                    &state,
                    json!({ "type": "connected", "connectionId": connection_id }).to_string(),
                );
                let session = Arc::clone(self);
                let connection_id = connection_id.clone();
                tokio::spawn(async move {
                    session.nudge_or_reset_control(connection_id).await;
                });
            }
        }
        Ok(())
    }

    pub async fn handle_frame(&self, meta: &PeerMeta, frame: RelayFrame) {
        let mut state = self.state.lock().await;

        match (&meta.version, &meta.kind, frame) {
            (RelayVersion::V1, PeerKind::V1Server, relay_frame) => {
                if let Some(target) = &state.v1_client {
                    forward_frame(target, relay_frame);
                }
            }
            (RelayVersion::V1, PeerKind::V1Client, relay_frame) => {
                if let Some(target) = &state.v1_server {
                    forward_frame(target, relay_frame);
                }
            }
            (RelayVersion::V2, PeerKind::V2ServerControl, RelayFrame::Text(text)) => {
                if text_is_ping(&text) {
                    let pong = json!({ "type": "pong", "ts": now_ms() }).to_string();
                    if let Some(control) = &state.server_control {
                        if control.meta.peer_id == meta.peer_id {
                            send_or_close(control, OutboundMessage::Text(pong));
                        }
                    }
                }
            }
            (RelayVersion::V2, PeerKind::V2ServerControl, RelayFrame::Binary(_)) => {}
            (RelayVersion::V2, PeerKind::V2Client { connection_id }, relay_frame) => {
                if let Some(server) = state.server_data.get(connection_id) {
                    forward_frame(server, relay_frame);
                } else {
                    info!(
                        server_id = %self.key.server_id,
                        version = %self.key.version.as_str(),
                        connection_id = %connection_id,
                        buffered_frames = state.pending.get(connection_id).map(VecDeque::len).unwrap_or(0) + 1,
                        "服务端数据通道未就绪，先缓存客户端帧"
                    );
                    buffer_pending_locked(&mut state, connection_id, relay_frame);
                }
            }
            (RelayVersion::V2, PeerKind::V2ServerData { connection_id }, relay_frame) => {
                if let Some(clients) = state.clients.get(connection_id) {
                    for target in clients.values() {
                        forward_frame(target, relay_frame.clone());
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn handle_ping(&self, meta: &PeerMeta, payload: Bytes) {
        let state = self.state.lock().await;
        let target = match &meta.kind {
            PeerKind::V1Server => state.v1_server.as_ref(),
            PeerKind::V1Client => state.v1_client.as_ref(),
            PeerKind::V2ServerControl => state.server_control.as_ref(),
            PeerKind::V2ServerData { connection_id } => state.server_data.get(connection_id),
            PeerKind::V2Client { connection_id } => state
                .clients
                .get(connection_id)
                .and_then(|clients| clients.get(&meta.peer_id)),
        };
        if let Some(handle) = target {
            send_or_close(handle, OutboundMessage::Pong(payload));
        }
    }

    pub async fn unregister(&self, meta: &PeerMeta) {
        let mut state = self.state.lock().await;
        match &meta.kind {
            PeerKind::V1Server => {
                clear_if_same(&mut state.v1_server, meta.peer_id);
            }
            PeerKind::V1Client => {
                clear_if_same(&mut state.v1_client, meta.peer_id);
            }
            PeerKind::V2ServerControl => {
                clear_if_same(&mut state.server_control, meta.peer_id);
            }
            PeerKind::V2Client { connection_id } => {
                let mut now_empty = false;
                if let Some(clients) = state.clients.get_mut(connection_id) {
                    clients.remove(&meta.peer_id);
                    now_empty = clients.is_empty();
                }
                if now_empty {
                    state.clients.remove(connection_id);
                    state.pending.remove(connection_id);
                    if let Some(server) = state.server_data.remove(connection_id) {
                        info!(
                            server_id = %self.key.server_id,
                            version = %self.key.version.as_str(),
                            connection_id = %connection_id,
                            "最后一个客户端已断开，关闭对应服务端数据通道"
                        );
                        close_handle(&server, 1001, "Client disconnected");
                    }
                    info!(
                        server_id = %self.key.server_id,
                        version = %self.key.version.as_str(),
                        connection_id = %connection_id,
                        "会话已结束，向控制通道发送 disconnected"
                    );
                    notify_control_locked(
                        &state,
                        json!({ "type": "disconnected", "connectionId": connection_id })
                            .to_string(),
                    );
                }
            }
            PeerKind::V2ServerData { connection_id } => {
                if matches!(
                    state.server_data.get(connection_id),
                    Some(handle) if handle.meta.peer_id == meta.peer_id
                ) {
                    state.server_data.remove(connection_id);
                }
                info!(
                    server_id = %self.key.server_id,
                    version = %self.key.version.as_str(),
                    connection_id = %connection_id,
                    client_sockets = state.clients.get(connection_id).map(HashMap::len).unwrap_or(0),
                    "服务端数据通道已断开，关闭对应客户端通道"
                );
                if let Some(clients) = state.clients.get(connection_id) {
                    for client in clients.values() {
                        close_handle(client, 1012, "Server disconnected");
                    }
                }
            }
        }
    }

    async fn nudge_or_reset_control(self: Arc<Self>, connection_id: String) {
        let timings = self.timings.clone();
        sleep(timings.initial_nudge_delay).await;
        if !self.has_client_socket(&connection_id).await {
            return;
        }
        if self.has_server_data_socket(&connection_id).await {
            return;
        }

        self.send_sync_to_control().await;

        sleep(timings.second_nudge_delay).await;
        if !self.has_client_socket(&connection_id).await {
            return;
        }
        if self.has_server_data_socket(&connection_id).await {
            return;
        }

        let state = self.state.lock().await;
        if let Some(control) = &state.server_control {
            warn!(
                server_id = %self.key.server_id,
                connection_id = %connection_id,
                "控制通道长时间无响应，执行主动重置"
            );
            close_handle(control, 1011, "Control unresponsive");
        }
    }

    async fn has_client_socket(&self, connection_id: &str) -> bool {
        let state = self.state.lock().await;
        state
            .clients
            .get(connection_id)
            .map(|clients| !clients.is_empty())
            .unwrap_or(false)
    }

    async fn has_server_data_socket(&self, connection_id: &str) -> bool {
        let state = self.state.lock().await;
        state.server_data.contains_key(connection_id)
    }

    async fn send_sync_to_control(&self) {
        let state = self.state.lock().await;
        let connection_ids = collect_client_connection_ids(&state);
        info!(
            server_id = %self.key.server_id,
            version = %self.key.version.as_str(),
            connection_ids = ?connection_ids,
            "控制通道补发同步提示"
        );
        notify_control_locked(
            &state,
            json!({ "type": "sync", "connectionIds": connection_ids }).to_string(),
        );
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn text_is_ping(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(|v| v.as_str())
                .map(|s| s == "ping")
        })
        .unwrap_or(false)
}

fn buffer_pending_locked(state: &mut SessionState, connection_id: &str, frame: RelayFrame) {
    let pending = state.pending.entry(connection_id.to_string()).or_default();
    pending.push_back(frame);
    while pending.len() > PENDING_FRAME_LIMIT {
        pending.pop_front();
    }
}

fn flush_pending_locked(state: &mut SessionState, connection_id: &str, handle: &PeerHandle) {
    let Some(mut pending) = state.pending.remove(connection_id) else {
        return;
    };
    while let Some(frame) = pending.pop_front() {
        forward_frame(handle, frame);
    }
}

fn collect_client_connection_ids(state: &SessionState) -> Vec<String> {
    let mut ids: Vec<String> = state
        .clients
        .iter()
        .filter_map(|(id, clients)| (!clients.is_empty()).then_some(id.clone()))
        .collect();
    let unique: HashSet<_> = ids.drain(..).collect();
    let mut ids: Vec<String> = unique.into_iter().collect();
    ids.sort();
    ids
}

fn count_live_sockets(state: &SessionState) -> usize {
    let v1 = usize::from(state.v1_server.is_some()) + usize::from(state.v1_client.is_some());
    let control = usize::from(state.server_control.is_some());
    let server_data = state.server_data.len();
    let clients: usize = state.clients.values().map(HashMap::len).sum();
    v1 + control + server_data + clients
}

fn forward_frame(target: &PeerHandle, frame: RelayFrame) {
    let message = match frame {
        RelayFrame::Text(text) => OutboundMessage::Text(text),
        RelayFrame::Binary(bytes) => OutboundMessage::Binary(bytes),
    };
    send_or_close(target, message);
}

fn notify_control_locked(state: &SessionState, message: String) {
    if let Some(control) = &state.server_control {
        send_or_close(control, OutboundMessage::Text(message));
    }
}

fn close_handle(handle: &PeerHandle, code: u16, reason: &str) {
    let _ = handle.tx.try_send(OutboundMessage::Close {
        code,
        reason: reason.to_string(),
    });
}

fn clear_if_same(slot: &mut Option<PeerHandle>, peer_id: PeerId) {
    if matches!(slot.as_ref(), Some(handle) if handle.meta.peer_id == peer_id) {
        *slot = None;
    }
}

pub fn generate_connection_id() -> String {
    format!("conn_{}", Uuid::new_v4().simple())
}

fn send_or_close(handle: &PeerHandle, message: OutboundMessage) {
    match handle.tx.try_send(message) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            let _ = handle.tx.try_send(OutboundMessage::Close {
                code: 1013,
                reason: "Slow consumer".to_string(),
            });
        }
        Err(TrySendError::Closed(_)) => {}
    }
}
