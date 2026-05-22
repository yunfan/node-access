use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use crate::session::{RelayLimitsConfig, RelayTimingConfig};

const DEFAULT_BIND: &str = "127.0.0.1:8787";
const DEFAULT_LOG_FILTER: &str = "info";
const DEFAULT_DIAGNOSTICS: bool = false;
const DEFAULT_INITIAL_NUDGE_MS: u64 = 10_000;
const DEFAULT_SECOND_NUDGE_MS: u64 = 5_000;
const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_CLIENTS_PER_CONNECTION: usize = 8;
const DEFAULT_MAX_SOCKETS_PER_SESSION: usize = 64;
const DEFAULT_MAX_OUTBOUND_QUEUE_MESSAGES: usize = 256;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_MAX_MESSAGES_PER_WINDOW: usize = 240;
const DEFAULT_RATE_LIMIT_WINDOW_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub bind_addr: SocketAddr,
    pub log_filter: String,
    pub diagnostics: bool,
    pub timings: RelayTimingConfig,
    pub limits: RelayLimitsConfig,
}

impl RelayConfig {
    pub fn from_env() -> Result<Self, String> {
        let bind_addr = parse_socket_addr("RELAY_BIND", DEFAULT_BIND)?;
        let log_filter = env::var("RELAY_LOG").unwrap_or_else(|_| DEFAULT_LOG_FILTER.to_string());
        let diagnostics = parse_bool("RELAY_DIAGNOSTICS", DEFAULT_DIAGNOSTICS)?;

        let timings = RelayTimingConfig {
            initial_nudge_delay: duration_ms("RELAY_INITIAL_NUDGE_MS", DEFAULT_INITIAL_NUDGE_MS)?,
            second_nudge_delay: duration_ms("RELAY_SECOND_NUDGE_MS", DEFAULT_SECOND_NUDGE_MS)?,
        };

        let limits = RelayLimitsConfig {
            max_frame_bytes: parse_usize("RELAY_MAX_FRAME_BYTES", DEFAULT_MAX_FRAME_BYTES)?,
            max_clients_per_connection: parse_usize(
                "RELAY_MAX_CLIENTS_PER_CONNECTION",
                DEFAULT_MAX_CLIENTS_PER_CONNECTION,
            )?,
            max_sockets_per_session: parse_usize(
                "RELAY_MAX_SOCKETS_PER_SESSION",
                DEFAULT_MAX_SOCKETS_PER_SESSION,
            )?,
            max_outbound_queue_messages: parse_usize(
                "RELAY_MAX_OUTBOUND_QUEUE_MESSAGES",
                DEFAULT_MAX_OUTBOUND_QUEUE_MESSAGES,
            )?,
            idle_timeout: duration_ms("RELAY_IDLE_TIMEOUT_MS", DEFAULT_IDLE_TIMEOUT_MS)?,
            max_messages_per_window: parse_usize(
                "RELAY_MAX_MESSAGES_PER_WINDOW",
                DEFAULT_MAX_MESSAGES_PER_WINDOW,
            )?,
            rate_limit_window: duration_ms(
                "RELAY_RATE_LIMIT_WINDOW_MS",
                DEFAULT_RATE_LIMIT_WINDOW_MS,
            )?,
        };

        Ok(Self {
            bind_addr,
            log_filter,
            diagnostics,
            timings,
            limits,
        })
    }
}

fn parse_socket_addr(key: &str, default: &str) -> Result<SocketAddr, String> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse()
        .map_err(|error| format!("{key} must be a valid socket address: {error}"))
}

fn parse_usize(key: &str, default: usize) -> Result<usize, String> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    raw.parse::<usize>()
        .map_err(|error| format!("{key} must be a positive integer: {error}"))
}

fn parse_bool(key: &str, default: bool) -> Result<bool, String> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "{key} must be a boolean (true/false, 1/0, yes/no, on/off)"
        )),
    }
}

fn duration_ms(key: &str, default_ms: u64) -> Result<Duration, String> {
    let raw = env::var(key).unwrap_or_else(|_| default_ms.to_string());
    let millis = raw
        .parse::<u64>()
        .map_err(|error| format!("{key} must be milliseconds as u64: {error}"))?;
    Ok(Duration::from_millis(millis))
}
