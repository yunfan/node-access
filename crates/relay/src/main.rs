use std::net::SocketAddr;

use relay::{run_server_with_state, AppState, RelayConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RelayConfig::from_env()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.log_filter.clone()));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    if config.diagnostics {
        info!(
            bind_addr = %config.bind_addr,
            log_filter = %config.log_filter,
            diagnostics = config.diagnostics,
            initial_nudge_ms = config.timings.initial_nudge_delay.as_millis(),
            second_nudge_ms = config.timings.second_nudge_delay.as_millis(),
            max_frame_bytes = config.limits.max_frame_bytes,
            max_clients_per_connection = config.limits.max_clients_per_connection,
            max_sockets_per_session = config.limits.max_sockets_per_session,
            max_outbound_queue_messages = config.limits.max_outbound_queue_messages,
            idle_timeout_ms = config.limits.idle_timeout.as_millis(),
            max_messages_per_window = config.limits.max_messages_per_window,
            rate_limit_window_ms = config.limits.rate_limit_window.as_millis(),
            "已启用中继诊断日志"
        );
    }

    let bind_addr: SocketAddr = config.bind_addr;
    run_server_with_state(
        bind_addr,
        AppState::new(config.timings, config.limits).with_diagnostics(config.diagnostics),
    )
    .await?;
    Ok(())
}
