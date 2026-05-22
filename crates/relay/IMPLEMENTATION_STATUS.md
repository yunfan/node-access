# relay Implementation Status

This file records what `crates/relay` currently implements and what is
explicitly not implemented.

## Implemented

### Protocol compatibility

- `GET /health` returns JSON status.
- `GET /ws` is the WebSocket endpoint.
- Non-upgrade requests to `/ws` return `426`.
- Unknown paths return `404`.
- `serverId` and `role` are validated.
- Relay versions `1` and `2` are supported.
- Missing `v` falls back to v1 behavior.
- Sessions are isolated by `(serverId, version)`.

### v1 behavior

- Single active `server` socket per session.
- Single active `client` socket per session.
- Same-role replacement closes the previous socket.
- Bidirectional text/binary forwarding between current v1 server and client.

### v2 behavior

- `server` without `connectionId` acts as control socket.
- `server` with `connectionId` acts as server-data socket.
- `client` acts as client socket.
- Missing client `connectionId` is auto-generated.
- Single active control socket per session.
- Single active server-data socket per `connectionId`.
- Multiple clients may share one `connectionId`.
- Same-identity replacement closes the previous server-side socket.

### Control-plane behavior

- Control socket receives initial `sync`.
- Client connect emits `connected`.
- Last client disconnect emits `disconnected`.
- Control `ping` receives `pong`.
- Missing server-data after client connect triggers `sync` nudge.
- Continued control non-response triggers forced control close.

### Data-plane behavior

- v2 client -> server-data forwarding.
- v2 server-data -> client forwarding.
- Fanout from server-data to multiple clients on one `connectionId`.
- Binary frame forwarding.
- Pending frame buffering before server-data exists.
- Pending frame flush when server-data connects.
- Pending frame queue bounded to latest 200 frames per `connectionId`.

### Disconnect behavior

- Last client disconnect closes matching server-data socket.
- Server-data disconnect closes matching clients so they reconnect.

### Public-service hardening

- Maximum inbound frame size limit.
- Maximum client count per `connectionId`.
- Maximum live socket count per `(serverId, version)` session.
- Bounded outbound per-socket queue.
- Slow-consumer fail-closed behavior on outbound queue saturation.
- Idle socket timeout.
- Per-socket inbound message-rate limit.

### Configuration model

- Runtime configuration is environment-variable based.
- Built-in defaults live in `src/config.rs`.
- Missing environment variables fall back to compiled defaults.
- Deployment target is a single binary plus environment variables.

### Testing

- Integration coverage for protocol validation, forwarding, replacement,
  disconnect behavior, pending buffering, abuse controls, idle timeout, and
  rate limiting.
- Manual test client exists at `src/bin/relay-test-client.rs`.

## Not implemented

### Security / abuse controls not yet present

- Byte-rate limiting per socket. Current rate limiting is message-count based,
  not byte-volume based.
- Global process-wide connection cap across all sessions.
- Global process-wide memory budget enforcement for buffered frames and queues.
- Pre-upgrade HTTP handshake rate limiting or early handshake shedding.
- Origin-based or token-based admission control.
- IP-based rate limiting or IP reputation controls.
  This is intentionally omitted for the current zero-trust model.

### Observability not yet present

- Metrics export.
- Structured counters for rejects, timeouts, or slow-consumer closes.
- Admin/status endpoint beyond `/health`.

### Deployment / operations not yet present

- Reverse-proxy deployment example.
- systemd service file.
- Container image / Dockerfile.
- CI wiring in the monorepo root.

### Deployment / operations present

- Single-binary Linux x64 musl release build script at
  `scripts/build-linux-release.sh`.

### Transport / scaling not yet present

- Multi-process or multi-node shared session routing.
- External state backend.
- Sticky-routing layer for horizontal scale.

## Current verification baseline

Verified locally in this workspace with:

```bash
PATH="$HOME/.cargo/bin:$PATH" cargo fmt --all -- --check
PATH="$HOME/.cargo/bin:$PATH" cargo test
```

Latest known result at time of writing: `26 passed, 0 failed`.
