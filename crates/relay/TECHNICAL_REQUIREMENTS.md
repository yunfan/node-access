# Relay Technical Requirements

This document defines the required external behavior for `crates/relay`.
The implementation and tests must continue to satisfy these requirements.

## Scope

The relay is a self-hosted WebSocket service for node-access deployments. It
must preserve the relay protocol behavior while not depending on any managed
edge runtime.

## Requirements

### REQ-001 Health endpoint

- `GET /health` returns HTTP 200.
- Response body is JSON with `{"status":"ok"}` semantics.

### REQ-002 WebSocket endpoint

- `GET /ws` is the only WebSocket upgrade endpoint.
- Non-upgrade requests to `/ws` return HTTP 426.
- Unknown paths return HTTP 404.

### REQ-003 Query validation

- `serverId` is required.
- `role` must be `server` or `client`.
- `v` accepts `1` or `2`.
- Missing or blank `v` falls back to `1`.
- Invalid `v` returns HTTP 400.

### REQ-004 Version isolation

- Sessions are isolated by `(serverId, version)`.
- v1 and v2 connections for the same `serverId` never share routing state.

### REQ-005 v1 routing

- v1 supports one `server` socket and one `client` socket per session.
- A new v1 socket of the same role replaces the previous one.
- Frames are forwarded unchanged between the current v1 server and client.

### REQ-006 v2 connection roles

- `role=server` without `connectionId` is the server control socket.
- `role=server` with `connectionId` is a per-connection server data socket.
- `role=client` is a client socket.
- If a v2 client omits `connectionId`, the relay generates one.

### REQ-007 v2 replacement semantics

- There is at most one server control socket per `(serverId, v2)` session.
- There is at most one server data socket per `connectionId`.
- A new control or server data socket replaces the old one using a policy close.
- Multiple client sockets may share one `connectionId`.

### REQ-008 Control messages

- Control channel messages are JSON text messages.
- The relay must handle `ping` by replying with `pong`.
- When a client connects, control sockets are notified with
  `{"type":"connected","connectionId":"..."}`.
- When the last client for a `connectionId` disconnects, control sockets are
  notified with `{"type":"disconnected","connectionId":"..."}`.
- When a control socket connects, it immediately receives
  `{"type":"sync","connectionIds":[...]}`.

### REQ-009 Data forwarding

- Client data frames are routed to the matching server data socket.
- Server data frames are routed to all client sockets for the same `connectionId`.
- The relay does not inspect or modify payload bytes beyond transport framing.

### REQ-010 Pending frame buffering

- If client frames arrive before the server data socket exists, the relay buffers
  them by `connectionId`.
- Buffer size is bounded to 200 frames per `connectionId`.
- When the server data socket connects, pending frames are flushed in order.

### REQ-011 Disconnect semantics

- When the last client for a `connectionId` disconnects, the relay clears its
  pending buffer, closes the matching server data socket, and emits
  `disconnected`.
- When a server data socket disconnects, all matching client sockets are closed
  so the client re-handshakes.

### REQ-012 Control liveness recovery

- When a client connects and there is no server data socket yet, the relay
  nudges the control socket after a delay by sending `sync`.
- If the control path remains unresponsive after the second delay, the relay
  force-closes the control socket so the daemon reconnects.

### REQ-013 Self-hosted deployment model

- The relay must run as a plain HTTP WebSocket server behind a reverse proxy.
- TLS termination is expected to happen at the gateway, not inside the relay.

### REQ-014 Test coverage

- Integration tests must cover health, validation, v2 control signaling,
  pending-frame flush, and disconnect behavior.
- A reusable test client must exist for manual verification.

### REQ-015 Public relay resource controls

- The relay must enforce a maximum inbound frame size.
- The relay must enforce a maximum number of client sockets per `connectionId`.
- The relay must enforce a maximum number of live sockets per `(serverId, version)` session.
- Exceeding these limits must fail closed with an explicit policy or size-related
  WebSocket close.

### REQ-016 Abuse-control test coverage

- Integration tests must cover frame-size rejection.
- Integration tests must cover client-per-connection rejection.
- Integration tests must cover total session-socket rejection.

### REQ-017 Idle socket reaping

- The relay must enforce idle timeouts for accepted WebSocket connections.
- A socket that remains inactive past the configured timeout must be closed.
- Idle timeout must apply without relying on source IP or external identity.

### REQ-018 Ingress rate limiting

- The relay must enforce a per-socket inbound message-rate limit.
- Exceeding the limit must fail closed with a policy close.
- Rate limiting must apply without relying on source IP or external identity.
