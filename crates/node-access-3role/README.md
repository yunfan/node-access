# node-access 3-role

Rust `node-relay` and `node-access` tools for private network access through a
compatible relay service.

The relay service uses a compatible v2 WebSocket protocol:

- `serverId` is the shared room/node identity.
- `node-relay` connects as the v2 server control socket and opens server data
  sockets when access clients appear.
- `node-access` connects as a v2 client data socket.
- A small encrypted multiplexing protocol runs inside the data socket to carry
  SOCKS5 and provider/visitor TCP streams.

## Build

```bash
cargo build -p node-access-3role
```

## Relay

Deploy a compatible relay service behind TLS, then pass its public base URL to
both tools with `-relay wss://relay.example.com`.

## Cloud side: node-relay

Run this in the environment whose private network you want to access:

```bash
cargo run -p node-access-3role --bin node-relay -- \
  -relay wss://relay.example.com \
  -id my-cloud-node \
  -secret "shared-encryption-key"
```

## Local side: SOCKS5 access

```bash
cargo run -p node-access-3role --bin node-access -- \
  -relay wss://relay.example.com \
  -node my-cloud-node \
  -secret "shared-encryption-key" \
  -name laptop \
  -socks5 127.0.0.1:1080
```

Configure local applications to use `127.0.0.1:1080` as a SOCKS5 proxy. DNS is
handled by the SOCKS client; use SOCKS5-hostname mode when you want resolution
to happen from the cloud side.

## Provider / visitor TCP mappings

Provider exposes a TCP service from one access node:

```bash
cargo run -p node-access-3role --bin node-access -- \
  -relay wss://relay.example.com \
  -node my-cloud-node \
  -secret "shared-encryption-key" \
  -name provider-box \
  --no-socks5 \
  -provider ssh:127.0.0.1:22:laptop
```

Visitor creates a local listener that reaches the named provider:

```bash
cargo run -p node-access-3role --bin node-access -- \
  -relay wss://relay.example.com \
  -node my-cloud-node \
  -secret "shared-encryption-key" \
  -name laptop \
  --no-socks5 \
  -visitor ssh:127.0.0.1:2222:laptop
```

Then connect to `127.0.0.1:2222`.

## Legacy flag mapping

| Old flag                  | New flag / behavior                         |
| ------------------------- | ------------------------------------------- |
| `-relay`                  | relay base URL                              |
| `-node` / `-id`           | shared relay server ID                      |
| `-secret`                 | optional E2E encryption key, not auth       |
| `-socks5`                 | local SOCKS5 listen address                 |
| `-provider name:host:port[:auth]` | provider TCP mapping, optional auth |
| `-visitor name:host:port[:auth]`  | visitor TCP mapping, optional auth  |
| `-list`                   | accepted, but relay has no directory        |

The binaries also accept the equivalent double-dash forms, but single-dash long
flags are the documented interface.

`-hub` is accepted only as a compatibility alias for `-relay`.
`-secret` is never sent to the relay for identity checks; it derives the local
AES-GCM key used to encrypt/decrypt mux frames between the two endpoint tools.

Provider/visitor auth is separate from `-secret`:

- `-provider ssh:127.0.0.1:22:laptop` only allows visitors that present
  `laptop`.
- `-visitor ssh:127.0.0.1:2222:laptop` presents the visitor auth value.
- A provider with no fourth `:auth` field is public to any visitor that knows
  the provider name and relay session.

Extra dashboards are not part of this package; the relay service is the only
external dependency.
