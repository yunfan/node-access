# node-access 2-role

All-in-one Rust `node-access` for private network access through a compatible
relay service.

This package intentionally has no `node-relay` binary. Every process runs the
same `node-access` executable and registers itself as a node named by `-name`.
It can expose local services with `-provider` and map services from other nodes
with `-visitor`.

## Build

```bash
cargo build -p node-access-2role
```

## Relay

Deploy a compatible relay service behind TLS, then pass its public base URL with
`-relay wss://relay.example.com`.

## Provider node

Expose a local service from node `devbox-a`:

```bash
cargo run -p node-access-2role --bin node-access -- \
  -relay wss://relay.example.com \
  -name devbox-a \
  -secret "shared-encryption-key" \
  -provider ssh:127.0.0.1:22:laptop
```

## Visitor node

Map `devbox-a`'s `ssh` service to local port `2222`:

```bash
cargo run -p node-access-2role --bin node-access -- \
  -relay wss://relay.example.com \
  -name laptop \
  -secret "shared-encryption-key" \
  -visitor devbox-a:ssh:127.0.0.1:2222:laptop
```

Then connect to `127.0.0.1:2222`.

Multiple nodes can expose the same service name because the visitor names the
target node explicitly:

```bash
-visitor devbox-a:ssh:127.0.0.1:2222:laptop
-visitor devbox-b:ssh:127.0.0.1:2223:laptop
```

## Flag compatibility

The documented interface keeps the old single-dash long flag style, with
`-relay` replacing the old `-hub`.

| Flag                                | Meaning                                     |
| ----------------------------------- | ------------------------------------------- |
| `-relay`                            | relay base URL                              |
| `-secret`                           | optional E2E encryption key, not auth       |
| `-name`                             | current node-access node name               |
| `-provider service:host:port[:auth]` | expose local service, optional auth         |
| `-visitor node:service:host:port[:auth]` | map remote node service locally        |
| `-list`                             | prints local config summary                 |

`-hub` is accepted only as a compatibility alias for `-relay`.
`-secret` is never sent to the relay for identity checks; it derives the local
AES-GCM key used to encrypt/decrypt mux frames between peer `node-access`
processes.

Provider/visitor auth is separate from `-secret`:

- `-provider ssh:127.0.0.1:22:laptop` only allows visitors that present
  `laptop`.
- `-visitor devbox-a:ssh:127.0.0.1:2222:laptop` presents the visitor auth value.
- A provider with no fourth `:auth` field is public to any visitor that knows
  the node and service name.
