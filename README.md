# node-access

Standalone Rust relay and node access tools for private network access.

This repository contains:

- `crates/relay`: self-hosted relay server used by the node tools.
- `crates/node-access-3role`: 3-role version with `node-relay` plus
  `node-access`.
- `crates/node-access-2role`: all-in-one version with only `node-access`.

## Build

```bash
cargo build --workspace
```

## Release

GitHub Actions builds release artifacts when a version tag is pushed:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow uploads Linux x64 musl binaries for `relay`, the all-in-one
`node-access`, the 3-role `node-access`, and `node-relay`, plus a tarball and
`SHA256SUMS`.

## Relay

Run a local relay server:

```bash
cargo run -p relay --bin relay
```

For public use, deploy it behind TLS and pass the public WebSocket URL to the
node tools with `-relay wss://relay.example.com`.

## 3-role Version

Use `node-relay` on the network side and `node-access` on access nodes.

```bash
cargo run -p node-access-3role --bin node-relay -- \
  -relay wss://relay.example.com \
  -id my-node \
  -secret "shared-encryption-key"

cargo run -p node-access-3role --bin node-access -- \
  -relay wss://relay.example.com \
  -node my-node \
  -secret "shared-encryption-key" \
  -visitor ssh:127.0.0.1:2222:laptop
```

See `crates/node-access-3role/README.md`.

## 2-role Version

Every process runs the same `node-access` binary. `-name` is the current node
name. Providers expose services from the current node; visitors target another
node explicitly.

```bash
cargo run -p node-access-2role --bin node-access -- \
  -relay wss://relay.example.com \
  -name devbox-a \
  -secret "shared-encryption-key" \
  -provider ssh:127.0.0.1:22:laptop

cargo run -p node-access-2role --bin node-access -- \
  -relay wss://relay.example.com \
  -name laptop \
  -secret "shared-encryption-key" \
  -visitor devbox-a:ssh:127.0.0.1:2222:laptop
```

See `crates/node-access-2role/README.md`.

## Security Model

`-secret` is only an end-to-end encryption key for frames exchanged by the node
tools. It is not sent to the relay and is not an identity credential.

Provider/visitor `auth` is service-level access control:

- `provider service:host:port` is public.
- `provider service:host:port:auth` requires a visitor with the same auth.
