# relay

Rust relay server for self-hosted node-access deployments.

## Run

```bash
cargo run -p relay --bin relay
```

## Build Linux x64 Release

For a single Linux x64 binary that does not depend on the target machine's
`glibc`, build the `musl` target:

```bash
./scripts/build-linux-release.sh
```

Output artifact:

```bash
release-artifacts/relay-linux-x64-musl
```

The script targets:

```bash
x86_64-unknown-linux-musl
```

It will auto-install the Rust target if needed. On Debian/Ubuntu, if `musl-gcc`
is missing, install:

```bash
sudo apt-get update && sudo apt-get install -y musl-tools
```

Environment variables:

- `RELAY_BIND` default: `127.0.0.1:8787`
- `RELAY_LOG` default: `info`
- `RELAY_DIAGNOSTICS` default: `false`
- `RELAY_INITIAL_NUDGE_MS` default: `10000`
- `RELAY_SECOND_NUDGE_MS` default: `5000`
- `RELAY_MAX_FRAME_BYTES` default: `65536`
- `RELAY_MAX_CLIENTS_PER_CONNECTION` default: `8`
- `RELAY_MAX_SOCKETS_PER_SESSION` default: `64`
- `RELAY_MAX_OUTBOUND_QUEUE_MESSAGES` default: `256`
- `RELAY_IDLE_TIMEOUT_MS` default: `120000`
- `RELAY_MAX_MESSAGES_PER_WINDOW` default: `240`
- `RELAY_RATE_LIMIT_WINDOW_MS` default: `10000`

All defaults are built into the binary. If an environment variable is unset, the
compiled default is used automatically.

Diagnostic logging:

- Set `RELAY_DIAGNOSTICS=true` to emit additional startup and connection
  lifecycle logs.
- This includes effective startup config, query validation failures, sync
  nudges, socket registration and teardown, buffering before server-data attach,
  and rate-limit / frame-size / idle-timeout diagnostics.
- When enabled, the event messages are emitted as Chinese labels so they are
  easier to visually scan in operations logs.

## Test client

```bash
cargo run -p relay --bin relay-test-client -- control --base-url ws://127.0.0.1:8787 --server-id demo
cargo run -p relay --bin relay-test-client -- client --base-url ws://127.0.0.1:8787 --server-id demo --send-text hello
cargo run -p relay --bin relay-test-client -- server-data --base-url ws://127.0.0.1:8787 --server-id demo --connection-id conn1
```

## Requirements

Behavior is defined in [TECHNICAL_REQUIREMENTS.md](./TECHNICAL_REQUIREMENTS.md).

Current implementation coverage and explicit gaps are tracked in
[IMPLEMENTATION_STATUS.md](./IMPLEMENTATION_STATUS.md).
