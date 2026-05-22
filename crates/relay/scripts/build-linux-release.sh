#!/usr/bin/env bash
set -euo pipefail

TARGET="x86_64-unknown-linux-musl"
BIN_NAME="relay"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd "${PKG_DIR}/../.." && pwd)"
OUT_DIR="${PKG_DIR}/release-artifacts"
OUT_BIN="${OUT_DIR}/${BIN_NAME}-linux-x64-musl"

cd "${PKG_DIR}"

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found in PATH" >&2
  exit 1
fi

if ! rustup target list --installed | grep -qx "${TARGET}"; then
  echo "Installing Rust target ${TARGET}"
  rustup target add "${TARGET}"
fi

if command -v apt-get >/dev/null 2>&1 && ! command -v musl-gcc >/dev/null 2>&1; then
  echo "musl-gcc not found. On Debian/Ubuntu install musl-tools:" >&2
  echo "  sudo apt-get update && sudo apt-get install -y musl-tools" >&2
fi

echo "Building ${BIN_NAME} for ${TARGET}"
cargo build -p relay --release --target "${TARGET}"

mkdir -p "${OUT_DIR}"
cp "${WORKSPACE_DIR}/target/${TARGET}/release/${BIN_NAME}" "${OUT_BIN}"
chmod +x "${OUT_BIN}"

echo
echo "Built artifact:"
echo "  ${OUT_BIN}"
echo
echo "Sanity check:"
file "${OUT_BIN}" || true
ldd "${OUT_BIN}" || true
