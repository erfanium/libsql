#!/usr/bin/env bash
# Build sqld (MiniTurso mode), start it locally, and run the E2E suite.
# Requires a recent Node.js (node:test) and cargo.
set -euo pipefail
cd "$(dirname "$0")/../../.."   # libsql workspace root

cargo build -p libsql-server

export SQLD_DB_PATH="${SQLD_DB_PATH:-$(mktemp -d)/sqld}"
export MINITURSO_DATA_DIR="${MINITURSO_DATA_DIR:-$(mktemp -d)/platform}"
export SQLD_HTTP_LISTEN_ADDR="${SQLD_HTTP_LISTEN_ADDR:-127.0.0.1:3010}"
export MINITURSO_ADMIN_LISTEN_ADDR="${MINITURSO_ADMIN_LISTEN_ADDR:-127.0.0.1:3011}"
export MINITURSO_ADMIN_KEY="${MINITURSO_ADMIN_KEY:-miniturso-admin-key-change-me}"
export MINITURSO_VERSION="${MINITURSO_VERSION:-dev}"

./target/debug/sqld --no-welcome &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 90); do
  if curl -fsS "http://${SQLD_HTTP_LISTEN_ADDR}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

cd libsql-server/tests/e2e
if [ ! -d node_modules ]; then
  npm ci --no-audit --no-fund
fi
node e2e.js "http://${SQLD_HTTP_LISTEN_ADDR}" --admin-url "http://${MINITURSO_ADMIN_LISTEN_ADDR}" --admin-key "$MINITURSO_ADMIN_KEY"
