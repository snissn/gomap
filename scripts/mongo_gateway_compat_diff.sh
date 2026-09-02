#!/usr/bin/env bash
set -euo pipefail

# The Docker image is deliberately pinned. The Go command never starts Docker,
# so unit tests have no container dependency.
IMAGE="mongo:7.0.14"
OUT=""
SMOKE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT="$2"; shift 2 ;;
    --smoke) SMOKE="-smoke"; shift ;;
    *) echo "usage: $0 --out <artifact-dir> [--smoke]" >&2; exit 2 ;;
  esac
done
[[ -n "$OUT" ]] || { echo "--out is required" >&2; exit 2; }
NAME="gomap-compatdiff-$$"
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --rm --name "$NAME" -p 127.0.0.1::27017 "$IMAGE" --bind_ip_all >/dev/null
PORT=""
for _ in $(seq 1 30); do
  PORT="$(docker port "$NAME" 27017/tcp 2>/dev/null | sed -n '1s/.*://p')"
  if [[ -n "$PORT" ]] && docker exec "$NAME" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; then
    break
  fi
  PORT=""
  sleep 1
done
[[ -n "$PORT" ]] || { echo "reference MongoDB did not become ready" >&2; exit 3; }
BIN="$(mktemp "${TMPDIR:-/tmp}/mongo_gateway_compat_diff.XXXXXX")"
trap 'rm -f "$BIN"; cleanup' EXIT
GOWORK=off go build -o "$BIN" ./cmd/mongo_gateway_compat_diff
"$BIN" -reference-uri "mongodb://127.0.0.1:${PORT}/?directConnection=true" -reference-image "$IMAGE" -out "$OUT" $SMOKE
