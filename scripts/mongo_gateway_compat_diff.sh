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
PORT="$(docker port "$NAME" 27017/tcp | sed -n '1s/.*://p')"
for _ in $(seq 1 30); do docker exec "$NAME" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1 && break; sleep 1; done
GOWORK=off go run ./cmd/mongo_gateway_compat_diff -reference-uri "mongodb://127.0.0.1:${PORT}/?directConnection=true" -out "$OUT" $SMOKE
