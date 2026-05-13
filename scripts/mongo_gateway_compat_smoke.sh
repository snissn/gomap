#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_compat_smoke_XXXXXX")}"
DOCS="${DOCS:-1000}"
INDEXES="${INDEXES:-0}"
READS="${READS:-$DOCS}"
RANGE_READS="${RANGE_READS:-0}"
UPDATES="${UPDATES:-0}"
DELETES="${DELETES:-0}"
BATCH_SIZE="${BATCH_SIZE:-100}"
MONGO_MODE="${MONGO_MODE:-docker}"
MONGO_URI="${MONGO_URI:-mongodb://127.0.0.1:27017}"
MONGO_CLIENT_MODES="${MONGO_CLIENT_MODES:-driver}"
TREEDB_CLIENT_MODES="${TREEDB_CLIENT_MODES:-driver}"
TREEDB_DOCUMENT_FORMATS="${TREEDB_DOCUMENT_FORMATS:-bson}"
TITLE="${TITLE:-Mongo Gateway Compatibility Smoke}"
TIMEOUT="${TIMEOUT:-10m}"
export BATCH_SIZE

exec "$ROOT/scripts/mongo_gateway_compare.sh" \
  --out "$OUT_DIR" \
  --docs "$DOCS" \
  --indexes "$INDEXES" \
  --reads "$READS" \
  --range-reads "$RANGE_READS" \
  --updates "$UPDATES" \
  --deletes "$DELETES" \
  --mongo-mode "$MONGO_MODE" \
  --mongo-uri "$MONGO_URI" \
  --mongo-client-modes "$MONGO_CLIENT_MODES" \
  --treedb-client-modes "$TREEDB_CLIENT_MODES" \
  --treedb-document-formats "$TREEDB_DOCUMENT_FORMATS" \
  --title "$TITLE" \
  --timeout "$TIMEOUT" \
  "$@"
