#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
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
# mongo_gateway_compare.sh reads BATCH_SIZE from the environment and passes it
# to cmd/mongo_gateway_bench.
export BATCH_SIZE

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      exec "$ROOT/scripts/mongo_gateway_compare.sh" "$arg"
      ;;
  esac
done

compare_args=("$ROOT/scripts/mongo_gateway_compare.sh")
forward_args=()
for arg in "$@"; do
  case "$arg" in
    --out)
      forward_args+=("$arg")
      ;;
    --out=*)
      forward_args+=(--out "${arg#--out=}")
      ;;
    --docs=*|--indexes=*|--reads=*|--range-reads=*|--updates=*|--deletes=*|--mongo-mode=*|--mongo-uri=*|--mongo-client-modes=*|--treedb-client-modes=*|--treedb-document-formats=*|--title=*|--timeout=*)
      forward_args+=("${arg%%=*}" "${arg#*=}")
      ;;
    *)
      forward_args+=("$arg")
      ;;
  esac
done

has_forward_flag() {
  local flag=$1
  local arg
  if ((${#forward_args[@]})); then
    for arg in "${forward_args[@]}"; do
      if [[ "$arg" == "$flag" ]]; then
        return 0
      fi
    done
  fi
  return 1
}

append_default_flag() {
  local flag=$1
  local value=$2
  if ! has_forward_flag "$flag"; then
    exec_args+=("$flag" "$value")
  fi
}

if ! has_forward_flag "--out"; then
  OUT_DIR="${OUT_DIR:-$(mktemp -d "$TMP_BASE/gomap_mongo_gateway_compat_smoke_XXXXXX")}"
  compare_args+=(--out "$OUT_DIR")
fi

exec_args=("${compare_args[@]}")
append_default_flag --docs "$DOCS"
append_default_flag --indexes "$INDEXES"
append_default_flag --reads "$READS"
append_default_flag --range-reads "$RANGE_READS"
append_default_flag --updates "$UPDATES"
append_default_flag --deletes "$DELETES"
append_default_flag --mongo-mode "$MONGO_MODE"
append_default_flag --mongo-uri "$MONGO_URI"
append_default_flag --mongo-client-modes "$MONGO_CLIENT_MODES"
append_default_flag --treedb-client-modes "$TREEDB_CLIENT_MODES"
append_default_flag --treedb-document-formats "$TREEDB_DOCUMENT_FORMATS"
append_default_flag --title "$TITLE"
append_default_flag --timeout "$TIMEOUT"
if ((${#forward_args[@]})); then
  exec_args+=("${forward_args[@]}")
fi
exec "${exec_args[@]}"
