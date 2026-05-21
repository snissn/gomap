#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/treedb_column_graph_glove_demo.sh [--run]

Opt-in real-dataset smoke for TreeDB column_graph native-reader search.
By default this is a dry run: it prints the download and benchmark command and
does not fetch data. Pass --run to download GloVe 6B 50d if missing, build a
temporary TreeDB column_graph index, reopen it, and run the demo search path.

Environment:
  DATA_DIR   cache directory, default: $HOME/.cache/treedb-column-graph/glove
  DB_DIR     TreeDB demo directory, default: /tmp/treedb-column-graph-glove-db
  ROWS       number of GloVe rows to load, default: 10000
  TOPK       search TopK, default: 10
  EF_SEARCH  graph search ef_search, default: 128
USAGE
}

run=false
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi
if [[ "${1:-}" == "--run" ]]; then
  run=true
elif [[ $# -gt 0 ]]; then
  usage >&2
  exit 2
fi

DATA_DIR="${DATA_DIR:-$HOME/.cache/treedb-column-graph/glove}"
DB_DIR="${DB_DIR:-/tmp/treedb-column-graph-glove-db}"
ROWS="${ROWS:-10000}"
TOPK="${TOPK:-10}"
EF_SEARCH="${EF_SEARCH:-128}"
ZIP_URL="${GLOVE_ZIP_URL:-https://nlp.stanford.edu/data/glove.6B.zip}"
ZIP_PATH="$DATA_DIR/glove.6B.zip"
GLOVE_PATH="$DATA_DIR/glove.6B.50d.txt"

cat <<EOF
dataset=GloVe 6B 50d
url=$ZIP_URL
cache=$DATA_DIR
glove_file=$GLOVE_PATH
db_dir=$DB_DIR
rows=$ROWS top_k=$TOPK ef_search=$EF_SEARCH
EOF

if [[ "$run" != true ]]; then
  cat <<EOF
dry_run=true
to_run:
  scripts/treedb_column_graph_glove_demo.sh --run
EOF
  exit 0
fi

mkdir -p "$DATA_DIR"
if [[ ! -f "$GLOVE_PATH" ]]; then
  if [[ ! -f "$ZIP_PATH" ]]; then
    curl -L "$ZIP_URL" -o "$ZIP_PATH"
  fi
  unzip -p "$ZIP_PATH" glove.6B.50d.txt > "$GLOVE_PATH"
fi

GOWORK="${GOWORK:-off}" go run ./cmd/treedb_column_graph_demo \
  -dir "$DB_DIR" \
  -reset \
  -glove "$GLOVE_PATH" \
  -rows "$ROWS" \
  -top-k "$TOPK" \
  -ef-search "$EF_SEARCH" \
  -max-decoded-blocks "${MAX_DECODED_BLOCKS:-4}"
