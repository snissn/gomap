#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${COLUMN_VECTOR_DEEP1B_DIR:="${HOME}/.cache/gomap/deep1b"}"
: "${COLUMN_VECTOR_DEEP1B_DOWNLOAD:=1}"
: "${COLUMN_VECTOR_DEEP1B_COMPRESSIONS:=none,zstd}"
: "${RUN_10M:=false}"
: "${RUN_BUILD_OPEN_DECODE:=false}"
: "${BENCHTIME:=500ms}"
: "${COUNT:=1}"

export COLUMN_VECTOR_DEEP1B=1
export COLUMN_VECTOR_DEEP1B_DIR
export COLUMN_VECTOR_DEEP1B_DOWNLOAD
export COLUMN_VECTOR_DEEP1B_COMPRESSIONS

shape_re='1m'
if [[ "${RUN_10M}" == "true" || "${RUN_10M}" == "1" ]]; then
  export COLUMN_VECTOR_DEEP1B_10M=1
  shape_re='1m|10m'
fi

bench_name='BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine'
if [[ "${RUN_BUILD_OPEN_DECODE}" == "true" || "${RUN_BUILD_OPEN_DECODE}" == "1" ]]; then
  export COLUMN_VECTOR_DEEP1B_BUILD_OPEN_DECODE=1
  bench_name='BenchmarkColumnVectorGraphDeep1BPersisted(SearchCosine|BuildOpenDecode)'
fi

cd "${ROOT_DIR}"
GOWORK=off go test ./experiments/colgranule \
  -run '^$' \
  -bench "^${bench_name}/(${shape_re})/" \
  -benchmem \
  -benchtime "${BENCHTIME}" \
  -count "${COUNT}"
