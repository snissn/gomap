#!/usr/bin/env bash
set -euo pipefail

out=${1:?usage: run.sh OUTPUT_DIR}
repeats=${REPEATS:-2}
concurrent_commits=${CONCURRENT_COMMITS:-32}
tmp_root=${TMPDIR:-/tmp}
mkdir -p "$out"

sha=$(git rev-parse HEAD)
dirty=false
if [[ -n $(git status --porcelain) ]]; then
  dirty=true
fi
filesystem=unknown
if command -v findmnt >/dev/null 2>&1; then
  filesystem=$(findmnt -n -o FSTYPE -T "$tmp_root" 2>/dev/null || true)
  filesystem=${filesystem:-unknown}
fi

{
  echo 'schema=dgraph-durability-v2'
  echo "sha=$sha"
  echo "dirty=$dirty"
  echo "go_version=$(go version)"
  echo "kernel=$(uname -srmo)"
  echo "host=$(hostname)"
  echo "tmp_root=$tmp_root"
  echo "filesystem=$filesystem"
  echo "repeats=$repeats"
  echo 'fixed_fixture_seed=1'
  echo 'fixed_timer_boundary=operations_only'
  echo 'durability_classes=relaxed,durable'
  echo 'accepted_runs=all_correctness-passing_repeats'
  echo 'excluded_runs=none'
} >"$out/metadata.env"

fixed_cmd=(go test ./benchmarks/dgraph_durability -run '^$' -bench '^BenchmarkDgraphShapedMixedFixed' -benchmem -benchtime=1x -count="$repeats")
concurrent_cmd=(go test ./benchmarks/dgraph_durability -run '^$' -bench '^BenchmarkDgraphShapedConcurrentAcknowledgement' -benchmem -benchtime="${concurrent_commits}x" -count="$repeats")
mvcc_cmd=(go test ./TreeDB/mvcc -run '^$' -bench '^BenchmarkCommitAtGetAtInterleaved$' -benchmem -count="$repeats")

{
  printf 'GOWORK=off '
  printf '%q ' "${fixed_cmd[@]}"
  printf '\nGOWORK=off '
  printf '%q ' "${concurrent_cmd[@]}"
  printf '\nGOWORK=off '
  printf '%q ' "${mvcc_cmd[@]}"
  printf '\n'
} >"$out/commands.txt"

GOWORK=off "${fixed_cmd[@]}" | tee "$out/fixed_mixed.txt"
GOWORK=off "${concurrent_cmd[@]}" | tee "$out/concurrent_acknowledgement.txt"
GOWORK=off "${mvcc_cmd[@]}" | tee "$out/mvcc_interleaved.txt"
