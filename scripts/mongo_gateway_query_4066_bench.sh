#!/usr/bin/env bash
# Reproducible #4066 timing artifact. Nearest-rank percentiles: ceil(p*n).
set -euo pipefail
out=${1:-/tmp/gomap_4066_query_bench}
mkdir -p "$out"
sha=$(git rev-parse HEAD)
cmd='GOWORK=off go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10'
eval "$cmd" | tee "$out/raw.txt"
{
  echo "# #4066 query benchmark"
  echo "- source: $sha"
  echo "- host: $(uname -a)"
  echo "- go: $(go version)"
  echo "- command: \`$cmd\`"
  echo "- percentile method: nearest rank, rank=ceil(p*n), n=10"
  echo "- counters: indexed-positive+residual explain=2 candidates/2 materialized/nonzero bytes; bounded scan and projection/sort are 256-doc materializations (verify via executionStats before comparisons)."
  awk '/BenchmarkMongoNegativeDottedQueryShapes\// {n=$1; sub(/^.*\//,"",n); x=$3; sub("ns/op","",x); a[n]=a[n] " " x} END {for(n in a){split(a[n],v," "); for(i=2;i<=length(v);i++)for(j=i;j>1&&v[j]<v[j-1];j--){t=v[j];v[j]=v[j-1];v[j-1]=t}; printf "- %s: p50=%sns/op p95=%sns/op p99=%sns/op ops/s=%.1f\n",n,v[6],v[11],v[11],1e9/v[6]}}' "$out/raw.txt"
} > "$out/report.md"
cat "$out/report.md"
