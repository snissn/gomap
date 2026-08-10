#!/usr/bin/env bash
# Reproducible #4066 timing artifact. Nearest-rank percentiles: ceil(p*n).
set -euo pipefail
out=${1:-/tmp/gomap_4066_query_bench}
mkdir -p "$out"
sha=$(git rev-parse HEAD)
cmd='GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10'
eval "$cmd" | tee "$out/raw.txt"
{
  echo "# #4066 query benchmark"
  echo "- source: $sha"
  echo "- host: $(uname -a)"
  echo "- go: $(go version)"
  echo "- command: \`$cmd\`"
  echo "- percentile method: nearest rank, rank=ceil(p*n), n=10"
  echo "- counters: indexed-positive+residual explain=2 candidates/2 materialized/nonzero bytes; bounded scan and projection/sort are 256-doc materializations (verify via executionStats before comparisons)."
  awk '/BenchmarkMongoNegativeDottedQueryShapes\// {n=$1; sub(/^.*\//,"",n); ns=$3; b=$5; al=$7; sub("ns/op","",ns); sub("B/op","",b); sub("allocs/op","",al); a[n]=a[n] " " ns; if(!(n in lo)||b<lo[n])lo[n]=b;if(b>hi[n])hi[n]=b;if(!(n in alo)||al<alo[n])alo[n]=al;if(al>ahi[n])ahi[n]=al} END {for(n in a){split(a[n],v," "); for(i=2;i<=length(v);i++)for(j=i;j>1&&v[j]<v[j-1];j--){t=v[j];v[j]=v[j-1];v[j-1]=t}; printf "- %s: p50=%sns/op p95=%sns/op p99=%sns/op ops/s=%.1f B/op=%s-%s allocs/op=%s-%s\n",n,v[6],v[11],v[11],1e9/v[6],lo[n],hi[n],alo[n],ahi[n]}}' "$out/raw.txt"
} > "$out/report.md"
cat "$out/report.md"
