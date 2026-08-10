#!/usr/bin/env bash
# Reproducible #4066 timing artifact. Nearest-rank percentiles: ceil(p*n).
set -euo pipefail
out=${1:-/tmp/gomap_4066_query_bench}
mkdir -p "$out"
sha=$(git rev-parse HEAD)
cmd='GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run ^$ -bench ^BenchmarkMongoNegativeDottedQueryShapes$ -benchtime=100x -count=10'
GOWORK=off GOCACHE=/tmp/gomap_4066_bench_gocache go test ./TreeDB/mongo_gateway -run '^$' -bench '^BenchmarkMongoNegativeDottedQueryShapes$' -benchtime=100x -count=10 | tee "$out/raw.txt"
test "$(grep -c 'BenchmarkMongoNegativeDottedQueryShapes/' "$out/raw.txt")" -eq 40
for shape in indexed_positive_residual bounded_negative_scan dotted_projection dotted_sort_materialized; do
  test "$(grep -c "BenchmarkMongoNegativeDottedQueryShapes/$shape" "$out/raw.txt")" -eq 10
done
{
  echo "# #4066 query benchmark"
  echo "- source: $sha"
  echo "- host: $(uname -a)"
  echo "- go: $(go version)"
  echo "- command: \`$cmd\`"
  echo "- percentile method: nearest rank, rank=ceil(p*n), n=10"
  echo "- counters: indexed-positive+residual preflight/explain: equality result=1, candidates=2, materialized=2, materializedBytes>0; bounded-negative/preflight=256 docs; dotted projection/sort preflight=256 docs, bounded materialization."
  awk '/BenchmarkMongoNegativeDottedQueryShapes\// {n=$1; sub(/^.*\//,"",n); ns=$3; b=$6; al=$8; sub("ns/op","",ns); sub("B/op","",b); sub("allocs/op","",al); c[n]++; a[n,c[n]]=ns; if(!(n in lo)||b<lo[n])lo[n]=b;if(b>hi[n])hi[n]=b;if(!(n in alo)||al<alo[n])alo[n]=al;if(al>ahi[n])ahi[n]=al} END {for(n in c){for(i=2;i<=c[n];i++)for(j=i;j>1&&a[n,j]<a[n,j-1];j--){t=a[n,j];a[n,j]=a[n,j-1];a[n,j-1]=t}; p50=a[n,int((50*c[n]+99)/100)]; p95=a[n,int((95*c[n]+99)/100)]; p99=a[n,int((99*c[n]+99)/100)]; printf "- %s: p50=%sns/op p95=%sns/op p99=%sns/op ops/s=%.1f B/op=%s-%s allocs/op=%s-%s\n",n,p50,p95,p99,1e9/p50,lo[n],hi[n],alo[n],ahi[n]}}' "$out/raw.txt"
} > "$out/report.md"
cat "$out/report.md"
