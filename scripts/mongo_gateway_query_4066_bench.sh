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
  echo
  echo "- source: $sha"
  echo "- platform: os=$(uname -s) arch=$(uname -m)"
  echo "- go: $(go version)"
  echo "- command: \`$cmd\`"
  echo "- percentile method: nearest rank, rank=ceil(p*n), n=10"
  awk '
  /BenchmarkMongoNegativeDottedQueryShapes\// {
    n=$1; sub(/^.*\//,"",n)
    for (i=1; i<=NF; i++) {
      if ($i == "ns/op") ns=$(i-1)
      if ($i == "B/op") b=$(i-1)
      if ($i == "allocs/op") al=$(i-1)
      if ($i == "candidate_docs") cd=$(i-1)
      if ($i == "materialized_docs") md=$(i-1)
      if ($i == "materialized_bytes") mb=$(i-1)
      if ($i == "returned_docs") rd=$(i-1)
    }
    if (ns == "" || b == "" || al == "" || cd == "" || md == "" || mb == "" || rd == "") bad=1
    if (n == "indexed_positive_residual" && !(cd == 192 && md == 192 && rd == 20 && mb > 0)) bad=1
    if (n == "bounded_negative_scan" && !(cd == 256 && md == 86 && rd == 86 && mb == 0)) bad=1
    if (n == "dotted_projection" && !(cd == 256 && md == 256 && rd == 256 && mb == 0)) bad=1
    if (n == "dotted_sort_materialized" && !(cd == 256 && md == 256 && rd == 256 && mb == 0)) bad=1
    c[n]++; a[n,c[n]]=ns
    if (!(n in lo) || b<lo[n]) lo[n]=b; if (b>hi[n]) hi[n]=b
    if (!(n in alo) || al<alo[n]) alo[n]=al; if (al>ahi[n]) ahi[n]=al
    candidate[n]=cd; materialized[n]=md; bytes[n]=mb; returned[n]=rd
  }
  END {
    if (bad || length(c) != 4) exit 1
    for (n in c) {
      if (c[n] != 10) exit 1
      for(i=2;i<=c[n];i++)for(j=i;j>1&&a[n,j]<a[n,j-1];j--){t=a[n,j];a[n,j]=a[n,j-1];a[n,j-1]=t}
      p50=a[n,int((50*c[n]+99)/100)]; p95=a[n,int((95*c[n]+99)/100)]; p99=a[n,int((99*c[n]+99)/100)]
      printf "- %s: p50=%sns/op p95=%sns/op p99=%sns/op ops/s=%.1f B/op=%s-%s allocs/op=%s-%s explain={returned:%s,candidates:%s,materialized:%s,materializedBytes:%s}\n",n,p50,p95,p99,1e9/p50,lo[n],hi[n],alo[n],ahi[n],returned[n],candidate[n],materialized[n],bytes[n]
    }
  }' "$out/raw.txt"
} > "$out/report.md"
cat "$out/report.md"
