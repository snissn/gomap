#!/usr/bin/env bash
set -euo pipefail
# Use separate quiet windows for PHASE=build and PHASE=measure. Compilation and
# provenance checks are outside query timing. See cohere_scale_harness.md.
: "${TREEDB_SCALE_DATA:?set exported dataset directory}"
: "${TREEDB_SCALE_DB:?set persistent DB directory}"
: "${TREEDB_SCALE_PHASE:?set build, measure, write, write_schema, or write_schema_roots (writes need disposable DB copy; roots requires TREEDB_SCALE_PRECEDING_RUN)}"
: "${TREEDB_SCALE_LABEL:?set unique evidence label}"
: "${TREEDB_SCALE_RESULTS:?set evidence directory}"
[[ "$TREEDB_SCALE_LABEL" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "invalid evidence label" >&2; exit 1; }
case "$TREEDB_SCALE_PHASE" in
  build|measure) ;;
  write|write_schema|write_schema_roots)
    [[ "${TREEDB_SCALE_WRITE_COPY:-}" == 1 ]] || { echo "write phases require TREEDB_SCALE_WRITE_COPY=1 and a disposable DB copy" >&2; exit 1; } ;;
  *) echo "invalid phase" >&2; exit 1 ;;
esac
[[ "$(pwd -P)" == "$(git rev-parse --show-toplevel)" ]] || { echo "run from the repository root" >&2; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo "retained runs require a clean committed source tree" >&2; exit 1; }
mkdir -p "$TREEDB_SCALE_RESULTS"
# Preserve artifacts made by the earlier flat-directory wrapper, too.
for old_artifact in "$TREEDB_SCALE_RESULTS/collections-$TREEDB_SCALE_LABEL.test" "$TREEDB_SCALE_RESULTS/$TREEDB_SCALE_LABEL-$TREEDB_SCALE_PHASE.log"; do
  [[ ! -e "$old_artifact" && ! -L "$old_artifact" ]] || { echo "refusing to overwrite evidence: $old_artifact" >&2; exit 1; }
done
run_dir="$TREEDB_SCALE_RESULTS/$TREEDB_SCALE_LABEL-$TREEDB_SCALE_PHASE"
# mkdir is the atomic reservation: concurrent invocations cannot share output.
mkdir "$run_dir"
export GOWORK=off
export GOMAXPROCS="${GOMAXPROCS:-6}"
export TREEDB_SCALE_SOURCE_COMMIT
TREEDB_SCALE_SOURCE_COMMIT=$(git rev-parse HEAD)
bench_binary="$run_dir/collections.test"
bench_log="$run_dir/run.log"
{
  printf 'source_commit=%s\n' "$TREEDB_SCALE_SOURCE_COMMIT"
  printf 'source_tree=%s\n' "$(git rev-parse HEAD^{tree})"
  # Full path/blob inventory binds runtime dependencies, fixtures and harness;
  # artifact-only descendants must preserve these bytes (see the usage note).
  git ls-tree -r HEAD > "$run_dir/source-blobs.txt"
  git ls-tree HEAD TreeDB internal
  git ls-tree -r HEAD -- TreeDB/collections/typed_graph_scale_diagnostic_test.go TreeDB/collections/typed_write_768_diagnostic_test.go benchmarks/vector_db_compare/prepare_cohere_scale.py benchmarks/vector_db_compare/run_cohere_scale.sh
  sha256sum "$run_dir/source-blobs.txt" "$TREEDB_SCALE_DATA/manifest.json"
  go version
  go test -c -trimpath -o "$bench_binary" ./TreeDB/collections
  [[ "$(git rev-parse HEAD)" == "$TREEDB_SCALE_SOURCE_COMMIT" && -z "$(git status --porcelain)" ]] || { echo "source changed during compilation" >&2; exit 1; }
  sha256sum "$bench_binary"
  go version -m "$bench_binary"
  /usr/bin/time -v taskset -c "${TREEDB_SCALE_CPUS:-0-5}" "$bench_binary" -test.run '^TestTypedGraphScaleDiagnostic$' -test.v -test.count=1 -test.timeout=45m
} 2>&1 | tee "$bench_log"
