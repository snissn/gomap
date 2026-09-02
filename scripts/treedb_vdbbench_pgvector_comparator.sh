#!/usr/bin/env bash
# Capture a pgvector HNSW VectorDBBench setup/benchmark artifact for issue #2601.
#
# Defaults are intentionally safe: the script starts pgvector, captures health
# and a VDBBench dry-run, then stops the container without running the full
# benchmark. Set RUN_FULL=true only in a no-concurrent-benchmark window.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
stamp=$(date +%Y%m%d_%H%M%S)
OUT=${OUT:-/tmp/treedb_vdbbench_2601_pgvector_${stamp}}
VECTORDBBENCH_DIR=${VECTORDBBENCH_DIR:-}
IMAGE=${PGVECTOR_IMAGE:-pgvector/pgvector:pg16}
CONTAINER_NAME=${CONTAINER_NAME:-gomap2601-pgvector-${stamp}}
POSTGRES_DB=${POSTGRES_DB:-vectordb}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(18))
PY
)}
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-0}
RUN_FULL=${RUN_FULL:-false}
KEEP_CONTAINER=${KEEP_CONTAINER:-false}
ALLOW_CONCURRENT_BENCHMARKS=${ALLOW_CONCURRENT_BENCHMARKS:-false}
CASE_TYPE=${CASE_TYPE:-Performance1536D50K}
K=${K:-10}
NUM_CONCURRENCY=${NUM_CONCURRENCY:-1,8,32}
CONCURRENCY_DURATION=${CONCURRENCY_DURATION:-30}
HNSW_M=${HNSW_M:-16}
EF_CONSTRUCTION=${EF_CONSTRUCTION:-128}
EF_SEARCH=${EF_SEARCH:-128}
DB_LABEL=${DB_LABEL:-pgvector-hnsw-2601-${stamp}}
TASK_LABEL=${TASK_LABEL:-$DB_LABEL}
VDBBENCH_TIMEOUT=${VDBBENCH_TIMEOUT:-36000}
# Optional additional VectorDBBench CLI args, parsed with Python shlex so
# custom-case runs can pass quoted descriptions without changing this script.
VDBBENCH_EXTRA_ARGS=${VDBBENCH_EXTRA_ARGS:-}

if [[ -z "$VECTORDBBENCH_DIR" ]]; then
  echo "VECTORDBBENCH_DIR is required (path to snissn/vectordbbench checkout)" >&2
  exit 2
fi
if [[ ! -d "$VECTORDBBENCH_DIR" ]]; then
  echo "VECTORDBBENCH_DIR does not exist: $VECTORDBBENCH_DIR" >&2
  exit 2
fi
if [[ -e "$OUT" && -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  echo "OUT must be new or empty: $OUT" >&2
  exit 2
fi
mkdir -p "$OUT"/{commands,logs,vdbbench-results,pgdata}

if [[ "$PORT" == "0" ]]; then
  PORT=$(python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(('127.0.0.1', 0))
print(sock.getsockname()[1])
sock.close()
PY
)
fi

redact_artifact_password() {
  find "$OUT" -path "$OUT/pgdata" -prune -o -type f -print0 \
    | xargs -0 grep -Il -- "$POSTGRES_PASSWORD" 2>/dev/null \
    | while IFS= read -r file; do
        perl -0pi -e "s/\Q$POSTGRES_PASSWORD\E/<redacted-artifact-local-password>/g" "$file"
      done
}

write_status() {
  local status=$1
  local reason=${2:-}
  python3 - "$OUT" "$status" "$reason" <<'PY'
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
out = Path(sys.argv[1])
status = sys.argv[2]
reason = sys.argv[3]
files = []
for path in sorted(out.rglob('*')):
    if not path.is_file():
        continue
    rel = path.relative_to(out)
    if rel.parts and rel.parts[0] == 'pgdata':
        continue
    files.append(str(rel))
manifest = {
    'schema_version': 'treedb-vdbbench-pgvector-comparator/v1',
    'status': status,
    'reason': reason,
    'generated_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
    'artifact_root': str(out),
    'files': files[:500],
    'files_truncated': len(files) > 500,
}
(out / 'status_manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
PY
}

cleanup() {
  local exit_code=$?
  set +e
  if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    docker logs "$CONTAINER_NAME" > "$OUT/logs/pgvector_container_cleanup.log" 2>&1
    if [[ "$KEEP_CONTAINER" != "true" ]]; then
      docker stop "$CONTAINER_NAME" > "$OUT/commands/pgvector_stop.stdout.txt" 2> "$OUT/commands/pgvector_stop.stderr.txt"
    fi
  fi
  docker ps -a --filter "name=$CONTAINER_NAME" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' > "$OUT/health_docker_ps_after_cleanup.txt" 2>&1
  find "$OUT/vdbbench-results" -maxdepth 5 -type f 2>/dev/null | sort > "$OUT/vdbbench_results_files.txt"
  redact_artifact_password
  if [[ $exit_code -ne 0 && ! -f "$OUT/status_manifest.json" ]]; then
    write_status "failed_or_aborted" "script exited $exit_code; see command logs"
  fi
  exit $exit_code
}
trap cleanup EXIT
trap 'write_status aborted_by_signal "received TERM/INT"; exit 130' INT TERM

assert_no_other_vdbbench() {
  if [[ "$ALLOW_CONCURRENT_BENCHMARKS" == "true" ]]; then
    return 0
  fi
  local matches
  matches=$(ps -axo pid,command | grep -F -- '-m vectordb_bench.cli.vectordbbench' | grep -v grep | grep -v "pgvector-hnsw-2601-${stamp}" || true)
  if [[ -n "$matches" ]]; then
    printf '%s\n' "$matches" > "$OUT/concurrent_vdbbench_processes_blocker.txt"
    echo "Refusing full pgvector run while another VDBBench process exists; see $OUT/concurrent_vdbbench_processes_blocker.txt" >&2
    return 1
  fi
}

run_logged() {
  local name=$1
  shift
  local rc
  printf '%q ' "$@" > "$OUT/commands/${name}.command.txt"
  printf '\n' >> "$OUT/commands/${name}.command.txt"
  set +e
  "$@" > "$OUT/commands/${name}.stdout.txt" 2> "$OUT/commands/${name}.stderr.txt"
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$OUT/commands/${name}.exit_code"
  return "$rc"
}

validate_full_result() {
  python3 - "$OUT" <<'PY'
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
out = Path(sys.argv[1])
results = sorted((out / 'vdbbench-results' / 'PgVector').glob('result_*_pgvector.json'))
if not results:
    status = 'failed_missing_result_json'
    reason = 'VDBBench command completed without a PgVector result JSON'
else:
    data = json.loads(results[-1].read_text())
    failed = [item for item in data.get('results', []) if item.get('label') != ':)']
    if not failed:
        sys.exit(0)
    first = failed[0]
    metrics = first.get('metrics') or {}
    status = 'failed_vdbbench_case'
    reason = (
        f"VDBBench result label={first.get('label')!r}; "
        f"max_load_count={metrics.get('max_load_count')} qps={metrics.get('qps')}"
    )
manifest = {
    'schema_version': 'treedb-vdbbench-pgvector-comparator/v1',
    'status': status,
    'reason': reason,
    'generated_at': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
    'artifact_root': str(out),
    'claim_quality': 'none; failed VDBBench cases are not comparator evidence',
}
(out / 'status_manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
print(reason, file=sys.stderr)
sys.exit(1)
PY
}

{
  echo "artifact_root=$OUT"
  echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "gomap_commit=$(git -C "$repo_root" rev-parse HEAD)"
  echo "gomap_branch=$(git -C "$repo_root" branch --show-current)"
  echo "vectordbbench_dir=$VECTORDBBENCH_DIR"
  echo "vectordbbench_commit=$(git -C "$VECTORDBBENCH_DIR" rev-parse HEAD 2>/dev/null || true)"
  echo "vectordbbench_branch=$(git -C "$VECTORDBBENCH_DIR" branch --show-current 2>/dev/null || true)"
  echo "docker_version=$(docker --version)"
  echo "python3_version=$(python3 --version)"
  echo "uv_version=$(uv --version 2>/dev/null || true)"
  uname -a | sed 's/^/uname=/'
  sysctl -n machdep.cpu.brand_string 2>/dev/null | sed 's/^/cpu_brand=/' || true
  echo "image=$IMAGE"
  echo "container=$CONTAINER_NAME"
  echo "host=$HOST"
  echo "port=$PORT"
  echo "case_type=$CASE_TYPE"
  echo "k=$K"
  echo "num_concurrency=$NUM_CONCURRENCY"
  echo "concurrency_duration=$CONCURRENCY_DURATION"
  echo "hnsw_m=$HNSW_M"
  echo "ef_construction=$EF_CONSTRUCTION"
  echo "ef_search=$EF_SEARCH"
  echo "run_full=$RUN_FULL"
  echo "vdbbench_extra_args=$VDBBENCH_EXTRA_ARGS"
} > "$OUT/context.txt"

docker image inspect "$IMAGE" > "$OUT/pgvector_image_inspect.json" 2>&1 || true
docker info > "$OUT/docker_info.txt" 2>&1 || true

printf 'docker run -d --name %q -e POSTGRES_PASSWORD=<redacted> -e POSTGRES_DB=%q -p %q:%q:5432 -v %q:/var/lib/postgresql/data %q\n' \
  "$CONTAINER_NAME" "$POSTGRES_DB" "$HOST" "$PORT" "$OUT/pgdata" "$IMAGE" > "$OUT/commands/pgvector_start.command.txt"
docker run -d \
  --name "$CONTAINER_NAME" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -p "$HOST:$PORT:5432" \
  -v "$OUT/pgdata:/var/lib/postgresql/data" \
  "$IMAGE" \
  > "$OUT/commands/pgvector_start.stdout.txt" \
  2> "$OUT/commands/pgvector_start.stderr.txt"
echo "$CONTAINER_NAME" > "$OUT/container_name.txt"

for _ in $(seq 1 120); do
  if docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d postgres > "$OUT/health_pg_isready_postgres.txt" 2>&1; then
    # The postgres entrypoint briefly starts a temporary server, shuts it down,
    # then starts the final server. Require readiness to remain true across a
    # short delay so we do not race the temporary shutdown.
    sleep 2
    if docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d postgres > "$OUT/health_pg_isready_postgres.txt" 2>&1; then
      break
    fi
  fi
  sleep 1
done
if ! docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d postgres > "$OUT/health_pg_isready_postgres.txt" 2>&1; then
  docker logs "$CONTAINER_NAME" > "$OUT/logs/pgvector_container_startup_failure.log" 2>&1 || true
  echo "pgvector container did not become healthy" >&2
  exit 1
fi
# Wait for SQL execution, not only socket readiness. pg_isready can report
# accepting connections while the postgres entrypoint is still finalizing.
for _ in $(seq 1 120); do
  if docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d postgres -Atc 'select 1;' > "$OUT/health_postgres_sql.txt" 2> "$OUT/health_postgres_sql.stderr.txt"; then
    break
  fi
  sleep 1
done
if ! grep -qx '1' "$OUT/health_postgres_sql.txt"; then
  docker logs "$CONTAINER_NAME" > "$OUT/logs/pgvector_container_sql_ready_failure.log" 2>&1 || true
  echo "pgvector container did not become SQL-ready" >&2
  exit 1
fi

docker ps --filter "name=$CONTAINER_NAME" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' > "$OUT/health_docker_ps.txt"
# Make database creation explicit and logged; tolerate a race where POSTGRES_DB
# appears between the check and createdb.
docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d postgres -Atc "select 1 from pg_database where datname = '$POSTGRES_DB';" > "$OUT/commands/check_database.stdout.txt" 2> "$OUT/commands/check_database.stderr.txt" || true
if ! grep -qx '1' "$OUT/commands/check_database.stdout.txt"; then
  docker exec "$CONTAINER_NAME" createdb -U "$POSTGRES_USER" "$POSTGRES_DB" > "$OUT/commands/create_database.stdout.txt" 2> "$OUT/commands/create_database.stderr.txt" || true
  docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d postgres -Atc "select 1 from pg_database where datname = '$POSTGRES_DB';" > "$OUT/commands/check_database_after_create.stdout.txt" 2> "$OUT/commands/check_database_after_create.stderr.txt"
  grep -qx '1' "$OUT/commands/check_database_after_create.stdout.txt"
fi
docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" > "$OUT/health_pg_isready.txt" 2>&1 || true
docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS vector;' > "$OUT/commands/create_extension.stdout.txt" 2> "$OUT/commands/create_extension.stderr.txt"
docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -Atc "select version(); select extname || '=' || extversion from pg_extension where extname='vector'; show shared_buffers; show maintenance_work_mem; show max_parallel_workers;" > "$OUT/pgvector_versions.txt" 2> "$OUT/pgvector_versions.stderr.txt"
docker logs "$CONTAINER_NAME" > "$OUT/logs/pgvector_container_before_vdbbench.log" 2>&1 || true

extra_args=()
if [[ -n "$VDBBENCH_EXTRA_ARGS" ]]; then
  mapfile -t extra_args < <(python3 - "$VDBBENCH_EXTRA_ARGS" <<'PY'
import shlex
import sys
for arg in shlex.split(sys.argv[1]):
    print(arg)
PY
)
fi

common_cmd=(
  uv run --no-sync
  --with click --with pydantic --with pyyaml --with environs
  --with pandas --with polars --with pyarrow --with psutil --with pytz
  --with tqdm --with plotly --with ujson --with hdrhistogram
  --with scikit-learn --with s3fs --with oss2
  --with psycopg --with psycopg-binary --with pgvector
  python -m vectordb_bench.cli.vectordbbench pgvectorhnsw
  --user-name "$POSTGRES_USER"
  --password "$POSTGRES_PASSWORD"
  --host "$HOST"
  --port "$PORT"
  --db-name "$POSTGRES_DB"
  --m "$HNSW_M"
  --ef-construction "$EF_CONSTRUCTION"
  --ef-search "$EF_SEARCH"
  --case-type "$CASE_TYPE"
  --k "$K"
  --num-concurrency "$NUM_CONCURRENCY"
  --concurrency-duration "$CONCURRENCY_DURATION"
  --db-label "$DB_LABEL"
  --task-label "$TASK_LABEL"
  "${extra_args[@]}"
)
export PYTHONPATH="$VECTORDBBENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
export RESULTS_LOCAL_DIR="$OUT/vdbbench-results"
export LOG_FILE="$OUT/vdbbench_pgvectorhnsw.log"

(
  cd "$VECTORDBBENCH_DIR"
  run_logged vdbbench_pgvectorhnsw_dry_run "${common_cmd[@]}" --skip-load --skip-search-serial --skip-search-concurrent --dry-run
)

if [[ "$RUN_FULL" != "true" ]]; then
  write_status setup_smoke_complete "RUN_FULL=false; only health checks and dry-run were executed"
  echo "artifact_root=$OUT"
  echo "status=setup_smoke_complete"
  exit 0
fi

assert_no_other_vdbbench
(
  cd "$VECTORDBBENCH_DIR"
  run_logged vdbbench_pgvectorhnsw "${common_cmd[@]}"
)

docker logs "$CONTAINER_NAME" > "$OUT/logs/pgvector_container_after_vdbbench.log" 2>&1 || true
docker exec "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "select count(*) from vdbbench_table_test; select pg_size_pretty(pg_total_relation_size('vdbbench_table_test')); select pg_size_pretty(pg_indexes_size('vdbbench_table_test'));" > "$OUT/pgvector_postrun_table_stats.txt" 2> "$OUT/pgvector_postrun_table_stats.stderr.txt" || true
validate_full_result
write_status complete "full pgvectorhnsw VDBBench run completed"
echo "artifact_root=$OUT"
echo "status=complete"
