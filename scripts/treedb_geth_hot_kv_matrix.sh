#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/treedb_geth_hot_kv_matrix.sh [--out DIR] [--geth-repo DIR] [--profile-dir DIR]

Runs the integrated geth/Nitro hot-KV soak harness through go-ethereum's
node.OpenDatabase(..., db.engine=<engine>) / ethdb path. The harness lives in
benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go and is intentionally run
from a go-ethereum checkout so it exercises the downstream backend adapters.

Environment overrides:
  GETH_REPO              go-ethereum/Nitro checkout. Required unless --geth-repo is set.
  RUN_DIR                Output dir. Default: /tmp/treedb_geth_hot_kv_matrix_<timestamp>
  ENGINES                Comma-separated engines. Default: pebble,leveldb,treedb
  KEYS                  Records per run. Default: 30000
  READS                 Random point reads per run. Default: 12000
  KEY_SHAPES            Comma list. Default: geth-mixed,single-prefix
  VALUE_SHAPES          Comma list. Default: geth-mixed
  VALUE_SIZES           Comma list. Default: 128,512
  BATCH_TARGET_BYTES    Comma list. Default: 102400,1048576
  DELETE_RANGE_WIDTH    DeleteRange width. Default: 100
  DELETE_RANGES_PER_BATCH Batch DeleteRange calls per batch.Write. Default: 100
  PROFILE_DIR           Optional pprof output root; profiles only PROFILE_ENGINES.
  PROFILE_ENGINES       Comma-separated engines to profile. Default: treedb
  KEEP                  Keep per-run DB dirs when true. Default: false
  GOFLAGS               Extra go flags honored by go run.

Examples:
  GETH_REPO=/path/to/go-ethereum scripts/treedb_geth_hot_kv_matrix.sh

  # Quick smoke:
  GETH_REPO=/path/to/go-ethereum KEYS=1000 READS=300 \
    KEY_SHAPES=geth-mixed VALUE_SIZES=128 BATCH_TARGET_BYTES=102400 \
    scripts/treedb_geth_hot_kv_matrix.sh

  # TreeDB profile after selecting a representative shape:
  GETH_REPO=/path/to/go-ethereum ENGINES=treedb KEY_SHAPES=geth-mixed \
    VALUE_SIZES=128 BATCH_TARGET_BYTES=102400 PROFILE_DIR=/tmp/geth_hotkv_profiles \
    scripts/treedb_geth_hot_kv_matrix.sh
USAGE
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
harness="$repo_root/benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go"
run_dir=${RUN_DIR:-}
geth_repo=${GETH_REPO:-}
profile_dir=${PROFILE_DIR:-}

require_value() {
  if [[ $# -lt 2 || "${2:-}" == -* ]]; then
    echo "missing value for $1" >&2
    usage >&2
    exit 2
  fi
}

abs_path() {
  python3 -c 'import os, sys; print(os.path.abspath(sys.argv[1]))' "$1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      require_value "$@"
      run_dir=$2
      shift 2
      ;;
    --geth-repo)
      require_value "$@"
      geth_repo=$2
      shift 2
      ;;
    --profile-dir)
      require_value "$@"
      profile_dir=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$geth_repo" ]]; then
  echo "GETH_REPO or --geth-repo is required" >&2
  exit 2
fi
geth_repo=$(abs_path "$geth_repo")
if [[ ! -d "$geth_repo" ]]; then
  echo "GETH_REPO does not exist: $geth_repo" >&2
  exit 2
fi
if [[ ! -f "$geth_repo/go.mod" ]]; then
  echo "GETH_REPO is not a Go module checkout: $geth_repo" >&2
  exit 2
fi
if [[ -z "$run_dir" ]]; then
  run_dir="${TMPDIR:-/tmp}/treedb_geth_hot_kv_matrix_$(date -u +%Y%m%dT%H%M%SZ)"
fi
run_dir=$(abs_path "$run_dir")
if [[ -n "$profile_dir" ]]; then
  profile_dir=$(abs_path "$profile_dir")
fi
mkdir -p "$run_dir"

engines=${ENGINES:-pebble,leveldb,treedb}
keys=${KEYS:-30000}
reads=${READS:-12000}
key_shapes=${KEY_SHAPES:-geth-mixed,single-prefix}
value_shapes=${VALUE_SHAPES:-geth-mixed}
value_sizes=${VALUE_SIZES:-128,512}
batch_targets=${BATCH_TARGET_BYTES:-102400,1048576}
delete_range_width=${DELETE_RANGE_WIDTH:-100}
delete_ranges_per_batch=${DELETE_RANGES_PER_BATCH:-100}
profile_engines=${PROFILE_ENGINES:-treedb}
keep=${KEEP:-false}

IFS=, read -r -a key_shape_arr <<< "$key_shapes"
IFS=, read -r -a value_shape_arr <<< "$value_shapes"
IFS=, read -r -a value_size_arr <<< "$value_sizes"
IFS=, read -r -a batch_target_arr <<< "$batch_targets"

summary_tsv="$run_dir/matrix_results.tsv"
summary_md="$run_dir/matrix_results.md"
: > "$summary_tsv"
printf 'key_shape\tvalue_shape\tvalue_size\tbatch_target_bytes\tengine\twrite_ops_sec\tread_ops_sec\titerate_keys_sec\tdelete_range_keys_sec\tsize_bytes\tpost_delete_size_bytes\tjson\n' >> "$summary_tsv"

run_index=0
for key_shape in "${key_shape_arr[@]}"; do
  for value_shape in "${value_shape_arr[@]}"; do
    for value_size in "${value_size_arr[@]}"; do
      for batch_target in "${batch_target_arr[@]}"; do
        run_index=$((run_index + 1))
        label="$(printf '%02d' "$run_index")_${key_shape}_${value_shape}_v${value_size}_b${batch_target}"
        label=${label//[^A-Za-z0-9_.-]/_}
        case_dir="$run_dir/$label"
        mkdir -p "$case_dir"
        json_out="$case_dir/results.json"
        stdout_log="$case_dir/stdout.md"
        stderr_log="$case_dir/stderr.log"
        workdir="$case_dir/db"
        args=(
          "$harness"
          -n "$keys"
          -reads "$reads"
          -engines "$engines"
          -key-shape "$key_shape"
          -value-shape "$value_shape"
          -value-size "$value_size"
          -batch-target-bytes "$batch_target"
          -delete-range-width "$delete_range_width"
          -delete-ranges-per-batch "$delete_ranges_per_batch"
          -workdir "$workdir"
          -out "$json_out"
        )
        if [[ "$keep" == "true" ]]; then
          args+=( -keep )
        fi
        if [[ -n "$profile_dir" ]]; then
          case_profile_dir="$profile_dir/$label"
          mkdir -p "$case_profile_dir"
          args+=( -profile-dir "$case_profile_dir" -profile-engines "$profile_engines" )
        fi
        echo "[matrix] $label" | tee -a "$run_dir/matrix.log"
        (
          cd "$geth_repo"
          go run "${args[@]}"
        ) > "$stdout_log" 2> "$stderr_log"
        python3 - "$json_out" "$summary_tsv" "$key_shape" "$value_shape" "$value_size" "$batch_target" <<'PY'
import json, sys
json_path, tsv_path, key_shape, value_shape, value_size, batch_target = sys.argv[1:]
with open(json_path) as f:
    doc = json.load(f)
with open(tsv_path, 'a') as out:
    for run in doc['runs']:
        out.write('\t'.join([
            key_shape,
            value_shape,
            value_size,
            batch_target,
            run['engine'],
            f"{run['write_ops_sec']:.0f}",
            f"{run['read_ops_sec']:.0f}",
            f"{run['iterate_keys_sec']:.0f}",
            f"{run['delete_range_keys_sec']:.0f}",
            str(run['size_bytes']),
            str(run.get('post_delete_size_bytes', 0)),
            json_path,
        ]) + '\n')
PY
        if [[ "$keep" != "true" ]]; then
          rm -rf "$workdir"
        fi
      done
    done
  done
done

python3 - "$summary_tsv" "$summary_md" <<'PY'
import csv, sys
from collections import defaultdict

tsv, md = sys.argv[1:]
rows = list(csv.DictReader(open(tsv), delimiter='\t'))

def fmt(n):
    try:
        return f"{int(float(n)):,}"
    except Exception:
        return n

with open(md, 'w') as out:
    out.write('# geth/Nitro hot KV matrix\n\n')
    out.write('Integrated node.OpenDatabase / ethdb benchmark. DeleteRange keys/sec counts affected keys/sec, not range calls/sec. Size bytes is loaded DB size before destructive DeleteRange; post-delete bytes is measured after close/reopen verification.\n\n')
    out.write('| key shape | value shape | value size | batch target bytes | engine | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes | post-delete bytes |\n')
    out.write('|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|\n')
    for r in rows:
        out.write('| {key_shape} | {value_shape} | {value_size} | {batch_target_bytes} | {engine} | {write} | {read} | {iterate} | {delete} | {size} | {post_delete} |\n'.format(
            key_shape=r['key_shape'], value_shape=r['value_shape'], value_size=fmt(r['value_size']),
            batch_target_bytes=fmt(r['batch_target_bytes']), engine=r['engine'], write=fmt(r['write_ops_sec']),
            read=fmt(r['read_ops_sec']), iterate=fmt(r['iterate_keys_sec']), delete=fmt(r['delete_range_keys_sec']),
            size=fmt(r['size_bytes']), post_delete=fmt(r['post_delete_size_bytes'])))
    out.write('\n## TreeDB ratios versus Pebble\n\n')
    groups = defaultdict(dict)
    for r in rows:
        key = (r['key_shape'], r['value_shape'], r['value_size'], r['batch_target_bytes'])
        groups[key][r['engine']] = r
    out.write('| key shape | value shape | value size | batch target bytes | write ratio | read ratio | iterate ratio | DeleteRange ratio | size ratio |\n')
    out.write('|---|---|---:|---:|---:|---:|---:|---:|---:|\n')
    for key in sorted(groups):
        g = groups[key]
        if 'treedb' not in g or 'pebble' not in g:
            continue
        t, p = g['treedb'], g['pebble']
        def ratio(metric):
            pv = float(p[metric])
            tv = float(t[metric])
            if pv == 0:
                return 'n/a'
            return f"{tv/pv:.3f}x"
        out.write('| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n'.format(
            key[0], key[1], fmt(key[2]), fmt(key[3]), ratio('write_ops_sec'), ratio('read_ops_sec'),
            ratio('iterate_keys_sec'), ratio('delete_range_keys_sec'), ratio('size_bytes')))
PY

echo "geth hot KV matrix complete"
echo "  run dir: $run_dir"
echo "  tsv:     $summary_tsv"
echo "  report:  $summary_md"
