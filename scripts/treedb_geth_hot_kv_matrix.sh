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
  TREEDB_READ_INTEGRITIES Comma list of TreeDB modes. Default: verify
                         Values: verify, unsafe-skip-checksums (unsafe benchmark ceiling)
  ITERATION_MODES       Comma list. Default: value. Values: value,key-only
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

  # TreeDB checksum ceiling and key-only split:
  GETH_REPO=/path/to/go-ethereum ENGINES=treedb KEY_SHAPES=geth-mixed \
    VALUE_SIZES=128 BATCH_TARGET_BYTES=102400 \
    TREEDB_READ_INTEGRITIES=verify,unsafe-skip-checksums ITERATION_MODES=value,key-only \
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
treedb_read_integrities=${TREEDB_READ_INTEGRITIES:-verify}
iteration_modes=${ITERATION_MODES:-value}
delete_range_width=${DELETE_RANGE_WIDTH:-100}
delete_ranges_per_batch=${DELETE_RANGES_PER_BATCH:-100}
profile_engines=${PROFILE_ENGINES:-treedb}
keep=${KEEP:-false}

IFS=, read -r -a key_shape_arr <<< "$key_shapes"
IFS=, read -r -a value_shape_arr <<< "$value_shapes"
IFS=, read -r -a value_size_arr <<< "$value_sizes"
IFS=, read -r -a batch_target_arr <<< "$batch_targets"
IFS=, read -r -a read_integrity_arr <<< "$treedb_read_integrities"
IFS=, read -r -a iteration_mode_arr <<< "$iteration_modes"

summary_tsv="$run_dir/matrix_results.tsv"
summary_md="$run_dir/matrix_results.md"
: > "$summary_tsv"
printf 'key_shape\tvalue_shape\tvalue_size\tbatch_target_bytes\ttreedb_read_integrity\titeration_mode\tengine\tread_integrity\twrite_ops_sec\tread_ops_sec\titerate_keys_sec\tdelete_range_keys_sec\tsize_bytes\tpost_delete_size_bytes\tjson\n' >> "$summary_tsv"

run_index=0
for key_shape in "${key_shape_arr[@]}"; do
  for value_shape in "${value_shape_arr[@]}"; do
    for value_size in "${value_size_arr[@]}"; do
      for batch_target in "${batch_target_arr[@]}"; do
        for read_integrity in "${read_integrity_arr[@]}"; do
          for iteration_mode in "${iteration_mode_arr[@]}"; do
            run_index=$((run_index + 1))
            label="$(printf '%02d' "$run_index")_${key_shape}_${value_shape}_v${value_size}_b${batch_target}_${read_integrity}_${iteration_mode}"
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
              -treedb-read-integrity "$read_integrity"
              -iteration-mode "$iteration_mode"
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
            python3 - "$json_out" "$summary_tsv" "$key_shape" "$value_shape" "$value_size" "$batch_target" "$read_integrity" "$iteration_mode" <<'PY'
import json, sys
json_path, tsv_path, key_shape, value_shape, value_size, batch_target, treedb_read_integrity, iteration_mode = sys.argv[1:]
with open(json_path) as f:
    doc = json.load(f)
with open(tsv_path, 'a') as out:
    for run in doc['runs']:
        out.write('\t'.join([
            key_shape,
            value_shape,
            value_size,
            batch_target,
            treedb_read_integrity,
            iteration_mode,
            run['engine'],
            run.get('read_integrity', 'n/a'),
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
  done
done

phase_counters_tsv="$run_dir/phase_counters.tsv"
phase_stat_deltas_tsv="$run_dir/phase_stat_deltas.tsv"

python3 - "$summary_tsv" "$summary_md" "$phase_counters_tsv" "$phase_stat_deltas_tsv" <<'PY'
import csv, json, sys
from collections import defaultdict

tsv, md, phase_tsv, phase_stat_tsv = sys.argv[1:]
rows = list(csv.DictReader(open(tsv), delimiter='\t'))
json_cache = {}
phases = ['write', 'read', 'iterate', 'delete_range', 'reopen_verify']

stat_key_groups = {
    'crc32_checks': ['treedb.vlog.read.crc32_checks_total', 'treedb.cache.vlog_read.crc32_checks_total'],
    'grouped_hits': ['treedb.vlog.grouped_frame_cache.hits', 'treedb.cache.vlog_grouped_frame_cache.hits'],
    'grouped_misses': ['treedb.vlog.grouped_frame_cache.misses', 'treedb.cache.vlog_grouped_frame_cache.misses'],
    'grouped_stores': ['treedb.vlog.grouped_frame_cache.stores', 'treedb.cache.vlog_grouped_frame_cache.stores'],
    'mmap_hits': ['treedb.vlog.mmap_read.hits', 'treedb.cache.vlog_mmap.read.hits'],
    'mmap_miss_out_of_range': ['treedb.vlog.mmap_read.miss_out_of_range', 'treedb.cache.vlog_mmap.read.miss_out_of_range'],
    'mmap_miss_no_mapping': ['treedb.vlog.mmap_read.miss_no_mapping', 'treedb.cache.vlog_mmap.read.miss_no_mapping'],
    'mmap_miss_dead_mapping_cap': ['treedb.vlog.mmap_read.miss_dead_mapping_cap', 'treedb.cache.vlog_mmap.read.miss_dead_mapping_cap'],
    'mmap_fallback_readat': ['treedb.vlog.mmap_read.fallback_readat', 'treedb.cache.vlog_mmap.read.fallback_readat'],
    'outer_leaf_loads': ['treedb.process.read_path.outer_leaf.loads_total'],
    'outer_leaf_point_loads': ['treedb.process.read_path.outer_leaf.point_loads_total'],
    'outer_leaf_iterator_loads': ['treedb.process.read_path.outer_leaf.iterator_loads_total'],
    'outer_leaf_checksum_verifications': ['treedb.process.read_path.outer_leaf.checksum.verifications_total'],
    'outer_leaf_checksum_skips': ['treedb.process.read_path.outer_leaf.checksum.skips_total'],
    'delete_range_calls': ['treedb.cache.delete_range.calls_total'],
    'delete_range_batch_calls': ['treedb.cache.delete_range.batch_calls_total'],
    'delete_range_batch_writes': ['treedb.cache.delete_range.batch_writes_total'],
    'delete_range_input_ranges': ['treedb.cache.delete_range.input_ranges_total'],
    'delete_range_effective_ranges': ['treedb.cache.delete_range.effective_ranges_total'],
    'delete_range_coalesced_ranges': ['treedb.cache.delete_range.coalesced_ranges_total'],
    'delete_range_iterators': ['treedb.cache.delete_range.iterators_total'],
    'delete_range_snapshot_iterators': ['treedb.cache.delete_range.snapshot_iterators_total'],
    'delete_range_backend_iterators': ['treedb.cache.delete_range.backend_iterators_total'],
    'delete_range_memtable_iterators': ['treedb.cache.delete_range.memtable_iterators_total'],
    'delete_range_queue_iterators': ['treedb.cache.delete_range.queue_iterators_total'],
    'delete_range_visited_keys': ['treedb.cache.delete_range.visited_keys_total'],
    'delete_range_tombstone_keys': ['treedb.cache.delete_range.tombstone_keys_total'],
    'delete_range_materialized_keys': ['treedb.cache.delete_range.materialized_keys_total'],
    'delete_range_materialized_key_bytes': ['treedb.cache.delete_range.materialized_key_bytes_total'],
    'delete_range_fast_path_clears': ['treedb.cache.delete_range.fast_path_clears_total'],
    'delete_range_backend_direct_batches': ['treedb.cache.delete_range.backend_direct_batches_total'],
    'delete_range_backend_direct_keys': ['treedb.cache.delete_range.backend_direct_keys_total'],
    'range_span_layers': ['treedb.cache.range_span.layers_total'],
    'range_span_active_layers': ['treedb.cache.range_span.active_layers'],
    'range_span_active_spans': ['treedb.cache.range_span.active_spans'],
    'range_span_input': ['treedb.cache.range_span.input_total'],
    'range_span_effective': ['treedb.cache.range_span.effective_total'],
    'range_span_keys_materialized': ['treedb.cache.range_span.keys_materialized_total'],
    'range_span_point_overrides': ['treedb.cache.range_span.point_overrides_total'],
    'range_span_point_probes': ['treedb.cache.range_span.point_probes_total'],
    'range_span_point_hits': ['treedb.cache.range_span.point_hits_total'],
    'range_span_iterator_probes': ['treedb.cache.range_span.iterator_probes_total'],
    'range_span_iterator_skips': ['treedb.cache.range_span.iterator_skips_total'],
    'range_span_range_only_queued_units': ['treedb.cache.range_span.range_only_queued_units_total'],
    'range_span_range_only_flushed': ['treedb.cache.range_span.range_only_flushed_total'],
    'range_span_spans_flushed': ['treedb.cache.range_span.spans_flushed_total'],
    'range_span_flush_batches': ['treedb.cache.range_span.flush_batches_total'],
}

vlog_stat_names = [
    'crc32_checks', 'grouped_hits', 'grouped_misses', 'grouped_stores',
    'mmap_hits', 'mmap_miss_out_of_range', 'mmap_miss_no_mapping',
    'mmap_miss_dead_mapping_cap', 'mmap_fallback_readat',
    'outer_leaf_loads', 'outer_leaf_point_loads', 'outer_leaf_iterator_loads',
    'outer_leaf_checksum_verifications', 'outer_leaf_checksum_skips',
]
delete_range_stat_names = [
    'delete_range_calls', 'delete_range_batch_calls', 'delete_range_batch_writes',
    'delete_range_input_ranges', 'delete_range_effective_ranges', 'delete_range_coalesced_ranges',
    'delete_range_iterators', 'delete_range_snapshot_iterators', 'delete_range_backend_iterators',
    'delete_range_memtable_iterators', 'delete_range_queue_iterators', 'delete_range_visited_keys', 'delete_range_tombstone_keys',
    'delete_range_materialized_keys', 'delete_range_materialized_key_bytes',
    'delete_range_fast_path_clears', 'delete_range_backend_direct_batches', 'delete_range_backend_direct_keys',
    'range_span_layers', 'range_span_active_layers', 'range_span_active_spans',
    'range_span_input', 'range_span_effective', 'range_span_keys_materialized',
    'range_span_iterator_probes', 'range_span_iterator_skips',
    'range_span_range_only_queued_units', 'range_span_range_only_flushed', 'range_span_spans_flushed',
    'range_span_flush_batches',
]

def fmt(n):
    try:
        return f"{int(float(n)):,}"
    except Exception:
        return n

def load_doc(path):
    if path not in json_cache:
        with open(path) as f:
            json_cache[path] = json.load(f)
    return json_cache[path]

def run_for_row(r):
    doc = load_doc(r['json'])
    for run in doc.get('runs', []):
        if run.get('engine') == r['engine']:
            return run
    return None

def stat_value(delta, name):
    for key in stat_key_groups[name]:
        if key in delta:
            return int(delta[key])
    return 0

def phase_delta(r, phase):
    run = run_for_row(r)
    if not run:
        return {}
    return run.get('phases', {}).get(phase, {}).get('stat_delta') or {}

def metric_ratio(a, b, metric):
    denom = float(b[metric])
    if denom == 0:
        return 'n/a'
    return f"{float(a[metric]) / denom:.3f}x"

phase_rows = []
for r in rows:
    for phase in phases:
        delta = phase_delta(r, phase)
        if not delta:
            continue
        vals = {name: stat_value(delta, name) for name in stat_key_groups}
        if not any(vals.values()):
            continue
        phase_rows.append((r, phase, vals))

with open(phase_tsv, 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow([
        'key_shape', 'value_shape', 'value_size', 'batch_target_bytes', 'treedb_read_integrity',
        'iteration_mode', 'engine', 'run_read_integrity', 'phase', *stat_key_groups.keys(), 'json'
    ])
    for r, phase, vals in phase_rows:
        writer.writerow([
            r['key_shape'], r['value_shape'], r['value_size'], r['batch_target_bytes'], r['treedb_read_integrity'],
            r['iteration_mode'], r['engine'], r['read_integrity'], phase,
            *(vals[name] for name in stat_key_groups), r['json'],
        ])

with open(phase_stat_tsv, 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerow([
        'key_shape', 'value_shape', 'value_size', 'batch_target_bytes', 'treedb_read_integrity',
        'iteration_mode', 'engine', 'run_read_integrity', 'phase', 'stat_key', 'delta', 'json'
    ])
    for r in rows:
        run = run_for_row(r)
        if not run:
            continue
        for phase in phases:
            delta = (run.get('phases', {}).get(phase, {}).get('stat_delta') or {})
            for key in sorted(delta):
                value = delta[key]
                if not value:
                    continue
                writer.writerow([
                    r['key_shape'], r['value_shape'], r['value_size'], r['batch_target_bytes'], r['treedb_read_integrity'],
                    r['iteration_mode'], r['engine'], r['read_integrity'], phase, key, value, r['json'],
                ])

with open(md, 'w') as out:
    out.write('# geth/Nitro hot KV matrix\n\n')
    out.write('Integrated node.OpenDatabase / ethdb benchmark. DeleteRange keys/sec counts affected keys/sec, not range calls/sec. Size bytes is loaded DB size before destructive DeleteRange; post-delete bytes is measured after close/reopen verification. TreeDB read-integrity labels identify checksum-verified runs and the explicitly unsafe checksum-disabled ceiling. Iteration mode labels distinguish value materialization from key-only traversal.\n\n')
    out.write('| key shape | value shape | value size | batch target bytes | TreeDB read-integrity | iteration mode | engine | run read-integrity | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes | post-delete bytes |\n')
    out.write('|---|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|\n')
    for r in rows:
        out.write('| {key_shape} | {value_shape} | {value_size} | {batch_target_bytes} | {treedb_read_integrity} | {iteration_mode} | {engine} | {read_integrity} | {write} | {read} | {iterate} | {delete} | {size} | {post_delete} |\n'.format(
            key_shape=r['key_shape'], value_shape=r['value_shape'], value_size=fmt(r['value_size']),
            batch_target_bytes=fmt(r['batch_target_bytes']), treedb_read_integrity=r['treedb_read_integrity'],
            iteration_mode=r['iteration_mode'], engine=r['engine'], read_integrity=r['read_integrity'], write=fmt(r['write_ops_sec']),
            read=fmt(r['read_ops_sec']), iterate=fmt(r['iterate_keys_sec']), delete=fmt(r['delete_range_keys_sec']),
            size=fmt(r['size_bytes']), post_delete=fmt(r['post_delete_size_bytes'])))

    out.write('\n## TreeDB ratios versus Pebble\n\n')
    groups = defaultdict(dict)
    for r in rows:
        key = (r['key_shape'], r['value_shape'], r['value_size'], r['batch_target_bytes'], r['treedb_read_integrity'], r['iteration_mode'])
        groups[key][r['engine']] = r
    out.write('| key shape | value shape | value size | batch target bytes | TreeDB read-integrity | iteration mode | write ratio | read ratio | iterate ratio | DeleteRange ratio | size ratio |\n')
    out.write('|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|\n')
    for key in sorted(groups):
        g = groups[key]
        if 'treedb' not in g or 'pebble' not in g:
            continue
        t, p = g['treedb'], g['pebble']
        out.write('| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n'.format(
            key[0], key[1], fmt(key[2]), fmt(key[3]), key[4], key[5], metric_ratio(t, p, 'write_ops_sec'), metric_ratio(t, p, 'read_ops_sec'),
            metric_ratio(t, p, 'iterate_keys_sec'), metric_ratio(t, p, 'delete_range_keys_sec'), metric_ratio(t, p, 'size_bytes')))

    iter_groups = defaultdict(dict)
    for r in rows:
        if r['engine'] != 'treedb':
            continue
        key = (r['key_shape'], r['value_shape'], r['value_size'], r['batch_target_bytes'], r['treedb_read_integrity'], r['engine'])
        iter_groups[key][r['iteration_mode']] = r
    comparable = [(key, g) for key, g in sorted(iter_groups.items()) if 'value' in g and 'key-only' in g]
    if comparable:
        out.write('\n## TreeDB key-only vs value iteration deltas\n\n')
        out.write('Ratios are `key-only / value` for otherwise identical TreeDB runs. Iterate CRC counters come from the per-phase TreeDB stat deltas.\n\n')
        out.write('| key shape | value shape | value size | batch target bytes | read-integrity | write ratio | read ratio | iterate ratio | DeleteRange ratio | value iterate CRC | key-only iterate CRC | CRC delta |\n')
        out.write('|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|\n')
        for key, g in comparable:
            value = g['value']
            key_only = g['key-only']
            value_crc = stat_value(phase_delta(value, 'iterate'), 'crc32_checks')
            key_crc = stat_value(phase_delta(key_only, 'iterate'), 'crc32_checks')
            out.write('| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n'.format(
                key[0], key[1], fmt(key[2]), fmt(key[3]), key[4],
                metric_ratio(key_only, value, 'write_ops_sec'), metric_ratio(key_only, value, 'read_ops_sec'),
                metric_ratio(key_only, value, 'iterate_keys_sec'), metric_ratio(key_only, value, 'delete_range_keys_sec'),
                fmt(value_crc), fmt(key_crc), fmt(key_crc - value_crc)))

    if any(any(vals[name] for name in vlog_stat_names) for _, _, vals in phase_rows):
        out.write('\n## TreeDB value-log read counters\n\n')
        out.write('Per-phase deltas from TreeDB `Stat()` output. CRC counts are value-log record CRC32 computations; grouped counters reflect grouped-frame cache activity; mmap columns split hits, misses, and ReadAt fallback reads. Outer-leaf columns identify B-tree leaf pages read from the value-log-backed leaf log. Full machine-readable read counters are in `phase_counters.tsv`; all nonzero parseable TreeDB stat deltas are in `phase_stat_deltas.tsv`.\n\n')
        out.write('| key shape | value size | batch target bytes | read-integrity | iteration mode | engine | phase | crc32 checks | grouped hits | grouped misses | grouped stores | mmap hits | mmap miss OOR | mmap miss no-map | mmap miss dead-cap | mmap ReadAt fallback | outer leaf loads | outer leaf point loads | outer leaf iterator loads | outer leaf cksum verifies | outer leaf cksum skips |\n')
        out.write('|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n')
        for r, phase, vals in phase_rows:
            if not any(vals[name] for name in vlog_stat_names):
                continue
            out.write('| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n'.format(
                r['key_shape'], fmt(r['value_size']), fmt(r['batch_target_bytes']), r['read_integrity'],
                r['iteration_mode'], r['engine'], phase,
                fmt(vals['crc32_checks']), fmt(vals['grouped_hits']), fmt(vals['grouped_misses']), fmt(vals['grouped_stores']),
                fmt(vals['mmap_hits']), fmt(vals['mmap_miss_out_of_range']), fmt(vals['mmap_miss_no_mapping']),
                fmt(vals['mmap_miss_dead_mapping_cap']), fmt(vals['mmap_fallback_readat']),
                fmt(vals['outer_leaf_loads']), fmt(vals['outer_leaf_point_loads']), fmt(vals['outer_leaf_iterator_loads']),
                fmt(vals['outer_leaf_checksum_verifications']), fmt(vals['outer_leaf_checksum_skips'])))

    delete_phase_rows = [(r, phase, vals) for r, phase, vals in phase_rows if any(vals[name] for name in delete_range_stat_names)]
    if delete_phase_rows:
        out.write('\n## TreeDB DeleteRange counters\n\n')
        out.write('Per-phase DeleteRange counters from TreeDB `Stat()`. Input ranges are submitted non-empty ranges; effective ranges are exact adjacent/overlap-coalesced write-plan ranges; materialized keys are copied into point tombstones by the cached fallback. Span columns report command-WAL range-span overlay activity. Full machine-readable counters are in `phase_counters.tsv`; all nonzero parseable TreeDB stat deltas are in `phase_stat_deltas.tsv`.\n\n')
        out.write('| key shape | value size | batch target bytes | read-integrity | iteration mode | engine | phase | db calls | batch calls | batch writes | input ranges | effective ranges | coalesced ranges | visited keys | materialized keys | materialized key bytes | tombstone keys | iterators | snapshot iters | backend iters | memtable iters | queue iters | fast clears | backend direct batches | backend direct keys | span layers | span active | span input | span effective | span materialized keys | span iter probes | span iter skips | span queued units | span flushed | spans flushed | span flush batches |\n')
        out.write('|---|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n')
        for r, phase, vals in delete_phase_rows:
            cells = [
                r['key_shape'], fmt(r['value_size']), fmt(r['batch_target_bytes']), r['read_integrity'],
                r['iteration_mode'], r['engine'], phase,
                fmt(vals['delete_range_calls']), fmt(vals['delete_range_batch_calls']), fmt(vals['delete_range_batch_writes']),
                fmt(vals['delete_range_input_ranges']), fmt(vals['delete_range_effective_ranges']), fmt(vals['delete_range_coalesced_ranges']),
                fmt(vals['delete_range_visited_keys']), fmt(vals['delete_range_materialized_keys']),
                fmt(vals['delete_range_materialized_key_bytes']), fmt(vals['delete_range_tombstone_keys']),
                fmt(vals['delete_range_iterators']), fmt(vals['delete_range_snapshot_iterators']),
                fmt(vals['delete_range_backend_iterators']), fmt(vals['delete_range_memtable_iterators']),
                fmt(vals['delete_range_queue_iterators']),
                fmt(vals['delete_range_fast_path_clears']), fmt(vals['delete_range_backend_direct_batches']),
                fmt(vals['delete_range_backend_direct_keys']),
                fmt(vals['range_span_layers']), fmt(vals['range_span_active_spans']),
                fmt(vals['range_span_input']), fmt(vals['range_span_effective']),
                fmt(vals['range_span_keys_materialized']), fmt(vals['range_span_iterator_probes']),
                fmt(vals['range_span_iterator_skips']), fmt(vals['range_span_range_only_queued_units']),
                fmt(vals['range_span_range_only_flushed']), fmt(vals['range_span_spans_flushed']),
                fmt(vals['range_span_flush_batches']),
            ]
            out.write('| ' + ' | '.join(cells) + ' |\n')

PY

echo "geth hot KV matrix complete"
echo "  run dir:         $run_dir"
echo "  tsv:             $summary_tsv"
echo "  phase counters:  $phase_counters_tsv"
echo "  stat deltas:     $phase_stat_deltas_tsv"
echo "  report:          $summary_md"
