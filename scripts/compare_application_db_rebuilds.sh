#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  compare_application_db_rebuilds.sh /path/to/source/application.db

Environment:
  OUT_DIR                          Output directory. Default: /home/mikers/.application-db-compare-<timestamp>
  TREEDB_REPO                      TreeDB repo to use for rebuild/compact. Default: /home/mikers/dev/snissn/gomap-phasehook-active
  COSMOS_DB_REPO                   Cosmos DB repo to use. Default: /home/mikers/dev/snissn/cosmos-db
  TREEDB_PROFILE                   TreeDB open profile. Default: command_wal_relaxed
  TREEDB_FORCE_CHECKPOINT_ON_WRITE Forwarded to cosmos-db TreeDB adapter. Default: 0
  GOLEVELDB_BLOCK_SIZE             Override goleveldb SST block size for rebuild/compact. Default: 4096
  GOLEVELDB_BLOCK_RESTART_INTERVAL Override goleveldb block restart interval for rebuild/compact. Default: 16
  BATCH_COUNT                      Flush after this many keys. Default: 50000
  BATCH_BYTES                      Flush after this many buffered bytes. Default: 33554432

Outputs:
  - results.json / results.md in OUT_DIR
  - copies in /tmp/<out-basename>.json and /tmp/<out-basename>.md
USAGE
}

SOURCE_APP_DB="${1:-${SOURCE_APP_DB:-}}"
if [[ -z "${SOURCE_APP_DB}" ]]; then
  usage
  exit 1
fi

if [[ ! -d "${SOURCE_APP_DB}" ]]; then
  echo "source application.db directory does not exist: ${SOURCE_APP_DB}" >&2
  exit 1
fi

TREEDB_REPO="${TREEDB_REPO:-/home/mikers/dev/snissn/gomap-phasehook-active}"
COSMOS_DB_REPO="${COSMOS_DB_REPO:-/home/mikers/dev/snissn/cosmos-db}"
TREEDB_PROFILE="${TREEDB_PROFILE:-command_wal_relaxed}"
TREEDB_FORCE_CHECKPOINT_ON_WRITE="${TREEDB_FORCE_CHECKPOINT_ON_WRITE:-0}"
GOLEVELDB_BLOCK_SIZE="${GOLEVELDB_BLOCK_SIZE:-4096}"
GOLEVELDB_BLOCK_RESTART_INTERVAL="${GOLEVELDB_BLOCK_RESTART_INTERVAL:-16}"
BATCH_COUNT="${BATCH_COUNT:-50000}"
BATCH_BYTES="${BATCH_BYTES:-33554432}"
STAMP="$(date +%Y%m%d%H%M%S)"
OUT_DIR="${OUT_DIR:-/home/mikers/.application-db-compare-${STAMP}}"

for required in "${TREEDB_REPO}" "${COSMOS_DB_REPO}"; do
  if [[ ! -d "${required}" ]]; then
    echo "required directory does not exist: ${required}" >&2
    exit 1
  fi
done

if ! command -v /usr/bin/time >/dev/null 2>&1; then
  echo "/usr/bin/time is required" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}/"{logs,bin,helpermod,leveldb,treedb}

HELPER_DIR="${OUT_DIR}/helpermod"
HELPER_BIN="${OUT_DIR}/bin/comparedb"
LEVELDB_PARENT="${OUT_DIR}/leveldb"
TREEDB_PARENT="${OUT_DIR}/treedb"
RESULTS_JSON="${OUT_DIR}/results.json"
RESULTS_MD="${OUT_DIR}/results.md"
TMP_RESULTS_JSON="/tmp/$(basename "${OUT_DIR}").json"
TMP_RESULTS_MD="/tmp/$(basename "${OUT_DIR}").md"

cat > "${HELPER_DIR}/go.mod" <<GOMOD
module comparedb

go 1.26

require (
	github.com/cosmos/cosmos-db v0.0.0
	github.com/snissn/gomap v0.0.0
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
)

replace github.com/cosmos/cosmos-db => ${COSMOS_DB_REPO}
replace github.com/snissn/gomap => ${TREEDB_REPO}
GOMOD

cat > "${HELPER_DIR}/main.go" <<'GOEOF'
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	dbm "github.com/cosmos/cosmos-db"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type stepResult struct {
	Mode             string         `json:"mode"`
	Backend          string         `json:"backend,omitempty"`
	Profile          string         `json:"profile,omitempty"`
	SourcePath       string         `json:"source_path,omitempty"`
	DestParent       string         `json:"dest_parent,omitempty"`
	DestDBPath       string         `json:"dest_db_path,omitempty"`
	Keys             uint64         `json:"keys,omitempty"`
	KeyBytes         uint64         `json:"key_bytes,omitempty"`
	ValueBytes       uint64         `json:"value_bytes,omitempty"`
	FlushCount       int            `json:"flush_count,omitempty"`
	FlushBytes       int64          `json:"flush_bytes,omitempty"`
	WallSeconds      float64        `json:"wall_seconds"`
	MaxRSSKBObserved int64          `json:"max_rss_kb_observed,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
	CompactStats     map[string]any `json:"compact_stats,omitempty"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		fatalf("expected subcommand: rebuild | compact-leveldb | compact-treedb")
	}
	switch os.Args[1] {
	case "rebuild":
		runRebuild(os.Args[2:])
	case "compact-leveldb":
		runCompactLevelDB(os.Args[2:])
	case "compact-treedb":
		runCompactTreeDB(os.Args[2:])
	default:
		fatalf("unknown subcommand %q", os.Args[1])
	}
}

func runRebuild(args []string) {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	backend := fs.String("backend", "", "goleveldb or treedb")
	source := fs.String("source", "", "source leveldb application.db path")
	destParent := fs.String("dest-parent", "", "destination parent directory")
	flushCount := fs.Int("flush-count", 50000, "flush after N keys")
	flushBytes := fs.Int64("flush-bytes", 32<<20, "flush after N buffered bytes")
	leveldbBlockSize := fs.Int("leveldb-block-size", 4096, "goleveldb block size")
	leveldbBlockRestartInterval := fs.Int("leveldb-block-restart-interval", 16, "goleveldb block restart interval")
	if err := fs.Parse(args); err != nil {
		fatalErr(err)
	}
	if *backend == "" || *source == "" || *destParent == "" {
		fatalf("backend, source, and dest-parent are required")
	}

	destDBPath := filepath.Join(*destParent, "application.db")
	if err := os.RemoveAll(destDBPath); err != nil {
		fatalErr(err)
	}
	if err := os.MkdirAll(*destParent, 0o755); err != nil {
		fatalErr(err)
	}

	start := time.Now()
	src, err := leveldb.OpenFile(*source, &opt.Options{Filter: filter.NewBloomFilter(10)})
	if err != nil {
		fatalErr(fmt.Errorf("open source leveldb: %w", err))
	}
	defer src.Close()

	var dbBackend dbm.BackendType
	switch strings.ToLower(strings.TrimSpace(*backend)) {
	case "goleveldb":
		dbBackend = dbm.GoLevelDBBackend
	case "treedb":
		dbBackend = dbm.TreeDBBackend
	default:
		fatalf("unsupported backend %q", *backend)
	}

	var dst dbm.DB
	switch dbBackend {
	case dbm.GoLevelDBBackend:
		dst, err = dbm.NewGoLevelDBWithOpts("application", *destParent, newGoLevelDBOptions(*leveldbBlockSize, *leveldbBlockRestartInterval))
		if err != nil {
			fatalErr(fmt.Errorf("open destination backend=%s block_size=%d restart_interval=%d: %w", dbBackend, *leveldbBlockSize, *leveldbBlockRestartInterval, err))
		}
	default:
		dst, err = dbm.NewDB("application", dbBackend, *destParent)
		if err != nil {
			fatalErr(fmt.Errorf("open destination backend=%s: %w", dbBackend, err))
		}
	}

	batch := dst.NewBatchWithSize(int(min64(*flushBytes, 128<<20)))
	iter := src.NewIterator(nil, nil)
	defer iter.Release()

	var keys uint64
	var keyBytes uint64
	var valueBytes uint64
	var bufferedBytes int64
	lastProgress := time.Now()

	flush := func() {
		if bufferedBytes == 0 {
			return
		}
		if err := batch.Write(); err != nil {
			fatalErr(fmt.Errorf("write batch: %w", err))
		}
		batch = dst.NewBatchWithSize(int(min64(*flushBytes, 128<<20)))
		bufferedBytes = 0
	}

	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		value := append([]byte(nil), iter.Value()...)
		if err := batch.Set(key, value); err != nil {
			fatalErr(fmt.Errorf("batch set: %w", err))
		}
		keys++
		keyBytes += uint64(len(key))
		valueBytes += uint64(len(value))
		bufferedBytes += int64(len(key) + len(value))
		if bufferedBytes >= *flushBytes || (*flushCount > 0 && keys%uint64(*flushCount) == 0) {
			flush()
		}
		if keys%1_000_000 == 0 {
			now := time.Now()
			if now.Sub(lastProgress) >= 5*time.Second {
				log.Printf("rebuild progress backend=%s keys=%d elapsed=%s maxrss_kb=%d", dbBackend, keys, now.Sub(start).Round(time.Second), maxRSSKB())
				lastProgress = now
			}
		}
	}
	if err := iter.Error(); err != nil {
		fatalErr(fmt.Errorf("iterate source: %w", err))
	}
	flush()
	if err := batch.Close(); err != nil {
		fatalErr(fmt.Errorf("close batch: %w", err))
	}
	if err := dst.Close(); err != nil {
		fatalErr(fmt.Errorf("close destination: %w", err))
	}

	res := stepResult{
		Mode:             "rebuild",
		Backend:          string(dbBackend),
		SourcePath:       *source,
		DestParent:       *destParent,
		DestDBPath:       destDBPath,
		Keys:             keys,
		KeyBytes:         keyBytes,
		ValueBytes:       valueBytes,
		FlushCount:       *flushCount,
		FlushBytes:       *flushBytes,
		WallSeconds:      time.Since(start).Seconds(),
		MaxRSSKBObserved: maxRSSKB(),
	}
	if dbBackend == dbm.GoLevelDBBackend {
		res.Extra = map[string]any{
			"block_size":             *leveldbBlockSize,
			"block_restart_interval": *leveldbBlockRestartInterval,
			"compression":            opt.SnappyCompression.String(),
			"filter":                 "bloom-10",
		}
	}
	writeJSON(res)
}

func runCompactLevelDB(args []string) {
	fs := flag.NewFlagSet("compact-leveldb", flag.ExitOnError)
	destParent := fs.String("dest-parent", "", "destination parent directory")
	leveldbBlockSize := fs.Int("leveldb-block-size", 4096, "goleveldb block size")
	leveldbBlockRestartInterval := fs.Int("leveldb-block-restart-interval", 16, "goleveldb block restart interval")
	if err := fs.Parse(args); err != nil {
		fatalErr(err)
	}
	if *destParent == "" {
		fatalf("dest-parent is required")
	}

	start := time.Now()
	db, err := dbm.NewGoLevelDBWithOpts("application", *destParent, newGoLevelDBOptions(*leveldbBlockSize, *leveldbBlockRestartInterval))
	if err != nil {
		fatalErr(fmt.Errorf("open goleveldb destination block_size=%d restart_interval=%d: %w", *leveldbBlockSize, *leveldbBlockRestartInterval, err))
	}
	if err := db.ForceCompact(nil, nil); err != nil {
		_ = db.Close()
		fatalErr(fmt.Errorf("compact range: %w", err))
	}
	stats := db.Stats()
	if err := db.Close(); err != nil {
		fatalErr(fmt.Errorf("close goleveldb destination: %w", err))
	}
	res := stepResult{
		Mode:             "compact-leveldb",
		Backend:          string(dbm.GoLevelDBBackend),
		DestParent:       *destParent,
		DestDBPath:       filepath.Join(*destParent, "application.db"),
		WallSeconds:      time.Since(start).Seconds(),
		MaxRSSKBObserved: maxRSSKB(),
		Extra: map[string]any{
			"block_size":             *leveldbBlockSize,
			"block_restart_interval": *leveldbBlockRestartInterval,
			"compression":            opt.SnappyCompression.String(),
			"filter":                 "bloom-10",
			"stats":                  stats,
		},
	}
	writeJSON(res)
}

func runCompactTreeDB(args []string) {
	fs := flag.NewFlagSet("compact-treedb", flag.ExitOnError)
	destParent := fs.String("dest-parent", "", "destination parent directory")
	profileRaw := fs.String("profile", "command_wal_relaxed", treedb.ProfileFlagHelp)
	if err := fs.Parse(args); err != nil {
		fatalErr(err)
	}
	if *destParent == "" {
		fatalf("dest-parent is required")
	}
	profile, profileName, err := parseTreeDBProfile(*profileRaw)
	if err != nil {
		fatalErr(err)
	}

	start := time.Now()
	opts := treedb.OptionsFor(profile, filepath.Join(*destParent, "application.db"))
	db, err := treedb.Open(opts)
	if err != nil {
		fatalErr(fmt.Errorf("open treedb for compaction: %w", err))
	}
	stats, compactErr := db.CompactStorage(context.Background(), treedb.CompactStorageOptions{
		Mode:          treedb.CompactStorageFull,
		SyncEachPhase: true,
	})
	closeErr := db.Close()
	if compactErr != nil {
		fatalErr(fmt.Errorf("treedb compact storage: %w", compactErr))
	}
	if closeErr != nil {
		fatalErr(fmt.Errorf("close treedb after compact storage: %w", closeErr))
	}
	if err := treedb.VacuumIndexOffline(opts); err != nil {
		fatalErr(fmt.Errorf("treedb offline index vacuum after compact storage: %w", err))
	}
	res := stepResult{
		Mode:             "compact-treedb",
		Backend:          string(dbm.TreeDBBackend),
		Profile:          profileName,
		DestParent:       *destParent,
		DestDBPath:       filepath.Join(*destParent, "application.db"),
		WallSeconds:      time.Since(start).Seconds(),
		MaxRSSKBObserved: maxRSSKB(),
		CompactStats: map[string]any{
			"fully_compacted":                      stats.FullyCompacted,
			"phase_count":                          len(stats.Phases),
			"value_log_rewrite_records_copied":     stats.ValueLogRewrite.RecordsCopied,
			"value_log_rewrite_bytes_after":        stats.ValueLogRewrite.BytesAfter,
			"value_log_gc_segments_deleted":        stats.ValueLogGC.SegmentsDeleted,
			"leaf_generation_gc_files_deleted":     stats.LeafGenerationGC.FilesDeleted,
			"zero_byte_value_log_files_deleted":    stats.ZeroByteValueLogFilesDeleted,
			"remaining_value_log_rewrite_segments": stats.RemainingDebt.ValueLogRewriteSegments,
			"remaining_value_log_gc_segments":      stats.RemainingDebt.ValueLogGCSegments,
			"remaining_leaf_pack_generations":      stats.RemainingDebt.LeafPackGenerations,
			"remaining_leaf_gc_generations":        stats.RemainingDebt.LeafGCGenerations,
		},
	}
	writeJSON(res)
}

func parseTreeDBProfile(raw string) (treedb.Profile, string, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALRelaxed)
	if !ok {
		return treedb.ProfileCommandWALRelaxed, "", fmt.Errorf("unsupported treedb profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, string(profile), nil
}

func maxRSSKB() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Maxrss
}

func newGoLevelDBOptions(blockSize, blockRestartInterval int) *opt.Options {
	return &opt.Options{
		Filter:               filter.NewBloomFilter(10),
		BlockSize:            blockSize,
		BlockRestartInterval: blockRestartInterval,
		Compression:          opt.SnappyCompression,
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fatalErr(err)
	}
}

func fatalErr(err error) {
	if err == nil {
		os.Exit(1)
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(2)
	}
	log.Fatalf("error: %v", err)
}

func fatalf(format string, args ...any) {
	log.Fatalf(format, args...)
}
GOEOF

echo "[compare] building helper binary..."
(cd "${HELPER_DIR}" && go mod download all && GOFLAGS=-mod=mod go build -o "${HELPER_BIN}" .)

safe_du_bytes() {
  local path="$1"
  if [[ ! -e "${path}" ]]; then
    echo 0
    return
  fi
  du -sb "${path}" 2>/dev/null | awk '{print $1}'
}

extract_max_rss_kb() {
  local time_file="$1"
  awk -F': *' '/Maximum resident set size \(kbytes\)/ {print $2}' "${time_file}" | tail -n1
}

run_timed_step() {
  local name="$1"
  shift
  local json_out="${OUT_DIR}/logs/${name}.json"
  local time_out="${OUT_DIR}/logs/${name}.time.txt"
  echo "[compare] ${name}..."
  /usr/bin/time -v -o "${time_out}" "$@" >"${json_out}"
}

SOURCE_BYTES="$(safe_du_bytes "${SOURCE_APP_DB}")"

run_timed_step rebuild_leveldb \
  "${HELPER_BIN}" rebuild \
  -backend goleveldb \
  -source "${SOURCE_APP_DB}" \
  -dest-parent "${LEVELDB_PARENT}" \
  -leveldb-block-size "${GOLEVELDB_BLOCK_SIZE}" \
  -leveldb-block-restart-interval "${GOLEVELDB_BLOCK_RESTART_INTERVAL}" \
  -flush-count "${BATCH_COUNT}" \
  -flush-bytes "${BATCH_BYTES}"

LEVELDB_BUILD_BYTES="$(safe_du_bytes "${LEVELDB_PARENT}/application.db")"
LEVELDB_BUILD_MAX_RSS_KB="$(extract_max_rss_kb "${OUT_DIR}/logs/rebuild_leveldb.time.txt")"

run_timed_step compact_leveldb \
  "${HELPER_BIN}" compact-leveldb \
  -dest-parent "${LEVELDB_PARENT}" \
  -leveldb-block-size "${GOLEVELDB_BLOCK_SIZE}" \
  -leveldb-block-restart-interval "${GOLEVELDB_BLOCK_RESTART_INTERVAL}"

LEVELDB_FINAL_BYTES="$(safe_du_bytes "${LEVELDB_PARENT}/application.db")"
LEVELDB_FINAL_MAX_RSS_KB="$(extract_max_rss_kb "${OUT_DIR}/logs/compact_leveldb.time.txt")"

run_timed_step rebuild_treedb \
  env \
  TREEDB_OPEN_PROFILE="${TREEDB_PROFILE}" \
  TREEDB_FORCE_CHECKPOINT_ON_WRITE="${TREEDB_FORCE_CHECKPOINT_ON_WRITE}" \
  "${HELPER_BIN}" rebuild \
  -backend treedb \
  -source "${SOURCE_APP_DB}" \
  -dest-parent "${TREEDB_PARENT}" \
  -flush-count "${BATCH_COUNT}" \
  -flush-bytes "${BATCH_BYTES}"

TREEDB_BUILD_BYTES="$(safe_du_bytes "${TREEDB_PARENT}/application.db")"
TREEDB_BUILD_MAX_RSS_KB="$(extract_max_rss_kb "${OUT_DIR}/logs/rebuild_treedb.time.txt")"

run_timed_step compact_treedb \
  "${HELPER_BIN}" compact-treedb \
  -dest-parent "${TREEDB_PARENT}" \
  -profile "${TREEDB_PROFILE}"

TREEDB_FINAL_BYTES="$(safe_du_bytes "${TREEDB_PARENT}/application.db")"
TREEDB_FINAL_MAX_RSS_KB="$(extract_max_rss_kb "${OUT_DIR}/logs/compact_treedb.time.txt")"

python3 - <<'PY' \
  "${SOURCE_APP_DB}" \
  "${SOURCE_BYTES}" \
  "${TREEDB_REPO}" \
  "${COSMOS_DB_REPO}" \
  "${TREEDB_PROFILE}" \
  "${TREEDB_FORCE_CHECKPOINT_ON_WRITE}" \
  "${GOLEVELDB_BLOCK_SIZE}" \
  "${GOLEVELDB_BLOCK_RESTART_INTERVAL}" \
  "${RESULTS_JSON}" \
  "${RESULTS_MD}" \
  "${TMP_RESULTS_JSON}" \
  "${TMP_RESULTS_MD}" \
  "${OUT_DIR}" \
  "${LEVELDB_BUILD_BYTES}" \
  "${LEVELDB_BUILD_MAX_RSS_KB}" \
  "${LEVELDB_FINAL_BYTES}" \
  "${LEVELDB_FINAL_MAX_RSS_KB}" \
  "${TREEDB_BUILD_BYTES}" \
  "${TREEDB_BUILD_MAX_RSS_KB}" \
  "${TREEDB_FINAL_BYTES}" \
  "${TREEDB_FINAL_MAX_RSS_KB}"
import json
import pathlib
import shutil
import sys

(
    source_app_db,
    source_bytes,
    treedb_repo,
    cosmos_db_repo,
    treedb_profile,
    treedb_force_checkpoint,
    goleveldb_block_size,
    goleveldb_block_restart_interval,
    results_json_path,
    results_md_path,
    tmp_results_json_path,
    tmp_results_md_path,
    out_dir,
    leveldb_build_bytes,
    leveldb_build_max_rss_kb,
    leveldb_final_bytes,
    leveldb_final_max_rss_kb,
    treedb_build_bytes,
    treedb_build_max_rss_kb,
    treedb_final_bytes,
    treedb_final_max_rss_kb,
) = sys.argv[1:]

out_dir = pathlib.Path(out_dir)
logs_dir = out_dir / "logs"

def load_json(name):
    return json.loads((logs_dir / f"{name}.json").read_text())

def load_time_maxrss(name):
    text = (logs_dir / f"{name}.time.txt").read_text().splitlines()
    for line in text:
        if line.startswith("Maximum resident set size (kbytes):"):
            return int(line.split(":", 1)[1].strip())
    return 0

def gib(n):
    return float(n) / (1024 ** 3)

rebuild_leveldb = load_json("rebuild_leveldb")
compact_leveldb = load_json("compact_leveldb")
rebuild_treedb = load_json("rebuild_treedb")
compact_treedb = load_json("compact_treedb")

summary = {
    "source": {
        "application_db": source_app_db,
        "bytes": int(source_bytes),
        "gib": gib(int(source_bytes)),
    },
    "config": {
        "treedb_repo": treedb_repo,
        "cosmos_db_repo": cosmos_db_repo,
        "treedb_profile": treedb_profile,
        "treedb_force_checkpoint_on_write": treedb_force_checkpoint,
        "goleveldb_block_size": int(goleveldb_block_size),
        "goleveldb_block_restart_interval": int(goleveldb_block_restart_interval),
    },
    "leveldb": {
        "rebuild": {
            **rebuild_leveldb,
            "disk_bytes_after": int(leveldb_build_bytes),
            "disk_gib_after": gib(int(leveldb_build_bytes)),
            "max_rss_kb": int(leveldb_build_max_rss_kb or load_time_maxrss("rebuild_leveldb")),
            "max_rss_gib": gib(int(leveldb_build_max_rss_kb or load_time_maxrss("rebuild_leveldb")) * 1024),
        },
        "final_compact": {
            **compact_leveldb,
            "disk_bytes_after": int(leveldb_final_bytes),
            "disk_gib_after": gib(int(leveldb_final_bytes)),
            "disk_bytes_delta_vs_rebuild": int(leveldb_final_bytes) - int(leveldb_build_bytes),
            "disk_gib_delta_vs_rebuild": gib(int(leveldb_final_bytes) - int(leveldb_build_bytes)),
            "max_rss_kb": int(leveldb_final_max_rss_kb or load_time_maxrss("compact_leveldb")),
            "max_rss_gib": gib(int(leveldb_final_max_rss_kb or load_time_maxrss("compact_leveldb")) * 1024),
        },
    },
    "treedb": {
        "rebuild": {
            **rebuild_treedb,
            "disk_bytes_after": int(treedb_build_bytes),
            "disk_gib_after": gib(int(treedb_build_bytes)),
            "max_rss_kb": int(treedb_build_max_rss_kb or load_time_maxrss("rebuild_treedb")),
            "max_rss_gib": gib(int(treedb_build_max_rss_kb or load_time_maxrss("rebuild_treedb")) * 1024),
        },
        "final_compact": {
            **compact_treedb,
            "disk_bytes_after": int(treedb_final_bytes),
            "disk_gib_after": gib(int(treedb_final_bytes)),
            "disk_bytes_delta_vs_rebuild": int(treedb_final_bytes) - int(treedb_build_bytes),
            "disk_gib_delta_vs_rebuild": gib(int(treedb_final_bytes) - int(treedb_build_bytes)),
            "max_rss_kb": int(treedb_final_max_rss_kb or load_time_maxrss("compact_treedb")),
            "max_rss_gib": gib(int(treedb_final_max_rss_kb or load_time_maxrss("compact_treedb")) * 1024),
        },
    },
}

summary["comparison"] = {
    "rebuild_time_seconds_delta_treedb_minus_leveldb": summary["treedb"]["rebuild"]["wall_seconds"] - summary["leveldb"]["rebuild"]["wall_seconds"],
    "rebuild_disk_bytes_delta_treedb_minus_leveldb": summary["treedb"]["rebuild"]["disk_bytes_after"] - summary["leveldb"]["rebuild"]["disk_bytes_after"],
    "rebuild_max_rss_kb_delta_treedb_minus_leveldb": summary["treedb"]["rebuild"]["max_rss_kb"] - summary["leveldb"]["rebuild"]["max_rss_kb"],
    "final_disk_bytes_delta_treedb_minus_leveldb": summary["treedb"]["final_compact"]["disk_bytes_after"] - summary["leveldb"]["final_compact"]["disk_bytes_after"],
    "final_step_time_seconds_delta_treedb_minus_leveldb": summary["treedb"]["final_compact"]["wall_seconds"] - summary["leveldb"]["final_compact"]["wall_seconds"],
    "final_step_max_rss_kb_delta_treedb_minus_leveldb": summary["treedb"]["final_compact"]["max_rss_kb"] - summary["leveldb"]["final_compact"]["max_rss_kb"],
}

results_json = pathlib.Path(results_json_path)
results_md = pathlib.Path(results_md_path)
results_json.write_text(json.dumps(summary, indent=2) + "\n")

md = []
md.append("# application.db rebuild comparison")
md.append("")
md.append(f"- source: `{source_app_db}`")
md.append(f"- source size: {summary['source']['bytes']} bytes ({summary['source']['gib']:.3f} GiB)")
md.append(f"- treedb repo: `{treedb_repo}`")
md.append(f"- cosmos-db repo: `{cosmos_db_repo}`")
md.append(f"- treedb profile: `{treedb_profile}`")
md.append(f"- goleveldb block size: `{goleveldb_block_size}`")
md.append(f"- goleveldb block restart interval: `{goleveldb_block_restart_interval}`")
md.append("")
md.append("| step | wall s | max rss GiB | disk GiB after | delta vs source GiB |")
md.append("|---|---:|---:|---:|---:|")
md.append(
    f"| goleveldb rebuild | {summary['leveldb']['rebuild']['wall_seconds']:.2f} | {summary['leveldb']['rebuild']['max_rss_gib']:.3f} | {summary['leveldb']['rebuild']['disk_gib_after']:.3f} | {summary['leveldb']['rebuild']['disk_gib_after'] - summary['source']['gib']:+.3f} |"
)
md.append(
    f"| goleveldb final compact | {summary['leveldb']['final_compact']['wall_seconds']:.2f} | {summary['leveldb']['final_compact']['max_rss_gib']:.3f} | {summary['leveldb']['final_compact']['disk_gib_after']:.3f} | {summary['leveldb']['final_compact']['disk_gib_after'] - summary['source']['gib']:+.3f} |"
)
md.append(
    f"| treedb rebuild | {summary['treedb']['rebuild']['wall_seconds']:.2f} | {summary['treedb']['rebuild']['max_rss_gib']:.3f} | {summary['treedb']['rebuild']['disk_gib_after']:.3f} | {summary['treedb']['rebuild']['disk_gib_after'] - summary['source']['gib']:+.3f} |"
)
md.append(
    f"| treedb final compact | {summary['treedb']['final_compact']['wall_seconds']:.2f} | {summary['treedb']['final_compact']['max_rss_gib']:.3f} | {summary['treedb']['final_compact']['disk_gib_after']:.3f} | {summary['treedb']['final_compact']['disk_gib_after'] - summary['source']['gib']:+.3f} |"
)
md.append("")
md.append("## Direct deltas")
md.append("")
md.append(f"- treedb rebuild wall time minus goleveldb rebuild: {summary['comparison']['rebuild_time_seconds_delta_treedb_minus_leveldb']:+.2f} s")
md.append(f"- treedb rebuild disk minus goleveldb rebuild: {gib(summary['comparison']['rebuild_disk_bytes_delta_treedb_minus_leveldb']):+.3f} GiB")
md.append(f"- treedb rebuild max rss minus goleveldb rebuild: {gib(summary['comparison']['rebuild_max_rss_kb_delta_treedb_minus_leveldb'] * 1024):+.3f} GiB")
md.append(f"- treedb final disk minus goleveldb final disk: {gib(summary['comparison']['final_disk_bytes_delta_treedb_minus_leveldb']):+.3f} GiB")
md.append(f"- treedb final step wall time minus goleveldb final step: {summary['comparison']['final_step_time_seconds_delta_treedb_minus_leveldb']:+.2f} s")
md.append(f"- treedb final step max rss minus goleveldb final step: {gib(summary['comparison']['final_step_max_rss_kb_delta_treedb_minus_leveldb'] * 1024):+.3f} GiB")
if summary["treedb"]["final_compact"].get("compact_stats"):
    rs = summary["treedb"]["final_compact"]["compact_stats"]
    md.append("")
    md.append("## TreeDB compact stats")
    md.append("")
    for key in ["fully_compacted", "phase_count", "value_log_rewrite_records_copied", "value_log_gc_segments_deleted", "leaf_generation_gc_files_deleted", "zero_byte_value_log_files_deleted"]:
        md.append(f"- {key}: {rs.get(key)}")
results_md.write_text("\n".join(md) + "\n")
shutil.copyfile(results_json, tmp_results_json_path)
shutil.copyfile(results_md, tmp_results_md_path)
PY

echo "[compare] results json: ${RESULTS_JSON}"
echo "[compare] results md:   ${RESULTS_MD}"
echo "[compare] tmp json:     ${TMP_RESULTS_JSON}"
echo "[compare] tmp md:       ${TMP_RESULTS_MD}"
