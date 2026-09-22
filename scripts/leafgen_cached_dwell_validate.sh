#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SRC=${1:?usage: leafgen_cached_dwell_validate.sh <application.db> [output-dir]}
OUT=${2:-/tmp/leafgen_cached_dwell_validate_$(date +%Y%m%d%H%M%S)}
DST="$OUT/application.db"
TREEMAP=(go run ./TreeDB/cmd/treemap)
PROFILE=${TREEDB_PROFILE:-bench_unsafe}
DWELL_SECONDS=${LEAFGEN_DWELL_SECONDS:-180}
SAMPLE_INTERVAL_SECONDS=${LEAFGEN_SAMPLE_INTERVAL_SECONDS:-15}
PACK_ENABLED=${TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE:-1}
PACK_MAX_BYTES=${TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_BYTES_TO_COPY:-268435456}
PACK_MAX_GENERATIONS=${TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_GENERATIONS:-32}
PACK_MIN_INTERVAL_MS=${TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MIN_INTERVAL_MS:-30000}
PACK_MIN_AGE_COMMITS=${TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MIN_PUBLISHED_AGE_COMMITS:-1}

mkdir -p "$OUT"
cp -a "$SRC" "$DST"
cd "$ROOT"

du_bytes() {
  if [[ -e "$1" ]]; then
    du -sb "$1" | awk '{print $1}'
  else
    echo 0
  fi
}

capture_sizes() {
  local phase=$1
  jq -n \
    --arg phase "$phase" \
    --argjson application_db_bytes "$(du_bytes "$DST")" \
    --argjson maindb_bytes "$(du_bytes "$DST/maindb")" \
    --argjson index_db_bytes "$(du_bytes "$DST/maindb/index.db")" \
    --argjson leaf_vlog_bytes "$(du_bytes "$DST/maindb/leaf_vlog")" \
    --argjson value_vlog_bytes "$(du_bytes "$DST/maindb/value_vlog")" \
    --argjson wal_bytes "$(du_bytes "$DST/wal")" \
    '{
      phase: $phase,
      application_db_bytes: $application_db_bytes,
      maindb_bytes: $maindb_bytes,
      index_db_bytes: $index_db_bytes,
      leaf_vlog_bytes: $leaf_vlog_bytes,
      value_vlog_bytes: $value_vlog_bytes,
      wal_bytes: $wal_bytes
    }'
}

{
  echo "source=$SRC"
  echo "copy=$DST"
  echo "profile=$PROFILE"
  echo "dwell_seconds=$DWELL_SECONDS"
  echo "sample_interval_seconds=$SAMPLE_INTERVAL_SECONDS"
  echo "pack_enabled=$PACK_ENABLED"
  echo "pack_max_bytes=$PACK_MAX_BYTES"
  echo "pack_max_generations=$PACK_MAX_GENERATIONS"
  echo "pack_min_interval_ms=$PACK_MIN_INTERVAL_MS"
  echo "pack_min_age_commits=$PACK_MIN_AGE_COMMITS"
} > "$OUT/run.txt"

capture_sizes before > "$OUT/size-before.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json > "$OUT/plan-before.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-before-force.json"

cat > "$OUT/leafgen_cached_dwell.go" <<'EOF'
package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/fs"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"

    treedb "github.com/snissn/gomap/TreeDB"
)

type sizeSnapshot struct {
    ApplicationDBBytes int64 `json:"application_db_bytes"`
    MainDBBytes        int64 `json:"maindb_bytes"`
    IndexDBBytes       int64 `json:"index_db_bytes"`
    LeafVLogBytes      int64 `json:"leaf_vlog_bytes"`
    ValueVLogBytes     int64 `json:"value_vlog_bytes"`
    WALBytes           int64 `json:"wal_bytes"`
}

type sample struct {
    Kind                          string            `json:"kind"`
    SampleIndex                   int               `json:"sample_index"`
    Profile                       string            `json:"profile"`
    ElapsedSeconds                float64           `json:"elapsed_seconds"`
    UnixNano                      int64             `json:"unix_nano"`
    MaintenancePhase              string            `json:"maintenance_phase,omitempty"`
    SchedulerState                string            `json:"scheduler_state,omitempty"`
    Reason                        string            `json:"reason,omitempty"`
    CommitSeq                     uint64            `json:"commit_seq,omitempty"`
    RSSKB                         uint64            `json:"rss_kb,omitempty"`
    HWMKB                         uint64            `json:"hwm_kb,omitempty"`
    Sizes                         sizeSnapshot      `json:"sizes"`
    LeafPackAttempts              uint64            `json:"leaf_pack_attempts"`
    LeafPackAdmitted              uint64            `json:"leaf_pack_admitted"`
    LeafPackRuns                  uint64            `json:"leaf_pack_runs"`
    LeafPackSkips                 uint64            `json:"leaf_pack_skips"`
    LeafPackSkipMinInt            uint64            `json:"leaf_pack_skip_min_interval"`
    LeafPackSkipWriteBurst        uint64            `json:"leaf_pack_skip_write_burst"`
    LeafPackSkipQueuePressure     uint64            `json:"leaf_pack_skip_queue_pressure"`
    LeafPackSkipForegroundIterators uint64          `json:"leaf_pack_skip_foreground_iterators"`
    LeafPackErrors                uint64            `json:"leaf_pack_errors"`
    LeafPackCanceled              uint64            `json:"leaf_pack_canceled"`
    LeafPackDeadline              uint64            `json:"leaf_pack_deadline"`
    LeafPackBytesCopied           uint64            `json:"leaf_pack_bytes_copied"`
    LeafPackExpectedBytes         uint64            `json:"leaf_pack_expected_reclaim_bytes"`
    LeafPackReclaimedBytes        uint64            `json:"leaf_pack_attributed_reclaim_bytes"`
    LeafPackReclaimPerCopyPPM     uint64            `json:"leaf_pack_attributed_reclaim_per_byte_copied_ppm"`
    LeafPackMinReclaimPerCopyPPM  uint64            `json:"leaf_pack_min_reclaim_per_byte_copied_ppm"`
    LeafPackStopLowYield          uint64            `json:"leaf_pack_stop_low_yield"`
    LeafPackPassesPeriodic        uint64            `json:"leaf_pack_passes_periodic"`
    LeafPackGCRuns                uint64            `json:"leaf_pack_gc_runs"`
    LeafPackGCEligible            uint64            `json:"leaf_pack_gc_eligible_generations"`
    LeafPackGCDeletedGen          uint64            `json:"leaf_pack_gc_deleted_generations"`
    LeafPackGCDeletedFiles        uint64            `json:"leaf_pack_gc_deleted_files"`
    LeafPackGCDeletedBytes        uint64            `json:"leaf_pack_gc_deleted_bytes"`
    LeafPackLastWallMS            string            `json:"leaf_pack_last_wall_ms,omitempty"`
    LeafPackLastSkipReason        string            `json:"leaf_pack_last_skip_reason,omitempty"`
    LeafPackLastGenCount          uint64            `json:"leaf_pack_last_selection_generations"`
    LeafPackLastCopyBytes         uint64            `json:"leaf_pack_last_bytes_copied"`
    LeafPackLastReclaimedBytes    uint64            `json:"leaf_pack_last_attributed_reclaim_bytes"`
    LeafPackLastReclaimPerCopyPPM uint64            `json:"leaf_pack_last_attributed_reclaim_per_byte_copied_ppm"`
    LeafPackWriteBurstGraceMS     uint64            `json:"leaf_pack_write_burst_grace_ms"`
    LeafPackMaxForegroundQueue    uint64            `json:"leaf_pack_max_foreground_queue"`
    Stats                         map[string]string `json:"stats,omitempty"`
}

func main() {
    var (
        dbDir         = flag.String("db", "", "application.db directory")
        profile       = flag.String("profile", string(treedb.ProfileBenchUnsafe), "TreeDB profile")
        dwellSeconds  = flag.Int("dwell-seconds", 180, "how long to keep the DB open")
        sampleSeconds = flag.Int("sample-interval-seconds", 15, "sample interval")
        emitFullStats = flag.Bool("full-stats", false, "embed the full Stats map into each sample")
    )
    flag.Parse()
    if *dbDir == "" {
        fatalf("missing -db")
    }
    if *dwellSeconds < 0 {
        fatalf("invalid -dwell-seconds %d", *dwellSeconds)
    }
    if *sampleSeconds <= 0 {
        fatalf("invalid -sample-interval-seconds %d", *sampleSeconds)
    }

    profileValue, ok := treedb.ParseBenchmarkProfile(*profile, treedb.ProfileBenchUnsafe)
    if !ok {
        fatalf("unsupported -profile %q; allowed: %s", *profile, treedb.BenchmarkProfileFlagHelp)
    }
    opts := treedb.OptionsForBenchmark(profileValue, *dbDir)
    if opts.ValueLog.Generational.Policy == treedb.ValueLogGenerationDefault {
        opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
    }
    opts.NotifyError = func(err error) {
        fmt.Fprintf(os.Stderr, "notify_error: %v\n", err)
    }
    db, err := treedb.Open(opts)
    if err != nil {
        fatalf("open: %v", err)
    }
    defer func() {
        if err := db.Close(); err != nil {
            fatalf("close: %v", err)
        }
    }()
    db.SetMaintenancePhase(treedb.MaintenancePhaseSteady)

    start := time.Now()
    enc := json.NewEncoder(os.Stdout)
    enc.SetEscapeHTML(false)
    emit := func(i int) error {
        stats := db.Stats()
        out := sample{
            Kind:                  "sample",
            SampleIndex:           i,
            Profile:               *profile,
            ElapsedSeconds:        time.Since(start).Seconds(),
            UnixNano:              time.Now().UnixNano(),
            MaintenancePhase:      stats["treedb.cache.vlog_generation.maintenance_phase"],
            SchedulerState:        stats["treedb.cache.vlog_generation.scheduler_state"],
            Reason:                stats["treedb.cache.vlog_generation.reason"],
            CommitSeq:             parseUint(stats["treedb.commit_seq"]),
            RSSKB:                 readProcStatusKB("VmRSS"),
            HWMKB:                 readProcStatusKB("VmHWM"),
            Sizes:                         captureSizes(*dbDir),
            LeafPackAttempts:              parseUint(stats["treedb.cache.vlog_generation.leaf_pack.attempts"]),
            LeafPackAdmitted:              parseUint(stats["treedb.cache.vlog_generation.leaf_pack.admitted"]),
            LeafPackRuns:                  parseUint(stats["treedb.cache.vlog_generation.leaf_pack.runs"]),
            LeafPackSkips:                 parseUint(stats["treedb.cache.vlog_generation.leaf_pack.skips"]),
            LeafPackSkipMinInt:            parseUint(stats["treedb.cache.vlog_generation.leaf_pack.skip.min_interval"]),
            LeafPackSkipWriteBurst:        parseUint(stats["treedb.cache.vlog_generation.leaf_pack.skip.write_burst"]),
            LeafPackSkipQueuePressure:     parseUint(stats["treedb.cache.vlog_generation.leaf_pack.skip.queue_pressure"]),
            LeafPackSkipForegroundIterators: parseUint(stats["treedb.cache.vlog_generation.leaf_pack.skip.foreground_iterators"]),
            LeafPackErrors:                parseUint(stats["treedb.cache.vlog_generation.leaf_pack.errors"]),
            LeafPackCanceled:              parseUint(stats["treedb.cache.vlog_generation.leaf_pack.canceled"]),
            LeafPackDeadline:              parseUint(stats["treedb.cache.vlog_generation.leaf_pack.deadline"]),
            LeafPackBytesCopied:           parseUint(stats["treedb.cache.vlog_generation.leaf_pack.bytes_copied"]),
            LeafPackExpectedBytes:         parseUint(stats["treedb.cache.vlog_generation.leaf_pack.expected_reclaim_bytes"]),
            LeafPackReclaimedBytes:        parseUint(stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_bytes"]),
            LeafPackReclaimPerCopyPPM:     parseUint(stats["treedb.cache.vlog_generation.leaf_pack.attributed_reclaim_per_byte_copied_ppm"]),
            LeafPackMinReclaimPerCopyPPM:  parseUint(stats["treedb.cache.vlog_generation.leaf_pack.min_reclaim_per_byte_copied_ppm"]),
            LeafPackStopLowYield:          parseUint(stats["treedb.cache.vlog_generation.leaf_pack.stop.low_yield"]),
            LeafPackPassesPeriodic:        parseUint(stats["treedb.cache.vlog_generation.maintenance.passes.with_leaf_pack.source.periodic"]),
            LeafPackGCRuns:                parseUint(stats["treedb.cache.vlog_generation.leaf_pack.gc.runs"]),
            LeafPackGCEligible:            parseUint(stats["treedb.cache.vlog_generation.leaf_pack.gc.eligible_generations"]),
            LeafPackGCDeletedGen:          parseUint(stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_generations"]),
            LeafPackGCDeletedFiles:        parseUint(stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_files"]),
            LeafPackGCDeletedBytes:        parseUint(stats["treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes"]),
            LeafPackLastWallMS:            stats["treedb.cache.vlog_generation.leaf_pack.last_wall_ms"],
            LeafPackLastSkipReason:        stats["treedb.cache.vlog_generation.leaf_pack.last_skip_reason"],
            LeafPackLastGenCount:          parseUint(stats["treedb.cache.vlog_generation.leaf_pack.last_selection.generations"]),
            LeafPackLastCopyBytes:         parseUint(stats["treedb.cache.vlog_generation.leaf_pack.last_bytes_copied"]),
            LeafPackLastReclaimedBytes:    parseUint(stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_bytes"]),
            LeafPackLastReclaimPerCopyPPM: parseUint(stats["treedb.cache.vlog_generation.leaf_pack.last_attributed_reclaim_per_byte_copied_ppm"]),
            LeafPackWriteBurstGraceMS:     parseUint(stats["treedb.cache.vlog_generation.leaf_pack.write_burst_grace_ms"]),
            LeafPackMaxForegroundQueue:    parseUint(stats["treedb.cache.vlog_generation.leaf_pack.max_foreground_queue"]),
        }
        if *emitFullStats {
            out.Stats = stats
        }
        return enc.Encode(out)
    }

    if err := emit(0); err != nil {
        fatalf("emit sample 0: %v", err)
    }
    deadline := start.Add(time.Duration(*dwellSeconds) * time.Second)
    ticker := time.NewTicker(time.Duration(*sampleSeconds) * time.Second)
    defer ticker.Stop()
    i := 1
    for {
        now := time.Now()
        if !now.Before(deadline) {
            break
        }
        wait := time.Until(deadline)
        select {
        case <-ticker.C:
            if err := emit(i); err != nil {
                fatalf("emit sample %d: %v", i, err)
            }
            i++
        case <-time.After(wait):
        }
    }
    if err := emit(i); err != nil {
        fatalf("emit final sample: %v", err)
    }
}

func captureSizes(root string) sizeSnapshot {
    return sizeSnapshot{
        ApplicationDBBytes: pathBytes(root),
        MainDBBytes:        pathBytes(filepath.Join(root, "maindb")),
        IndexDBBytes:       pathBytes(filepath.Join(root, "maindb", "index.db")),
        LeafVLogBytes:      pathBytes(filepath.Join(root, "maindb", "leaf_vlog")),
        ValueVLogBytes:     pathBytes(filepath.Join(root, "maindb", "value_vlog")),
        WALBytes:           pathBytes(filepath.Join(root, "wal")),
    }
}

func pathBytes(path string) int64 {
    info, err := os.Lstat(path)
    if err != nil {
        return 0
    }
    if !info.IsDir() {
        return info.Size()
    }
    var total int64
    _ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
        if err != nil || d.IsDir() {
            return nil
        }
        info, statErr := d.Info()
        if statErr != nil {
            return nil
        }
        total += info.Size()
        return nil
    })
    return total
}

func readProcStatusKB(key string) uint64 {
    data, err := os.ReadFile("/proc/self/status")
    if err != nil {
        return 0
    }
    prefix := key + ":"
    for _, line := range strings.Split(string(data), "\n") {
        if !strings.HasPrefix(line, prefix) {
            continue
        }
        fields := strings.Fields(line)
        if len(fields) < 2 {
            return 0
        }
        return parseUint(fields[1])
    }
    return 0
}

func parseUint(s string) uint64 {
    if s == "" {
        return 0
    }
    v, err := strconv.ParseUint(s, 10, 64)
    if err == nil {
        return v
    }
    if i, err := strconv.ParseInt(s, 10, 64); err == nil && i > 0 {
        return uint64(i)
    }
    return 0
}

func fatalf(format string, args ...any) {
    fmt.Fprintf(os.Stderr, format+"\n", args...)
    os.Exit(1)
}
EOF

TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE="$PACK_ENABLED" \
TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_BYTES_TO_COPY="$PACK_MAX_BYTES" \
TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_GENERATIONS="$PACK_MAX_GENERATIONS" \
TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MIN_INTERVAL_MS="$PACK_MIN_INTERVAL_MS" \
TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MIN_PUBLISHED_AGE_COMMITS="$PACK_MIN_AGE_COMMITS" \
GOWORK=off go run "$OUT/leafgen_cached_dwell.go" \
  -db "$DST" \
  -profile "$PROFILE" \
  -dwell-seconds "$DWELL_SECONDS" \
  -sample-interval-seconds "$SAMPLE_INTERVAL_SECONDS" \
  > "$OUT/dwell.jsonl"

capture_sizes after_dwell > "$OUT/size-after-dwell.json"
jq -s '.' "$OUT/dwell.jsonl" > "$OUT/dwell-array.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json > "$OUT/plan-after.json"
GOWORK=off "${TREEMAP[@]}" leafgen-plan "$DST" -rw -json -force > "$OUT/plan-after-force.json"
ls -lh "$DST/maindb/leaf_vlog" > "$OUT/leaf-vlog-ls.txt"

jq -n \
  --arg source "$SRC" \
  --arg copy "$DST" \
  --arg profile "$PROFILE" \
  --argjson dwell_seconds "$DWELL_SECONDS" \
  --argjson sample_interval_seconds "$SAMPLE_INTERVAL_SECONDS" \
  --argjson pack_enabled "$PACK_ENABLED" \
  --argjson pack_max_bytes "$PACK_MAX_BYTES" \
  --argjson pack_max_generations "$PACK_MAX_GENERATIONS" \
  --argjson pack_min_interval_ms "$PACK_MIN_INTERVAL_MS" \
  --argjson pack_min_age_commits "$PACK_MIN_AGE_COMMITS" \
  --slurpfile before "$OUT/size-before.json" \
  --slurpfile after_dwell "$OUT/size-after-dwell.json" \
  --slurpfile dwell "$OUT/dwell-array.json" \
  --slurpfile plan_before "$OUT/plan-before.json" \
  --slurpfile plan_before_force "$OUT/plan-before-force.json" \
  --slurpfile plan_after "$OUT/plan-after.json" \
  --slurpfile plan_after_force "$OUT/plan-after-force.json" \
  '{
    source: $source,
    copy: $copy,
    profile: $profile,
    dwell_seconds: $dwell_seconds,
    sample_interval_seconds: $sample_interval_seconds,
    pack_maintenance: {
      enabled: ($pack_enabled != 0),
      max_bytes_to_copy: $pack_max_bytes,
      max_generations: $pack_max_generations,
      min_interval_ms: $pack_min_interval_ms,
      min_published_age_commits: $pack_min_age_commits
    },
    sizes: {
      before: $before[0],
      after_dwell: $after_dwell[0]
    },
    sample_count: ($dwell[0] | length),
    max_rss_kb: (($dwell[0] | map(.rss_kb // 0) | max) // 0),
    max_hwm_kb: (($dwell[0] | map(.hwm_kb // 0) | max) // 0),
    final_sample: (($dwell[0] | last) // null),
    leaf_pack_totals: {
      admitted: ((($dwell[0] | last) // {}) | .leaf_pack_admitted // 0),
      runs: ((($dwell[0] | last) // {}) | .leaf_pack_runs // 0),
      skip_write_burst: ((($dwell[0] | last) // {}) | .leaf_pack_skip_write_burst // 0),
      skip_queue_pressure: ((($dwell[0] | last) // {}) | .leaf_pack_skip_queue_pressure // 0),
      skip_foreground_iterators: ((($dwell[0] | last) // {}) | .leaf_pack_skip_foreground_iterators // 0),
      bytes_copied: ((($dwell[0] | last) // {}) | .leaf_pack_bytes_copied // 0),
      expected_reclaim_bytes: ((($dwell[0] | last) // {}) | .leaf_pack_expected_reclaim_bytes // 0),
      reclaimed_bytes: ((($dwell[0] | last) // {}) | .leaf_pack_attributed_reclaim_bytes // 0),
      reclaim_per_byte_copied_ppm: ((($dwell[0] | last) // {}) | .leaf_pack_attributed_reclaim_per_byte_copied_ppm // 0),
      min_reclaim_per_byte_copied_ppm: ((($dwell[0] | last) // {}) | .leaf_pack_min_reclaim_per_byte_copied_ppm // 0),
      stop_low_yield: ((($dwell[0] | last) // {}) | .leaf_pack_stop_low_yield // 0),
      gc_runs: ((($dwell[0] | last) // {}) | .leaf_pack_gc_runs // 0),
      gc_deleted_generations: ((($dwell[0] | last) // {}) | .leaf_pack_gc_deleted_generations // 0),
      gc_deleted_files: ((($dwell[0] | last) // {}) | .leaf_pack_gc_deleted_files // 0),
      gc_deleted_bytes: ((($dwell[0] | last) // {}) | .leaf_pack_gc_deleted_bytes // 0),
      last_reclaimed_bytes: ((($dwell[0] | last) // {}) | .leaf_pack_last_attributed_reclaim_bytes // 0),
      last_reclaim_per_byte_copied_ppm: ((($dwell[0] | last) // {}) | .leaf_pack_last_attributed_reclaim_per_byte_copied_ppm // 0),
      write_burst_grace_ms: ((($dwell[0] | last) // {}) | .leaf_pack_write_burst_grace_ms // 0),
      max_foreground_queue: ((($dwell[0] | last) // {}) | .leaf_pack_max_foreground_queue // 0)
    },
    plan_before: {
      default: {
        admission: $plan_before[0].Admission,
        current_generation_id: $plan_before[0].CurrentGenerationID,
        candidate_generation_ids: $plan_before[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_before[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_before[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_before[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_before[0].ExpectedReclaimPerByteCopiedPPM
      },
      force: {
        admission: $plan_before_force[0].Admission,
        current_generation_id: $plan_before_force[0].CurrentGenerationID,
        candidate_generation_ids: $plan_before_force[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_before_force[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_before_force[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_before_force[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_before_force[0].ExpectedReclaimPerByteCopiedPPM
      }
    },
    plan_after: {
      default: {
        admission: $plan_after[0].Admission,
        current_generation_id: $plan_after[0].CurrentGenerationID,
        candidate_generation_ids: $plan_after[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_after[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_after[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_after[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_after[0].ExpectedReclaimPerByteCopiedPPM
      },
      force: {
        admission: $plan_after_force[0].Admission,
        current_generation_id: $plan_after_force[0].CurrentGenerationID,
        candidate_generation_ids: $plan_after_force[0].CandidateGenerationIDs,
        expected_reclaim_bytes: $plan_after_force[0].ExpectedReclaimBytes,
        candidate_bytes_to_copy: $plan_after_force[0].CandidateBytesToCopy,
        expected_reclaim_ratio_ppm: $plan_after_force[0].ExpectedReclaimRatioPPM,
        expected_reclaim_per_byte_copied_ppm: $plan_after_force[0].ExpectedReclaimPerByteCopiedPPM
      }
    }
  }' > "$OUT/summary.json"

cat <<EOM
leafgen cached dwell validation complete
  output: $OUT
  source: $SRC
  copy: $DST
  profile: $PROFILE
  dwell.jsonl: $OUT/dwell.jsonl
  summary: $OUT/summary.json
EOM
