#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT=${1:-/tmp/leafgen_target_sweep_$(date +%Y%m%d%H%M%S)}
TARGETS_CSV=${LEAFGEN_SWEEP_TARGETS_CSV:-16777216,33554432,67108864,134217728,268435456}
KEY_COUNT=${LEAFGEN_SWEEP_KEY_COUNT:-100000}
HOT_KEY_COUNT=${LEAFGEN_SWEEP_HOT_KEY_COUNT:-25000}
ROUNDS=${LEAFGEN_SWEEP_ROUNDS:-6}
VALUE_BYTES=${LEAFGEN_SWEEP_VALUE_BYTES:-128}
PROFILE=${TREEDB_PROFILE:-bench_unsafe}
TREEMAP=(go run ./TreeDB/cmd/treemap)

mkdir -p "$OUT"
cd "$ROOT"

du_bytes() {
  if [[ -e "$1" ]]; then
    if du -sb "$1" >/dev/null 2>&1; then
      du -sb "$1" | awk '{print $1}'
    else
      du -sk "$1" | awk '{print $1 * 1024}'
    fi
  else
    echo 0
  fi
}

if (( HOT_KEY_COUNT > KEY_COUNT )); then
  echo "hot key count must be <= key count" >&2
  exit 1
fi

cat > "$OUT/leafgen_target_sweep.go" <<'EOF'
package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "time"

    treedb "github.com/snissn/gomap/TreeDB"
)

type result struct {
    DBDir       string            `json:"db_dir"`
    Profile     string            `json:"profile"`
    LeafTarget  int64             `json:"leaf_target_bytes"`
    KeyCount    int               `json:"key_count"`
    HotKeyCount int               `json:"hot_key_count"`
    Rounds      int               `json:"rounds"`
    ValueBytes  int               `json:"value_bytes"`
    ElapsedMS   int64             `json:"elapsed_ms"`
    Stats       map[string]string `json:"stats"`
}

func main() {
    var (
        dbDir       = flag.String("db", "", "application.db directory")
        profile     = flag.String("profile", string(treedb.ProfileBenchUnsafe), "profile")
        leafTarget  = flag.Int64("leaf-target", 0, "leaf_vlog segment target bytes")
        keyCount    = flag.Int("key-count", 100000, "total initial keys")
        hotKeyCount = flag.Int("hot-key-count", 25000, "keys rewritten each round")
        rounds      = flag.Int("rounds", 6, "rewrite rounds")
        valueBytes  = flag.Int("value-bytes", 128, "value size")
    )
    flag.Parse()
    if *dbDir == "" {
        fatalf("missing -db")
    }
    if *leafTarget <= 0 {
        fatalf("invalid -leaf-target %d", *leafTarget)
    }
    if *keyCount <= 0 || *hotKeyCount <= 0 || *hotKeyCount > *keyCount || *rounds <= 0 || *valueBytes <= 0 {
        fatalf("invalid workload parameters")
    }
    if err := os.MkdirAll(filepath.Dir(*dbDir), 0o755); err != nil {
        fatalf("mkdir parent: %v", err)
    }

    profileValue, ok := treedb.ParseBenchmarkProfile(*profile, treedb.ProfileBenchUnsafe)
    if !ok {
        fatalf("unsupported -profile %q; allowed: %s", *profile, treedb.BenchmarkProfileFlagHelp)
    }
    opts := treedb.OptionsForBenchmark(profileValue, *dbDir)
    opts.BackgroundCheckpointInterval = -1
    opts.BackgroundCheckpointIdleDuration = -1
    opts.MaxWALBytes = -1
    opts.BackgroundIndexVacuumInterval = -1
    opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
    opts.ValueLog.Generational.LeafSegmentTargetBytes = *leafTarget

    db, err := treedb.Open(opts)
    if err != nil {
        fatalf("open: %v", err)
    }
    defer func() {
        if err := db.Close(); err != nil {
            fatalf("close: %v", err)
        }
    }()
    db.SetMaintenancePhase(treedb.MaintenancePhaseRestore)

    start := time.Now()
    base := strings.Repeat("a", *valueBytes)
    hot := strings.Repeat("h", *valueBytes)
    cold := strings.Repeat("c", *valueBytes)
    for i := 0; i < *keyCount; i++ {
        key := []byte(fmt.Sprintf("k%08d", i))
        val := []byte(fmt.Sprintf("%s-%08d", base[:max(1, *valueBytes-9)], i))
        if len(val) < *valueBytes {
            pad := strings.Repeat("x", *valueBytes-len(val))
            val = append(val, pad...)
        } else if len(val) > *valueBytes {
            val = val[:*valueBytes]
        }
        if err := db.Set(key, val); err != nil {
            fatalf("initial set %d: %v", i, err)
        }
    }
    if err := db.Checkpoint(); err != nil {
        fatalf("checkpoint initial: %v", err)
    }
    for round := 0; round < *rounds; round++ {
        for i := 0; i < *hotKeyCount; i++ {
            key := []byte(fmt.Sprintf("k%08d", i))
            src := hot
            if round%2 == 1 {
                src = cold
            }
            val := []byte(fmt.Sprintf("%s-r%02d-k%08d", src[:max(1, *valueBytes-14)], round, i))
            if len(val) < *valueBytes {
                pad := strings.Repeat(string('a'+rune(round%26)), *valueBytes-len(val))
                val = append(val, pad...)
            } else if len(val) > *valueBytes {
                val = val[:*valueBytes]
            }
            if err := db.Set(key, val); err != nil {
                fatalf("round %d set %d: %v", round, i, err)
            }
        }
        if err := db.Checkpoint(); err != nil {
            fatalf("checkpoint round %d: %v", round, err)
        }
    }

    out := result{
        DBDir:       *dbDir,
        Profile:     *profile,
        LeafTarget:  *leafTarget,
        KeyCount:    *keyCount,
        HotKeyCount: *hotKeyCount,
        Rounds:      *rounds,
        ValueBytes:  *valueBytes,
        ElapsedMS:   time.Since(start).Milliseconds(),
        Stats:       db.Stats(),
    }
    enc := json.NewEncoder(os.Stdout)
    enc.SetIndent("", "  ")
    if err := enc.Encode(out); err != nil {
        fatalf("encode: %v", err)
    }
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func fatalf(format string, args ...any) {
    fmt.Fprintf(os.Stderr, format+"\n", args...)
    os.Exit(1)
}
EOF

IFS=',' read -r -a TARGETS <<< "$TARGETS_CSV"
RUNS_JSONL="$OUT/runs.jsonl"
: > "$RUNS_JSONL"
for target in "${TARGETS[@]}"; do
  target=$(echo "$target" | xargs)
  if [[ -z "$target" ]]; then
    continue
  fi
  label_mib=$((target / 1048576))
  run_dir="$OUT/target-${target}B"
  db_dir="$run_dir/application.db"
  mkdir -p "$run_dir"
  GOWORK=off go run "$OUT/leafgen_target_sweep.go" \
    -db "$db_dir" \
    -profile "$PROFILE" \
    -leaf-target "$target" \
    -key-count "$KEY_COUNT" \
    -hot-key-count "$HOT_KEY_COUNT" \
    -rounds "$ROUNDS" \
    -value-bytes "$VALUE_BYTES" \
    > "$run_dir/build.json"
  GOWORK=off "${TREEMAP[@]}" leafgen-plan "$db_dir" -rw -json > "$run_dir/plan.json"
  GOWORK=off "${TREEMAP[@]}" leafgen-plan "$db_dir" -rw -json -force > "$run_dir/plan-force.json"
  du_bytes "$db_dir" > "$run_dir/application_db_bytes.txt"
  du_bytes "$db_dir/maindb/leaf_vlog" > "$run_dir/leaf_vlog_bytes.txt"
  cp "$db_dir/maindb/leaf_vlog/manifest.json" "$run_dir/manifest.json"
  jq -n \
    --argjson leaf_target_bytes "$target" \
    --argjson leaf_target_mib "$label_mib" \
    --argjson application_db_bytes "$(cat "$run_dir/application_db_bytes.txt")" \
    --argjson leaf_vlog_bytes "$(cat "$run_dir/leaf_vlog_bytes.txt")" \
    --slurpfile build "$run_dir/build.json" \
    --slurpfile plan "$run_dir/plan.json" \
    --slurpfile plan_force "$run_dir/plan-force.json" \
    '{
      leaf_target_bytes: $leaf_target_bytes,
      leaf_target_mib: $leaf_target_mib,
      elapsed_ms: $build[0].elapsed_ms,
      application_db_bytes: $application_db_bytes,
      leaf_vlog_bytes: $leaf_vlog_bytes,
      commit_seq: ($build[0].stats["treedb.commit_seq"] // "0" | tonumber),
      leaf_manifest_generations: ($build[0].stats["treedb.leaf_generation.generations.total"] // "0" | tonumber),
      planner_admission: $plan[0].Admission,
      force_admission: $plan_force[0].Admission,
      candidate_generation_ids: $plan_force[0].CandidateGenerationIDs,
      candidate_bytes_to_copy: $plan_force[0].CandidateBytesToCopy,
      expected_reclaim_bytes: $plan_force[0].ExpectedReclaimBytes,
      expected_reclaim_per_byte_copied_ppm: $plan_force[0].ExpectedReclaimPerByteCopiedPPM
    }' >> "$RUNS_JSONL"
done

jq -n \
  --arg profile "$PROFILE" \
  --arg targets_csv "$TARGETS_CSV" \
  --argjson key_count "$KEY_COUNT" \
  --argjson hot_key_count "$HOT_KEY_COUNT" \
  --argjson rounds "$ROUNDS" \
  --argjson value_bytes "$VALUE_BYTES" \
  --slurpfile runs "$RUNS_JSONL" \
  '{
    profile: $profile,
    targets_csv: $targets_csv,
    workload: {
      key_count: $key_count,
      hot_key_count: $hot_key_count,
      rounds: $rounds,
      value_bytes: $value_bytes
    },
    runs: ($runs | map(. + {
      application_db_mib: ((((.application_db_bytes / 1048576.0) * 1000) | round) / 1000),
      leaf_vlog_mib: ((((.leaf_vlog_bytes / 1048576.0) * 1000) | round) / 1000)
    }))
  }' > "$OUT/summary.json"

cat <<EOM
leaf generation target sweep complete
  output: $OUT
  summary: $OUT/summary.json
EOM
