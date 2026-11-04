#!/usr/bin/env bash
# Fixed original six-pair script, diagnostic opt-in only; never acceptance evidence.
set -euo pipefail
cd "$GITHUB_WORKSPACE"
test "$(git rev-parse HEAD)" = "$EXPECTED_HARNESS_HEAD"
test -z "$(git status --porcelain)"
python3 -B - <<'CONTEXT'
import hashlib,json,os,platform,shutil
from pathlib import Path
out=Path(os.environ['CAPTURE_OUT'])
context={'schema':1,'platform':platform.platform(),'python':platform.python_version(),
 'affinity':sorted(os.sched_getaffinity(0)),
 'environment':{k:os.environ.get(k) for k in ['GOMAXPROCS','GOMEMLIMIT','GOGC','GODEBUG','GOFLAGS','GOEXPERIMENT','GOWORK','GOTOOLCHAIN','ImageOS','ImageVersion','RUNNER_OS','RUNNER_ARCH']},'tools':{}}
for name in ['go','python3','bash','timeout','git']:
 p=Path(shutil.which(name)).resolve();context['tools'][name]={'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
(out/'runtime-context.json').write_text(json.dumps(context,indent=2,sort_keys=True)+'\n')
CONTEXT
timeout --signal=TERM --kill-after=5s 10s go version > "$CAPTURE_OUT/go-version.txt" 2>&1
test "$(timeout --signal=TERM --kill-after=5s 10s go env GOVERSION)" = go1.26.10
timeout --signal=TERM --kill-after=5s 10s go env -json GOVERSION GOOS GOARCH GOAMD64 CGO_ENABLED CC CXX GOFLAGS GOEXPERIMENT GOROOT GOCACHE GOMODCACHE GOTOOLCHAIN GOWORK > "$CAPTURE_OUT/go-env.json"
python3 -B - <<'BUILD_CONTEXT'
import hashlib,json,os,shlex,shutil
from pathlib import Path
root=Path(os.environ['CAPTURE_OUT']);env=json.loads((root/'go-env.json').read_text());rows={}
for name in ['CC','CXX']:
 command=shlex.split(env.get(name,''))
 actual=shutil.which(command[0]) if command else None
 if actual:
  p=Path(actual).resolve();rows[name]={'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
 else:
  rows[name]={'unavailable':True,'configured':env.get(name)}
for name in ['compile','link','asm']:
 p=Path(env['GOROOT'])/'pkg/tool'/('{}_{}'.format(env['GOOS'],env['GOARCH']))/name
 rows[name]={'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
(root/'build-tools.json').write_text(json.dumps(rows,indent=2,sort_keys=True)+'\n')
BUILD_CONTEXT
cd "$CAPTURE_OUT"
sample_count=6
if ((sample_count % 2 != 0)); then
  echo "sample_count must be even to balance off/auto row order"
  exit 2
fi

mapfile -t cpu_models < <(
  sed -n 's/^model name[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo | sort -u
)
cpu_model="unknown"
if ((${#cpu_models[@]} == 1)); then
  cpu_model="${cpu_models[0]}"
elif ((${#cpu_models[@]} > 1)); then
  cpu_model="ambiguous: ${cpu_models[*]}"
fi
min_cpu_efficiency_frac="0.95"
if [[ "${cpu_model}" == "AMD EPYC 7763 64-Core Processor" ]]; then
  min_cpu_efficiency_frac="0.93"
elif [[ "${cpu_model}" == "AMD EPYC 9V74 80-Core Processor" ]]; then
  # Fifteen recent hosted aggregates on this exact model ranged
  # 0.9396-1.0026 (geomean 0.9698). Keep a one-point margin below
  # that observed low while rejecting a material seven-percent CPU
  # efficiency regression.
  min_cpu_efficiency_frac="0.93"
fi

# This gate measures the explicit bench_unsafe compression ceiling,
# not the command_wal_durable default used by production callers.
# Keep it independent of the unconfigured flush-admission default
# under test in #2788. Force admission off for both rows so the
# settled auto/off comparison stays on the old serial/default-off
# flush boundary and measures the deferred value-log and leaf-log
# work rather than span-native admission changes. The checker sums
# the total= fields from value_vlog plus leaf_vlog and verifies the
# intended raw-user and leaf-compression modes independently of the
# throughput bound.
run_vlog_row() {
  local db="$1"
  local output="$2"
  local cpu_profile_prefix="$3"
  if GOMAP_DIAGNOSTIC_STEADY_RUSAGE="${cpu_profile_prefix}.rusage.json" go -C "$GITHUB_WORKSPACE" run ./cmd/unified_bench \
    -profile fast \
    -keys 1000000 \
    -valsize 512 \
    -batchsize 1024 \
    -dbs "${db}" \
    -test batch_write_steady \
    -cpuprofile "${cpu_profile_prefix}" \
    -cpuprofile-tests batch_write_steady \
    -val-pattern random \
    -format markdown \
    -progress=false \
    -keep=false \
    -seed 1 \
    -treedb-force-value-pointers=true \
    -treedb-value-log-threshold=1 \
    -treedb-vlog-dict-train-bytes=-1 \
    -treedb-flush-admission-policy=off \
    > "${output}" 2>&1; then
    printf '%s\t0\n' "$db:$cpu_profile_prefix" >> row-exits.tsv
  else
    row_status=$?
    printf '%s\t%s\n' "$db:$cpu_profile_prefix" "$row_status" >> row-exits.tsv
    echo "${db} benchmark failed"
    tail -n 80 "${output}" || true
    return 1
  fi
}

off_logs=()
auto_logs=()
off_cpu_profiles=()
auto_cpu_profiles=()
benchmark_failures=()
record_vlog_row() {
  local db="$1"
  local output="$2"
  local cpu_profile_prefix="$3"
  if ! run_vlog_row "${db}" "${output}" "${cpu_profile_prefix}"; then
    benchmark_failures+=("${db}:${output}")
  fi
}
for sample in $(seq 1 "${sample_count}"); do
  off_log="$CAPTURE_OUT/vlog_auto_incompressible_off_sample${sample}.txt"
  auto_log="$CAPTURE_OUT/vlog_auto_incompressible_auto_sample${sample}.txt"
  off_cpu_profile_prefix="$CAPTURE_OUT/vlog_auto_incompressible_off_sample${sample}"
  auto_cpu_profile_prefix="$CAPTURE_OUT/vlog_auto_incompressible_auto_sample${sample}"
  off_cpu_profile="${off_cpu_profile_prefix}_batch_write_steady_treedb_vlog_off.pprof"
  auto_cpu_profile="${auto_cpu_profile_prefix}_batch_write_steady_treedb_vlog_auto.pprof"
  off_logs+=("${off_log}")
  auto_logs+=("${auto_log}")
  off_cpu_profiles+=("${off_cpu_profile}")
  auto_cpu_profiles+=("${auto_cpu_profile}")
  echo "incompressible gate sample ${sample}/${sample_count}"

  if ((sample % 2 == 1)); then
    echo "sample ${sample} order=off-auto"
    record_vlog_row treedb_vlog_off "${off_log}" "${off_cpu_profile_prefix}"
    record_vlog_row treedb_vlog_auto "${auto_log}" "${auto_cpu_profile_prefix}"
  else
    echo "sample ${sample} order=auto-off"
    record_vlog_row treedb_vlog_auto "${auto_log}" "${auto_cpu_profile_prefix}"
    record_vlog_row treedb_vlog_off "${off_log}" "${off_cpu_profile_prefix}"
  fi
done

checker_args=()
for sample in $(seq 0 $((sample_count - 1))); do
  checker_args+=( -off-log "${off_logs[$sample]}" -auto-log "${auto_logs[$sample]}" )
  checker_args+=( -off-cpu-profile "${off_cpu_profiles[$sample]}" -auto-cpu-profile "${auto_cpu_profiles[$sample]}" )
done

# #3861 makes this a settled non-regression and persistent-storage
# tradeoff gate; #3863 uses exact-function CPU samples so hosted runner
# descheduling remains visible in raw wall throughput without becoming
# the blocking product-work metric.
# EPYC 7763 and EPYC 9V74 use 0.93x, and every other or unknown CPU uses 0.95x.
# One lucky sample cannot override a mostly
# failing sample set, and every pair retains the 1.02x storage ceiling.
summary_log="vlog_auto_incompressible_summary.txt"
{
  echo "incompressible gate cpu_model=${cpu_model}"
  echo "incompressible gate min_cpu_efficiency_frac=${min_cpu_efficiency_frac}"
  echo "incompressible gate benchmark_failures=${#benchmark_failures[@]}"
  if ((${#benchmark_failures[@]} > 0)); then
    printf 'incompressible gate benchmark_failure=%s\n' "${benchmark_failures[@]}"
  fi
} > "${summary_log}"
# Materialize only the stock standard tool selected by this Go installation.
# Its actual path/hash/buildinfo is retained before checker/postprocessing use.
timeout --signal=TERM --kill-after=5s 60s go tool -n pprof > pprof-tool-path.txt
pprof_tool="$(cat pprof-tool-path.txt)"
test -f "$pprof_tool"
sha256sum "$pprof_tool" > pprof-tool-sha256.txt
timeout --signal=TERM --kill-after=5s 10s go version -m "$pprof_tool" > pprof-tool-buildinfo.txt
set +e
go -C "$GITHUB_WORKSPACE" run .github/scripts/check_vlog_auto_incompressible.go \
  "${checker_args[@]}" \
  -min-samples "${sample_count}" \
  -min-cpu-efficiency-frac "${min_cpu_efficiency_frac}" \
  -min-cpu-seconds 0.25 \
  -max-size-frac 1.02 \
  2>&1 | tee -a "${summary_log}"
checker_status=${PIPESTATUS[0]}
set -e

{
  echo "## Incompressible value-log aggregate"
  echo '```text'
  cat "${summary_log}"
  echo '```'
} >> "${GITHUB_STEP_SUMMARY}"
printf '%s\n' "$checker_status" > checker.exit
view_failures=0
for sample in $(seq 1 "$sample_count"); do
  for mode in off auto; do
    profile="vlog_auto_incompressible_${mode}_sample${sample}_batch_write_steady_treedb_vlog_${mode}.pprof"
    set +e
    timeout --signal=TERM --kill-after=5s 15s go tool pprof -top -unit=seconds -nodecount=0 -symbolize=none "$profile" > "${profile}.top.txt" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" > "${profile}.top.exit"
    if ((status != 0)); then view_failures=$((view_failures + 1)); fi
    printf '\n=== ORIGINAL PROFILE %s ===\n' "$profile"
    cat "${profile}.top.txt"
  done
done
set +e
python3 -B "$GITHUB_WORKSPACE/.github/scripts/steady_rusage_receipts.py" "$CAPTURE_OUT" > process-usage-validation.json 2> process-usage-validation.stderr
receipt_status=$?
set -e
printf '%s\n' "$receipt_status" > process-usage-validation.exit
cat process-usage-validation.json
if ((${#benchmark_failures[@]} > 0)); then exit 1; fi
if ((checker_status != 0)); then exit "$checker_status"; fi
if ((view_failures != 0 || receipt_status != 0)); then exit 1; fi
