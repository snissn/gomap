#!/usr/bin/env bash
set -euo pipefail
# Execute the unchanged wrapper with synthetic Go output, never real Go.
original="${1:?original guardrail path required}"
tmp="$(mktemp -d "${TMPDIR:?task-owned TMPDIR required}/5342-wrapper.XXXXXXXX")"
printf 'retained_control_dir=%s\n' "$tmp"
for scenario in pass setup_fail_then_pass both_fail missing_target false_pass_then_real; do
  case_dir="$tmp/$scenario"
  mkdir -p "$case_dir/repo/scripts" "$case_dir/repo/cmd/unified_bench" "$case_dir/bin" "$case_dir/tmp"
  cp "$original" "$case_dir/repo/scripts/check_read_snapshot_guardrail.sh"
  cat > "$case_dir/bin/go" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
count=0
if [[ -f "$FIXTURE_STATE" ]]; then read -r count < "$FIXTURE_STATE"; fi
count=$((count+1))
printf '%s\n' "$count" > "$FIXTURE_STATE"
if [[ "$*" != 'test -json -p 1 . -run ^TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail$ -count=1' ]]; then
  echo 'unexpected Go command' >&2
  exit 91
fi
emit_pass() { printf '%s\n' '{"Action":"pass","Test":"TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail"}'; }
case "$FIXTURE_SCENARIO" in
  pass) emit_pass ;;
  setup_fail_then_pass)
    if [[ $count == 1 ]]; then echo 'synthetic dependency acquisition error' >&2; exit 7; else emit_pass; fi ;;
  both_fail) echo 'synthetic persistent setup error' >&2; exit 8 ;;
  missing_target) printf '%s\n' '{"Action":"pass","Test":"UnrelatedTest"}' ;;
  false_pass_then_real)
    emit_pass
    if [[ $count == 1 ]]; then exit 9; fi ;;
esac
STUB
  chmod 755 "$case_dir/bin/go"
  if env PATH="$case_dir/bin:$PATH" TMPDIR="$case_dir/tmp" FIXTURE_STATE="$case_dir/count" FIXTURE_SCENARIO="$scenario" \
      bash "$case_dir/repo/scripts/check_read_snapshot_guardrail.sh" > "$case_dir/output.log" 2>&1; then rc=0; else rc=$?; fi
  case "$scenario" in
    pass) expected=0; calls=1 ;;
    setup_fail_then_pass|false_pass_then_real) expected=4; calls=2 ;;
    both_fail|missing_target) expected=5; calls=2 ;;
  esac
  actual_calls="$(cat "$case_dir/count")"
  printf '%s exit=%s expected=%s calls=%s expected_calls=%s\n' "$scenario" "$rc" "$expected" "$actual_calls" "$calls"
  [[ "$rc" == "$expected" && "$actual_calls" == "$calls" ]]
  case "$scenario" in
    setup_fail_then_pass) grep -F -q 'synthetic dependency acquisition error' "$case_dir/output.log" ;;
    both_fail) grep -F -q 'synthetic persistent setup error' "$case_dir/output.log" ;;
    missing_target) grep -F -q 'did not run' "$case_dir/output.log" ;;
  esac
done
printf 'PASS: original wrapper preserves Go failures, named witness, call counts and exits 4/5\n'
