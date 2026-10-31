#!/usr/bin/env bash
set -euo pipefail
case "${ROLE:?literal workflow role required}" in populate) expected_hit=false ;; restore) expected_hit=true ;; *) exit 91 ;; esac
evidence="$RUNNER_TEMP/5342-$ROLE"
mkdir -p "$evidence/tmp"
export TMPDIR="$evidence/tmp"
printf 'role=%s\ncache_hit=%s\nexpected_hit=%s\n' "$ROLE" "${SETUP_GO_CACHE_HIT-unknown}" "$expected_hit" > "$evidence/cache-expectation.txt"
result=0
if [[ ${SETUP_GO_CACHE_HIT-unknown} != "$expected_hit" ]]; then result=1; fi
for phase in before after; do
  if timeout --kill-after=1s 5s bash ./scripts/observe_read_snapshot_cache.sh > "$evidence/observation-$phase.log" 2>&1; then rc=0; else rc=$?; fi
  printf 'exit=%s\n' "$rc" > "$evidence/observation-$phase.exit"
  cat "$evidence/observation-$phase.log"
  if [[ $rc != 0 ]]; then result=1; fi
  if [[ "$phase" == after || "$ROLE" == restore ]]; then
    for wanted in 'extracted_module_dir=present' 'zip=present bytes=6403' 'ziphash=present bytes=47'; do
      if ! grep -F -x -q "[cache-observation] $wanted" "$evidence/observation-$phase.log"; then result=1; fi
    done
  fi
  if [[ "$phase" == before ]]; then
    printf '%s\n' 'bash ./scripts/check_read_snapshot_guardrail.sh (unchanged public wrapper)' > "$evidence/guardrail.command"
    set +e
    bash ./scripts/check_read_snapshot_guardrail.sh 2>&1 | tee "$evidence/guardrail.log"
    statuses=("${PIPESTATUS[@]}")
    set -e
    printf 'guardrail=%s\ntee=%s\n' "${statuses[0]}" "${statuses[1]}" > "$evidence/guardrail.exit"
    if [[ ${statuses[0]} != 0 || ${statuses[1]} != 0 ]]; then result=1; fi
  fi
done
printf 'role_result=%s\n' "$result" > "$evidence/role.exit"
exit "$result"
