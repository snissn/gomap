#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-./bin/unified-bench}"

CPU_PPROF="${1:-/tmp/review_cpu_batch_write_treedb.pprof}"
MUTEX_PPROF="${2:-/tmp/mutex}"
BLOCK_PPROF="${3:-/tmp/review_block}"
TRACE_FILE="${4:-/tmp/trace}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing $1" >&2; exit 2; }
}

need go
need awk
need head
need uname
need date

if [[ ! -x "$BIN" ]]; then
  echo "ERROR: unified-bench not executable: $BIN" >&2
  exit 2
fi

NOW="$(date '+%Y-%m-%dT%H:%M:%S%z')"

echo "=============================="
echo " TreeDB perf profile review"
echo "=============================="
echo "Date:        $NOW"
echo "OS:          $(uname -a)"
echo "Binary:      $BIN"
echo "CPU pprof:   $CPU_PPROF"
echo "Mutex pprof: $MUTEX_PPROF"
echo "Block pprof: $BLOCK_PPROF"
echo "Trace:       $TRACE_FILE"
echo


make unified-bench
time $BIN -test batch_write -dbs treedb -profile fast -keys 2000000 -format markdown -blockprofile /tmp/review_block -cpuprofile /tmp/review_cpu -mutexprofile /tmp/mutex -trace /tmp/trace

echo "---- System context (best-effort) ----"
if command -v sysctl >/dev/null 2>&1; then
  sysctl -n hw.ncpu 2>/dev/null | awk '{print "hw.ncpu:     " $0}'
  sysctl -n hw.memsize 2>/dev/null | awk '{print "hw.memsize:  " $0}'
fi
echo "GOMAXPROCS:  ${GOMAXPROCS:-"(unset)"}"
echo

pprof_top() {
  # args: <label> <pprof_mode_flag_or_empty> <file>
  local label="$1"
  local modeflag="$2"
  local file="$3"

  if [[ ! -f "$file" ]]; then
    echo "---- $label ----"
    echo "MISSING: $file"
    echo
    return 0
  fi

  echo "---- $label: top (cum) ----"
  if [[ -n "$modeflag" ]]; then
    go tool pprof "$modeflag" -top -cum "$BIN" "$file" 2>/dev/null | head -n 80 || true
  else
    go tool pprof -top -cum "$BIN" "$file" 2>/dev/null | head -n 80 || true
  fi
  echo

  echo "---- $label: top (flat) ----"
  if [[ -n "$modeflag" ]]; then
    go tool pprof "$modeflag" -top "$BIN" "$file" 2>/dev/null | head -n 80 || true
  else
    go tool pprof -top "$BIN" "$file" 2>/dev/null | head -n 80 || true
  fi
  echo
}

pprof_top "CPU PROFILE" "" "$CPU_PPROF"
pprof_top "MUTEX PROFILE" "" "$MUTEX_PPROF"
pprof_top "BLOCK PROFILE" "" "$BLOCK_PPROF"

escape_pprof_regex() {
  local s="$1"
  # Escape regex metacharacters so pprof -list treats it as a literal.
  s="${s//\\/\\\\}"
  s="${s//\./\\\.}"
  s="${s//\*/\\*}"
  s="${s//\+/\\+}"
  s="${s//\?/\\?}"
  s="${s//\[/\\[}"
  s="${s//\]/\\]}"
  s="${s//\(/\\(}"
  s="${s//\)/\\)}"
  s="${s//\{/\\{}"
  s="${s//\}/\\}}"
  s="${s//\^/\\^}"
  s="${s//\$/\\$}"
  s="${s//\|/\\|}"
  echo "$s"
}

pprof_top_symbols() {
  local file="$1"
  local max="${2:-10}"
  go tool pprof -top -cum "$BIN" "$file" 2>/dev/null \
    | awk -v max="$max" 'BEGIN{n=0}
      /^[[:space:]]*[0-9]+[[:space:]]/{
        name=$NF
        if (name != "" && name != "(inline)") {
          print name
          n++
        }
        if (n>=max) exit
      }'
}

echo "---- CPU hot symbols (suggested for pprof -list) ----"
if [[ -f "$CPU_PPROF" ]]; then
  pprof_top_symbols "$CPU_PPROF" 10 | nl -ba
  echo
else
  echo "(cpu profile missing)"
fi
echo

echo "---- Trace ----"
if [[ -f "$TRACE_FILE" ]]; then
  echo "Trace file exists. Open with:"
  echo "  go tool trace $TRACE_FILE"
else
  echo "MISSING: $TRACE_FILE"
fi
echo

echo "=============================="
echo " End of report"
echo "=============================="
echo

if [[ -f "$CPU_PPROF" ]]; then
  echo "---- CPU: line-level hotspots (auto) ----"
  pprof_top_symbols "$CPU_PPROF" 10 | while IFS= read -r sym; do
    esc="$(escape_pprof_regex "$sym")"
    echo
    echo ">>> go tool pprof -list '$esc' \"$BIN\" \"$CPU_PPROF\""
    go tool pprof -list "$esc" "$BIN" "$CPU_PPROF" 2>/dev/null || true
  done
  echo
fi

if [[ -f "$BLOCK_PPROF" ]]; then
  echo "---- BLOCK: line-level hotspots (auto) ----"
  pprof_top_symbols "$BLOCK_PPROF" 5 | while IFS= read -r sym; do
    esc="$(escape_pprof_regex "$sym")"
    echo
    echo ">>> go tool pprof -list '$esc' \"$BIN\" \"$BLOCK_PPROF\""
    go tool pprof -list "$esc" "$BIN" "$BLOCK_PPROF" 2>/dev/null || true
  done
  echo
fi

