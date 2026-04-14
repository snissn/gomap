#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

OUT_ROOT="${OUT_DIR:-$ROOT/artifacts/perf}"
TS=$(date +%Y%m%d%H%M%S)
OUT="${OUT_ROOT}/treedb_forceptr_matrix_${TS}"

KEYS="${KEYS:-4000000}"
VALSIZE="${VALSIZE:-128}"
BATCHSIZE="${BATCHSIZE:-1000}"
TESTS="${TESTS:-batch_write,random_write,batch_delete}"
PROFILES="${PROFILES:-fast}"
VARIANTS="${VARIANTS:-base,prefix,columnar,columnar_prefix}"

PPROF="${PPROF:-1}"
PPROF_TESTS="${PPROF_TESTS:-$TESTS}"

EXTRA_ARGS="${EXTRA_ARGS:-}"

PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    PYTHON_BIN="python"
  fi
fi

mkdir -p "$OUT"/{runs,pprof}

META="$OUT/meta.txt"
{
  echo "ts=$TS"
  echo "git_rev=$(git rev-parse HEAD)"
  echo "git_branch=$(git rev-parse --abbrev-ref HEAD)"
  echo "root=$ROOT"
  echo "keys=$KEYS"
  echo "valsize=$VALSIZE"
  echo "batchsize=$BATCHSIZE"
  echo "tests=$TESTS"
  echo "profiles=$PROFILES"
  echo "variants=$VARIANTS"
  echo "pprof=$PPROF"
  echo "pprof_tests=$PPROF_TESTS"
  echo "extra_args=$EXTRA_ARGS"
} >"$META"

echo "treedb forceptr matrix" >&2
echo "out=$OUT" >&2
echo "keys=$KEYS valsize=$VALSIZE batchsize=$BATCHSIZE tests=$TESTS profiles=$PROFILES variants=$VARIANTS" >&2

make unified-bench >/dev/null

variant_flags() {
  case "$1" in
    base)
      echo "-treedb-force-value-pointers"
      ;;
    prefix)
      echo "-treedb-force-value-pointers -treedb-leaf-prefix-compression"
      ;;
    columnar)
      echo "-treedb-force-value-pointers -treedb-index-columnar-leaves"
      ;;
    columnar_prefix)
      echo "-treedb-force-value-pointers -treedb-index-columnar-leaves -treedb-leaf-prefix-compression"
      ;;
    *)
      return 1
      ;;
  esac
}

IFS=',' read -r -a profile_list <<<"$PROFILES"
IFS=',' read -r -a variant_list <<<"$VARIANTS"

for profile in "${profile_list[@]}"; do
  if [[ -z "$profile" ]]; then
    continue
  fi
  for variant in "${variant_list[@]}"; do
    if [[ -z "$variant" ]]; then
      continue
    fi

    if ! flags="$(variant_flags "$variant")"; then
      echo "unknown variant: $variant (known: base,prefix,columnar,columnar_prefix)" >&2
      exit 2
    fi

    run_file="$OUT/runs/${profile}_${variant}.md"
    echo "--- profile=$profile variant=$variant ---" >&2

    pprof_args=""
    if [[ "$PPROF" != "0" ]]; then
      pprof_dir="$OUT/pprof/${profile}_${variant}"
      mkdir -p "$pprof_dir"
      pprof_args="-cpuprofile $pprof_dir/cpu -cpuprofile-tests $PPROF_TESTS"
    fi

    # shellcheck disable=SC2086
    ./bin/unified-bench \
      -dbs treedb \
      -profile "$profile" \
      -keys "$KEYS" \
      -valsize "$VALSIZE" \
      -batchsize "$BATCHSIZE" \
      -test "$TESTS" \
      -format markdown \
      -progress=false \
      $pprof_args \
      $flags \
      $EXTRA_ARGS \
      2>&1 | tee "$run_file" >/dev/null
  done
done

SUMMARY="$OUT/summary.md"
"$PYTHON_BIN" - "$OUT" >"$SUMMARY" <<'PY'
import os
import re
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
meta = {}
meta_path = out_dir / "meta.txt"
if meta_path.exists():
    for line in meta_path.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            meta[k.strip()] = v.strip()

tests = [t.strip() for t in meta.get("tests", "").split(",") if t.strip()]
profiles = [p.strip() for p in meta.get("profiles", "").split(",") if p.strip()]
variants = [v.strip() for v in meta.get("variants", "").split(",") if v.strip()]

def parse_size(s):
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG]iB)\s*$", s)
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2)
    mul = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}[unit]
    return int(n * mul)

def parse_dir_usage(line, prefix):
    if not line.startswith(prefix):
        return None, None
    m_total = re.search(r"\btotal=([0-9.]+)\s*([KMG]iB)", line)
    if not m_total:
        return None, None
    total = parse_size(f"{m_total.group(1)} {m_total.group(2)}")
    m_value = re.search(r"\bvalue=([0-9.]+)\s*([KMG]iB)", line)
    value = None
    if m_value:
        value = parse_size(f"{m_value.group(1)} {m_value.group(2)}")
    return total, value

def fmt_bytes(n):
    if not n:
        return "-"
    for unit, mul in [("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)]:
        if n >= mul:
            return f"{n / mul:.1f} {unit}"
    return f"{n} B"

def title_test(t: str) -> str:
    return " ".join(w.capitalize() for w in t.split("_"))

def parse_run(text):
    out = {"ops": {}, "opts": {}, "disk": {}}

    # ops/sec lines (printed before markdown)
    for t in tests:
        name = title_test(t)
        m = re.search(rf"^{re.escape(name)} / TreeDB = ([0-9,]+)\s*$", text, flags=re.M)
        if m:
            out["ops"][t] = int(m.group(1).replace(",", ""))
            continue
        # fallback: parse markdown table rows
        m = re.search(rf"^\s*{re.escape(name)}\s+([0-9,]+)\s*$", text, flags=re.M)
        if m:
            out["ops"][t] = int(m.group(1).replace(",", ""))

    # resolved options block (banner)
    in_opts = False
    for line in text.splitlines():
        if line.strip() == "TreeDB options (resolved):":
            in_opts = True
            continue
        if in_opts:
            if not line.startswith(" "):
                break
            l = line.strip()
            if not l or l.startswith("notes:") or l.startswith("-"):
                continue
            if "=" in l:
                k, v = l.split("=", 1)
                out["opts"][k.strip()] = v.strip()

    # disk usage (end-of-run)
    m = re.search(r"^## Disk Usage \(End of Run\)\s*$", text, flags=re.M)
    if m:
        after = text[m.end() :]
        for line in after.splitlines():
            line = line.strip()
            if line.startswith("maindb/index.db:"):
                sz = line.split(":", 1)[1].strip()
                out["disk"]["maindb_index"] = parse_size(sz)
            elif line.startswith("dictdb/index.db:"):
                sz = line.split(":", 1)[1].strip()
                out["disk"]["dictdb_index"] = parse_size(sz)
            elif line.startswith("maindb/wal:"):
                total, value = parse_dir_usage(line, "maindb/wal:")
                if total is not None:
                    out["disk"]["maindb_wal_total"] = total
                if value is not None:
                    out["disk"]["maindb_wal_value"] = value
            elif line.startswith("maindb/value_vlog:"):
                total, value = parse_dir_usage(line, "maindb/value_vlog:")
                if total is not None:
                    out["disk"]["maindb_value_vlog_total"] = total
                if value is not None:
                    out["disk"]["maindb_value_vlog_value"] = value
            elif line.startswith("maindb/leaf_vlog:"):
                total, value = parse_dir_usage(line, "maindb/leaf_vlog:")
                if total is not None:
                    out["disk"]["maindb_leaf_vlog_total"] = total
                if value is not None:
                    out["disk"]["maindb_leaf_vlog_value"] = value
            elif line.startswith("dictdb/wal:"):
                total, value = parse_dir_usage(line, "dictdb/wal:")
                if total is not None:
                    out["disk"]["dictdb_wal_total"] = total
                if value is not None:
                    out["disk"]["dictdb_wal_value"] = value
            elif line.startswith("dictdb/value_vlog:"):
                total, value = parse_dir_usage(line, "dictdb/value_vlog:")
                if total is not None:
                    out["disk"]["dictdb_value_vlog_total"] = total
                if value is not None:
                    out["disk"]["dictdb_value_vlog_value"] = value
            if line == "```":
                break
    return out

runs = {}
for profile in profiles:
    for variant in variants:
        p = out_dir / "runs" / f"{profile}_{variant}.md"
        if not p.exists():
            continue
        runs[(profile, variant)] = parse_run(p.read_text())

lines = []
lines.append("# TreeDB perf matrix (force pointers)")
lines.append("")
lines.append(f"- git: `{meta.get('git_rev', '-')}` (`{meta.get('git_branch', '-')}`)")
lines.append(f"- keys: {meta.get('keys', '-')}, valsize: {meta.get('valsize', '-')}, batchsize: {meta.get('batchsize', '-')}")
lines.append(f"- tests: `{meta.get('tests', '-')}`")
lines.append(f"- profiles: `{meta.get('profiles', '-')}`")
lines.append(f"- variants: `{meta.get('variants', '-')}`")
lines.append(f"- outdir: `{out_dir}`")
lines.append("")

for profile in profiles:
    lines.append(f"## Profile: {profile}")
    lines.append("")
    header = ["variant"] + tests + ["index.db", "external(total)", "dict/index.db"]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---"] + ["---:"] * (len(header) - 1)) + "|")
    for variant in variants:
        run = runs.get((profile, variant))
        if not run:
            continue
        ops = run["ops"]
        disk = run["disk"]
        row = [f"[{variant}](runs/{profile}_{variant}.md)"]
        for t in tests:
            v = ops.get(t)
            row.append(f"{v:,}" if isinstance(v, int) else "-")
        row.append(fmt_bytes(disk.get("maindb_index")))
        external_total = sum(int(disk.get(k, 0) or 0) for k in ("maindb_wal_total", "maindb_value_vlog_total", "maindb_leaf_vlog_total"))
        row.append(fmt_bytes(external_total if external_total > 0 else None))
        row.append(fmt_bytes(disk.get("dictdb_index")))
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    lines.append(f"- pprof dir: `pprof/` (cpu profiles emitted per test when enabled)")
    lines.append("")

sys.stdout.write("\n".join(lines))
PY

echo "" >&2
echo "done:" >&2
echo "  summary: $SUMMARY" >&2
echo "  runs:    $OUT/runs/" >&2
echo "  pprof:   $OUT/pprof/" >&2
