#!/usr/bin/env bash
# Serialize this capture with other timed jobs on the same host.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
: "${R1_OUT:?Set R1_OUT to a new artifact directory}"
if [[ -e "$R1_OUT" ]]; then
  echo "R1_OUT already exists; preserve prior evidence and choose a new directory" >&2
  exit 2
fi
mkdir -p "$R1_OUT"
R1_OUT=$(cd "$R1_OUT" && pwd)
export GOWORK=off
R1_GO=${R1_GO:-go}
R1_MODE=${R1_MODE:-r1}
case "$R1_MODE" in
  r1|r1-mutation-sweep) ;;
  *) echo "unsupported R1_MODE" >&2; exit 2 ;;
esac
printf '%s\n' "$R1_MODE" > "$R1_OUT/mode.txt"
python3 scripts/r1_collection_source.py > "$R1_OUT/source.json"
python3 - "$R1_OUT/host.json" <<'PYMETA'
import json,os,pathlib,sys,time
metadata={'capture_class':'nonqualifying_rehearsal_unless_reviewed_landed_frozen','started_unix':time.time(),'hostname':os.uname().nodename,'uname':list(os.uname()),'cpu_count':os.cpu_count(),'loadavg':list(os.getloadavg())}
pathlib.Path(sys.argv[1]).write_text(json.dumps(metadata,indent=2)+'\n')
PYMETA
"$R1_GO" version > "$R1_OUT/go-version.txt"
"$R1_GO" env GOOS GOARCH CGO_ENABLED GOROOT GOCACHE GOMODCACHE GOWORK GOTOOLCHAIN CC CXX CGO_CFLAGS CGO_CPPFLAGS CGO_CXXFLAGS CGO_LDFLAGS > "$R1_OUT/go-env.txt"
R1_CC=$("$R1_GO" env CC)
"$R1_CC" --version > "$R1_OUT/cc-version.txt"
"$R1_GO" build -o "$R1_OUT/collection_workload_bench" ./cmd/collection_workload_bench 2> "$R1_OUT/build.stderr"
"$R1_GO" version -m "$R1_OUT/collection_workload_bench" > "$R1_OUT/buildinfo.txt"
python3 - "$R1_OUT/collection_workload_bench" > "$R1_OUT/binary.sha256" <<'PY'
import hashlib,pathlib,sys
p=pathlib.Path(sys.argv[1]);print(hashlib.sha256(p.read_bytes()).hexdigest(),p.name)
PY
python3 - "$R1_OUT/args.json" "$@" <<'PY'
import json,pathlib,sys
pathlib.Path(sys.argv[1]).write_text(json.dumps(sys.argv[2:],indent=2)+'\n')
PY
"$R1_OUT/collection_workload_bench" "$R1_MODE" -source-manifest "$R1_OUT/source.json" "$@" > "$R1_OUT/packet.json" 2> "$R1_OUT/run.stderr"
python3 scripts/r1_collection_source.py > "$R1_OUT/source-after.json"
if ! cmp -s "$R1_OUT/source.json" "$R1_OUT/source-after.json"; then
  echo "source drifted during capture; preserve packet as rejected, do not qualify" >&2
  exit 1
fi
if [[ "$R1_MODE" == r1-mutation-sweep ]]; then
  # Producer artifacts cannot qualify their own source or completed-run receipt.
  # The acceptance owner supplies independent pins only after observing capture.
  "$R1_OUT/collection_workload_bench" r1-mutation-sweep-validate -semantic-only \
    -source-manifest "$R1_OUT/source.json" "$R1_OUT/packet.json" > "$R1_OUT/validation.txt"
else
  "$R1_OUT/collection_workload_bench" r1-validate -source-manifest "$R1_OUT/source.json" "$R1_OUT/packet.json" > "$R1_OUT/validation.txt"
fi
if [[ "$R1_MODE" == r1 ]]; then
  python3 scripts/r1_collection_summary.py "$R1_OUT/packet.json" > "$R1_OUT/summary.json"
fi
python3 - "$R1_OUT/host.json" <<'PY'
import json,os,pathlib,sys,time
p=pathlib.Path(sys.argv[1]);data=json.loads(p.read_text());data['finished_unix']=time.time();data['loadavg_after']=list(os.getloadavg());p.write_text(json.dumps(data,indent=2)+'\n')
PY
printf 'R1 packet: %s\n' "$R1_OUT/packet.json"
