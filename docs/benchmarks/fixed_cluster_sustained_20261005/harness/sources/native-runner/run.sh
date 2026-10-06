#!/bin/bash
set -euo pipefail
base=/home/mikers/gomap-1242-v4-assigned-owner-semantic-red-root-v1
cd "$base"
test ! -e run.started
load=$(cut -d' ' -f1 /proc/loadavg)
available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
free=$(df -B1 --output=avail /home/mikers | tail -1 | tr -d ' ')
printf 'load=%s\nmem_available_kib=%s\nfast_disk_free_bytes=%s\n' "$load" "$available" "$free" > receipts/admission.txt
awk -v n="$load" 'BEGIN {exit !(n<4)}'
test "$available" -ge 16777216
test "$free" -ge 53687091200
printf '%s  %s\n' 61e7455a40a2fdfcdab99e881cd30ba10e216e3d0f32ab5f8e59d10cac4ecf57 /home/mikers/gomap-q5-evidence/toolchains/go1.26.0-linux-amd64/bin/go | sha256sum -c - > receipts/toolchain-check.log
python3 - <<'PROCS'
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import pathlib
bad=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit(): continue
 try:
  if (p/'exe').resolve().name in {'go','compile','link','asm','cgo'}: bad.append(p.name)
 except (OSError, RuntimeError): pass
assert not bad, bad
PROCS
touch run.started
set +e
/usr/bin/time -v timeout --kill-after=20s 300s systemd-run --user --scope --unit="gomap-1242-v4-assigned-red-$$" -p MemoryMax=8G -p MemorySwapMax=0 /bin/bash "$base/inner.sh" > receipts/scope.log 2>&1
result=$?
set -e
printf '%s\n' "$result" > receipts/scope.exit
exit "$result"
