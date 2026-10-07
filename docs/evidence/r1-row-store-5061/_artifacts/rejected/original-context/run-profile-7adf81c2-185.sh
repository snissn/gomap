#!/usr/bin/env bash
set -euo pipefail
cd /home/mikers/gomap-r1-growth-probe-185
export GOROOT=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64
export R1_GO="$GOROOT/bin/go"
export GOCACHE=/home/mikers/.cache/gomap-r1-growth-probe-go1264
export GOMODCACHE=/home/mikers/go/pkg/mod
export GOWORK=off GOTOOLCHAIN=local GOMAXPROCS=16 GOGC=100 GOMEMLIMIT=off GOFLAGS=''
probe_out=/home/mikers/gomap-r1-growth-evidence-20261005/profile-postgc-7adf81c2
mkdir -p /home/mikers/gomap-r1-growth-evidence-20261005
mkdir "$probe_out"
mkdir "$probe_out/benchmark-tmp"
export TMPDIR="$probe_out/benchmark-tmp"
test "$(git rev-parse HEAD)" = 7adf81c2afcf2f6de2f9d76303f6b09bba5b4db4
test -z "$(git status --porcelain)"
PYTHONPATH=scripts "$R1_GO" env -json > "$probe_out/go-env.json"
PYTHONPATH=scripts python3 - <<'PY' > "$probe_out/source-before.json"
import json,os
from r1_lifecycle_capture import source
print(json.dumps(source(os.environ['R1_GO']),indent=2,sort_keys=True))
PY
python3 - <<'PY' > "$probe_out/runner-before.json"
import json,os,platform,time
print(json.dumps({'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'loadavg':os.getloadavg(),'cpu_count':os.cpu_count(),'uname':platform.uname()._asdict(),'environment':{k:os.environ[k] for k in ('GOROOT','GOCACHE','GOMODCACHE','GOWORK','GOTOOLCHAIN','GOMAXPROCS','GOGC','GOMEMLIMIT','GOFLAGS','TMPDIR')}},indent=2))
PY
df -Pk "$TMPDIR" > "$probe_out/filesystem.txt"
"$R1_GO" test -c -o "$probe_out/collections.test" ./TreeDB/collections > "$probe_out/build.log" 2>&1
"$R1_GO" version -m "$probe_out/collections.test" > "$probe_out/binary-buildinfo.txt"
sha256sum "$probe_out/collections.test" > "$probe_out/binary-sha256.txt"
printf '%s\n' 'collections.test -test.run=^TestR1SupportedProfileGrowthProbe$ -test.count=1 -test.timeout=2m -test.v' > "$probe_out/command.txt"
set +e
"$probe_out/collections.test" '-test.run=^TestR1SupportedProfileGrowthProbe$' -test.count=1 -test.timeout=2m -test.v > "$probe_out/probe.log" 2>&1
probe_status=$?
set -e
printf '%s\n' "$probe_status" > "$probe_out/exit-code.txt"
PYTHONPATH=scripts python3 - <<'PY' > "$probe_out/source-after.json"
import json,os
from r1_lifecycle_capture import source
print(json.dumps(source(os.environ['R1_GO']),indent=2,sort_keys=True))
PY
cmp "$probe_out/source-before.json" "$probe_out/source-after.json"
python3 - <<'PY' > "$probe_out/runner-after.json"
import json,os,time
print(json.dumps({'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'loadavg':os.getloadavg()},indent=2))
PY
printf 'probe exit=%s retained=%s\n' "$probe_status" "$probe_out"
exit "$probe_status"
