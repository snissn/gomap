#!/bin/bash
set -euo pipefail
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export PATH="$GOROOT/bin:$PATH"
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
export GOPATH=/home/mikers/.gvm/pkgsets/go1.25/global
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache
export GOWORK=off
owned=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-default-path-repair
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair
mkdir -p "$out/bin"
go env -json > "$out/go-env.json"
uname -a > "$out/uname.txt"
lscpu > "$out/lscpu.txt"
for mode in base original; do
  cd "$owned/$mode"
  python3 - "$out/$mode-source.json" <<'PY'
import hashlib,json,pathlib,sys
files={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(pathlib.Path('.').rglob('*')) if p.is_file()}
pathlib.Path(sys.argv[1]).write_text(json.dumps(files,sort_keys=True,indent=2)+'\n')
PY
  go test -c -trimpath -buildvcs=false -o "$out/bin/$mode-db.test" ./TreeDB/db > "$out/$mode-db-build.stdout" 2> "$out/$mode-db-build.stderr"
  go test -c -trimpath -buildvcs=false -o "$out/bin/$mode-public.test" ./TreeDB > "$out/$mode-public-build.stdout" 2> "$out/$mode-public-build.stderr"
done
sha256sum "$out"/bin/* > "$out/binary-sha256.txt"
