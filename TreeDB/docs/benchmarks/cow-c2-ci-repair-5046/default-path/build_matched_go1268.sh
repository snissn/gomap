#!/bin/bash
set -euo pipefail
export GOROOT=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/toolchains/go1.26.8
export PATH="$GOROOT/bin:$PATH"
export GOMODCACHE=/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod
export GOPATH=/home/mikers/.gvm/pkgsets/go1.25/global
export GOCACHE=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-default-path-repair/cache-go1.26.8
export GOWORK=off
owned=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-default-path-repair
out=/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair
mkdir -p "$out/bin"
go env -json > "$out/go1268-env.json"
uname -a > "$out/go1268-uname.txt"
lscpu > "$out/go1268-lscpu.txt"
for mode in base repaired-v3; do
  cd "$owned/$mode"
  identity="$out/base-source.json"
  if [ "$mode" = repaired-v3 ]; then identity="$out/repair-v3-source.json"; fi
  python3 - "$identity" <<'PYCODE'
import hashlib,json,pathlib,sys
actual={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('.').rglob('*') if p.is_file()}
expected=json.loads(pathlib.Path(sys.argv[1]).read_text())
assert actual==expected, {'extra':sorted(actual.keys()-expected.keys()), 'missing':sorted(expected.keys()-actual.keys()), 'changed':[k for k in actual.keys()&expected.keys() if actual[k]!=expected[k]]}
PYCODE
  for kind in db public; do
    pkg=./TreeDB/db; if [ "$kind" = public ]; then pkg=./TreeDB; fi
    go test -c -trimpath -buildvcs=false -o "$out/bin/go1268-$mode-$kind.test" "$pkg" > "$out/go1268-$mode-$kind-build.stdout" 2> "$out/go1268-$mode-$kind-build.stderr"
    echo $? > "$out/go1268-$mode-$kind-build.exit"
    go version -m "$out/bin/go1268-$mode-$kind.test" > "$out/go1268-$mode-$kind-buildinfo.txt"
  done
  go run "$out/sizes_with_owners.go" > "$out/go1268-$mode-sizes.stdout" 2> "$out/go1268-$mode-sizes.stderr"
  echo $? > "$out/go1268-$mode-sizes.exit"
done
sha256sum "$out"/bin/go1268-*.test > "$out/go1268-binary-sha256.txt"
for mode in original repaired-v2 repaired-v3; do
  cd "$owned/$mode"
  go run "$out/constructors.go" > "$out/go1268-$mode-constructors.stdout" 2> "$out/go1268-$mode-constructors.stderr"
  echo $? > "$out/go1268-$mode-constructors.exit"
done
