#!/bin/bash
set -euo pipefail
cd /home/mikers/gomap-r1-native-appender-metadata
out=/home/mikers/gomap-r1-native-appender-metadata-red-f84e
mkdir -p "$out"
exec >"$out/validation.log" 2>&1
trap 'status=$?; printf "%s\n" "$status" >"$out/exit-status"; git status --porcelain >"$out/status-after.txt"' EXIT
git rev-parse HEAD >"$out/source.txt"
git status --porcelain >"$out/status.txt"
test ! -s "$out/status.txt"
"$GOROOT/bin/go" version
"$GOROOT/bin/go" test -c ./TreeDB/db -o "$out/db.test"
sha256sum "$out/db.test" >"$out/binary.sha256"
"$out/db.test" -test.run '^TestR1NativeAppenderCreationMetadataBoundedAcrossRotations$' -test.v -test.count=1 -test.timeout=3m
