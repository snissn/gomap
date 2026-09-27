#!/usr/bin/env bash
# Read-only derived projection of one externally labelled resource observation.
# Exact parent verification is reused, never regenerated to replace a failure.
set -euo pipefail
asset=${1:?one or multi}
block=${2:?external block 1 through 5}
candidate=03a3b73dd2fc6888e603307df5c3a00f0a879342
[[ "$candidate" =~ ^[0-9a-f]{40}$ && "$block" =~ ^[1-5]$ ]]
case "$asset" in one) asset_id=1;; multi) asset_id=2;; *) exit 2;; esac
root="/home/mikers/gomap-4753-integrated-${candidate:0:9}"
sha256sum -c "$root/launch-package-pins.txt"
sha256sum -c "$root/measurement-package-pins.txt"
sha256sum -c "$root/reader-path-amendment-pins.txt"
parent="$root/serving/$asset-block$block"
comparison="$root/comparison/block$block"
out="$root/resources/$asset-block$block"
sha256sum -c "$root/frozen-input-and-binary-pins.txt"
sha256sum -c "$root/frozen-build-dependency-pins.txt"
sha256sum -c "$parent/published-report-pins.txt"
sha256sum -c "$out/frozen-pins.txt"
sha256sum -c "$comparison/frozen-pins.txt"
test "$(< "$comparison/exit-status.txt")" = 0
test "$(< "$out/exit-status.txt")" = 0
test ! -e "$out/reader-inventory-v2.json"
sha() { sha256sum "$1" | cut -d ' ' -f1; }
jq -n --arg dir "$comparison" --arg receipt "$(sha "$comparison/result.json")" --arg exit "$(sha "$comparison/exit-status.txt")" \
 --argjson asset "$asset_id" --argjson block "$block" --slurpfile args "$parent/replay-args.json" --slurpfile command "$comparison/command.json" '
 def value($key): $args[0] | index($key) as $i | .[$i+1];
 {Reports:[{Asset:$asset,Block:$block,Selected:5,Path:value("-report"),
 Pins:{Report:value("-report-sha256"),Fixture:value("-fixture-sha256"),Command:value("-command-sha256"),Executable:value("-executable-sha256"),Variant:value("-variant-descriptor-sha256"),TruthArtifact:value("-truth-artifact-sha256"),TruthContent:value("-truth-content-sha256")},
 Receipt:{Kind:"in-process-comparer",Artifact:{Path:($dir+"/result.json"),SHA256:$receipt},ExitStatus:{Path:($dir+"/exit-status.txt"),SHA256:$exit},Command:$command[0]}}]}' > "$out/reader-inventory-v2.json"
sha256sum "$out/reader-inventory-v2.json" "$out/frozen-header.json" "$out/resources.jsonl" "$out/exit-status.txt" > "$out/published-reader-pins-v2.txt"
export GOMAP_F_REDUCE_PLAN="$out/reader-inventory-v2.json" GOMAP_F_REDUCE_PLAN_SHA256=$(sha "$out/reader-inventory-v2.json")
export GOMAP_F_RESOURCE_HEADER="$out/frozen-header.json" GOMAP_F_RESOURCE_HEADER_SHA256=$(sha "$out/frozen-header.json")
export GOMAP_F_RESOURCE_RECEIPT="$out/resources.jsonl" GOMAP_F_RESOURCE_RECEIPT_SHA256=$(sha "$out/resources.jsonl")
export GOMAP_F_RESOURCE_EXIT_STATUS="$out/exit-status.txt" GOMAP_F_RESOURCE_EXIT_STATUS_SHA256=$(sha "$out/exit-status.txt")
export GOROOT=/home/mikers/gomap-q5-evidence/toolchains/go1.26.0-linux-amd64
export PATH="$GOROOT/bin:$PATH" GOENV=off GOWORK=off GOTOOLCHAIN=local
test "$(go env GOVERSION)" = go1.26.0
export GOMAXPROCS=2 GOMEMLIMIT=3GiB
reader="$root/candidate/bin/resource-reader.test"

mkdir "$out/reader-v2"
set +e
bash "$root/f-bounded.sh" "$out/reader-v2" 5m "$reader" -test.run '^TestFResourceRetained$' -test.v
status=$?
set -e
printf '%s\n' "$status" > "$out/reader-exit-status-v2.txt"
cp "$out/reader-v2/run.log" "$out/reader-result-v2.txt"
sha256sum "$out/reader-result-v2.txt" "$out/reader-exit-status-v2.txt"
bash "$root/f-preserve.sh" > "$out/post-reader-preservation-v2.txt" 2>&1
exit "$status"
