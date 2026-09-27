#!/usr/bin/env bash
set -euo pipefail
action=${1:?prepare or run or read}
asset=${2:?one or multi}
case "$asset" in one) packs=16; domain_graphs=false; step=1;; multi) packs=64; domain_graphs=true; step=4;; *) exit 2;; esac
root=/home/mikers/gomap-4753-integrated-03a3b73dd
parent="$root/serving/$asset-block1"
out="$root/physical/$asset-block1"
head=03a3b73dd2fc6888e603307df5c3a00f0a879342
sha() { sha256sum "$1" | cut -d ' ' -f1; }
sha256sum --quiet -c "$root/launch-package-pins.txt"
sha256sum --quiet -c "$root/measurement-package-pins.txt"
sha256sum --quiet -c "$root/reader-path-amendment-pins.txt"
sha256sum --quiet -c "$root/frozen-input-and-binary-pins.txt"
sha256sum --quiet -c "$root/frozen-build-dependency-pins.txt"
sha256sum --quiet -c "$parent/published-report-pins.txt"
test "$(git -C "$root/candidate/source" rev-parse HEAD)" = "$head"
test -z "$(git -C "$root/candidate/source" status --porcelain)"
mapfile -d '' -t args < <(jq -jr '.[] | ., "\u0000"' "$parent/replay-args.json")
test "${#args[@]}" = 18
report=$(jq -er 'index("-report") as $i | .[$i+1]' "$parent/replay-args.json")
report_sha=$(jq -er 'index("-report-sha256") as $i | .[$i+1]' "$parent/replay-args.json")
cmd=("$root/candidate/bin/treedb_vector_partition_bench" physical-resources -out "$out/resources.json"
 -source-checkout "$root/candidate/source" -head-sha "$head" -executable-sha256 "$(sha "$root/candidate/bin/treedb_vector_partition_bench")" -- "${args[@]}")
if [[ "$action" = prepare ]]; then
 test "$(getconf PAGESIZE)" = 4096
 test ! -e "$out"
 mkdir -p "$out"
 jq -ncj --args '$ARGS.positional' -- "${cmd[@]}" > "$out/command.json"
 # Prospective header is derived from pinned parent/manifest coordinates,
 # never from the observed receipt. The reader verifies actual manifest anchors.
 jq --argjson packs "$packs" --argjson domain_graphs "$domain_graphs" --argjson step "$step" --arg head "$head" --arg exe "$(sha "$root/candidate/bin/treedb_vector_partition_bench")" --arg source "$root/candidate/source" --arg report_sha "$report_sha" --slurpfile command "$out/command.json" '
  if .head_sha!=$head or .config.partitions!=$packs or .config.logical_domain_count!=16 or .config.ef_search!=[96] or .config.top_k!=10 or .variant.partition_generation!=1 then error("parent coordinate drift") else . end |
  {kind:"physical_resources",contract:"untimed_retained_pack_physical_resources_not_qualification_v1",
   scope:"new collector opens pinned assets through retained attribution searchers; mapped extents are not RSS; logical handles are not OS FDs; chunks are fully opened/validated, not query touches; metadata and per-search scratch are conservative bounds, not heap/peak measurements; post-close counters prove owned handle release, not Go reclamation; excludes router, native topology and historical measured-process resources",
   replay_receipt:("REPLAY_ACCEPTED_NOT_QUALIFICATION report_sha256="+$report_sha+" rows="+(.rows|length|tostring)+"\n"),
   head_sha:$head,executable_sha256:$exe,source_checkout:$source,command:$command[0],
   parent_execution_id:.execution_id,parent_head_sha:.head_sha,parent_executable_sha256:.executable_sha256,
   manifest_integrity_digest:.variant.manifest_integrity_digest,generation:.variant.partition_generation,
   domain_graphs:$domain_graphs,serving_partitions:[range(0;$packs;$step)],top_k:10,ef_search:[96],
   go_version:"go1.26.0",goos:"linux",goarch:"amd64",page_size:4096,host:.host}' "$report" > "$out/header.json"
 cp "$root/resources/$asset-block1/reader-inventory-v2.json" "$out/parent-inventory.json"
 sha256sum "$out/command.json" "$out/header.json" "$out/parent-inventory.json" "$parent/replay-args.json" "$root/frozen-input-and-binary-pins.txt" > "$out/frozen-pins.txt"
 sha256sum "$out/frozen-pins.txt"
 exit
fi
sha256sum --quiet -c "$out/frozen-pins.txt"
diff "$out/command.json" <(jq -ncj --args '$ARGS.positional' -- "${cmd[@]}")
export GOROOT=/home/mikers/gomap-q5-evidence/toolchains/go1.26.0-linux-amd64
export PATH="$GOROOT/bin:$PATH" GOENV=off GOWORK=off GOTOOLCHAIN=local GOMAXPROCS=2 GOMEMLIMIT=3GiB TMPDIR="$root/tmp"
if [[ "$action" = run ]]; then
 test ! -e "$out/run.log"
 test ! -e "$out/resources.json"
 exec bash "$root/f-bounded.sh" "$out" 40m "${cmd[@]}"
fi
test "$action" = read
test "$(< "$out/exit-status.txt")" = 0
test ! -e "$out/reader"
mkdir "$out/reader"
sha256sum "$out/resources.json" "$out/exit-status.txt" "$out/header.json" "$out/parent-inventory.json" > "$out/published-pins.txt"
export GOMAP_PHYSICAL_HEADER="$out/header.json" GOMAP_PHYSICAL_HEADER_SHA256=$(sha "$out/header.json")
export GOMAP_PHYSICAL_PARENT="$out/parent-inventory.json" GOMAP_PHYSICAL_PARENT_SHA256=$(sha "$out/parent-inventory.json")
export GOMAP_PHYSICAL_RECEIPT="$out/resources.json" GOMAP_PHYSICAL_RECEIPT_SHA256=$(sha "$out/resources.json")
export GOMAP_PHYSICAL_EXIT="$out/exit-status.txt" GOMAP_PHYSICAL_EXIT_SHA256=$(sha "$out/exit-status.txt")
set +e
bash "$root/f-bounded.sh" "$out/reader" 5m "$root/candidate/bin/resource-reader.test" -test.run '^TestPhysicalRetained4775TopologyProjection$' -test.v
status=$?
set -e
bash "$root/f-preserve.sh" > "$out/post-reader-preservation.txt" 2>&1
exit "$status"
