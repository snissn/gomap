# Versioned baseline update-tail bridge (provisional construction)

This version applies to H's update acknowledgement/concurrent owned-read/process
IO extension, following merge of actual main a51b4795cebc194c7a158b262242668877932934
into H base b1bb478b90eed1afeddc09c6d13a189e26d3a449 (merge
aee6e8aaa08aa62386bfc506abd8689db79eb060). Exact new canonical five-input
hashes are in canonical-source-hashes.json; exact external seven replacement/
checker input hashes are in source-hashes.json. The contextual patch SHA256 is
`0d13b6f526a3987dd3b5922a998ff6682e8bf942c88fe8b6ce1a78344221b772`. It reconstructs those seven files from external copies of the
new canonical additions; the canonical memory overlay bytes remain unchanged.

The replacement H Go retains the same acknowledgement timing, chronological
samples and full concurrent-reader validation. Only the already-reviewed
baseline capability boundaries differ: reject any filter-on setting, report
negative support absent with a distinct baseline schema, reject any actual
negative-filter stats including invented zero counters, and supply the local
quantile helper absent from the original package. No engine backport occurs.
The baseline memory capture retains fixed10 runtime controls, GOTMPDIR, strict
exact original/control HEAD/trees, exactly five allowed additions, pinned
external inputs/toolchain and before/after freeze checks. The canonical H
observation adapter is preserved in final source; the baseline adapter rejects
N stats in every applicable boundary, including post-reopen GC stats.

Keep baseline/runtime, the earlier four historical documentation artifacts,
prototype manifests/freezes/binaries and all streams unchanged. This new
source-manifest.template.json is a Mac-path source template, not retained
preparation acceptance. Its canonical_head=7e1 remains the prototype protocol
identity; actual current published/landed H source identity must be bound
separately by Git objects and current hashes. Regenerate actual host/compiler/
module/path/environment and original/control manifests/freezes after landing.
Do not add A's Go file as a sixth baseline addition.

External proposal: /tmp/gomap-4895-baseline-compat-update-tail-review-fix-proposal.
Original/control constructor overlays and source manifests are labeled
construction-only. Their packet checks do not replace a full prepared freeze.
See ../../README.md for timing/sample/reader/process-IO boundaries. At40 full
acknowledgement samples, p99 and p99.9 both equal empirical max (pilot8 likewise).
This is finite sample support, not statistically resolved p99.9 or an SLO.
Required next gates: independent exact-source review, current-head CI, landing,
fresh preparation/freezes, root timing grant, repeated retained workflows, and
separate native maintenance/pin/GC correctness and recovery-debt evidence.

Review fixes align elapsed support through final phase capture including joined
reader quantiles, contain all eight phases, enforce coherent chronological
process/phase IO and exact configuration types. Prior proposal/constructor paths
remain historical; this inventory requires fresh preparation after review/landing.
