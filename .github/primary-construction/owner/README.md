# PRIMARY observer rebind — sealed source for independent review

This packet rebinds the existing allocation diagnostic to the actual failed
comparison: B `8e1133085b4052e344a73063f28c2ab142485c73`, tree
`6b0fc6bd9f6a22621c85526342727509fc3bcafe`; C
`b24dea5e339cf94d419346ac9b38a8c90d6d3f02`, tree
`04c0fdc0cddb2bf2d788a45532d19da3159c20f1`. It contains no product repair,
new benchmark, layout/RNG change, execution controller or launch authority.
Source construction is complete; independent review and execution prerequisites
remain open. Static checks are not Go, format, build, test or capture evidence.

## Exact source and smallest change

`B-source.json` and `C-source.json` use the maintained constructor's complete
Git-tree inventory schema. `source-bindings.json` also pins the fixture, public
API/callers, affected product files, modules and unchanged strict gate. The full
original B-to-C diff is retained solely for source identity, not adoption.

Each arm has five overlay entries in `overlays.json`. Four shared payloads are
byte-identical to the admitted owner86f4ee08: instrumented original benchmark,
raw/profile helper, caching observer and caching controls. Each arm's caching
overlay was reconstructed from its actual source, adding only the existing
checkpoint-entry and ReadDir-start/finish seams. No old candidate db.go was
transplanted. The actual B/C caching preimages happen to be byte-identical;
their arm bindings, preimages and forward/inverse patches remain separate.
All five files round-trip exactly through their forward/inverse patches in
bounded in-memory text checks. Product trees and frozen4934 are untouched.

The original loop still calls `st.CommitAt(100, mutation, CommitDurable)`128
times after the same genuine DATA materialization, defaults and seed. Its
current diagnostic call line is190, original line184. ResetTimer/StopTimer
remain on their original sides of that loop; the observer's two extra STW
counter reads perturb diagnostic timing. The same original deferred d.Close
is invoked and its actual return is checked. The observer cannot turn a failed
Close or incomplete loop into a complete diagnostic. See `activation-map.json`.
The benchmark's name is not a different API. Optional logical/fallback branch
activation remains a runtime question; no capability is forced or cached here.

## Ownership, accounting and limits

The existing process-wide observer is exclusive by CAS. It retains only fixed
atomic scalar counters, never DB/backend/path/value owners. ReadDir completion
borrows the exact observer loaded before the original os.ReadDir. Releasing it
does not reset/reuse it: a late borrower can finish on the old GC-owned object
without charging a successor. Separate atomic snapshots are not transactional
or per-DB attribution. Checkpoint counts mean entry, including later refusal;
ReadDir counts describe that seam, not every allocation in CommitAt.

The unchanged fixed phase records and exclusive bounded output retain raw
TotalAlloc/Mallocs, N, quotients and remainders, bucket counters, seam phases,
N1 unobserved calibration and original checked lifecycle. The successful N128
receipt still says external profile validation is pending. No JSON receipt
alone establishes source/ELF identity, absence of descendants or acceptance.

Allocation profiles cover before Open through post-loop Stats, before Close,
with two GCs at each snapshot. They include setup/background/observer work and
cover a broader interval than raw loop counters. `accounting-contract.json`
requires both alloc_space and alloc_objects, all unfiltered sites/stacks,
disjoint sample accounting and signed unassigned cross-window residual. Keep
unknown, negative and ambiguous samples; never force equality, sum overlapping
cumulative frames, or divide the whole profile by128. No demonstrated dynamic
avoidable site plus lifetime-safe redundancy witness means STOP product repair.

## Qualification route and concrete blockers

The existing PRIMARY controls are unchanged: four tops, eight named outcomes,
seven leaves per normal/race mode. The earlier hosted six-top result included
two separate M0 controls and belongs to old4934; it is not new-arm qualification.
Exact new source format/compiler/selected normal/race checks are UNRUN. No M0
overlay is included. ROOT owns minimal maintained hosted binding and review.

`runner-bindings.json` pins the existing Runner/capture/guard/cleanup sources and
their smallest interfaces. The original fixed B→C pair,128x/count1/full-rate
child remains the source recipe. Retain90s/arm,180s active,200s outer,10s
cleanup; original GMP1/CPU0/4GiB Go-memory,24GiB sampled RSS,10GiB free floor,
32MiB per profile/log and1GiB **whole root**. That root includes every newly
materialized source/Git/module/cache/tmp/binary/output byte. These recorded
bounds are not a claim of current compatible hosted enforcement. ROOT must
review actual affinity, quiet admission, stage/outer limits and positive drain.
No old host185 launcher or consumed grant is reusable on hosted infrastructure.

Three specific prerequisites must remain visible:

1. The old schema1 adapter expects SchemaVersion/Operations. This unchanged
   observer emits v2 Schema/N/RawDelta plus separate calibration. ROOT must bind
   its actual validator to these fields without weakening completion controls.
2. The old analyzer hardcodes line214; the actual diagnostic CommitAt is line190.
   Do not run the old classification unchanged. Preserve full raw samples and
   exact source/ELF identity when ROOT binds the retained pprof interpretation.
3. The retained metadata query stopped before payload on an image mismatch:
   expected20261025.353.1, observed20261101.360.1; no query/guard/Go or runtime
   rows. Historical row217 content is still missing. Actual SDK/CGO/runtime
   identity for the new exact pair is BLOCKING, not accepted image drift.

`prerequisites.json` deliberately leaves runtime, constructor admission, both
ELFs and capture admission null. Source review does not fill them. Future
construction/controls/capture each require the existing ROOT review/admission;
this owner has not implemented another adapter or selected a runtime. If an
existing seam cannot preserve original inputs or controls, return its precise
incompatibility and stop rather than substitute an arm or add a framework.

## Preserved readiness and closure

The b24 PRIMARY+2B/op and M0 CV.130945 failures remain; so do old d367/4934
PRIMARY failures, #4519 M0 samples, distinct Windows third-submit unknown and
canceled macOS vet. No retry, gate change, noise claim or qualification follows.
The c0a8 provisional4934 projection remains intact. ROOT's reported #5395 merge
and postmain qualification are separate from this source-only diagnostic.

No local Go/gofmt/build/test/decoder, benchmark, application-helper import,
SSH, host probe, transfer, fetch/push, CI dispatch or new worker was performed.
The failed Python construction syntax attempt made no file/source change and is
retained in `construction-attempts.json`; it has no qualification credit.
All packet file handles and source-reading subprocesses are closed at seal.
Host185 logical HOLD and all other FAIL/NULL/HOLDs remain. ROOT receives this
single sealed packet for its existing independent review, not a product-ready
or executable-ready claim.
