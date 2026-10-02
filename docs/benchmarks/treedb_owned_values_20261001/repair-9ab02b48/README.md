# Owned value-log reads: final mmap publication repair

The final candidate enables ordinary caller-owned pointer reads to use existing
sealed lazy mmap and bounded decoded-frame cache admission, and copies a cached
value directly into its final destination. The shared remap boundary serializes
actual-size budget admission and mapping publication across sibling files. This
packet qualifies and independently reviews runtime `9ab02b48ce2397410940a7e42cc22bbf95a96c5a`;
it does not relabel earlier binaries or review acceptance as current-head CI.

## Repeated results and accepted costs

Five fresh-process repetitions per revision, Linux Go1.26.3, GOMAXPROCS=4,
GOMEMLIMIT=1GiB. Times below are medians with observed min–max, ns/op. The primary
95-cell/40-process packet compares merged base `a68a84c7195e0c5d8c339343a385b40acf818d03`,
previous candidate `ab2e9124fcca72710b896cf78577fa6bd37a11b0`, and final 9ab.

| Boundary | Base | Previous ab2 | Final 9ab | Allocation/interpretation |
| --- | ---: | ---: | ---: | --- |
| Public ordinary owned pointer Get | 3098 (3090–3167) | — | 1928 (1911–1951) | −37.8%; 315 B/op, integer 1 alloc/op unchanged |
| Existing capped fallback, nil | 1092 (1088–1114) | — | 1147 (1120–1171) | **+55 ns, +5.04%, separated ranges**; 512 B/op, integer 1 unchanged |
| Existing capped fallback, reused | 1035 (1026–1041) | — | 1046 (1040–1053) | +1.06%; 0 B/op, 0 alloc/op |
| Warm mapped decode, cache disabled | 63049 (62923–64152) | — | 63148 (63088–63690) | +0.16%, overlapping ranges; 32768 B/op, integer 1 |
| OS-warm open/map/read/admit/close | 81466 (81224–81579) | 89517 (89039–90624) | 87432 (87035–90097) | +7.32% vs base, −2.33% vs ab2; about +181 B/op, integer 13→15 vs base |
| Memoized count-denial, nil 512 B | 1089 (1065–1099) | 1092 (1091–1118) | 1096 (1092–1104) | +0.64% vs base, +0.37% vs ab2; 512 B/op, integer 1 |
| Memoized count-denial, reused | 1022 (1000–1032) | 1020 (1014–1053) | 1042 (1031–1055) | +1.96%/+2.16%, overlapping ranges; 0 B/op, 0 alloc/op |
| Concurrent two-file first admission | — | 27877 (27672–28391) | 27532 (27257–28062) | −1.24%, overlapping ranges; about 6,737 B/op, integer 23 unchanged |

The separately frozen exact capped-fallback repair control uses the same ab2/9ab
binaries, five alternating pairs, ten processes, twenty cells:

| Existing fallback row | ab2 | 9ab | Change |
| --- | ---: | ---: | --- |
| Nil destination | 1119 (1114–1154) | 1122 (1118–1138) | +3 ns, +0.268%; 512 B/op, integer 1 unchanged |
| Reused destination | 1042 (1038–1051) | 1058 (1044–1112) | +16 ns, +1.536%; 0 B/op, 0 alloc/op |

The control retains its slower reused observation. Overlapping ranges do not
prove equivalence. The control compares ab2 to 9ab, so it cannot erase the primary
base→9ab +55 ns observation; cross-packet subtraction cannot attribute that cost.
The full change adds a lazy-eligibility attempt after an owned mapped miss; capped
misses exit before manager lock/Stat, but still execute eligibility/cap checks.
That source-level recurring guard cost is real; the precise 55 ns magnitude is
not causally attributed to it. The [independent review](independent-review.md)
explicitly accepts the whole candidate including +5.04% capped nil fallback and
+7.32% OS-warm lifecycle costs, in scope of the measured ordinary pointer-read
gain, unchanged fallback allocation/residency and preserved denial safety. No
universal improvement or concurrency scaling guarantee is claimed.

## Public route, fixture and memory proof

Each timed public operation has exactly one pointer hit, zero inline hits, one
CRC check and one cache hit. Transport is base fallback=1/mmap=0 and candidate
fallback=0/mmap=1. Thus the comparison does not substitute inline placement for
pointer placement. Observed median read rates are approximately 322789→518672/s.
The immutable canonical source fixture contains 32768 keys × 256 B, seventeen files,
5,659,493 B, fully checkpointed, closed and every-value verified. Both revisions
clone identical prerecorded record/dictionary/pointer bytes. Supported CopyFS
and production-order side-store/main physical-identity rebind change only private
index physical identities, outside timing. Source fixture bytes are verified
before and after, and the immutable source is never opened by a measured process.
Every clone is fully verified before timing and final closes are checked. Normal
public constructors and isolated pre-admitted cache-copy controls remain in the
[earlier source-bound packet](../README.md); they are mechanism evidence, not a
measurement of 9ab.

Public decoded logical raw is 9,389,893 B under 67,108,864 B; active mapped bytes
increase 236,469→873,560 B. Post-GC warm heap medians 13,096,920→13,084,112 B do not
isolate cache capacity. Setup-inclusive public peak RSS ranges 129740–139584 KiB
and 134864–140000 KiB overlap. Internal processes mix benchmark rows, so their
process peaks cannot be interpreted as per-row retained memory. Logical raw
length, pooled backing capacity, heap, active/retained mmap and process RSS are
separate quantities; this packet claims no physical-RSS reduction. Integer
allocs/op truncates fractional amortized allocations: 315 B/integer 1 does not prove
exactly one allocation or zero residual work beyond required owned output. The
historical f9 allocation profile also remains bound to its earlier source.

## Safety and exact provenance

The shared helper locks manager then file across actual Stat-sized sealed-budget
admission and mapping publication, rechecks the same registered file identity,
and covers ReadAppend, ReadUnsafe, ReadUnsafeTo and safe-range refresh. Current
files are serialized with demotion but only sealed files face sealed budgets.
Mapped hits avoid admission. Live dead-cap and unchanged-denial guards precede
the manager lock and are authoritatively rechecked under it; changed limits can
retry. No reservation counter was introduced. A failed mmap keeps the prior
mapping, unsafe views, retained lists/counts/bytes and remap counters intact.
The existing old mapping is retired only after successful mmap creation.

[Correctness checkpoint](correctness/mmap-admission-checkpoint.json) binds old-ab2
red publication/growth/O_WRONLY failure proofs and exact 9ab focused race (1.865s)
and vet passes. Earlier ownership, codec/CRC, templates/dictionaries, prefix,
eviction, segment identity, cap recovery and public reopen/GC proofs stay
source-bound in the original packet and current tests. No new broad suite was
run for the final evidence/docs/main integration.

| Immutable item | SHA256 |
| --- | --- |
| [Primary analysis](raw/repair-analysis.json) | `6c382fac604c4cac13b205c8d7089079f797b64abb7240104bfe20e27eb796cb` |
| [Actual source/binary freeze](raw/repair-source-freeze.json) | `28c71153df2362a6e0990955fc3f4d752a77c29ba31df96b2b5a9eee75b310da` |
| [Control capture](control/capture.json) | `afe28df3429f8e887b411a117a683518a720f4fe3355c01b5c2aa07e39c76380` |
| [Control before/after identity](control/identity-before.json) | `1d5b6a716a4eca96638e51086457a7f7d2bb29829fd478408be438501722fabd` |
| [Independent exact 9ab review](independent-review.md) | `ed48eb4e6326520cae6fe666f7aadaa6d2d9c7a958eb8d4542d65190a3f121a5` |

The freeze is immutable pre-capture metadata (`official_capture_started=false`);
[completed capture status](raw/repair-capture-status.txt) and raw process artifacts
record execution. The first rejected metadata freeze is retained separately;
final parser/plan/proposal changes are explicitly bound, with no runtime/binary
change. [Integration audit](integration-audit.json) records exact affected blobs
unchanged after main c9 integration; measured/reviewed identities remain 9ab.
Root owns current-head CI, unresolved P1 disposition and merge readiness.

The policy audit found only root and TreeDB AGENTS applicable to these paths,
plus CONTRIBUTING and API/storage/lifecycle contracts. Persistent pointers,
reachability-based GC, owned Get, checksum and identity/pin rules remain intact;
no disk format, default pointer threshold or API changes are made. Standalone
package artifacts use the AGENTS exception and are documented in both command
READMEs; unified-bench/benchprof profile formats and parsers are unchanged.

## Artifacts and reproduction

The primary capture retains 95 cells, forty processes and 120 separate stdout,
stderr and `/usr/bin/time -v` RSS files; parser gates names, routes, residency,
allocations, finite positive timings/RSS and successful exits. Twenty-five
untimed preparation processes include five compiles and validate-only full-value
route constructors; build IDs, version-m outputs, exact argv/env/cwd and source
inventories are retained. The control retains all thirty raw streams with process
statuses, complete argv/env/cwd, grant/preflight/log, actual binary hashes and
unchanged before/after source/fixture identities. Control explicitly adds
GOENV=off/GOTOOLCHAIN=local; exact environments are recorded rather than assumed.

[Packet inventory](packet-inventory.json) hashes every published file except
itself. Six complete source inventories are deterministically gzip-compressed
without loss; [compression mapping](source-inventory-compression.json) retains
the original filename, byte count and SHA, matching the frozen input hash. No
binary/archive or duplicated fixture payload is committed. Exact Git commits,
full source inventories, prepared deltas, compiled binary hashes/build IDs and
fixture hash inventory identify those remote inputs. Existing frozen directory:
`/mnt/fast4tb/treedb-quicksilver-4891-values-20261001/final-9ab02b48`.

To replay the analysis without mutating the retained packet, copy `raw/` into a
new scratch directory, then run:

```sh
python3 inputs/analyze-mmap-repair.py SCRATCH_RAW_DIR
```

To reconstruct a new capture, use the exact commits and declared overlays in
[the retained plan](inputs/mmap-repair-qualification-plan.md), install the public
benchmark donor and existing `../baseline-benchmark-overlay.go.txt` where absent,
and the identical qualification overlay on all three sources. The `.go.txt`
donors must be restored to their declared `_test.go` paths. Recreate and fully
verify the immutable fixture using its original constructor; preserve original
record/dictionary/pointer bytes. Run retained staging/preparation scripts from
the documented original workspace layout, or explicitly adapt only locations,
then produce and review a new actual source/binary freeze. Frozen hashes are not
portable promises of reproducible binaries; a rebuild needs fresh provenance.
Exclusive timing ownership is required before running:

```sh
bash inputs/qualify-mmap-repair.sh NEW_FROZEN_CAPTURE_DIR
```

The exact control script has fixed original paths and a no-overwrite output;
it records the completed root control, not authorization to repeat it. These
are standalone Go package artifacts, not unified-bench profile-dir outputs or
benchprof inputs. Earlier failed 933 and historical f9 packets are unchanged. Six `git diff --check`
warnings in the new packet are intentional untouched capture bytes: one blank
EOF in the root preflight and five trailing tab fields in `go version -m` output.
Authored documentation/scripts pass whitespace checks; frozen raw is not trimmed.
