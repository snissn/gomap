# Selected DATA AppendWork / pager Growth progress bound

Source-only root advice, 2026-10-16. No Go execution, numeric grant selection, publication or ACK is authorized by this report. Bound to source manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`; individual files and archive binding are recorded in the adjacent JSON. This is a structural operand for the final driver's complete preparation/cancel rows, not a replacement for their caller prefixes.

## Actual selected pager identity

The immutable archive's complete non-test Go scan finds exactly one caller of `SetPrimaryMetadataOwnerV6`: `primaryarena.newFileArena` (arena.go:199). It receives the independently opened PRIMARY-bank pager. `db.openWithLock` opens `index.db` through `pager.OpenWithOptions` and passes that SAME pager to `freelist.New` (db.go:2733-2761). `OpenOwnedFileWithMetadata` assigns `ownedFileMetadata`, a distinct field, and does not initialize `immutableReadRoots.retention`.

Consequently selected DATA `AppendWork.Step` uses `NewGrowthForPager` with a nil `immutableReadRoots.retention`; the ordinary `Owner.Add/Remove` branches in Growth are unreachable on this route. Add a nonconstructing census boolean and reject a nonnil DATA metadata owner. Do not silently extend this conclusion to PRIMARY-bank Growth. No production Growth owner-lifecycle change follows from this DATA proof.

## Pre-cursor operands and predicates

Use the same live DATA pager and allocator observed before cursor construction. Let `P0` be logical page count, `H0` allocator high water, `A0` allocated-vector length, `C0` current chunk count, `Z` positive chunk size in bytes, `Y0=max(0,growTarget)`, `V0` verified-chunk count, `W0p` prefetch-word count, `W0d` dirty-word count, `B0` ledger burned-tail count, and `N0` ledger candidate count. Require DATA `pageIDBase=0`, file-backed supported native route, nil DATA metadata owner, no other allocator/writer/reopen producer, no change to chunk/populate options, and one cursor/one selected delete preparation. No metadata candidate has been prepared before the materialization append stage. Existing background growth may consume the initial policy target; it cannot increase that target without another scheduling producer. Native `Growth` itself does not call `maybeSchedulePreGrow`.

Let `D` be the independently proved DATA append bound (from actual single DATA-leaf shape and input capacity, not B=1 alone). Define with checked arithmetic:

```
H = max(H0,P0)+D
A = A0+D
Kbase = max(C0, ceil(Y0/Z), ceil(H*PageSize/Z))
K = Kbase+D
V = max(V0, ceil(H/65536))
W = max(W0p,W0d,ceil(K/64))
Acap = max(4,2*A)
```

`K` is deliberately conservative: each of at most D logical Growth installations may add at most one policy chunk after rounding, and capacity never exceeds the maximum earlier capacity, initial policy target, or required logical size, plus that one chunk. `growLoop` capacity-only requests have `policy=false`; they do not add a policy chunk or create a larger target. If the captured route proves no async-policy branch, `Kbase` is sufficient, but retaining `+D` is safe. A stale unpublished Growth does not add installed capacity; retrying the same append cannot compound installed headroom. These arguments do not permit arbitrary concurrent ordinary writers or future independent cursors.

`V` and `W` also cover already oversized directories. `Acap` bounds a newly allocated vector when `len==cap`: at that cut cap is at most A, so max(4,2cap)<=Acap. Initial spare capacity need not be copied if it is larger than A. Keep any separate retained-envelope census of initial backing capacity; this progress-row formula does not replace it.

The ledger does not change during these DATA appends. Native allocator preparation/claim happens after materialization has completed. `reservedLocked` visits at most B0 burned ranges and N0 candidates, with its ordinary original owner-map lookup covered by the source's fixed record. If any control permits another claim before append, derive and use larger ledger caps for that control instead. The designated later successor cannot retroactively increase the preparation rows.

## Exact source admission rows

Pairs below are `(records,bytes)`, added componentwise with overflow checks. `s(T)` means actual `unsafe.Sizeof(T{})` (pointer types use actual pointer size), never a duplicated guessed layout. Use test-only typed forwards where visibility requires them. All expressions include the source literals from append_work.go / grow_work.go.

Append rows:

```
AE = (1,2*s(AppendWork)+256)                   repeated entry
AR = (1+A,(Acap+A)*s(allocatedPage))           possible vector replacement
AL = (1+B0+N0,64+16*B0+N0*s(reservation))     repeated ledger prefix
AB = (1,s(AppendWork))                        phase-0 binding
AD = (1,s(AppendWork))                        already-installed return
GN = (1,6*s(Growth)+256)                      create original Growth
GE = (1,2*s(Growth)+1024)                      repeated Growth entry
GC = (1,2*s(Growth)+1024)                      first capture
```

Each successful AR replaces the actual vector before another refusal, AB stores phase 1, GN stores the original Growth in AppendWork, and GC sets captured. AL and GE can repeat without progress and must be included in every resumed child row.

Growth branch-unit rows:

| Source phase | Records | Bytes | Saved progress |
| --- | ---: | ---: | --- |
| 0 primary/mapping directories | 1 | K*(48+2*s(mappingOwner))+256 | phase++ |
| 1 atomic directory | 1 | 48*K+256 | phase++ |
| 2 one copied old chunk | 4 | 192+2*s(mappingOwner) | position++ |
| 3 verified descriptor | 1 | 48*V+256 | nonnil output |
| 3 one old verified slot | 2 | 144 | position++ |
| 3 one new verified backing | 1 | 2*1024*8+256 | position++ |
| 4 prefetch descriptor | 1 | 16*W+256 | nonnil output |
| 4 one old prefetch word | 1 | 64 | position++ |
| 5 dirty descriptor | 1 | 16*W+512 | nonnil output |
| 5 one old dirty word | 1 | 64 | position++ |
| 6 file size/preallocate/truncate | 1 | 2048 | phase++ |
| 7 one new mapping | 3 | 2048+Populate*Z | mapped++ |
| 8 immutable carrier | 1 | 256 | phase=9 |
| 9 append install | 4 | 6*s(Growth)+2112 | allocator/page count installed |

This table selects the actual file-backed route; the memory-only mapping row would be `2048+2*Z` and requires a different predicate. The phase-9 row includes AppendWork's callback operands `(r=2,b=64)` exactly once. Empty branches change phase without charged work; they do not reset an earlier persistent position.

Let `GS` be the componentwise SUM of every row in that table (including both mutually exclusive verified-slot rows; that harmless overbound avoids a hidden branch choice). A sufficient next-persistent-progress price is:

```
Uappend = AE+AR+AL+AB+AD+GN+GE+GC+GS
```

Why a sum of one iteration per loop suffices: either the invocation passes all earlier phases, reaching one iteration of its first unfinished loop, or an earlier charged operation itself saves progress. If a later reserve refuses, that preceding saved step survives the yield. Replayed AE/AL/GE and every acyclic constructor before the first saved step are included. This bounds progress, not total traversal work or number of turns. A caller must add its complete repeated prefix before invoking AppendWork; using max(leaf rows) alone is insufficient.

## Unpublished Growth cancellation

For original `mappingOwner.issued` file mappings, cancellation consumes a page-aligned prefix and persists it in the mapping owner before returning. A sufficient next unmap unit is `(1,2*s(mappingOwner)+128+OSPageSize)`; the original constructor requires compatible chunk alignment. The legacy unissued branch can require the entire chunk: `(1,2*s(mappingOwner)+128+Z)`. Include the latter as a conservative upper bound even if the selected original constructor proves issued=true. The memory-only/nil row is no larger. Final Growth clear is `(1,6*s(Growth)+512)`.

```
UCgrowth = (1,2*s(mappingOwner)+128+max(Z,OSPageSize))
          +(1,6*s(Growth)+512)
UappendFailure = AE+AR+AL+AB+GN+GE+GC+UCgrowth
UappendCancelUninstalled = UCgrowth+(1,s(AppendWork))
```

The last pair covers direct AppendWork.Cancel after the first allocation; enclosing caller prefixes still apply. Growth's nil metadata owner eliminates direct Owner.Remove at its final clear. An installed AppendWork cancellation takes the separate persistent trie-retirement path; this report does not substitute the uninstalled formula for that path.

## Required checks before use

Check every multiplication, sum, ceil numerator, conversion to int/int64, and `K*Z` against the source's limits. Require H*PageSize representable in int64 and enough rounding headroom for the optional policy chunk. Invalid geometry makes the family unavailable; never wrap or clamp it into a grant. Preserve the source's independent unit-too-large errors.

The record/byte pairs above do not price newly retained mappings/backings; those belong in the separately composed retained envelope, including current source backing and old/new overlap. This report closes the requested pager-progress family only. It does not establish global U_prepare/U_cancel/U_finish/U_close, actual ACK, post-successor old-reader safety, or publication durability.
