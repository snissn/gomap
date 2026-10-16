# Selected one-leaf materialization progress units

Source-only root advice bound to immutable source manifest5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d. The 41 inspected/extracted files in finaldriver-zipper-progress-root-read are hash-verified against that manifest. This report composes the actual materialized input, frame reader, projection decoder/comparison, leaf selection, DATA append/write and installed-page validation prefixes. Original Builder child units are separately assigned. No numeric G or execution follows.

Use checked pairs (records,bytes); s(T) is the compiler's actual sizeof, P=page.PageSize, p=pointer size. O=directory.Count()+1 bounds input entries for the selected empty prefix and B=1 delete. D=max(1,baseLeafEntries+O) is the accepted bound on output frames/append attempts, including partial output before unsupported root split. R=D+I bounds retired IDs; A bounds each live/private allocated vector. Add is the complete original Owner.AddPendingWithOrdinalWork unit and Remove the existing actual removal unit. Sum named branch rows as a conservative next-persistent-progress bound; include repeated caller prefixes even after a child completes.

## Required selected predicates

The captured base operand must be one local DATA leaf, not an internal tree, outer leaf or alternate pager. The original basis may be a PRIMARY RAM directory; it is decoded by Input before the DATA leaf is selected. Prefix is empty, one selected delete, outerLeavesInValueLog=false, unboundedLeafPersistenceDiagnostic=false, and no independent writer/reader changes the actual mapping verification word during the private output write. The held old reader may remain alive without concurrent Get/Prefetch activity. The native plus designated successor root-table bound remains L=Npre+6.

Effective adaptiveLeafEncoding must be false. Its true branch in leafBuilderOptionsWithEntryRevisions acquires pooled scratch, may allocate, walks operations and may sort before Builder receives work; a G expression alone cannot admit that path. Exact effective leaf flags must also satisfy the separate Builder report: in particular LeafColumnar without LeafPrefixCompression is unsupported before reset; internal-only scratch paths must be unreachable. The selected one-leaf route does not use internal child traversal or the outer-leaf wrapper.

batchHasEntryRevisions is short-circuited if the captured DATA leaf already enables revisions. Otherwise it reads at most O revision fields before reset, with no separate work Reserve in source5cde. Prefer the actual supported leaf-revisions predicate; do not describe an enlarged G as adding execution admission for this scan. Subsequent split reset reuses the enclosing already determined revision flag and, with adaptive disabled, does not scan its immutable ops suffix.

All materialized/decoded keys must fit their original P-byte scratch. A component is one immutable leaf page, so key and value are each <=P and their copied sum <=2P. Directory keys and selected delete keys are <=P. These follow the real validated input geometry, not a new key limit. Ref-count branches are fixed inline metadata; empty value-log shape excludes adding external file references.

## Original frame reader and pager

For local DATA, Get=(3,512). A PRIMARY bank dispatch calls the same companion Get once; RAM-root lookup additionally requires the selected correction root-native-finaldriver-root-image-admission-addendum.md. Its original image table must admit table header and each link under the same RLock. With Lookup(L)=(1+L,s(table)+L*(p+s(entry))), a safe all-selected-route Get superbound is:

```
GetSelected = (7,1088)+Lookup(L)
FrameBegin = (1,s(NativeMVCCFrameReaderV6)+s(page.ChildRef))
FrameStep = (1,128)+GetSelected+(1,2*P)+(1,64)
FrameCancel = (1,s(NativeMVCCFrameReaderV6))
```

The 7/1088 Get prefix is DATA Get plus companion Get plus the existing RAM-case1/64. Local DATA uses the smaller Get. Step phase1 repeats its1/128 before Get; phase2 saves copied count after each actual copy, and phase3 saves completion. One byte (records1/bytes2) is enough for copy progress after its parent, while using full2P is a safe fixed-page bound. Empty value-log and page references exclude external COWLeafProjectionReader file/decode methods here. Ordinary nil Get semantics remain the same through the selected lookup fix.

## Projection decoder and comparison

Exact original node rows give:

```
DecodeBegin = (1,s(ProjectionDecoder)+s(Node))
DecodeStep = (1,P+s(LeafEntry))+(1,2*P)+(1,s(LeafEntry)+s(ChildRef))
DecodeCancel = (1,s(ProjectionDecoder))
CompareBegin = (1,s(ProjectionComparison))
CompareStep = (0,2)  # one persisted compared byte
CompareCancel = (1,s(ProjectionComparison))
```

Decoder header parsing has fixed field reads; its saved phase, predecessor index and copied offset prevent replay of a whole prefix chain. The source begins at the original restart interval, but no restart-count multiplier is required for next progress. If prefix==0 the key borrows its immutable suffix; otherwise total reconstructed key must fit the original scratch and each copy advances copied. Comparison saves offset before its next reservation and can progress with two bytes; the exact whole comparison bound (0,2P) may be used conservatively. Finish after the last compared byte has no separate reservation.

## Materialized input

The following include the selected three real Owner-admission corrections at birth, entries backing and component payload, root-native-finaldriver-materialized-admission-addendum.md finalSHA167936cbe358cccc1ff0528c71d4e0449fa68c85cd115eac54d0a41c9fefb3ea. Each compound reserve+Add must fit together before allocation; merely pricing the previously direct Owner.AddPending is not enough.

```
InputBirth = (1,2*s(MaterializedInputWorkV6))+Add
EntriesBirth = (1,2*O*s(batch.Entry))+Add
PayloadBirth = (2,6*P+2*s(batch.Entry))+Add
```

Exact step rows, summed once including exclusive alternatives, are:

| Phase | Bound |
| --- | --- |
| 0/1 root read | FrameBegin+FrameStep |
| 2 checksum/certificate | (3,7P) |
| 3 entries backing | EntriesBirth |
| 4/5 base read | FrameBegin+FrameStep |
| 6 base verify | (2,3P) |
| 7 directory entry | (1,P+2*s(PrimaryDirectoryEntry)) |
| 8/9 component read | FrameBegin+FrameStep |
| 20 component verify | (2,3P) |
| 21/22 | DecodeBegin+DecodeStep |
| 23 exact record match | (2,2P) |
| 24 copied payload | PayloadBirth |
| 10 sorted three-head merge | (3,6P+4*s(batch.Entry)) |

The merge bound includes three P-byte keys even though selected prefix is empty. Every successful phase10 step advances component/prefix/delete index before returning to the loop and appends within O capacity. Component index advances after payload birth or inline absence; it never needs all directory entries in one invocation. Child frame/decoder phases also persist. InputResult and the DB parent only copy fixed existing metadata after completion; the final writer must include its actual containing scalar traffic if a new wrapper prices it.

CloseStep calls DecodeCancel AGAIN on every invocation, then clears one component (1,2P+2*s(batch.Entry)) or one output descriptor (1,2*s(batch.Entry)), each with its own saved index. Final reset is (1,2*s(MaterializedInputWorkV6)). Therefore a safe close unit is DecodeCancel plus the sum of those three rows. The caller's CompletePendingWork entry runs before CloseStep and again after final alias detachment; include both actual parent rows and Remove. No charge is refunded before both component and output aliases are withdrawn.

## Leaf merge cursor and writer

LeafMerge.next has a repeated phase0 unit before decoder Begin, then saved decoder/comparison phases and a saved event/indices update. With ranges=nil on this route:

```
LeafMerge = (1,128)+DecodeBegin+DecodeStep+CompareBegin+CompareStep
          +(1,2*s(LeafEntry)+64)+(1,s(LeafEntry)+32)
```

An optional whole-key CompareStep=(0,2P) is safe. The last two rows cover choice/event consumption and old-entry emit. A deleted/tombstone record advances the same input cursor without output. The selected writer passes nil ranges; no range-loop price is omitted on this shape.

Let WB=s(MaterializedWriteWork), WF=s(materializedWriteFrame), and Reset/AddLeaf/Finish be the actual supported Builder child units from the companion report. Append is the complete root pager-growth/allocator Append prefix. The finite birth is (2,2WB+2WF)+Add, aggregating owner and first frame before allocation. Split frame birth is (1,2WF+P)+Add. A conservative writer step bound is the sum of:

| Writer phase | Required compound bound |
| --- | --- |
| 0 DATA load | (3,512)+(1,2P) |
| 13 verify and reset | (1,P+256)+Reset |
| 1 leaf selection/removal/add | LeafMerge+(1,256)+AddLeaf |
| 8 append/finalize | Append+(1,8)+Finish |
| 9 physical write and result | (5,2P+1024)+(1,P+128) |
| 14 split frame | (1,2WF+P)+Add |
| 15 split reset | Reset |

There are crucial restart prefixes. Phase13 does not save its checksum before Reset succeeds. Phase8 calls Append.Step on every retry even after installed (that completed child still charges1/s(AppendWork)), then charges1/8 before Finish. Phase9 repeats physical Pager.WriteWithWork before its final1/(P+128) result row; written=true alone is not completion and does not bypass the write on retry. Summing full Append and Finish safely covers this. The actual pager invalidation CAS is bounded only under the selected no concurrent mutation predicate; G cannot bound hostile contention. A retained old reader that is idle does not invalidate the predicate.

The initial frame is the sole DATA leaf. Every completed nonfinal split emits at least one entry before allocating its sibling; emitted entries<=baseLeafEntries+O, so D covers frames and all partial append attempts. Root split eventually returns the existing unsupported-continuation error; this report does not turn it into successful publication. A successful one-leaf native route has no internal recursion or output wrapper; those source phases are not included.

## Installed-page validation: whole replayed scan

Unlike the preceding saved loops, ValidateCOWInstalledPagesV6 has no saved frame cursor. Each full attempt scans all frames, and ValidateInstalledPageV6 scans both original/live and private allocated vectors plus the retirement inventory. The original native allocator wrapper has exactly one COWRetirementV1 group. A safe whole validation bound is:

```
OneInstalled = (1,2*s(AppendWork)+64)
             +2*A*(1,2*s(allocatedPage))
             +(1,2*s(COWRetirementV1))+R*(1,16)
ValidateAll = (1,2*WB)
            +D*((2,4*s(AppendWork)+2*s(ChildRef)+32)+OneInstalled)
```

This bound includes the exact no-reserve final alias clear covered by the original frame admission. It may overcount oldID==0 frames and shorter vectors; do not replace it by one frame or one allocated entry. The same allocator/ledger cut must protect validation through adoption. The callback/parent activation reserves before this scan remain mandatory.

VisitPagesWithWork has a saved cursor: first bind(1,s(MaterializedPageVisitWorkV6)), then each callback prefix(1,24). AdoptPagesWithWork has a saved adoption cursor with per-callback(1,s(AppendWork)+32). Their actual caller callback must be included. AdoptCOWInstalledPages is a nonfallible scan only after the whole ValidateAll and exact adoption under that protected cut.

CompleteWithWork clears one frame for(1,WF), saves head, then the container for(1,WB); a safe sum covers both. Cancel with outerLeavesOff has no leafAppend owner. It calls each original Append.Cancel (including genuine installed retirement or uninstalled Growth.Cancel) then(1,WF), saves head, and never drops an installed-page disposing edge on refusal. Include the completed Append.Cancel reset row before frame detachment and the caller's final containing refund. The accepted allocator/state and pager-growth reports supply those children.

## Remaining integration obligations

Evaluate the companion Builder child expression, bind supported flags/actual source leaf revisions, implement both selected execution-admission corrections, and use current sizes after all source changes. Every child/whole-scan expression needs its actual repeated backend/cursor entry. This report does not cover ChargedProjection top-level scanning, primary constructors, final-fence F, terminal coordinator/queue/DB.Close or retained-capacity accounting; those remain separately required. Overflow, unsupported flags or capability leave the finaldriver envelope unavailable before cursor birth.
