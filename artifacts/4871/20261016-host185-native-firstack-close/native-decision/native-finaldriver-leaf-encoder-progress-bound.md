# Node/leaf encoder next-progress child bound

The original plain/prefix and combined-columnar-prefix Builder child paths have finite source-derived next-progress units. Their formulas are below; add root's actual zipper parent prefixes rather than treating these as an end-to-end grant. No numeric allowance, native publication or Q32/1MiB fit is selected. No Go/runtime/source/Git/GitHub operations or children were used.

Input manifest `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`, archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`. All eight named inspected files were extracted from that archive and independently matched to the immutable full source manifest. JSON records their exact hashes. No mutable source inspected. The original actual33 fixture is `TreeDB/mvcc/native_prune_backend_route_v6_test.go:30-35`: ProfileNoWALFast, outer leaves false, pointer threshold2048. These labels alone do not prove the effective node-format flags; preserve root's actual pre-cursor option census.

## Names, inputs and supported shape

The relevant original child is **node.Builder**, not a separately allocated LeafBuilder. Calls are ResetWithOptionsWithWork; AddLeafEntryWithRevisionWithWorkAndReserve → AddLeafEntryWithRevisionWithWork or addColumnarPrefixWithWork → prepareColumnarAddScratch; FinishNoNodeWithWork → packLeafColumnarPrefixV2. All live in builder_work.go / builder_columnar_work.go; original byte stores, scratch pools and header/checksum are in builder.go.

Let vectors `(r,b)` add/max componentwise with checked arithmetic, `s(T)=target unsafe.Sizeof(T)`, `P=len(original page image)=page.PageSize`, `h=NodeHeaderSize`, `m=leafColumnarPrefixV2MetaSize`, `q=floor((P-h)/m)`, `e=s(leafColumnarPrefixV2Entry)`, `ah=s(byteArenaHandle)`, `cp=leafColumnarEntriesPoolMaxCap=1024`, `ap=builderByteArenaPoolMaxCap=64<<10`. These are original constants (builder.go:107-115); do not substitute pool warmness. `k=len(key)`, `v=len(value)`, `t=actual encoded value bytes` (ValuePtrSize/PackedValuePtrSize for pointer; zero tombstone; v inline). Use source-derived maxima K/V from root's selected decoded leaf and mutation bounds. `pv=page.ValuePtrSize`, `rv=page.EntryRevisionSize`. Prefix/plain finite path requires key<=embedded `leafPrevKeyBuf` length64 whenever prefix enabled (builder.go:44; builder_work:35). Combined-columnar prefix supports longer keys, with original uint16 prefix/suffix validation and full original key-copy admission.

Required selected shape: adaptive leaf encoding false before option selection; native finite codec mode, no unbounded diagnostic fallback; either !LeafColumnar or LeafColumnar&&LeafPrefixCompression; no internal-base-delta child; fresh builder (or explicitly bounded old internal scratch); no outstanding adding/packing on reset. Columnar handle from successful original combined Reset must exist before optional empty-builder entry-vector growth. Parent key/revision selection and ProjectionDecoder/ProjectionComparison are root-owned and not duplicated here.

## Reset and plain/prefix path

ResetWithOptionsWithWork (columnar_work:33-53) actual unit:

```
Reset=(1,s(Builder))
     + combined*((2,cp*e))
     + (nOldInternal,nOldInternal*s(internalBaseDeltaEntry))
```

It reserves BEFORE original ResetWithOptions, including ReleaseScratch, original pool acquisition and possible cold backing. Fresh frames have nOldInternal=0. Original combined Reset acquires an entry handle/capacity; original maximum returned pool capacity bounds cold and warm variants. Reset does not initialize/zero the page image; parent owns image birth. Leaf scratch release returns up to three original handles, does not clear all entry-array capacity and has no cardinality loop; only original old internal-entry clear contributes nOldInternal.

Plain/prefix Add (builder_work:27-52) admits the entire potential compare and mutation before original addLeafEntryWithRevision:

```
p = prefix && nonRestart ? min(k,len(previousKey)) : 0
PlainAdd=(p+6,2*p+3*k+v+pv+rv+64)
PlainFinish=(2,P+h)
```

Use p<=min(K,64) on the finite prefix route; restart/no-prefix p=0. Actual compare may stop early, but the unit still charges p. There is no independently resumable compare cursor in this path: **whole PlainAdd plus repeated parent prefix must fit**. Original mutation updates count/previous-key state only after this admission. ErrNodeFull remains original full/split control, never a permission to consume the selected event. With nonzero soft reserve and nonempty noncolumnar builder, AndReserve refuses ErrBuilderWorkFormat before Add; a larger G cannot fix an unsupported format/reserve configuration (builder_work:82-93). Finish admits original header writes and whole-page checksum, and creates no Node wrapper.

## Combined-columnar-prefix Add

Actual reservations and persistent progress (columnar_work:114-191):

1. Bind original borrowed immutable entry `(1,64)`. It stores adding, key/value/ptr/flags/revision/probe; later calls enforce the SAME operands.
2. Each compared byte `(1,2)`; c.probe/c.prefixReady persist. Restart yields no comparison. A mismatch stores prefixReady before later scratch or append can refuse. No repeated prefix search.
3. Exact capacity/uint16/soft-fill checks occur before new scratch or entry mutation. They are fixed scalar/head checks, not input-sized scans; ErrNodeFull clears the borrowed-entry state and preserves original parent event custody.
4. prepareColumnarAddScratch reserves the following actual sum (59-109):

```
Scratch = needEntries*(1,q*e)
        + needKeyArena*(2,ap+ah)
        + needInlineValueArena*(2,ap+ah)
```

The conservative cold maximum is `(5,q*e+2*(ap+ah))`. Original pools use 4096-byte cold buffers but may retain64KiB; ap is the maintained pool cap, not an invented fixture allowance. needEntries requires count==0; make has zero old entries to copy. Original Reset's entry handle is retained, so the optional header allocation branch for a foreign handle-less builder is excluded by supported construction. Each successful scratch preparation records pointers/scratchReady before the later append reservation. It therefore constitutes persistent child progress even if append then yields. Later additional inline-value arena acquisition follows the same path and cannot be skipped merely because a previous pointer entry set scratchReady.
5. Original append unit `(6,2*k+v+t+e+64)` before suffix/value/metadata/previous-key writes. t<=max(v,pv). The original Add validates page capacity before writing. Combined long-key previous-key allocation/copy is within this whole key-byte unit; do not apply plain path's64 limit to combined format. After original Add returns, clearColumnarAdd detaches borrowed entry operands.

The Add next-progress bound is max of bind, one probe, Scratch and whole append, **not K probes in one indivisible unit**. Scratch and append need not be summed to avoid a stall: scratch success changes persistent scratch pointers/ready before append may yield. This differs from reservation-normalizer scratch work, where atomic Owner Add must fit together with its preceding array reservation before a stored phase changes.

## Combined pack and final header

packLeafColumnarPrefixV2 (208-292) recomputes a fixed-size directory-offset prelude on every call: count/flags, key/value byte totals and original offsets only. It does not scan the entries for offsets. Unit sequence is:

| Source | Actual unit | Stored progress |
|---|---|---|
|232|`(1,64)`|packing=true and initial key/value offsets|
|244 all-inline value bulk copy|`(1,2*len(valueArena))`|phase0→1; arena length must equal original value-byte total|
|256 all-inline/all-pointer key bulk copy|`(1,2*len(keyArena))`|phase1→2; arena length must equal original suffix-byte total|
|268 each metadata/payload entry|`(8,2*(suffixLen+valueLen+pv)+m+rv)`|c.index advances exactly one on finite work; offsets persist|
|281 scratch release/final offsets|`(3,64)`|original up-to-three pool returns, aliases cleared; packed=true,phase3|
|builder_work:63|`(2,P+h)`|actual original header/checksum; no Node allocation|

A page-capacity invariant from original Add bounds key/value arenas separately by P and bounds each stored suffix+inline-value payload by P; hence safe page-shaped maxima for these units are `(1,2P)` and `(8,2*(P+pv)+m+rv)`. They do not depend on all33 historical entries fitting in one work turn. The number of entry iterations is at most q. Full original keys can exceed suffix lengths; Add's K/V grant and pool append capacity checks are separate. All-inline and all-pointer specialized bulk copies preserve original mixed-pointer/inline packing logic; no alternate encoder is introduced.

After packed=true, a later header reservation refusal resumes pack as an immediate success; it does not copy or release scratch again. If the parent hasn't persisted its phase after a fully successful header/checksum, Finish may be invoked again and repay that fixed checksum unit. Root must include its actual repeated parent reservations. Header finalization itself has no child completion bit outside packing; do not infer it from output page ID.

## Child expression and parent lifting

A conservative typed supported-format child envelope is:

```
Uleaf = max(Reset,
            PlainAdd(K,V), PlainFinish,
            (1,64), (1,2),
            (5,q*e+2*(ap+ah)),
            (6,2*K+V+max(V,pv)+e+64),
            (1,2*P),
            (8,2*(P+pv)+m+rv),
            (3,64), (2,P+h))
```

Keep flags to select reachable branches if a smaller formula is useful; summing these alternatives is also conservative but not necessary. No new s(Builder) per-entry charge is an **actual existing Reserve**: fixed identity/format/offset guard reads are child scalar prefixes, with no input-size loop. Any newly selected explicit containing-entry traffic must be named by the writer, not retroactively claimed admitted by a larger fixture G.

Root's verified parent lift remains necessary: materialized phase13 checksum `(1,P+256)` repeats before successful Reset; phase8 repeats completed Append.Step and `(1,8)` before Finish; split reset chooses the immutable suffix options. Repeated projection/merge/revision selection and allocator/pager units are outside this child advice. Internal fixed-layout sibling, if root's selected branch creates one, has Add `(6,k+64)` and Finish `(2,P+h)` (internal_builder_work:11-33), provided internalBaseDelta=false; root owns its parent routing/cardinality.

## Conditional admission holes / exclusions

There is a concrete unsupported-constructor branch in immutable source: **LeafColumnar=true and LeafPrefixCompression=false** receives only the base Reset work bytes, yet original Reset acquires/cold-allocates columnar-V2 entry scratch (builder.go:319-337). Later finite Add honestly rejects that format, but that does not undo already unadmitted constructor work. Similarly original InternalBaseDelta Reset acquires entries+arena; the leaf combined acquisition term does not price that constructor. These are not fixed by selecting a larger G: require original effective flags exclude them before those constructors, or a separately selected source admission seam. No production change is justified if the actual selected effective flags exclude both. Existing plain/nonzero soft-reserve refusal is also a capability boundary, not an I/O failure or permission to switch to nil work.

For the supported fresh one-DATA-leaf shape, I found no additional unadmitted input-sized scan or constructor in these node child paths. Persistent compare/pack cursors, maintained pool caps and whole-page checksum close the scalable work. This statement excludes option/adaptive heuristics, wrapper allocations, allocator/page persistence and any unmodeled original internal/base-delta path. It does not change accepted DATA/output/GC-service retention boundaries or enroll original pool storage into another Owner.

Reviewer /root/ordinary_review; exact gpt-6.1-sol/high, independent immutable-source advice, candidate edits false. This is a source-bound child formula, not source acceptance of mutable finaldriver implementation or runtime authorization.
