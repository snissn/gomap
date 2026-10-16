# Tree ChargedProjection next-progress bound

Read-only symbolic advice on immutable source `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d`, archive `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`. Reviewer: independently routed gpt-6.1-sol/high. No source changes, execution, numeric G, allowance, or publication acceptance. Six source files were extracted and individually verified against the manifest. This completes the previously excluded tree iterator child family; it does not replace its caller's repeated entry prefix.

Use checked vector addition and componentwise maximum, `(records,bytes)`. `s(T)` is actual original-package sizeof, `P=page.PageSize`, `K<=P` is the supported maximum comparison/copy key length. Reuse **FrameBegin, FrameStep, FrameCancel, DecodeBegin, DecodeStep, DecodeCancel, CompareBegin, CompareStep, CompareCancel** from root's exact-source zipper report. For a conservative whole comparison continuation use `CompareWhole=(0,2*K)`; its offset persists. The completed comparison has no extra child Reserve. Do not substitute a larger G for any unadmitted child operation identified separately in root's report.

## Constructor, admission, operation entry and witness

`tree/charged_projection.go:95-125`: existing `ChargedProjectionAllocationSize()` is `s(ChargedProjection)`; `ChargedProjectionRetainedCharge()` is the existing allocation charge of that size. The allocation contains all 50 fixed page frames, delta page, key arrays, base stack, decoder/comparison and mutex. There is no separately allocated page/key/stack backing at this constructor.

`New=(1,ChargedProjectionRetainedCharge()+s(tree.Tree))` is reserved before allocating/copying the tree header. It checks the original concrete frame-reader capability and certificate scalar identity, then leaves the iterator UNPOSITIONED. No DecodePrimaryDirectory, seek, pager read or callbacks occur. This is work admission, **not** new retained-owner admission: the original caller must already have admitted this exact allocation before calling it. The certificate owns no new Ref; the original captured root edge must survive every borrowed iterator view until Close.

`Begin=(1,s(node.LeafEntry)+64)` at line141; `BaseBegin=(1,64)` at line741. Both reserve before installing pending state. `Step` (185-206) adds no generic entry Reserve. Its mutex excludes simultaneous operation/state mutation; returned data are borrowed, not independently pinned.

`Record=(1,2*s(node.LeafEntry))` at627. `NativeMVCCSelectionV6` in `native_mvcc_selection_v6.go` reserves `(1,2*s(NativeMVCCSelectionV6)+len(record.Key))`; matching the stationary exact selection reserves `(1,2*s(tree.Tree)+2*s(node.LeafEntry)+2*len(record.Key))`. These price the original witness copies/hash/equality; no root rediscovery or allocation. Include these only where the real caller invokes them, and retain the source root until the borrowed key is no longer used.

## Top-level Step units

The following table lists actual local Reserve units. A phase transition occurs after each successful unit unless its compound child is called out. A table row is an alternative, not a claim all rows execute on every entry.

| Phases / source line | Local unit or original child |
|---|---|
|0,1 /211,222: certificate checksum then digest |each `(1,P+64)` |
|2 /230: adopt validated directory/base view |`(1,s(node.PrimaryOperand)+s(node.PrimaryDirectoryView))` |
|3 /239: source advance flags |`(1,64)` |
|4,5 /250,259 |`BaseBegin`, then `BaseStep` family below |
|6 /271,282,291 |`(1,8)` for advance/first/last, or `(1,24)` for seek setup |
|7 /300,310 |`(1,8)` for termination or `(1,s(node.PrimaryDirectoryEntry)+64)` for one pivot |
|8 /321 |`CompareBegin` |
|9 /326-338 |**`CompareWhole+(1,16)`** before lo/hi is saved |
|11 /341 |`(1,s(node.PrimaryDirectoryEntry)+64)` for one selected entry |
|12 /352 |`(1,32)` |
|13 /370 |`CompareBegin` |
|14 /375-391 |**`CompareWhole+(1,16)`** before merged-source selection is saved |
|16 /394 |inline absence `(1,s(node.LeafEntry))` |
|17 /403 |non-inline reference validation `(1,s(page.ChildRef)+32)` |
|33,18 /411,418 |`FrameBegin`, then `FrameStep`; completed frame advances phase without a trailing Reserve |
|19 /428 |`(1,P+s(node.Node))` checksum/type/single-record frame validation |
|20 /440 |`(1,P+32)` digest |
|21 /448 |`DecodeBegin` |
|22 /454-464 |**`DecodeStep+(1,s(node.LeafEntry))`** before publishing decoded record to c.record |
|23,31 /467-478 |`CompareBegin`, then `CompareWhole`; equality checked before phase25 |
|24 /481 |base record copy `(1,s(node.LeafEntry))` |
|25,26 /487-515 |optional `CompareBegin`, `CompareWhole` for start bound |
|27,28 /517-538 |optional `CompareBegin`, `CompareWhole` for end bound |
|29 /540 |successful position `(1,32)` |
|30 /549 |exhausted position `(1,s(node.LeafEntry)+32)` |
|40 /564,571 |copy `(1,2*amount)`, total amount<=K; then separate `(1,96)` |
|41 /588-600 |**`CompareWhole+(1,64)`** before skip-current state is cleared |
|32 /604 |out-of-bound physical advance `(1,32)` |

The bold compounds are necessary because the parent phase remains unchanged if the trailing Reserve refuses after child completion. On retry the completed child is observed again. Comparison/decode continuation offsets do not restart. Counting a whole bounded child is conservative and avoids depending on completion-call zero traffic. The phase40 copied offset persists before subsequent reservation; `(1,2*K)` bounds its largest chunk and `(0,2*K)` bounds total key-copy bytes. No previous copied prefix is recopied.

## Nested one-leaf BaseStep units

| Phases / source line | Unit |
|---|---|
|0 /764 |one old stack clear `(1,s(tree.CursorItem))`; stored clearAt progresses |
|0 /771 |reset `(1,64)` |
|1 /779 |ref guard `(1,s(page.ChildRef)+32)` |
|31,2 /792,799 |`FrameBegin`, `FrameStep` |
|3 /817 |stream CRC `(1,amount)`, amount<=P, cumulative bytes<=P; crcOffset persists |
|3 /840 |completed checksum and stack item install `(1,16+s(tree.CursorItem))` |
|4 /866 |nonzero base digest `(1,P+32)` |
|5 /876 |count/index selection `(1,32)` |
|6 /900,914 |seek terminal `(1,16)` or pivot `(1,8)` |
|7,8 /920,926 |`DecodeBegin`, then `DecodeStep` |
|9 /932 |`CompareBegin` |
|10 /938-949 |**`CompareWhole+(1,16)`** before binary-search lo/hi changes |
|12 /954 |leaf index store `(1,8)` |
|15,16 /977,984 |`DecodeBegin`, then `DecodeStep`; phase is saved before following record copy |
|17 /990 |record copy/position `(1,s(node.LeafEntry))` |
|20 /1003 |next/previous leaf index `(1,8)` |
|21 /1014 |one pop `(1,s(tree.CursorItem))` |
|30 /1037 |exhausted record `(1,s(node.LeafEntry)+32)` |

Internal-tree child lookup at phase13 `(1,P+s(page.ChildRef))` and parent traversal phase22 `(1,16)` are **not needed for this selected one DATA leaf**. Do not present this report as closing arbitrary-height internal decoding. The selected leaf depth is at most1. The embedded corruption guard/Close clear length remains `maxTraversalDepth=50` (`tree/tree.go:55-58`). Page-child refs, local DATA pager, actual fixed P frame output and no external leaf branch are required. Root's reader/capture identity and pager-image-admission conditions remain in force.

## Composition and finite progress

A usable child envelope is the componentwise maximum of `New, Begin, BaseBegin, Record, Selection, SelectionMatch`, every local table row, the named Frame/Decode/Compare units, and the bold compounds. Call it `U_projection`. Add the **real outer caller entry prefix** to every alternative that it wraps before combining with other families in G. There is no additional tree Step entry prefix. Do not multiply all binary-search or scan entries into an indivisible unit: each pivot, comparison offset, CRC offset, leaf index, directory position and phase survives budget refusal.

For finite traversal, with immutable selected DATA-leaf count n and validated directory count d, a lower/upper bound seek performs at most `ceil(log2(n+1))` base pivots and `ceil(log2(d+1))` directory pivots. A direction change adds one persisted key copy and a seek. An operation that filters physical entries advances the base index and/or directory position; equal keys advance both on the subsequent move. A conservative count for candidate iterations until a result/exhaustion is `n+d+2` for that operation, with up to one merge comparison, one delta-key validation comparison, and three bound/skip comparisons per candidate. The base performs up to one page frame acquisition for an initial seek on this one-leaf route; a non-inline selected delta can acquire its own fixed frame. Subsequent operations can seek/read again and must each be counted by the actual outer schedule. **BatchSize1 bounds the selected mutation, not the number of scanned physical records.** n/d must come from the immutable captured nodes/directory or source-derived page capacity, not historical33.

Certificate `Directory()` is a stored validated view, `Count()` a scalar, `Base()` a fixed operand decode, and `Entry(i)` one fixed physical record + borrowed key slice (`primaryarena/read_root_v6.go:238`, `node/primary_directory_v1.go:217-238`). No Search/DecodePrimaryDirectory call or whole prefix scan is hidden in these accessors. The separate plain tree primary directory helpers are not invoked by ChargedProjection. Its constructor and frame child retain the exact original source objects, without a fresh allocation/owner domain per call.

## Close reuse and scope

The exact-source terminal report already supplies the complete finite Close expression: start `(1,32)`, 50 stack clears `(1,s(CursorItem))`, two decoder and two comparison cancels, alias-clear `(1,2*s(LeafEntry)+s(PrimaryDirectoryEntry)+s(PrimaryDirectoryView)+s(node.Node)+256)`, original reader cancel, final `(1,s(tree.Tree)+96)`. Reuse it unchanged. Close detaches views before cancelling the frame reader and scrubs Tree/certificate last; the enclosing captured root/containing charge must survive until these original closes genuinely complete. No additional root Drop/refund is performed here.

Supported scope: fixed original capture/reader, one local DATA leaf (possibly viewed through a PRIMARY directory), outer leaves off, empty external-vlog reader route, keys/values within root's supported page bounds, B1 delete, no extra pre-ready producer, idle designated held reader. This establishes symbolic tree child work under those predicates, not source acceptance for a future driver, a complete retained union/Finish bound, numeric allowance, Q32 fit, or actual publication. No runtime was performed.

## Bindings

- `TreeDB/node/primary_directory_v1.go` SHA256 `532160409cbac93085b6884c29994fbdaf8801d8fc7fa4200f6aa9e35bca0230`.
- `TreeDB/internal/primaryarena/read_root_v6.go` SHA256 `ebb053bf4be6d291d6fb38aed2969572c06cc397337ad07b526e1a120567141c`.
- `TreeDB/tree/native_mvcc_selection_v6.go` SHA256 `9679a06002cacfde901e707ff314461d53d5c8005172b27aca1a6c2fda4bf834`.
- `TreeDB/tree/charged_projection.go` SHA256 `fbbba00616eff7eb91ec385faf22582b968133807bfbc9e1765f3bb2749e4c2d`.
- `TreeDB/tree/tree.go` SHA256 `1ddafdc83e6a4f8037593faeaf897ae039675187d78ab4168fd3c45b654f3564`.
- `TreeDB/tree/primary_directory.go` SHA256 `4f393a3d62d2d585b4fee908ff81308b5b7d21434745e7127906f4182ab2873e`.

Reused reports:
- `native-finaldriver-zipper-progress-bound.md` SHA256 `7145e8b12c57130495505afce2cc04360f83392f773d41dc0f30f1b4f8cd414b`.
- `native-finaldriver-zipper-progress-bound.json` SHA256 `0af33d54883ea78c3c53a5fedf0b92436860cf5959080db113fe250a8d2a9046`.
- `native-finaldriver-terminal-unit-bound.md` SHA256 `4e170295e7cb844c85fef3569899dd7991b6ebeb64c1b55c651af5ea78700e4d`.
- `native-finaldriver-terminal-unit-bound.json` SHA256 `e2c527c5874848a6794adeee3b4d616887cc377831d89f0754613682cc3499fe`.
