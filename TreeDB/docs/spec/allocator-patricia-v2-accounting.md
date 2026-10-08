# Patricia V2 allocator accounting and qualification

This contract covers the incompatible pre-alpha allocator representation in
#5108. The canonical prepared caller admits P=8192 existing pager pages and
Q=237942 total output pages summed across its source-derived per-root bounds,
including joint allocator and auxiliary output. The candidate state ceiling is
H=P+Q=246134 pages. Q does not bound the sum of all retained generation
high-water values. F=32 caps registered value-log files, pending leaf file IDs
and the prepared reader's registered files; it does not cap all retained handles.
V=64 separately caps visible resources, visible members, allocator debt and
seals. Base admission requires visible members and seals strictly below 64 and
allocator debt at most 62. Every retained old, captured, candidate and rollback
representation and its controls counts separately against the unchanged
128 MiB metadata tranche. These source limits do not admit a request whose
complete joint source/caller byte certificate is absent.

## Mutable state and emission

One value edge owns either a branch or a 256-page chunk. Branches split at the
first differing nibble and have at least two children; deletion removes empty
chunks and immediately transfers the sole child before releasing a unary
branch. For C>0 nonempty chunks there are at most C-1 branches. One chunk is the
direct root; empty materialized state has exactly one chunk sentinel. Dirty
emission writes actual dirty chunks and branches, or that sentinel, plus the
normalized reservation chain and one generation header. A forced unchanged
nonempty candidate rewrites its root, without rewriting unchanged descendants.

Visible candidates and the durable seal remain separate materializer
generations, with separate parent and commit bindings, metadata intervals,
reservations and horizons. A private build group exposes only its final
visible candidate. An ordinary queued prefix may contain several visible
candidates, each preceding the separate seal; input builder count cannot stand
in for actual visible-prefix count. Owned-manifest work joins the existing
finalization transaction.

Reservation entries occupy 24 bytes, with 163 entries per 4096-byte reservation
page after its 176-byte header. Tail sizing uses the actual assembled extent
cardinality after abandoned-prefix coalescence; every conflict recomputes its
fixed point. Production normalized base extents end at or before the original
minimum and have no target-metadata extent. Reusable metadata attempts at most
four chunks and requires its interval count to be strictly less than that
chunk's free count, so the selected path cannot collapse during sizing. Data,
target metadata, pending metadata retirement and abandoned appends keep their
separate classifications and exact normalized intervals.

## Full backing and creator lifetime

The Linux/amd64 Go 1.26.3 class expectations are:

| Backing | Raw bytes | Full class charge |
| --- | ---: | ---: |
| Patricia branch | 336 | 352 |
| State chunk including all 256 retirement entries | 2136 | 2304 |
| Immutable branch encoding plan | 592 | 640 (noscan) |
| Transaction header | 368 | 384 |
| Generation header object | 320 | 320 |
| Value edge embedded in its owner | 16 | No separate allocation |

These expectations require actual pinned Linux allocation witnesses. A raw
byte delta, page saving or Mac semantic pass is not a class certificate.
For C>0 the state-only class bound is 2304*C+352*(C-1); a valid high-water H
bounds C by ceil(H/256). At H=246134, C<=962 and the state-only bound
is 2554720 bytes per representation. Shared immutable subtrees may reduce unique backing,
but retained generation/rollback/candidate representations are conservatively
counted separately without an owner registry. Two representations are not the
complete caller: held physical cuts, queued visible candidates, the seal and
its rollback transaction can all remain live together.

Every existing charged branch/chunk, numeric-radix chunk, vector replacement
and creator wrapper is prepaid before birth. Full typed numeric-radix chunks
include all 32 slots and intrusive live/available links. Partial deletion does
not refund a class or rebind a surviving object to a newer request. Reuse of
an old creator remains valid after that request retires; its actual owning
references keep the original resident scope alive. Final release clears aliases and full owned
backing before dropping the last creating edge. Cumulative allocation debit is
not a live-byte refund ledger.

Retained allocator census includes allocator/COW/condition/writer controls,
activated-vector capacity, generation/tree classes, generation reservation
extent and metadata-ID capacities, transaction headers and allocated/abandoned
vector capacities, changed/replaced numeric-radix backing, prepared/candidate
controls, auxiliary/dirty-ID capacities, page-view vector capacity and every
retained page buffer or branch plan, physical-cut wrappers, shared reservation
ledger/radices, reservation headers/ID/coverage capacities and burned-tail
capacity. Shared creator wrappers/representations may be conservatively
counted more than once. `ResidentGenerationProfileV1` is a retained census,
not unique heap-instance counts or complete cumulative births.

The caller must additionally admit all resource sets, dependency manifest or
directory backing, snapshots and pins, visible-member/debt/seal/prefix storage,
root-publication candidate controls and terminal ownership, plus input/point
plans and output pager work. Transient materialization needs allocation-vector
sort copies, data-ID scratch, extent assembly with in-place normalization,
reservation page headers/IDs/digest scratch, encoding scratch and sink-specific
isolation copies. Full capacities and old/new growth overlap count; changing
slice length or limiting capacity on a view does not shrink the original
backing allocation. An ordinary generic sink may retain its copies indefinitely;
the candidate owns immutable branch plans and freshly encoded non-branch
buffers. The exact production sink is a zero-field immutable value; it creates
no ID directory, validation-store duplicate or retained callback.

## Recovery lifetime and maintenance

The exact fixed two-slot page/commit/digest bindings are validated before
candidate preparation. An overwritten root record remains retired through the
maximum commit of its own slot or either slot that names it as immediate
parent. Its manifest pages retain their own slot horizon. Shared inventory
partitioning adds no retained page-ID owner. Both direct preparation and the
queued seal use this helper; prior-seal auxiliary overlap retires last at the
selected current commit. There is no recursive history scan or format fallback.

Promotion/reuse remains strictly earlier than every opaque recoverable-root,
snapshot-pin and retained-history horizon; equality is protected. Bounded prune
retains the 512 node-visit, 64 examined-entry and 16 promotion limits with its
full traversal/mutation credit precharge. Failure or abort preserves exact
rollback aliases and conservative attempted tail burns. A prepared retry uses
its immutable candidate rather than applying fresher reuse authority.

The accepted scoped parent-lifetime fixture raises steady metadata pages from
86 to 88 and the multipage fixture from 34 to 37. Those concrete fixture counts
invalidate older candidate inventories/fit forecasts; they are not a general
plateau bound or a cap change. Physical allocator reuse never authorizes
persistent file deletion or index shrink.

## Closed integration and measurement gates

The allocator currently rejects a nonnil allocation-credit candidate prepare
with `ErrAllocationCertificateIncompleteV1` before retry, clone or mutation.
This fail-closed gate remains until one joint pre-WAL caller certificate covers
all resident and prospective backing, both materializers and complete terminal
lifetime at the unchanged 128 MiB budget. Tree-state bounds and ordinary scalar
limits alone do not establish that certificate. Existing charged low-level
births and resident adoption tests remain required; they do not certify the
unentered full finite publication path.

The full source-bound Linux semantic normal/race/fault/reopen/command-WAL
packet follows the scoped Mac parent-lifetime repair. Exact current physical
inventories, creator/control class witnesses, whole-caller fixed-capacity fit,
causal ACK evidence and the matched public storage/performance protocol remain
independent gates. No cap waiver, default retune or runtime activation follows
from this contract or compressed page counts.

The materializer now prepays its allocated-sort scratch, exact extent append
capacity, data-ID scratch, candidate page vector, reservation vector headers
and IDs, generation metadata IDs, dirty IDs, each separately allocated encoded
page and index plan, recording control, and conservative digest scratch/control
before those births. Scratch uses a separately created short resident scope; it is released only
after its full-capacity aliases are scrubbed. Its request debit is not refunded. The candidate and generation retain the creator of their aggregate
backing. The prepared wrapper prepays its existing creator edge; auxiliary
reused-range allocation consumes the same admitted mutation receipt as ordinary
page allocation. Generic retaining sinks remain ordinary-only. The digest
control allowance is conditional on the pinned non-boring Go 1.26.3 SHA-256
implementation; the full Linux compiled-input witness must verify that premise.
These predebits do not open the joint caller certificate or replace its ledger.


### Transaction epochs and failure ordering

New mutable births use the current transaction's `buildCreator`; a retained
candidate's historical creator only owns that candidate's existing backing.
Credited preparation prebirths the activation successor and admits the actual
activated-vector capacity before exposing the candidate. Activation consumes
that successor without a request callback, debit or backing birth. The ordinary
uncredited route still builds its successor during activation. The successor
prepays its header, both tracking radix headers, dirty-tree isolation and exact
pending-retirement replay scratch. The
header and mutable build edge are independent owning references. Ending the
matching resident epoch revokes private editing and the build edge while every
retained header, branch, chunk and vector keeps its original creator.

Retirement scratch, reserve-ID scratch, activated and burned-tail replacement
capacities, and legacy unbounded promotion capacity are paid before birth.
Replacement capacity is checked against both integer and element-width limits;
old and new backing coexist during growth. Stable generic sorting operates on
the admitted owned scratch and does not create a reflect/interface sorting
wrapper. Compiler-specific escape inputs and actual Linux classes still need
independent witnesses; source arithmetic is not a complete heap certificate.

Bounded pruning plans traversal, promotion, cursor and statistics without
changing the live transaction. Its full transient control/scratch class and all
prospective tree/tracking births are admitted before application. An activated
prefix pays its candidate-ID scratch and prune plan before `PublishBatch`;
application consumes the receipt without another account callback. A credited
legacy unbounded caller builds and prunes a private successor before the shared
transition. The ordinary uncredited legacy route retains its existing policy.
Ordinary direct publication builds and prunes its successor before ledger
visibility. Credited direct publication refuses before visibility because
Prepare has not staged that route's next-capability prune role; integration must
prebirth the exact role rather than invoke a debit after WAL durability. Bounds, capabilities and strict horizon comparisons are unchanged.

Abort admits dirty rollback isolation before removing reservation ownership.
Denial preserves the exact prepared candidate, rollback transaction and ledger
for retry. If ledger rollback rejects, the newly isolated root loses its aliases
and creating references. Successful abort installs the owned root, releases the
stage, and restores the saved statistics. A terminal or epoch transition never
refunds cumulative byte debit. Publication application errors after an admitted
receipt are invariant failures and preserve the allocator's failure/close owner;
they do not grant reuse or expose a partially paid request.

### Exact production sink and reserved interval

The direct durable-root preparation, fresh initialization, rebuild, queued
visible preparation and queued seal preparation all select
`NewOwnedCandidatePageSinkV1`. The returned private exact value retains no
fields; an active creator refuses ordinary `CandidatePageSinkV1` and arbitrary
sinks before transaction consumption, reservation or debit. The ordinary sink
keeps its independent duplicate-checking API and ID directory.

The materializer's prepaid recording control carries only the reservation's
start/count beside its already admitted page vector. Before every callback or
tail-write mark it checks a nonzero overflow-safe full interval, remaining
vector capacity and `id == start + len(pages)`. Wrong, repeated, skipped,
out-of-range and overflowing IDs fail there. Completion requires the exact
reserved count before exposing the immutable candidate. The same rule covers
append and reused intervals. Canonical postorder state emission increments one
ID per dirty object; normalized reservation pages follow, then one header.
A count disagreement refuses before a mismatched header callback. Production
pager writes consume only a complete candidate after this validation.

An attempted partial materialization still burns only the ledger's attempted
tail and restores the saved private transaction on preparation failure. Retry
uses that same allocator authority and avoids the burned interval. A skipped
append prefix is included in the admitted coverage capacity before ledger
edits; signed length additions and full element widths refuse overflow.
Encoding plans, page vectors and their unused capacity remain candidate-owned,
while every shared generation/tree/vector retains its creating edge through
publication, drain and rollback.

The focused control witnesses include real escaping allocator, COW, condition,
recording and bounded-prune objects, alongside the existing branch/chunk,
transaction/generation/candidate/prepared, creator, radix, reservation, cut and
digest controls. The empty sink has no separately allocated control class.
Pinned Mac witnesses are semantic/accounting component evidence; actual Linux
classes, compiler escape behavior, joint retained/cumulative fit and public
finite caller admission remain open. No production cap or refusal is relaxed.


### Explicit current request and retained resident scopes

`AllocationRequestCreditV1` carries only the current synchronous cumulative
reserve operation. `AllocationResidentCreditV1` is a caller-created scope from
the SAME installed native resident owner. `NewAllocationCreatorV1` reserves its
whole control class through request first, resident second, and retains only the
resident facet. Neither the creator, transaction, candidate nor an admitted
operation stores a current request. A successful request debit is never refunded
when resident preparation rejects. Old intrinsic creator slots remain unchanged;
the resident census charges the current request's complete loan, but charges the
new destination only for backing with no historical creator.

Birth and growth APIs receive the current request explicitly. Short materializer,
retirement, reserve, promotion, sort and bounded-plan scratch additionally receives
a distinct creator constructed from a separate short scope of that same owner.
Pointer inequality rejects the permanent creator as scratch; interface satisfaction
cannot prove installed provenance. The caller's scope factory and serializer must
prove that identity. Missing request or scratch refuses before the corresponding
birth. Full capacities and replacement overlap are admitted before allocation.
Scratch aliases are cleared at the operation boundary before the caller drops its
short scope. Resident refunds occur only at the last actual scope edge.

The credited prepared wrapper owns one additional activation-transaction pointer.
Resident census, exact generation/ledger edge checks, physical-cut profiles,
abort, terminal clear and shutdown all include that real transaction. Successful
activation transfers the edge to the live transaction. Ending an installed
resident epoch revokes the preborn successor's mutable edge as well as the
current builder's edge. A missing preborn successor or insufficient admitted
activated capacity refuses before reservation visibility, retaining exact retry
owners. This shape changes creator/class and whole-caller fit obligations.

`PrepareOwnedCOWCandidateRetiringWithAllocationRequestV1` is the explicit library
handoff; legacy nonnil `limits.AllocationCredit` remains unconditionally refused.
The shared DB caller must still construct all pre-WAL visible and async-seal
roles, dependency/debt/seal/snapshot/control backing and exact terminal ownership
through M's actual request and installed resident scope factories. A scalar
allowance followed by a post-WAL reserve is insufficient. No new standalone
allocator certificate, account or admission exemption is introduced. Ordinary
public recovery, queued and direct publication retain their existing nil-credit
behavior. The integrated finite guard and full fixed-capacity fit remain closed.

Credited transaction origin survives epoch completion in the existing bool
padding. Future births require both the synchronously borrowed current request
and the exact newly admitted mutable resident creator edge. Ending an epoch
revokes the fixed live, rollback, and preborn activation roles together; ordinary
uncredited wrappers retain their behavior. Re-admission loans the real existing
closure to the new request and creates only missing mutable edges, preserving
all intrinsic historical creators. This origin bit is expected to leave the
368-byte transaction geometry and 384-byte Linux class unchanged; actual
compiled class and whole-caller fit remain required before finite activation.
