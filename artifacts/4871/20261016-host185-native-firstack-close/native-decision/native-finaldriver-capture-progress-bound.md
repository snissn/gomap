# Original DATA and PRIMARY capture progress units

Coordinator source audit of immutable manifest
5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d,
archive 0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4.
The 102 extracted source files are individually hash-checked in
native-main-integration/finaldriver-capture-progress-root-read/input-bindings.json.
No source mutation, Go execution, publication or numeric allowance is claimed.

Pairs below mean (records, bytes), with checked componentwise addition and
multiplication. Every size is the actual target Go sizeof, not an estimate.
Use S=Snapshot, C=nativeStablePagerCaptureV6, I=indexGen, O=stablePagerOwnedOperations,
F=os.File, A=primaryArenaOwnerV5, R=IdentityPinRegistry, B=resourcePinnedBacking,
N=StableNamespaceToken, P=StableNamespaceCreationProof, T=StableResourceToken,
X=resourceAllocation, Q=resourceObligationBacking, Z=StableResourceSpec,
J=StableIdentity, W=StableIndexTokenConstructionWorkV6, H=identityPinState,
K=IdentityPin, E=registryCell[StableIdentity,*identityPinState].

Add(owner) denotes the complete same-owner AddPendingWithOrdinalWork including
its concrete bindings, resize and rollback preadmission. Refund denotes the
existing complete pending RemovalWork prefix through its next persistent
advance, including reset and the parent's repeated entry charges. Those rows
come from the owner/terminal reports and must be joined, not replaced by a
fixed constructor sizeof. A pending refund precedes every capture entry.

Precursor operands are actual directory and original file/proof names, the
original registry capacity and bindings, and the original current index/owner
identities. Let d=len(db.dir), f=max original DATA/PRIMARY file.Name lengths,
n=max(indexFileName, primaryIndexFileName lengths), p=max(d+11, all retained proof
parent name lengths). File(x)=StableFileMetadataCharge(x), checked. The d+11
alternative covers a previously missing proof's newly duplicated parent. The native parent
opens db.dir and duplicates add their literal suffixes; a conservative observed
proof/file-name maximum may be used when both actual roles are recorded.
No reopen, other producer, index cutover or concurrent ref mutation occurs.

## Capture phase rows and persistence

The actual nativeStablePagerCaptureV6.step runs a root/index/sequence check on
every turn. Its phase is saved after every successful phase. The outer seal
preparation also saves which of the two captures it is driving. Thus two
captures increase total turns, not the next-progress unit by a traversal factor.
Summing each row below once is a conservative capture unit; alternatively take
the maximum of each correctly lifted phase unit. Parent seal/cursor prefixes
still must be added.

| Phase | Admitted local pair and required child |
|---|---|
| 0 original maintenance Snapshot | (3,2S+2C+2I) + Add(PRIMARY metadata) |
| 1 missing namespace parent | (2,2File(d)+2d) + Add(PRIMARY metadata) |
| 1 missing creation proof | (18,2P+4(File(d+11)+File(f+16))+4n) + Add(PRIMARY metadata) after selected namespace correction |
| 1 proof Bind | (1,256) + (8,2N+2File(p+11)+4n) + Add(PRIMARY metadata) after selected namespace correction |
| 2 original file stat/identity/spec | (3,2Z+2J+256) |
| 3 original Observe | complete corrected Observe lookup, atomic owner growth/rehash/install/shrink unit |
| 4 original callback construction | Operations below |
| 5 original token construction | Token below |
| 6 original Unobserve | Lookup + (1,2H+128); idle partial cancellation additionally uses selected atomic owner removal |
| 7 drop constructor callback edge | OperationsRelease from terminal report; this successful constructor edge is nonlast |
| 7 drop constructor namespace edge | NamespaceRelease from terminal report; successful token retains independent edge |
| 7 detach constructor aliases | (1,2Z) + (1,2W) |

Creation suffix lengths above use 11 for #stable-pin and 16 for
#stable-sync-pin. Counting the Windows child persistence alternative is a safe
bound on Linux. The actual fixture is Linux. The creation proof must not be
silently omitted merely because existing fixtures commonly cached one.

Phase 0's original newDurableCandidateStableIndexSnapshotV1 calls SnapshotPool.Get,
which creates exactly one Snapshot, and increments the original wrapper borrow
and capture counter. No snapshot registry traversal or reader dependency birth
is hidden in that helper. It stores workOwned/capturePhase=4 in the SAME Snapshot.

Phase 1 persists a newly opened parent before proof work, and persists the proof
in the existing index/primary owner before Bind. The repeated (1,256) generation
observation and the entire Bind child MUST fit together. Selected correction
9bc3a628bfd2b5e29df3dea2cdb0f9efe2a32a532b291e5999c1b19159d9fec0
adds actual owner work admission and retains a failed native Bind's SAME partial
token in the capture, rather than performing unmetered synchronous refund.

## Original owned callback construction

Every entry repeats Eop=(1,2C+S+I+F+O). The saved operationsPhase stores each
successful child. A conservative sufficient unit is Eop plus the sum of:

- callback/path birth: (2,2O+2f) + Add(PRIMARY metadata);
- original cleanup custody: (2,2R+2B) + Add(PRIMARY metadata);
- original physical arena retain: (1,2A).

The original custody constructor increments its registry edge only after actual
owner admission. It performs no table traversal. Arena retain is under its own
mutex and has no loop. Operations.Retain in token phase 2 is a scalar CAS loop;
the selected no-concurrent-ref-mutation interval makes the first attempt succeed.

## Original index token construction

Every successful phase/stringIndex is saved. Phase 1 also saves each of three
partial allocations before proceeding to the next child. Use the following
actual units, with the whole phase 1 local row repeated before whichever of
its children is still missing:

| Phase | Pair and children |
|---|---|
| 0 policies/spec | (2,2Z)+(2v,2v*sizeof(canonicalReachabilityRequirements[0])) |
| 1 token/empty obligations/pinned backing | (12,2*(T+3X+Q+B)+2File(f+11)) + 3*Add(PRIMARY metadata) |
| 2 original callback retain | (1,2T) |
| 3 one owned descriptor string | (1,2*len(source)+2*sizeof(string)); stringIndex persists |
| 4 duplicate exact held file | (1,2File(f+11)) |
| 5 pinned stat/identity | (2,2J+256) |
| 6 original registry pin | Lookup + (1,2K+2H) |
| 7 exact link/final initializer | (3,2T+2N) |
| detach | (1,2W) |

For the selected ReachabilityIndexFile route v=1: it is the first actual
canonical requirement and both the admitted and shared policy loops stop there.
Using full len(canonicalReachabilityRequirements) is also conservative. The six
descriptor sources are Kind, LogicalLane, ResourceID, DiagnosticPath,
Reachability and empty StableIdentityOverride.Platform; bound by their actual
selected constants, not arbitrary path length. A whole sum of their six rows is
safe but not necessary for one saved stringIndex advance.

Lookup=(1,platformBytes+16)+capacity*(1,2E+2H). The table load invariant keeps an
empty slot and probes under one original registry mutex. Capture identity is
canonicalized to Generation=0; Pin's logical idx.id is canonicalized to the
same original state, so it does not create an extra state. At most two Observe
calls can create new physical states before readiness. Use the checked actual
two-step original capacity transform (zero→16, otherwise at most doubling per
birth, tombstone rehash may retain capacity). Terminal successor capacity needs
its own already-selected source transform; this report does not assume absence
of that successor.

The scoped nonnil metadata predicate makes BindCleanupRegistry an attachment
only, with no newOriginalObservationCleanup allocation. The accepted token
spec rejects nonempty logical obligations, exact RID membership, imported
physical backings and arbitrary callbacks. completeStableResourceTokenV6
therefore has no variable inventory clone, retains its original namespace via
one locked scalar increment, and creates no generic diagnostics. Linux
filepath.ToSlash leaves selected constant names unchanged. The passed default
operation closures capture no state and the actual owned operations are kept.

## Error and terminal composition

Capture closeStep first completes any pending refund, detaches the original
token constructor once, then repeats (1,2Z) before its still-live child sequence.
The token/callback/Snapshot/namespace/proof/parent/observation fields are cleared
or their release phases advanced before a later yield. Thus join that repeated
(1,2Z) and refund prefix to EACH terminal child unit from the resource/terminal
reports. Parent close uses (1,2F+128); final capture scrub uses (1,2C).
Unobserve after failed capture can be last idle and requires the corrected
same-owner atomic removal, even though a successful token's Unobserve is nonlast.

This report closes constructor rows on the selected route subject to the
explicit corrected namespace/Observe/Unobserve implementations. It is not a
complete global G, production review, retained-envelope acceptance or runtime
PASS. The writer must use actual typed helper rows for those changed children,
and the final source review must bind this advice to the resulting bytes.
