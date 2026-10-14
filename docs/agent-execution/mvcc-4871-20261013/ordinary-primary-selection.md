# Ordinary public PRIMARY selection and causal repairs (#5111)

This packet follows the immutable custody/authority-cut snapshot d73eac2f and
its artifact-only descendant efd744df. It does not qualify PR5127 or Native
MVCC. Raw source bindings and logs live in sibling evidence/ordinary-5111.

## Production selection and layered publication

The main backend in public.go openResolved selects existing
IndexPrimaryDirectory for CommandWAL plus MemtableMode="cow_btree", immediately
before main db.Open and after dictionary/template options are copied. Fresh
stores and ordinary reopen use the existing V6 arena/capsule authority. A
nonempty DATA-selector store requires a rebuild and is not migrated. Existing
explicit selection and no_wal_fast behavior remain in force. Automatic main
selection does not change default dictionary or disabled-template policy.

Foreground grouped WriteSync still appends/syncs its existing command-WAL frame
and installs the prepared C1 cut without changing the backend StateToken.
Checkpoint builds/promotes the original PRIMARY transaction through the existing
bounded flush group. Fresh cuts bind the exact successor backend StateToken;
held cuts retain their original basis, values and resource leases. Prepared
WaitThrough custody remains. There is no checkpoint on ACK or second publisher.

The original unset-selector diagnostic on efd was RED: foreground C1 worked,
but dirty checkpoint omitted PRIMARY. Exactly the same retained overlay is GREEN
with this packet. The ordinary production fixture additionally checks both
command-WAL profiles, inline and forced persistent pointers, actual PRIMARY
three-page authority cuts, successor handoff, a later changed-key publication,
Close and ordinary reopen. Eighteen profile/mode/explicit-selector rows and both
nonempty DATA rebuild rows cover selection and existing behavior.

## Same original recovery fallback transaction

Selection exposed an actual Close RED: matching selected/fallback LSNs named
different physical directory addresses. V6 encodes independent complete
immutable directories into fixed capsule slots, so their physical root page IDs
necessarily differ even for the same logical tree.

Existing fallback refresh and CopyPrimaryRoot remain. Only V6 root equality
compares the authoritative complete captured capsule views: actual matching slot
records, retained images, exact arena UUID, system root and digest, checked page
checksum/digest, and every directory byte except physical PageID [0,8) and
checksum [8,12). Flags/count [12,16), reserved bytes and all canonical operands
remain exact. Ordinary DATA/V5 retains its original page-ID predicate. Coverage
must still match the requested current LSN. Missing/malformed views, changed
keys/system records/coverage and real pre-write authority refusal cannot falsely
converge. No tree scan, new authority, closure/history reconstruction or WAL frame
is introduced. The equality path allocates no new directory image.

## First predecessor cleanup refusal

Independent review found that a supersession Abort refusal was retried by Close,
after disposing the sealed successor. The original group now records the first
current-constructor refusal in its existing custody cells before returning.
Cleanup retains that receipt, both original transactions, tracker debt,
registry and necessary pins on the same physical owner before releasing existing
serialization and teardown locks. It does not retry either action or refund DATA.

A genuine original-object RED consumes the actual predecessor edge inside the
configured production constructor factory; the admitted zipper still seals its
actual successor before predecessor disposal refuses. The repaired test proves
both original objects survive Close/repeat, first-error identity survives, and
shutdown releases locks without refund. A print-only source-bound overlay on the
original physical DropReadRootConstructionV6 records exactly one original Abort
attempt across supersession, Close, repeated Close and shutdown.

## Receipts, CI and remaining gates

| Raw receipt prefix | Result |
| --- | --- |
| public-selection-red | Unset-selector original RED on efd |
| public-selection-original-green | Same immutable diagnostic GREEN |
| selection-normal | Genuine selected command-WAL Close fallback RED |
| fallback-cause | Actual fixed-directory header/physical identity diagnostic |
| selection-normal-final | Public selection, basis, pointer, Close/reopen GREEN |
| supersession-first-red | Original predecessor first-refusal RED |
| supersession-abort-count | Actual Abort attempted once, GREEN |
| selection-custody-fallback-normal | Original custody and V6/DATA fallback positives/negatives GREEN |
| selection-custody-fallback-race | Same targeted public/backend seams under race |
| selection-docs | Canonical docs tests |

Production/test changes select full existing CI under the merged ci_impact
contract, retaining TreeDB required and every inventoried TreeDB/race consumer;
local forecasts authorize no omissions. The merged public cost harness is
unchanged. Its command-WAL/cow_btree options use this same late selection; C1
foreground publications remain distinct from PRIMARY dirty-checkpoint dispatch.
No retained performance collection or acceptance claim is made by these tests.

Remaining #5111 obligations include all 19 actual custody contracts, the actual
backend Snapshot completion/basis-capacity boundary after last physical cleanup,
OpenStableChildFile containing/file/name admission and transfer/disposal,
complete persistent value/leaf creation/registration/retirement, arbitrary parent
Close uncertainty, the retained baseline no-rotated-leaf race RED, platform
namespace contracts, and frozen numeric/matrix/fault/cost gates. The unavailable
8f20fd204 packet may overlap independently repaired seams; exact-byte
reconciliation remains required if recovered. No foreign value/leaf source was
reconstructed or credited, and no existing gate was waived.
