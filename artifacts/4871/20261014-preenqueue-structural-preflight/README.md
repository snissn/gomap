# Native MVCC structural preflight (not qualification)

Source checkpoint: `e7aa8d8b5a894932d1b15306bcdd4437a983d3e6` (nine focused source paths; two foreign diagnostics preserved).

The original Map mutation engine now supports reserve-before-effect work admission.
Ordinary Set uses that same engine without Work-only key scans. Focused differential,
refusal, fork immutability, byte/capacity and callback checks pass; full package raw exit 0.
New API capability absence is recorded as capability RED, not an invented old runtime regression.
Small fresh-fork replacements cost 11/15/21 records for 1/2/4 entries with matching value layout
and four-byte fixture keys. Full fork/split geometry refuses Q32 honestly; repeated insufficient
allowance need not progress. These are Map-stage results, not public qualification.

The same original transaction preallocates private promotion-image storage before enqueue.
Constructor storage/refusal/replay/physical-abort controls pass. Both actual public COW cases
still explicitly refuse phase16, with native ACK=0/cut=0 while ordinary writes complete.
The COW companion now attempts an ordinary CommitAt after every successful nonterminal return,
including ACK-bearing returns. Protected 33/80/4096 test bytes remain unchanged (77f14c80...).
Runs use exact Go1.26.3, GOWORK=off, explicit 90s test timeout and public-batch external 120s bound.

Current source-derived arithmetic is an architectural warning: fresh-compose >=15 plus
physical capsule >=8 plus smallest frozen Map fork replacement 11 already totals >=34 records,
before promotion, activation, Consume and dependency work. This is not a universal impossibility
proof or measured full-public count. Preborn promotion alone cannot establish this algorithm's
Q32 viability. A bounded shared-seam architecture decision precedes further refinement.

Native publication progress, literal Q32/1MiB accounting, physical Finish, public performance,
all original format/lifecycle/fault/platform/review/CI/merge gates and parent completion stay OPEN.
No benchmark speedup or activation claim is made by this packet.
