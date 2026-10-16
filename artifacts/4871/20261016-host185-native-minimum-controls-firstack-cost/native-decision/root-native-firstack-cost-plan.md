# Next bounded diagnostic: public cost through first native ACK

This is a proposed next experiment, conditional on repair07 focused normal/race
and genuine once-ACK/paused-Finish DB.Close controls passing. It grants no source
or runtime lease. Original Q32 and final qualification remain unchanged.

Use the already proved selected shape: fresh reopened NoWALFast DB, 33 physical
versions, floor32, one DATA leaf, empty cache overlay, background pruning off,
held old snapshot, BatchSize1, and the same checked pre-cursor diagnostic G.
Keep the actual successor Commit34 at the genuine detached/accounting-incomplete
Finish boundary, then original Finish and exactly one ACK, Close and reopen.
No new history or general supported-shape claim is implied.

The primary measurement is the sum of wall time spent inside the actual public
PruneVersions calls from cursor birth through that first ACK. Report call count,
maximum call time, all actual work records/bytes, and exactly one pruned record
and batch. Report the whole interval through ACK separately, including the
designated writer and observations; do not present it as native-only time.
Group any subsidiary samples by starting cursor phase, explicitly because a
public call can cross multiple phases. Do not label those exact phase costs.

Prefer one small benchmark using the maintained diagnostic setup/oracles and
test-only helpers accepting testing.TB where appropriate. Do not copy production
logic or weaken the full ready-envelope, original receipt, held-reader or reopen
oracles. Stop timers around setup, precursor census, observations, designated
writer and final cleanup. Benchmark allocation counters are process allocation
during the public-call windows, not goroutine-specific or retained memory. Report
retained P separately using its existing source-bound meaning.

Prebuild the exact frozen benchmark candidate outside timing. Select three fresh
processes, one complete first-ACK operation each, -benchtime=1x/-count=1, on a
quiet host; retain every raw sample and stop on first semantic failure. Preserve
source/build/harness/ELF and environment identities, raw counters and distributions.
This establishes an initial feature-cost baseline, not a before/after speedup or
full pruning performance result: no equivalent old first-ACK route is qualified.
Do not compare it to legacy full-pass pruning or divide unrelated operation cost.

Choose any subsequent bounded profile from these costs and an actual hypothesis;
do not launch a broad profile campaign by default. After this boundary, the
accepted resumable snapshot-minimum design is a justified candidate for bounded
traversal progress under unchanged Q32, independent of the unresolved complete
atomic-publication contract. Neither this experiment nor that scan repair alone
closes continuing-write progress, C1/C3/all19 or the remaining graph gates.
