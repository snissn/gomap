# External CPU admission policy

The PRIMARY gate and its shared `ProcessObservation` consumers use
`external-cpu-v2`: reject a full observation interval if any external process
uses **at least 10% of one logical CPU**, or the sum of observed external
process CPU reaches **50% of one logical CPU**. Record warnings for each process
at or above the previous 2% threshold. These percentages are CPU seconds divided
by elapsed seconds, without division by host core count.

These are fixed practical admission limits for shared workstations, not an
experimentally calibrated bound on timing error. A 10% process represents one
CPU second per ten wall seconds; the aggregate limit bounds distributed work at
half a CPU even if every individual process is below 10%. Mild desktop work
should not automatically invalidate a run. The preserved #5269 controlled
failure observed GNOME at 2.1343612909678398% (29 ticks over
13.587202936410904 seconds at 100 ticks/second) and approximately 4.784% total
external CPU. That activity is admitted under this policy with a warning;
**the original failed qualification stays failed under its original policy**.
No benchmark has established that this activity affected those results.

There are no command-name exemptions. Kernel workers count as external, even
when the benchmark may have induced their work; the observations cannot reliably
attribute that work. Only the existing runner ancestor/descendant exclusions
apply. No affinity discount is made: sampled process placement cannot establish
where execution occurred over an interval, and old artifacts have no such proof.

Raw per-process identity/start-time/tick observations and elapsed time remain in
the artifacts, including exited processes and PID reuse. Summary records add the
policy identity, explicit denominator, thresholds, warnings, observed aggregate,
and aggregate rejection flag. The existing `busy` field contains rejected
processes, or all positive contributors when the aggregate limit rejects, so all
existing callers continue to enforce both limits. The aggregate is partial:
processes entirely between 250ms observations can be missed, and a first-seen
process can include pre-window CPU. CPU admission does
not establish I/O isolation, lack of timing noise, or physical host exclusivity.

Preflight acquisition duration and cap, measured observation boundary, planned
sample retention, timing/allocation/activation predicates, and runtime identity
checks are unchanged. Historical reports describing the 2% policy remain
historical evidence, not the current policy definition.

The independent frozen M0 observation runner (`.github/scripts/mvcc_m0_observation.py`)
loads an exact SHA-256-pinned historical observer from its externally frozen
product input and retains its 2% protocol. It does not import the maintained
observer. Changing it requires a new input/protocol freeze, rather than silently
reinterpreting existing frozen executions. The active maintenance collector
(`mvcc_maintenance_collect.py`) uses the current policy.
