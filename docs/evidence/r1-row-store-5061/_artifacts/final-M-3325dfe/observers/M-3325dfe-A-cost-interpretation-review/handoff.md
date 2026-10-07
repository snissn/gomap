The checkpoint allocation increase must be reported: typed median whole-process allocation is1,110,120→3,829,760B for one call (+2,719,640B, about3.45×), and allocation count5849→6534. Timing43.7225→65.6895ms is inconclusive on both sides (throughput spreads3.8878 and0.9589). Timing noise does not erase the allocation observation.

The original samples show existing low/high allocation clusters on both sides, with a different frequency in five fresh-DB repetitions of one process:

| Rep | Baseline B/call | M B/call | Baseline allocations | M allocations |
|---|---:|---:|---:|---:|
|0|101,400|1,121,168|5,454|5,866|
|1|1,110,048|3,848,160|5,849|6,795|
|2|1,110,120|3,829,760|5,848|6,534|
|3|3,935,272|3,886,368|6,639|6,643|
|4|1,116,592|1,109,568|5,864|5,844|

Baseline already contains a3.94MB sample, above the largest M sample. Thus the measured median shift is real, but the packet does not prove a new deterministic candidate allocation mode or explain away the shift as background work. The pending causal attribution must remain explicit.

The unchanged harness reads whole-process TotalAlloc/Mallocs around manager.FlushAll plus backend Checkpoint, one operation; concurrent publisher allocations are included. The backend checkpoint waits through the captured visible sequence, closes the command-WAL prefix and may scan cleanup. These entry-function bodies and the WAL scanner are byte-identical across baseline/M, which rules out a new loop in those bodies but does not prove transitive runtime equality. Final stats are taken after checkpoint and storage census; they have no phase-start counters.

The final typed manifest sizes match exactly per repetition (902258B then904853B). Cumulative manifest builds are511/635/637/640/637 baseline and504/630/634/625/619 M; cumulative encoded work is roughly246–307MB on both. Cleanup scans are1/3/3/3/3 versus1/3/3/2/3. Append-buffer allocation counts match17–21 by repetition. These observations support variable publication/drain scheduling as a plausible explanation; they cannot assign the extra2.72MB to a particular mechanism. The A end census has no systematic persistent inflation (baseline64.48–77.05MB, M60.28–72.86MB) but is a finite mutation-heavy snapshot, not the sustained D retention guard.

All20 stable matched groups have throughput ratio at least0.9599407; none breaches a15% loss guard. That statement does not cover the72 inconclusive groups or the newly enabled typed public range. Typed GetInto349.538→59.897us is stable (5.83567×), with recorded B/call89175.104→91750.352 (about+2.9%) and allocations151.5→150.723. Prepared batch135.203→138.816us and complete range48.152→48.619us are stable; their B/call and allocation counts decrease slightly. Preserve all original source identities and all inconclusive labels.

Recommendation: retain the checkpoint allocation delta as an unresolved one-call maintenance cost in the report; do not call it zero, harmless noise, a proven candidate regression, or a separately attributed justified cost. There is no stable throughput-guard breach demonstrated here, and the original evidence alone does not establish a new deterministic allocation regression. Any need for a stronger causal conclusion belongs to root’s acceptance decision; it would require actual checkpoint-start/end work counters or allocation attribution, not interpreting timing spread as allocation noise. This advisory grants no acceptance or waiver and performed no new measurement, build, tests or changes. Exact original hashes/source references are in handoff.json.
