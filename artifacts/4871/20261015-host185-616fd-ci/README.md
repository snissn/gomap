# Host185: exact 616fd CI and bounded native progress

Ordinary candidate: `616fd1497b3e483f40bfbf3cefe4538d250bb33a`; baseline: `5bca87ffe6e1bfba2d693264d2dc82936adc234f`. PR5127 remains draft and blocked. This packet preserves failures as well as passes; it does not accept #5111, native #4878, or graph #4871.

## Hosted performance

The M0 run passes all ten legacy/public repetitions and unchanged 10% stability gates. Foreground-p99 CV is 3.36%. Root independently re-parsed all raw samples; three non-gate CVs differ by one floating-point ULP, recorded with the Python versions and a 1e-14 aggregate-only comparison tolerance. Raw samples and gate decisions match exactly. This does not attribute the earlier head's M0 failure.

The original five-way paired comparison fails GetVersioned: +13.59%, zero bytes/allocations. DATA -3.33%, seek +0.63%, iterator +3.84%, and tiny durable batch +1.33% pass their original gates. Eight fixed alternating AB/BA pairs use the same event baseline, Go1.26.9 and CPU/GOMAXPROCS settings. All six executable sizes/hashes match their measured bindings; retained exact-head checker output recomputes the original summary. PRIMARY was skipped after the raw gate failure and has no result. The successor workflow is being corrected to collect this independent gate sequentially after successful prerequisites even if the raw threshold fails.

The 147 MB raw-path artifact zip and its six executables remain in GitHub artifact11998141728 and in the local source directory; this Git packet retains their hashes, API digest, provenance and complete raw samples. The M0 API artifact receipt identifies artifact11997012282. See [hosted raw-path run](https://github.com/snissn/gomap/actions/runs/38784682785) and [hosted M0 run](https://github.com/snissn/gomap/actions/runs/38784682771).

## Correctness and diagnosis

Snapshot11 has40 successful checks/statuses,12failed and3running; it is not a final aggregate. Check snapshots and complete available failed-job logs are preserved under live-startup. Windows C3 admission passes all four original cases (6.32s,1.26s,0.76s,1.11s) at unchanged deadlines; parent9.46s. The earlier head timeout remains unattributed. Exact pass events and the successful Windows-core4 full log are retained alongside the failing jobs. The current repair batch covers a DBState pointer escaping through a test hook, a real generic resource Stats/namespace release race, a coordinator TryLock oracle invalidated by independent scheduler contention, and Windows unsupported-path/failure-cleanup fixtures. No platform behavior, deadline, benchmark threshold or no-ACK requirement is waived.

The two fixed five-second CPU profiles used the exact hosted baseline/candidate executables on busy host185. They are diagnostic only. Their one-pair timings are not acceptance evidence or a causal attribution of the hosted regression. The private one-shot reset optimization follows a separate source audit of unreachable exported-reader fields.

## Native progress

Original Q32/1MiB work reached the SAME actual private candidate and original private-promotion preparation. With the cursor paused before promotion, two real newer durable DATA writes moved both slots; exact parent/sequence/slot validation refused before acquisition, and stale/direct cancellation preserved both newer data and checkpoint/reopen. Source manifests are identical before/after each retained control. These are bounded prerequisite milestones. Actual native publication, Consume, Finish and paused-Finish/newer-writer qualification remain outstanding: Q32 remains RED and ACK remains 0. Native source recovery publication follows its next tested capture milestone; manifests here bind these controls without claiming a final source freeze.

All files are byte-preserving copies. manifest.json records both original and stored hashes; larger files use deterministic gzip. Product/harness changes invalidate affected acceptance evidence and require the applicable gates on the successor.
