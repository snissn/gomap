# Completed f041 validation and read-cost diagnostic

This artifact retains the completed local normal suite, final hosted check inventory, failure extracts and bounded read-cost diagnosis for draft PR #5127 at f0415773f13a522dea5c1debdaecba09d15585dd. It is rejected evidence, not an accepted candidate.

The full local normal run had exact tracked-byte before/after equality: 83 packages passed, 21 had no tests, two failed. The only three top-level failures were two DB fixtures being repaired and one Unix-socket path exceeding the platform limit; the latter passes with the same product/test source and a short task-local TMPDIR. Hosted final state: 35 successful checks, 17 failed checks including the required-gate aggregate. Earlier independent review R1/R2/R3 is retained in artifact commit ce893e0c63895cf2af4bd06b01b4b9a285555133.

The read-cost overlays passed focused correctness. Local CPU profiles are diagnostic only. The uninstrumented comparison collected zero samples because the unchanged quiet-host preflight failed. Native production integration and original Q32 acceptance remain incomplete/RED/ACK0.
