# Coordinator decision: complete published-state lifetime integration

Target: PR #5127, reviewed head a8c52ed66fc587d4823f020cf15ea1c3ee8c1ebc.

The recurring root cause is incomplete integration of the shared ordinary/native Set disposal contract. The new final Set continuation clears its mutable map and backing aliases when the exact Set reaches zero references. Existing readers that retained only a shallow DBState or acquired a Set after dropping publication exclusion cannot use that Set safely. Correcting Snapshot capture and two refresh publishers alone did not cover the other current-state consumers and publishers.

The coordinator accepts independent findings R4 and R5 and authorizes one coherent repair of this complete invariant. This is the architecture disposition required before another repair head in the same lifetime category. R1-R3 are resolved at a8c52ed66. The candidate remains rejected.

Required invariant:

- Each actual published-state replacement that can drop the selected Set must exclude in-flight Snapshot capture with the established rootReuseMu then db.mu order. Initial-open and truly exclusive offline paths must have explicit source-backed exclusions; do not add recursive locks inside already-fenced helpers.
- A current-state consumer that uses a Set after releasing its state-selection lock must acquire that exact Set before dropping exclusion, hold it until its final map/tree/iterator/zipper/dictionary use ends, and complete its original release. No post-unlock increment, retry after missing-file failure, or weakening of final alias scrubbing qualifies.
- Every topology-only replacement must carry oldState.primaryRoot unchanged, transferring the same single current ownership edge. It must neither invent an additional edge nor drop the existing edge. Real logical-root replacement still uses the original activation and withdrawal protocol.
- Audit every production state.Store and every state.ValueLogSet use, including shallow db.State() inputs. Snapshot-backed readers, independently acquired CurrentSet inputs, scalar-only checks, and exclusive offline inputs are distinct safe domains only when the actual caller establishes them.
- Public State() is a shallow metadata observation, not a new lifetime-bearing reader API. Pointer-bearing reads must remain inside a live Snapshot or an explicit internal capture lifetime. Clarify that existing boundary where needed.

Repair ownership is the ordinary completion lane only; the native lane must not cherry-pick overlapping shared changes. Root owns Git/GitHub publication and later exact-source integration.

Validation must exercise actual late capture versus refresh, omitted publisher versus capture, real outer-leaf pointers, and topology-only changes on an actual PRIMARY root. It must show pin-before-use, last-use-before-release, correct slot/root drainage, and final governor/physical completion. Retain a short call-site table with the repaired or safe disposition of each relevant path. Use focused normal/race checks and preserve the unchanged raw-path, public shrink/output and hosted gates.

Separate evidence: a8c52ed66's hosted M0 stability capture failed foreground-p99 CV 0.2450147914 against its existing 0.10 threshold. All ten legacy operations completed without abort and the public classification passed. This is an unresolved variability gate; it is neither an attributed latency regression nor an accepted waiver. Other hosted checks are still in progress at this decision.
