# Production source review for issue #5338

Review performed by the independent cause-review worker against the local candidate, using direct source inspection and comparison with the stated baseline. This is a source-bound engineering review, not a GitHub approval or merge decision.

- Review snapshot (UTC): 2026-10-30T18:57:16+00:00
- Checkout: `/workspace/scratch/0d9ab3f5d72e/gomap-5338`
- Baseline HEAD: `d7db5409e20412f4dac4e1a3f902484b05e014e1`
- Baseline tree: `39a87e47f3740e4056f25196da54a9f4e35eb159`
- Outcome: no remaining blocking defect identified in the production changes listed below. This conclusion is bounded by static review and does not assert that undiscovered defects are absent.

## Reviewed source identity

The SHA-256 hashes bind this note to the full current bytes of each changed production file. They are candidate worktree hashes, not hashes of the baseline versions. Test and documentation changes are outside this table.

| File | SHA-256 |
| --- | --- |
| `TreeDB/caching/cow_native_semantic.go` | `9eac6b8b7e6153744af157e902a17dfe9a060ec3d7c193272ad3e02475ab7ee0` |
| `TreeDB/db/native_publication_observation_testhook.go` | `19b62e8ee70f78a33fef8449a9b9c6f858492707d6fcff568fa45fe47aa91124` |
| `TreeDB/collections/column_publish_plan.go` | `ca34be6bc4138a35c13b634e41bb83ec07004b7ebc9fc243cdb8208f70ccf9f1` |
| `TreeDB/collections/column_publish_write.go` | `4046a8baeb994faed3df815b866dfcf2696d96e590ab4c4c2a73d0d9505cb9d5` |
| `TreeDB/collections/native_engine_handoff.go` | `a5e12c54978ce61bb158977ef94c259d2948d9be6e81ba6a3fdc8c40c87e4e06` |
| `TreeDB/collections/native_json_txn.go` | `2afe184c2499b89a513cd0195da063ba9ead4d450f811b6aef2236fd7577ffc1` |
| `TreeDB/collections/native_typed_engine.go` | `d44580dea4fb9c983ffe84d43f7b937d5c6a72ae356c1f46fc390ff09bf5e296` |
| `TreeDB/collections/native_typed_materialize.go` | `3f92ad9cd8cd7f1731e67b1340a2fed1e608e65e28abc9a43e2ee6a16ca3759e` |
| `TreeDB/collections/native_typed_state.go` | `3371bb7073ee6bd92f904d8ce1a3dd5a48f4a6c83ec271e8f6f2b1a4a483d18e` |
| `TreeDB/collections/native_vector_graph.go` | `31d10fb38ac11f4da8bc4d0c92cf3afe9538c510a1048b5f7fbba3eb858d4e80` |
| `TreeDB/collections/typed_column_publication.go` | `ea60d7cffb1ed35bcc760fdf57776de8c271eb2c6f7f7ea12ce7cfe8bcc487bc` |

## Findings resolved during review

1. **Asset custody on missing codec provenance.** An earlier candidate rejected a successful producer result before original work adopted its stable resources; the caller returns immediately on a prepare-hook error and had no plan cleanup owner. The reviewed candidate first clones and merges custody into the original work, then releases only the plan's independent reference if classification is missing. Created assets remain in the existing failed-work cleanup path.
2. **Generic producer behavior.** An earlier candidate returned the ordinary native classification helper's unsupported error for any single-vector producer policy it could not classify. The reviewed candidate records provenance only when classification succeeds and otherwise preserves the original generic builder's result and error behavior. The ordinary native consumer independently requires classified emitted typed output.
3. **Codec policy precision and fixed growth.** Provenance records the selected ordinal/vector producer policies; a compressor's uncompressed fallback is within that policy's existing promise. The four-policy mask does not claim to be actual stored compression. The candidate reserves an additional complete fixed typed-producer result allowance for result/channel growth. Header, recipe, work and build-result growth also flows through existing `unsafe.Sizeof` charges; the prepared-assets scalar occupies existing alignment padding. No source-sized backing, new configuration lookup or validator factory was added by provenance propagation.

## Invariants checked

- The original source-root guard remains. Materialization uses a private typed-header effective root, initialized from the immutable recipe; the shared recipe, typed data receipt, LSN/member identity and admitted graph capacity are preserved.
- A successor is advanced only through the existing captured-prefix handoff. The typed suffix clone proves the original boundary is an ancestor. The paired manifest result is bound to the same collection and original work metadata, and its old catalog root, interpretation and base root must match current state and the marker's effective root. Original column/vector schema compatibility is checked. General changed-root physical-only maintenance remains refused.
- Source codec provenance originates in the actual immutable prepared producer options/columns, is copied only after the producer joins, and is OR-combined through asset merges and successful collection work. It survives removal of the old typed prefix. A bound graph promise rejects any unpromised ordinal or vector policy. Successful ordinary work rejects emitted typed assets with missing classification; a zero mask can therefore describe no newly emitted typed source, including graph-only or deletion-only progress.
- Preassignment checks bind graph census to document generation and physical source root. Logical change returns a conflict. An exclusive physical-handoff refusal permits at most one entirely fresh preflight, after closing the previous transaction and before any assignment. Assigned, uncertain, logically installed, accepted or ambiguous outcomes do not retry. Joined cleanup errors are preserved and disallow retry. `Consumed` alone is custody transfer rather than assignment evidence.
- Zero initial manifest roots, graph-only captured prefixes, repeated successors and multiple newer graph headers are structurally supported by the same pairing and header-copy rules. Original captured headers are never edited.
- The opt-in observation seam runs after acknowledgement and before exact-seal checkpoint, uses a bounded nonblocking mailbox, carries scalar receipt data only, and has no state, retry, callback or seal authority. With no registration its hot path is an atomic load and branch.

## Limits

This worker performed no test, race, vet, benchmark or crash execution in the final review and does not attest to their results; execution evidence belongs to the integrating worker's exact commands and logs. Earlier inspection also covered the unchanged allocation, typed-suffix, metadata-comparison and caller-cleanup paths needed to assess the changed code. This note does not qualify broader compaction or maintenance rebases, arbitrary codec-policy changes, performance or all lifecycle schedules.

The controlled diagnostic demonstrated stale recipe-root versus handed-off manifest-root behavior. This repair addresses that source-bounded defect. The note does not attribute the original historical CI failure's hidden materialization cause to that witness, nor claim that the original incident has been reproduced.
