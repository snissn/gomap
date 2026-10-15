# Bounded native original-index capture recovery checkpoint

Native source `4594e24aee1d468690c3c57639e83d42ebb86e8b`, tree `2be345dec45bcc615dc9011a4d5004027911f1d3`, is pushed and remote-ref verified. All10,308 source files exactly match the before/after manifests of rerun3 (SHA25664307447b3b698ca389ecc10ce826a052610ed16afffc7fcb461e93009529c15). The checkpoint is48changed paths from b9ce; it is a recoverable work-in-progress commit, not accepted/merged native support.

Rerun3 passes actual original two-index captures on the selected+anchor route under unchanged Q32/1MiB after149calls; the SAME-generation/count assertions pass at seal stage11. It then explicitly refuses the still-unwired publication stage26 with ACK0. Both-slot newer durable writer/stale/cancel and old-reader/checkpoint/reopen controls pass. Original Builder/Candidate abort and admitted clone controls also pass.

Root review identified a policy traversal before work admission and discarded final callback refunds at partial-token cancellation. Both are repaired in this source. The new before-token/before-Retain/after-Retain controls verify zero-work refusal and exact return of metadata bytes, original capture counter and wrapper borrows to the warmed canonical baseline. Earlier failed construction/control logs are retained. Actual Publish/Consume/Finish and paused-Finish/newer-writer proof remain outstanding; full native Q32 remains RED and ACK0. No unsupported platform or final cleanup contract is waived.

Ordinary616fd CI is now complete:42successful checks/statuses and14failures including aggregate, zero canceled. Final Windows6 failure is the already identified test-hook escape guard; race1 passes. This packet adds the final check snapshot, complete failure inventory, Windows6 and aggregate logs. The existing raw performance and other failed logs remain in [the preceding packet](../20261015-host185-616fd-ci/README.md). Ordinary source/performance repair remains active and PR5127 remains draft.

manifest.json binds byte-preserving originals and stored copies. Files over200kB use deterministic gzip. This packet changes no product/harness bytes and grants no qualification.
