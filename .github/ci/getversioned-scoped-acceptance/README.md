# Scoped original timing observations

This is a single-use proposal for PR 5397. The original 99d measurements remain failed. Only their two specified timing observations are eligible for an authenticated owner decision; all other required checks retain their existing success requirements. This mechanism does not establish the cause of either measurement.

The scoped job authenticates the original archive and re-evaluates its observations, then builds eight executables without running a benchmark and compares their hashes to the pinned originals. A later job authenticates the same-run preparation artifact and a unique, unedited owner decision bound to the actual head, merge, run, source and manifest. Missing evidence, mismatches, expired or late decisions, and non-timing failures refuse acceptance. `decision --event` prints the exact decision body for the authenticated run; it does not post or approve it.

The verifier and identity builds require hosted execution to qualify the exact successor. Source checks and synthetic controls do not prove that the original archive or builds pass in that environment.

For this successor, the specifically pinned, unedited owner comment for run 42118463071 is recognized as historical evidence only. A fresh, unique decision for the actual run remains mandatory; unknown or altered historical markers refuse acceptance. This refines the successor contract and does not change the earlier 7b6 run.
