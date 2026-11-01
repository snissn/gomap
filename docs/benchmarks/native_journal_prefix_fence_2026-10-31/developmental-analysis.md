# Existing native journal-fence inquiry

Four-cell existing-public-API native journal-fence developmental discriminator; graceful reopen only, not crash/power-loss durability, changed gateway ACK acceptance, original public YCSB throughput, strict-ingestion completion, or production optimization acceptance

| Cell | Insert sum, ms | Boundary sum, ms | Loop, ms | Diagnostic loop rows/s | Checkpoints | Command-WAL sync calls |
|---|---:|---:|---:|---:|---:|---:|
| durable-checkpoint | 111.521076 | 2496.683418 | 2608.422818 | 24.535899 | 64 | 128 |
| relaxed-fence | 3.310805 | 81.338773 | 84.708904 | 755.528604 | 0 | 64 |
| durable-fence | 86.457681 | 31.014421 | 117.558885 | 544.408022 | 0 | 128 |
| relaxed-checkpoint | 9.607861 | 2698.754851 | 2708.577378 | 23.628640 | 64 | 128 |

Both fence cells have zero loop checkpoint runs; both checkpoint cells have 64. Durable sync calls remain 128 in both cells; relaxed fence has 64 versus 128 checkpoint calls. These are bracketed observations, not an already-durable no-I/O optimization or crash proof.

One cell per profile/boundary, no alternating repeat or steady-state/public YCSB speedup claim.
Every operation and complete original/fresh lifecycle outcome retained; no replacement, exclusion or retry.
Post-create/post-loop snapshots bracket insert plus boundary loops; counts and summed durations are not disjoint wall fractions or durability authority.
Public and backend snapshots may expose the same counters; never sum domains. Snapshot differences can be gauges/maxima and are not all monotonic counters.
Fence loop checkpoints zero does not prove crash durability. Original graceful Close may checkpoint outside the loop.
This validates reachability/usefulness only; gateway integration, partial-DDL/auth/index/dictionary closure, held sync/failure/Close/crash proof and strict100k qualification remain required.

Analysis SHA256: b23f8287d8acc8420a9d5ad4d5889c4d213e712b4215a098bc63c7d9c5755a0e
