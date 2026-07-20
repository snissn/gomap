# Vector-partition M1 evidence ledger

These are bounded, one-iteration local measurements of the M1 persistence
contract, not a production ANN or multi-group throughput claim. They use the
real manifest codec, stable-authorized ready publication, and Raft snapshot
archive/install path. RSS is `/usr/bin/time -v` maximum RSS for the complete Go
test harness (compile/setup/runtime included), not an isolated steady-state
heap measurement.

| Artifact | Candidate | Scope |
| --- | --- | --- |
| `vector-partition-m1-collections.txt` (`a96ead752c77c47085af64b501c9f6eb47019fefa40db740bf62d40c4e36722f`) | `27cb544c2078baba7da9ff451125706daa1484f4` | 10k/100k/1M codec plus warm open and public status |
| `vector-partition-m1-raft-snapshot.txt` (`4ab829fb259d5c4e4e7040165b407d54b9882a63196a4dc74107802b5115c25a`) | `27cb544c2078baba7da9ff451125706daa1484f4` | 10k/100k/1M Raft archive and clean-target install |

The codec's declared 1M disjoint fixture is 12.55 metadata bytes/vector, below
the #3910 64 bytes/vector gate. `VectorPartitionStatusV1` validates the live
TVIS/base identity by design; its warm measurement must therefore not be read
as a constant-time pointer lookup. The exact commands, result rows, harness RSS,
and SHA-256 artifact hashes are recorded with the PR evidence.
