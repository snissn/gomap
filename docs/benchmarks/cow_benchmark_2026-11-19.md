# Standalone LOAD and native COW collection inserts

These observations use host185 on November 19, 2026 local time. The host
has an Intel i5-11400F (6 cores, 12 threads), Linux 6.8.0-138-generic, and
shared ext4 storage on the same local NVMe volume. GoMap and Go YCSB
were built with Go 1.26.10 and CGO enabled. The timed windows used the
shared measurement locks while ordinary services remained active.

## Standalone YCSB LOAD

The GoMap product executable and MongoDB 8.2.7 each served a fresh BSON
collection over local Mongo wire. The same Go YCSB client used 64 threads,
hashed keys and ten 100-byte fields. The client generated field values
for each run. Each trial passed the typed census and checked client and
server shutdown. Three pairs per ACK class ran in GoMap/MongoDB,
MongoDB/GoMap, GoMap/MongoDB order.

| Requested ACK | Rows per trial | GoMap median inserts/s (range) | MongoDB median inserts/s (range) | GoMap/MongoDB |
| --- | ---: | ---: | ---: | ---: |
| w=1, journal option absent | 100,000 | 60,503.9 (57,809.4–61,276.7) | 44,374.5 (39,311.7–45,716.4) | 1.36× |
| w=1, journal=true | 1,000 | 23.0 (21.1–23.1) | 1,692.6 (1,660.6–1,835.5) | 0.0136× |

GoMap used `command_wal_relaxed` for ordinary `w=1` and
`command_wal_durable` for journaled ACK. Each GoMap trial verified the
same live native bridge and resolved profile, a positive COW publication
delta before drain, and the exact count for the requested ACK class.
The ordinary case uses each engine's background durability policies.
Both comparison cells cover local requested ACKs under the specified
configuration. Power-loss equivalence and replication are outside scope.

Journaled GoMap sustained about 24 inserts/s in the smoke. The retained
journaled fixture uses 1,000 rows per trial at the same 64-client
concurrency. The gateway takes an exclusive standalone write-boundary
lock for each explicit journal request. The live counter recorded 1,000
physical sync boundaries per 1,000 journaled inserts in each GoMap sample.
[Standalone write-concern implementation](../../TreeDB/mongo_gateway/write_concern.go).

Throughput is YCSB's reported TOTAL rate. INSERT p99 describes each trial.
The post-ACK drain metric sums the checked GoMap `FlushAll` and
`CheckpointStorage` calls or records the checked MongoDB `fsync` call with
`lock:false`. The operator calls are distinct and measure remaining work
after each engine's background progress. Client shutdown, observer
coordination and the census are outside the drain metric.

| ACK | Trial | Inserts/s | INSERT p99 (ms) | Drain (ms) | COW publications |
| --- | --- | ---: | ---: | ---: | ---: |
| w1 | 01-gomap | 61,276.7 | 2.717 | 135.68 | 6,035 |
| w1 | 02-mongodb | 45,716.4 | 4.551 | 264.13 | — |
| w1 | 03-mongodb | 44,374.5 | 4.271 | 268.49 | — |
| w1 | 04-gomap | 57,809.4 | 2.817 | 141.57 | 6,205 |
| w1 | 05-gomap | 60,503.9 | 2.617 | 135.23 | 6,024 |
| w1 | 06-mongodb | 39,311.7 | 5.423 | 264.92 | — |
| journaled | 01-gomap | 23.1 | 2955.263 | 1.57 | 1,001 |
| journaled | 02-mongodb | 1,692.6 | 519.679 | 70.69 | — |
| journaled | 03-mongodb | 1,660.6 | 500.735 | 68.84 | — |
| journaled | 04-gomap | 21.1 | 5083.135 | 0.56 | 1,001 |
| journaled | 05-gomap | 23.0 | 2988.031 | 1.98 | 1,001 |
| journaled | 06-mongodb | 1,835.5 | 484.863 | 69.02 | — |

## Canonical collection inserts

Both owners use the same fixture, 16,000 documents, batches of 1,000,
the canonical index-vlog cell and `command_wal_relaxed`. The legacy timer
includes the final buffered publication. The native timer covers actual
COW publication through `Collection.InsertBatch`. Fixture creation is
outside the timer. The checked explicit checkpoint and ID census run after
the insert timer. Each row reports medians across three fresh samples.
Throughput is the inverse of median ns/doc.

| Format | Secondary indexes | Write owner | ns/doc | Docs/s | B/doc | Allocations/doc | Median checkpoint (ms) |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| BSON | 0 | Legacy backend | 1,351.0 | 740,192 | 1,798 | 1 | 34.62 |
| BSON | 0 | Native COW | 1,798.0 | 556,174 | 1,711 | 7 | 106.85 |
| JSON | 0 | Legacy backend | 1,223.0 | 817,661 | 1,767 | 1 | 34.16 |
| JSON | 0 | Native COW | 1,727.0 | 579,039 | 1,686 | 7 | 104.97 |
| BSON | 2 | Legacy backend | 2,447.0 | 408,664 | 3,351 | 1 | 49.90 |
| BSON | 2 | Native COW | 5,199.0 | 192,345 | 3,889 | 22 | 153.42 |

Native COW took 1.33× to 1.41× the legacy ns/doc with zero indexes,
and 2.12× with two BSON indexes. The two-index COW row allocated 22
times per document.

Every native sample recorded 16 COW publications across 16 batches
before the explicit checkpoint. Each legacy sample retained positive
`command_wal_publish` evidence. All samples verified 16,000 primary IDs
after checkpoint. These timings cover the insert matrix. SQLite, rewrite
and compacted footprint are outside scope.

The 100,000-document BSON native attempt hit `immutable memtable capacity
exhausted` at document 32,000 with batches of 16,000 and at document
65,000 in a separate attempt with batches of 1,000. The retained comparison
uses the largest fixture tested successfully across all three shapes in this
run. It keeps the standalone memory limits and introduces no intermediate
explicit checkpoint. Automatic background work follows each owner's
normal policy. The two failed attempts are in the receipts archive.

## Reproduction and receipts

The LOAD runner checkout was `942f14befa49c4c13b101510eb0e27b9b2927b9f`.
The canonical collection checkout was `2c37febdd58759252a7c2d27548b7cfcb926e0f3`.
The gateway and census were built from `56236b68727b684e8318a86eb76259e293ecef95`.
Both LOAD classes used Go YCSB source `3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`
and Docker image ID
`sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54`.
The archive includes the executable hashes, build commands, trial summaries,
per-sample results and raw observations.
[Receipts archive](cow_benchmark_2026-11-19/receipts.tar.gz) · [Archive checksum](cow_benchmark_2026-11-19/SHA256SUMS).

Run `--ack w1 --count 100000 --threads 64 --pairs 3` and
`--ack journaled --count 1000 --threads 64 --pairs 3` with fresh output
directories. [Standalone LOAD recipe](../../scripts/readme_mongodb_bench/README.md#standalone-load-comparison).

From the GoMap root, with a fresh output directory for each run

```sh
GOWORK=off ./scripts/bench_collections_canonical.sh -native-cow-companion \
  -skip-offline-rewrite -skip-full-leafgen -skip-sqlite -out-dir "$ZERO_DIR" \
  -docs 16000 -batch-size 1000 -indexes 0 -formats bson,json -benchtime 16000x -count 3
GOWORK=off ./scripts/bench_collections_canonical.sh -native-cow-companion \
  -skip-offline-rewrite -skip-full-leafgen -skip-sqlite -out-dir "$TWO_DIR" \
  -docs 16000 -batch-size 1000 -indexes 2 -formats bson -benchtime 16000x -count 3
```
