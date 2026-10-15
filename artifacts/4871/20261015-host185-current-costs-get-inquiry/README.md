# Exact5027d costs, failed raw gate, M0 and independent source audit

Product5027dc559f83dee04317910169c48d277fcce72f, tree a7710b6b41ebc62328a5282c959910193ce4cd0f; baseline5bca87ffe6e1bfba2d693264d2dc82936adc234f. This packet records actual completed evidence and preserves the failed Get gate. It grants no overall acceptance, mergeability, native success or final graph qualification. Subsequent source repairs are not relabeled as this candidate.

## Current hosted raw and PRIMARY

Run38812207864/job116688556482, actual Go1.26.9. Root reran the exact committed checker over all eight alternating pairs, verified all eight retained executable hashes, and independently reparsed the PRIMARY rows/activation/quietness evidence. Get fails the unchanged paired +5% limit; all other raw paths and enabled PRIMARY pass.

| Workload | Base to candidate ns/op | Base to candidate ops/s | B/op | allocs/op | Paired time delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| GetVersioned FAIL | 443.10 to485.35 | 2,256,826.90 to2,060,368.81 | 0 to0 | 0 to0 | +10.3409% |
| Conditional batch | 47,650.50 to48,014.50 | 20,986.14 to20,827.04 | 31944.5 to31502 | 127 to104 | +0.3493% |
| Snapshot seek | 303.20 to304.85 | 3,298,153.03 to3,280,301.79 | 0 to0 | 0 to0 | +0.3299% |
| Repeated iterator | 224.80 to222.00 | 4,448,398.58 to4,504,504.50 | 112 to112 | 2 to2 | -1.1322% |
| Durable tiny WriteSync | 188,651.50 to190,109.50 | 5,300.78 to5,260.13 | 4577 to4555.5 | 17 to17 | +0.3916% |
| Enabled PRIMARY operation | 15,282,265.50 to7,523,205.50 | 65.44 to132.92 | 280314 to228355 | 476 to391 | -51.1798% |

PRIMARY DATA-generation advances/op are2 to0. Its lifecycle/reopen activation and original sampled quietness checks pass. Raw timing has only its original process snapshot, not the PRIMARY sampled quiet series. Both original packet and independent verification are under performance-185/5027d-hosted.

## Current hosted fixed14 maintenance

Run38812742507, harness25ebe4d8c775a10274c360f2ff80aa49b5601599, product5027d versus5bca, pinned Go1.26.3. Reviewed successor changes only six literal source/head/output identities. One new dispatch was authorized after the product allocation repair. Every source manifest, four ELF hashes, full compiler binding, row and quiet observation is independently verified. All fourteen calls complete; none are replaced.

| Operation | Base to candidate ns/op | Base to candidate ops/s | B/op | allocs/op | Paired time delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Public dirty checkpoint | 13,647,465 to13,527,797 | 73.27 to73.92 | 459300 to471613 | 859 to886 | -1.3874% |
| Public vacuum | 3,472,211,264 to3,296,443,935 | 0.2880 to0.3034 | 186182936 to179753400 | 615931 to518238 | -0.4465% |

Checkpoint residual +12,313B/op (+2.68%) and+27 objects/op (+3.14%) remains unaccepted. Three pairs only; all individual values remain, including vacuum+16.33%. The earlier72e +40,990B/+32 objects retains its separate identity. Different runs are not a causal speedup estimate.

Cohort8/16 capture uses3722/3786B/op and14/14objects,11,939/11,625ns/op (83,759/86,022ops/s). Governed bytes reach0 at actual DB.Close. Immediate GC retains all8/16 paths and6528/13056 value-log bytes; no path-deletion claim. Last-reader cleanup54,893/92,519ns is one off-timer event each, not a latency distribution.

## Current M0

The actual M0 run is38812207970 (38812207908 is a separate sparse-catalog workflow). All20 original raw rows reproduce the exact committed summarizer and an independent arithmetic check. CV comparison allows only floating-point rounding of1e-14 relative/1e-15 absolute; the original0.10 gate is unchanged. Legacy vacuum/writer-pause/p99 CVs are0.4521%/2.9068%/2.8251%; all three gates pass. The original logical digest and917504 to131072 index-byte fixture are retained, public vacuum is available, with zero unexpected errors/exposure misses. Public p99 CV is17.27%, reported but never an original M0 stability gate. This is same-head stability/exposure evidence, not matched revision performance acceptance. Older failed M0 attempts remain in prior packets with unknown cause.

## Independent audit and causal diagnostic

Fresh exact gpt-6.1-sol source review inventories all539 changed paths, directly inspects123 relevant files/hunks and covers all19 allocation groups. Root verifies every candidate blob and content hash. No demonstrated ordinary correctness blocker; conditional SOURCE ACCEPT, overall HOLD for performance and incomplete gates. Inventory-only coverage is explicit. This is internal source review, not a hosted review or final fallback receipt.

Static comparison binds the original baseline and both candidate ELFs. Baseline is byte-identical across the prior/current hosted runs;31 selected candidate Get-route symbols share normalized instruction shape, but shifted addresses do not establish cost equality. Prior and current hosted CPUs are different EPYC models. Neither observation waives the same-run failed gate.

Root authorized exactly six diagnostic CPU profiles, order B,C50,C72,C72,C50,B, retained original ELFs, CPU0/GOMAXPROCS1,8s benchmark duration,30s per-process/180s total bound. All complete in about66s. Exact argv, environment, host, process names, CPU/frequency samples, profiles, offline pprof commands and disjoint protobuf sample partitions are retained. No retry or accepted timing measurement. Whole-process profiles include calibration/setup/teardown; printed benchmark N is not their operation denominator. The profile report identifies a concrete stack-defer mechanism in snapshot capture and a singleton-kind temporary allocation opportunity. Subsequent repairs require their own source binding, correctness review and affected performance refresh.

## Storage and scope

Manifest entries bind every published byte to its original local source hash. Large text/source maps are deterministic gzip; executables and SDK archives remain retained locally with hashes/provenance in binary-bindings.json, rather than copied into Git. Native connected production work remains separate; Q32/ACK and real publication/Finish remain open. No source, cap, workload, timer, gate or timeout is relaxed by this packet.
