# Resource Cost, Capacity, Replay Budget, and Backpressure Review

## Role / persona

You are a database performance engineer responsible for bounding write
amplification, replay time, memory growth, disk-retention debt, and backpressure
behavior under worst-case collection workloads.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/caching/backpressure.go`
- `TreeDB/caching/vlog_queue_metrics.go`
- `TreeDB/caching/log_writer.go`
- `TreeDB/internal/commitlog/writer.go`
- `TreeDB/internal/commitlog/writer_bench_test.go`
- `TreeDB/internal/valuelog/encode_cost_model.go`
- `TreeDB/internal/valuelog/writer.go`
- `TreeDB/collections/overhead_bench_test.go`
- `TreeDB/collections/bench_test.go`
- `cmd/collection_workload_bench`
- `cmd/collection_bench_matrix`
- `cmd/collection_bench_report`
- `cmd/unified_bench`

## Task

Review whether the spec provides enough cost modeling to keep collection WAL
durable-at-ack from creating unbounded disk growth, replay stalls, memory
blowups, or throughput collapse. This prompt complements the existing
benchmark/test prompt by demanding a predictive cost model and capacity limits,
not only benchmark cases.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Bytes written per document for no-index, one-index, multi-index, update,
  delete, template-v1, and future column-store parts.
- Inline root-delta threshold versus side-payload threshold.
- Segment size, rotation policy, compression policy, sync batching, and write
  coalescing.
- Replay memory bounds when many transactions share one base root and require
  accumulation/rebasing.
- Worst-case replay CPU and IO for 1K, 100K, 1M, and larger pending documents.
- Disk-retention debt when checkpointing, async publish, side-ref protection, or
  cleanup is stalled.
- Backpressure rules for pending WAL bytes, pending side-ref bytes, unpublished
  root deltas, replay accumulator size, and protected side files.
- Whether WAL-on relaxed versus durable sync modes need different cost gates.
- Whether existing benchmark commands can isolate collection WAL overhead from
  value-log, memtable, index, and native-wire overhead.
- Whether "regression <= X%" gates are enough without absolute latency, tail
  latency, memory, disk, and replay-time ceilings.
- Whether column-store side files require a separate capacity model before they
  are allowed to publish persistent roots.

## Focus questions

1. What is the expected WAL byte amplification per indexed document?
2. What happens if a collection receives acknowledged writes for hours while
   async publish is blocked?
3. What is the maximum memory recovery may use for replay-side accumulation?
4. At what size must root deltas move from inline WAL to side payload?
5. How are protected side refs charged to backpressure?
6. Can cleanup lag create unbounded value-log retention?
7. What exact benchmark labels and output columns should be added?
8. What capacity limits are enforced versus merely observed?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Unbounded resource risk
- Finding:
- Evidence:
- Worst-case workload:
- Bound missing:
- Required remediation:

## P1 - Required capacity model before WAL-on enablement

## P2 - Benchmark/reporting gaps

## P3 - Optimization opportunities

# Solution phase

## Exact spec edits
Include proposed formulas or tables for:
- WAL bytes/doc
- Side-ref bytes/doc
- Replay memory bound
- Replay time bound
- Cleanup debt bound
- Backpressure trigger

## Implementation constraints
- Hard limits:
- Soft limits:
- Backpressure behavior:
- Error behavior:
- Config defaults:

## Tests
- Unit tests:
- Stress tests:
- Fault/backpressure tests:
- Cleanup-lag tests:

## Benchmarks
For each benchmark:
- Command/package:
- Workload:
- Required columns:
- Gate:
- Reason:

## Sequencing
- Cost model required before PR1:
- Backpressure required before async/default enablement:
- Column-store capacity gates:

## Open questions
```

## Required solution phase

Propose concrete cost formulas, default limits, benchmark additions, and
backpressure constraints. For every limit, say whether it is a hard error,
blocking wait, adaptive flush trigger, or metric-only warning.

