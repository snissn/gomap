# Snapshot iterator paired performance gate

The hosted gate builds exactly one `TreeDB` test binary, records its SHA-256, and
uses `collect_snapshot_iterator_pairs.sh` to run every measured row on one CPU.
Each row has eight samples arranged as four `AB` and four `BA` pairs, where A is
the snapshot iterator and B is the public iterator baseline. The collector
annotates every raw benchmark row with its pair number and order.

`main.go` rejects incomplete, duplicated, non-finite, unbalanced, or improperly
ordered pairs. It gates the median of the per-pair `(snapshot/public - 1)` timing
deltas at 5%, while retaining independent snapshot/public medians solely as
diagnostic context. Any B/op or allocs/op median increase also fails the gate.

The evidence artifact includes the exact head, binary digest, runner image, CPU
model, pinned affinity, Go version, raw annotated samples, JSON result, and
Markdown summary. This is a same-binary relative comparison, not an absolute
cross-run performance result.
