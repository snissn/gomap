# HashDB Snapshots (Export/Restore)

HashDB supports a simple snapshot mechanism for moving data between stores:

- `(*hashdb.DB).Export(w io.Writer)` / `Restore(r io.Reader)`
- `(*hashdb.HashDB).Export(w io.Writer)` / `Restore(r io.Reader)`

## Key Properties

- Iteration/snapshot order is **arbitrary** (not sorted, not stable across runs).
- `Restore` is implemented using `ApplyBatchSync` (durable restore).
- Existing keys are overwritten.

## Minimal Example

```go
src, _ := hashdb.Open("./src")
defer src.Close()

_ = src.PutSync([]byte("a"), []byte("1"))

var buf bytes.Buffer
_ = src.Export(&buf)

dst, _ := hashdb.Open("./dst")
defer dst.Close()

_ = dst.Restore(bytes.NewReader(buf.Bytes()))
```

## When To Use This

- Tests, demos, and simple “copy the keyspace” workflows.
- Building higher-level snapshotting in a downstream system where ordering is not required.

If you need ordered iteration or stable key ordering, use TreeDB for snapshot scans.
