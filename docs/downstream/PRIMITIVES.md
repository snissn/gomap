# Downstream Storage Primitives

## TL;DR

This repo does not (yet) ship a dedicated “raft log” or “stable store” package, but TreeDB and HashDB
are intended to be reliable primitives you can build those on top of.

This document defines the recommended interfaces and the mapping to the existing engines.

## Who Is This For?

- Anyone building replication/consensus or a replicated state machine on top of this repo.
- Contributors who want to evolve storage without breaking downstream assumptions.

## Recommended Interfaces

The following interfaces are intentionally small and map directly onto `treedb` / `hashdb`.

### Stable Store (term/vote/config)

Persist small pieces of consensus metadata: current term, voted-for, and config/state machine metadata.

```go
type StableStore interface {
	Get(key []byte) ([]byte, error)
	SetSync(key, value []byte) error
	DeleteSync(key []byte) error
}
```

Notes:
- Use `*Sync` methods only; stable store values are treated as “committed” by the consensus system.
- Keyspace should be versioned/prefixed (see “Encoding & Versioning” below).

### Log Store (append + truncate)

Store the replicated log by monotonically increasing index.

```go
type LogStore interface {
	// AppendSync appends entries at the end of the log and makes them durable.
	AppendSync(entries [][]byte) (firstIndex, lastIndex uint64, err error)

	// Get returns the entry at index, or nil if missing/compacted.
	Get(index uint64) ([]byte, error)

	// TruncatePrefixSync removes all entries < firstIndexToKeep.
	TruncatePrefixSync(firstIndexToKeep uint64) error

	// TruncateSuffixSync removes all entries > lastIndexToKeep.
	TruncateSuffixSync(lastIndexToKeep uint64) error

	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
}
```

Implementation guidance:
- Encode log index keys so they sort by index for prefix scans.
- Keep log metadata (`first`, `last`) in the stable store keyspace.
- Use durable, atomic batch writes for multi-entry appends and truncations.

### State Machine Store (apply + snapshot)

Apply committed commands atomically and support snapshot/restore.

```go
type BatchOp struct {
	Type  uint8 // Put/Delete
	Key   []byte
	Value []byte
}

type StateMachineStore interface {
	ApplyBatchSync(ops []BatchOp) error
	Iterator(start, end []byte) (it Iterator, err error) // ordered, [start,end)
}

type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close() error
	Error() error
}
```

Snapshot/restore can be layered on top:
- **Snapshot:** iterate `[nil,nil)` and stream key/value pairs (ordered).
- **Restore:** re-apply key/value pairs via `ApplyBatchSync` into an empty store (or overwrite-in-place if your system allows).

## Engine Selection (Current)

### TreeDB (`github.com/snissn/gomap/TreeDB`, package `treedb`)

Recommended for:
- Log store and state machine store when you need **ordered iteration** and predictable range scans.
- Anything where “committed implies durable” via `SetSync` / `Batch.WriteSync`.

Rationale:
- Ordered B+Tree iterators with explicit bounds `[start,end)` and stable ordering.
- Coherent crash recovery across cached vs backend opens.

### HashDB (`github.com/snissn/gomap/HashDB`, package `hashdb`)

Recommended for:
- High-throughput random reads/writes and bulk batch-apply (`ApplyBatchSync`) when ordered iteration is not required.

Rationale:
- Extremely fast random access.
- Snapshot export exists, but iteration order is arbitrary; downstream snapshotting usually prefers ordered scans.

## Encoding & Versioning

Downstream systems should treat storage as an evolving schema.

Recommended rules:
- Prefix every keyspace with a version byte and a subsystem prefix, e.g.:
  - `0x01 "stable/" ...`
  - `0x01 "log/" ...`
  - `0x01 "sm/" ...`
- For log entries, encode indices as big-endian `uint64` so lexicographic order matches numeric order:
  - key: `0x01 "log/e/" + be64(index)`
- Keep metadata as explicit keys:
  - `0x01 "log/first"`, `0x01 "log/last"`

## Contract Tests

This repo includes cross-engine contract tests under `internal/contracttest/` covering:
- durability across simulated crashes,
- batch atomicity,
- snapshot/restore round-trips,
- basic concurrency and iterator bounds.

