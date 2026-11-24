package db

// MemtableWorkload describes the Open-time automatic memtable selection policy
// used when Options.MemtableMode is empty or "auto".
type MemtableWorkload uint8

const (
	// MemtableWorkloadDefault selects the default mutable adaptive memtable
	// policy for raw KV workloads when MemtableMode is empty or "auto".
	MemtableWorkloadDefault MemtableWorkload = iota
	// MemtableWorkloadSemantic selects the immutable COW B-tree memtable
	// ("cow_btree") and StandaloneCOWMemtableLimits at Open time for writable,
	// non-backend-only semantic/collection workloads when MemtableMode is empty
	// or "auto".
	MemtableWorkloadSemantic
)
