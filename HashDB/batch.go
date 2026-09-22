package hashdb

// BatchOpType is the type of an operation in a batch.
type BatchOpType uint8

const (
	// BatchOpPut represents a put/update operation in a batch.
	BatchOpPut BatchOpType = iota
	// BatchOpDelete represents a delete operation in a batch.
	BatchOpDelete
)

// BatchOp is a single mutation applied by ApplyBatch/ApplyBatchSync.
type BatchOp struct {
	Type  BatchOpType
	Key   []byte
	Value []byte // only for BatchOpPut
}

// PutOp constructs a put batch operation for ApplyBatch/ApplyBatchSync.
func PutOp(key, value []byte) BatchOp {
	return BatchOp{Type: BatchOpPut, Key: key, Value: value}
}

// DeleteOp constructs a delete batch operation for ApplyBatch/ApplyBatchSync.
func DeleteOp(key []byte) BatchOp {
	return BatchOp{Type: BatchOpDelete, Key: key}
}
