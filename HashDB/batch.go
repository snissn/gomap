package hashdb

// BatchOpType is the type of an operation in a batch.
type BatchOpType uint8

const (
	BatchOpPut BatchOpType = iota
	BatchOpDelete
)

// BatchOp is a single mutation applied by ApplyBatch/ApplyBatchSync.
type BatchOp struct {
	Type  BatchOpType
	Key   []byte
	Value []byte // only for BatchOpPut
}

func PutOp(key, value []byte) BatchOp {
	return BatchOp{Type: BatchOpPut, Key: key, Value: value}
}

func DeleteOp(key []byte) BatchOp {
	return BatchOp{Type: BatchOpDelete, Key: key}
}
