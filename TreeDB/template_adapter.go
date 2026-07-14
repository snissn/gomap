package treedb

import (
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
)

type templateKV struct {
	db *DB
}

func (kv templateKV) Get(key []byte) ([]byte, error) {
	return kv.db.Get(key)
}

func (kv templateKV) SetSync(key, value []byte) error {
	return kv.db.SetSync(key, value)
}

func (kv templateKV) DeleteSync(key []byte) error {
	return kv.db.DeleteSync(key)
}

func (kv templateKV) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	b := kv.db.NewBatch()
	if b == nil {
		return nil
	}
	return b
}

func (kv templateKV) AcquireStableTemplateSnapshot() (templatedb.StablePhysicalSnapshot, error) {
	if kv.db == nil {
		return nil, nil
	}
	// Cached WriteSync establishes journal durability, but the exact backend
	// index generation cannot certify a definition until the cached mutation has
	// crossed a checkpoint boundary. Capture is a rare publication operation,
	// so establish that physical boundary here before pinning the backend view.
	if kv.db.cached != nil {
		if err := kv.db.Checkpoint(); err != nil {
			return nil, err
		}
	}
	return acquireStableTemplateSnapshot(kv.db.backend), nil
}
