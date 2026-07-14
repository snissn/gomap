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

func (kv templateKV) AcquireStableTemplateSnapshot() templatedb.StablePhysicalSnapshot {
	if kv.db == nil {
		return nil
	}
	return acquireStableTemplateSnapshot(kv.db.backend)
}
