package treedb

import (
	treedbbatch "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
)

type templateKV struct {
	db *db.DB
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
	return templateBatch{b: b}
}

type templateBatch struct {
	b treedbbatch.Interface
}

func (tb templateBatch) Set(key, value []byte) error { return tb.b.Set(key, value) }
func (tb templateBatch) Delete(key []byte) error     { return tb.b.Delete(key) }
func (tb templateBatch) WriteSync() error            { return tb.b.WriteSync() }
func (tb templateBatch) Close() error                { return tb.b.Close() }
