package btreeonhashdb

import (
	"fmt"

	"github.com/snissn/gomap/HashDB"
)

// HashDBKV adapts HashDB to the KVStore interface.
type HashDBKV struct {
	Store *hashdb.HashDB
}

// PutMany batches multiple puts when available.
func (g *HashDBKV) PutMany(keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return fmt.Errorf("keys/vals length mismatch")
	}
	items := make([]hashdb.Item, len(keys))
	for i := range keys {
		items[i] = hashdb.Item{Key: keys[i], Value: vals[i]}
	}
	return g.Store.PutMany(items)
}

// NewTreeOnHashDB constructs a Tree backed by HashDB.
func NewTreeOnHashDB(store *hashdb.HashDB, treeID string, opts *Options) (*Tree, error) {
	kv := &HashDBKV{Store: store}
	return OpenTree(kv, treeID, opts)
}

func (g *HashDBKV) Get(key []byte) ([]byte, error) {
	return g.Store.Get(key)
}

func (g *HashDBKV) Put(key, value []byte) error {
	return g.Store.Put(key, value)
}

func (g *HashDBKV) Delete(key []byte) error {
	return g.Store.Delete(key)
}

func (g *HashDBKV) Flush() error {
	return nil
}
