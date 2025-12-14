package btree

import (
	"fmt"

	"github.com/snissn/gomap/HashDB"
)

// GomapKV adapts HashmapDistributed to the KVStore interface.
type GomapKV struct {
	Store *hashdb.HashmapDistributed
}

// PutMany batches multiple puts when available.
func (g *GomapKV) PutMany(keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return fmt.Errorf("keys/vals length mismatch")
	}
	items := make([]hashdb.Item, len(keys))
	for i := range keys {
		items[i] = hashdb.Item{Key: keys[i], Value: vals[i]}
	}
	return g.Store.AddMany(items)
}

// NewTreeOnGomap constructs a Tree backed by a gomap.HashmapDistributed.
func NewTreeOnGomap(store *hashdb.HashmapDistributed, treeID string) (*Tree, error) {
	kv := &GomapKV{Store: store}
	return OpenTree(kv, treeID)
}

func (g *GomapKV) Get(key []byte) ([]byte, error) {
	return g.Store.Get(key)
}

func (g *GomapKV) Put(key, value []byte) error {
	return g.Store.Add(key, value)
}

func (g *GomapKV) Delete(key []byte) error {
	return g.Store.Delete(key)
}

func (g *GomapKV) Flush() error {
	return nil
}
