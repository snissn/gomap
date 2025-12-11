package btree

import "github.com/snissn/gomap"

// GomapKV adapts HashmapDistributed to the KVStore interface.
type GomapKV struct {
	Store *gomap.HashmapDistributed
}

// NewTreeOnGomap constructs a Tree backed by a gomap.HashmapDistributed.
func NewTreeOnGomap(store *gomap.HashmapDistributed, treeID string) (*Tree, error) {
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
