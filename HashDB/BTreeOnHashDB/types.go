package btreeonhashdb

// MaxKeys defines the maximum number of keys a node can hold before splitting.
// A fan-out of 128 keeps nodes compact while providing good branching.
const MaxKeys = 128

// NodeID uniquely identifies a node within a tree.
type NodeID uint64

// NodeType distinguishes internal and leaf nodes.
type NodeType uint8

const (
	NodeInternal NodeType = 1
	NodeLeaf     NodeType = 2
)

// KVStore is the minimal interface the B+Tree expects from the backing store.
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// Options configures the BTree.
type Options struct {
	CacheSize int // Number of nodes to keep in memory. Default 128.
}
