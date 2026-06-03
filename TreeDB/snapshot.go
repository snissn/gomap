package treedb

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Snapshot is a consistent point-in-time view of the database.
//
// In cached mode, snapshots include writes that are buffered in memtables.
//
// TreeDB is pre-alpha; this interface may change without notice.
type Snapshot interface {
	Pager() *pager.Pager
	State() *backenddb.DBState

	Get(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	GetManyView(keys [][]byte, fn GetManyViewFunc) error
	GetUnsafe(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	HasMany(keys [][]byte) ([]bool, error)
	HasPrefixes(prefixes [][]byte) ([]bool, error)

	GetEntry(key []byte) (node.LeafEntry, error)
	GetEntryExact(key []byte) (node.LeafEntry, error)

	Close() error
}
