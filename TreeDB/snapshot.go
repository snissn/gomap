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
// Iterators created from a snapshot are owned by it: Close invalidates every
// outstanding iterator, which callers must still close to release its retained
// resources. After Close, point reads return db.ErrClosed, Pager and State
// return nil, and all iterator constructors return db.ErrClosed. TreeDB is
// pre-alpha; this interface may change without notice.
type Snapshot interface {
	Pager() *pager.Pager
	State() *backenddb.DBState

	Get(key []byte) ([]byte, error)
	GetAppend(key, dst []byte) ([]byte, error)
	GetVersioned(key []byte) ([]byte, EntryRevision, error)
	GetVersionedAppend(key, dst []byte) ([]byte, EntryRevision, error)
	GetManyView(keys [][]byte, fn GetManyViewFunc) error
	GetUnsafe(key []byte) ([]byte, error)
	Has(key []byte) (bool, error)
	HasMany(keys [][]byte) ([]bool, error)
	HasPrefixes(prefixes [][]byte) ([]bool, error)
	Iterate(start, end []byte, fn func(key, value []byte) error) error
	ReverseIterate(start, end []byte, fn func(key, value []byte) error) error
	Iterator(start, end []byte) (Iterator, error)
	ReverseIterator(start, end []byte) (Iterator, error)

	GetEntry(key []byte) (node.LeafEntry, error)
	GetEntryExact(key []byte) (node.LeafEntry, error)

	Close() error
}
