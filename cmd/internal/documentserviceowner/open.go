// Package documentserviceowner composes the command's one database lifetime.
package documentserviceowner

import (
	"errors"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

// Owner must outlive its listeners, requests and diagnostics callbacks. Backend
// is borrowed; Close releases the service, manager and original public DB.
type Owner struct {
	Backend *backenddb.DB
	Manager *collections.CollectionManager
	Service *documentservice.Service
	Stats   func() map[string]string
	Storage string
	closeDB func() error
	mu      sync.Mutex
}

// Open defaults to the ordinary native semantic owner. Legacy storage is an
// explicit process-wide choice for advanced routes, never a request fallback.
func Open(opts treedb.Options, legacyStorage bool) (*Owner, error) {
	var backend *backenddb.DB
	var cleanup func() error
	var stats func() map[string]string
	var maintenance *treedb.DeferredVectorBuildMaintenance
	storage := "shared_cow"
	if legacyStorage {
		var err error
		backend, cleanup, stats, maintenance, err = treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
		if err != nil {
			return nil, err
		}
		storage = "legacy"
	} else {
		opts.MemtableMode = "cow_btree"
		database, err := treedb.Open(opts)
		if err != nil {
			return nil, err
		}
		backend, err = database.NativeSemanticBackend()
		if err != nil {
			return nil, errors.Join(err, database.Close())
		}
		cleanup, stats, maintenance = database.Close, database.Stats, database.DeferredVectorBuildMaintenance()
	}
	manager := collections.NewCollectionManager(backend)
	return &Owner{Backend: backend, Manager: manager,
		Service: documentservice.NewWithDeferredVectorBuildMaintenance(manager, maintenance),
		Stats:   stats, Storage: storage, closeDB: cleanup}, nil
}

// Close is serialized and preserves errors from every owner. The constituent
// closes are idempotent; the original DB can retain a failed cleanup for retry.
func (o *Owner) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return errors.Join(o.Service.Close(), o.Manager.Close(), o.closeDB())
}
