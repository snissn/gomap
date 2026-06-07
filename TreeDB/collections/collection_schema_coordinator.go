package collections

import (
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type collectionSchemaCoordinator struct {
	schemaMu  sync.RWMutex
	domainsMu sync.Mutex
	domains   map[*collectionWriteDomain]struct{}
}

type collectionDBSchemaCoordinators struct {
	mu          sync.Mutex
	collections map[string]*collectionSchemaCoordinator
}

var collectionSchemaCoordinators sync.Map

func collectionSchemaCoordinatorForDBCollection(db *backenddb.DB, collection string) *collectionSchemaCoordinator {
	if db == nil || collection == "" {
		return nil
	}
	dbCoord := collectionDBSchemaCoordinatorForDB(db)
	if dbCoord == nil {
		return nil
	}
	dbCoord.mu.Lock()
	defer dbCoord.mu.Unlock()
	if dbCoord.collections == nil {
		dbCoord.collections = make(map[string]*collectionSchemaCoordinator)
	}
	coord := dbCoord.collections[collection]
	if coord == nil {
		coord = &collectionSchemaCoordinator{}
		dbCoord.collections[collection] = coord
	}
	return coord
}

func collectionDBSchemaCoordinatorForDB(db *backenddb.DB) *collectionDBSchemaCoordinators {
	if db == nil {
		return nil
	}
	coord := &collectionDBSchemaCoordinators{}
	var actual any
	var loaded bool
	if _, ok := db.RegisterCloseHookIfOpenAfter(func() bool {
		actual, loaded = collectionSchemaCoordinators.LoadOrStore(db, coord)
		return !loaded
	}, func() error {
		collectionSchemaCoordinators.Delete(db)
		return nil
	}); !ok {
		return nil
	}
	if loaded {
		return actual.(*collectionDBSchemaCoordinators)
	}
	return coord
}

func (coord *collectionSchemaCoordinator) registerDomain(domain *collectionWriteDomain) {
	if coord == nil || domain == nil {
		return
	}
	coord.domainsMu.Lock()
	defer coord.domainsMu.Unlock()
	if coord.domains == nil {
		coord.domains = make(map[*collectionWriteDomain]struct{})
	}
	coord.domains[domain] = struct{}{}
}

func (coord *collectionSchemaCoordinator) snapshotDomains() []*collectionWriteDomain {
	if coord == nil {
		return nil
	}
	coord.domainsMu.Lock()
	defer coord.domainsMu.Unlock()
	out := make([]*collectionWriteDomain, 0, len(coord.domains))
	for domain := range coord.domains {
		if domain != nil {
			out = append(out, domain)
		}
	}
	return out
}

func (c *Collection) collectionSchemaCoordinator() *collectionSchemaCoordinator {
	if c == nil {
		return nil
	}
	if c.writeDomain != nil && c.writeDomain.schemaCoordinator != nil {
		return c.writeDomain.schemaCoordinator
	}
	name := c.name
	if name == "" {
		name = c.meta.Name
	}
	return collectionSchemaCoordinatorForDBCollection(c.db, name)
}

func (c *Collection) lockCollectionSchemaRead() func() {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return func() {}
	}
	coord.schemaMu.RLock()
	return coord.schemaMu.RUnlock
}

func (c *Collection) lockCollectionSchemaWrite() func() {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return func() {}
	}
	coord.schemaMu.Lock()
	return coord.schemaMu.Unlock
}

func (c *Collection) flushCollectionWriteDomainsForSchemaMutation() error {
	if c == nil || c.db == nil {
		return nil
	}
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return c.flushBufferedWrites()
	}
	for _, domain := range coord.snapshotDomains() {
		if domain == nil {
			continue
		}
		if err := flushCollectionWriteDomain(c.db, domain); err != nil {
			return err
		}
	}
	return nil
}
