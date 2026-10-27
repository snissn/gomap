package collections

import (
	"errors"
	"fmt"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ErrIndexNameConflict identifies a cross-kind index name collision detected
// under the collection's schema authority.
var ErrIndexNameConflict = errors.New("collections: index name conflicts with another index kind")

// CollectionIndexDDL is an operation-scoped index publication owner. It is valid
// only inside WithVectorIndexDDL's callback. Call its methods instead of the
// collection's public DDL methods while that callback holds schema authority.
// Every accepted publication preserves the original partial scalar-DDL semantics.
type CollectionIndexDDL struct {
	mu         sync.Mutex
	active     bool
	collection *Collection
	meta       CollectionMeta
	vectors    map[string]VectorIndexDefinition
}

// WithVectorIndexDDL validates all vector declarations against one captured
// collection incarnation and schema before invoking fn. The same DDL authority
// remains held through every dependent publication made through the token.
// The callback must not recursively invoke public collection DDL.
func (c *Collection) WithVectorIndexDDL(definitions []VectorIndexDefinition, fn func(*CollectionIndexDDL) error) error {
	if c == nil {
		return errCollectionNil
	}
	if fn == nil {
		return errors.New("collections: nil index DDL callback")
	}
	if c.manager != nil && c.manager.isClosing() {
		return backenddb.ErrClosed
	}
	var meta CollectionMeta
	if c.nativeJSON != nil {
		if err := c.nativeVectorIndexDDLPreflight(func(meta CollectionMeta) error {
			_, err := validateVectorIndexDDL(meta, definitions, true)
			return err
		}); err != nil {
			return err
		}
		// Producer pressure may publish physical assets and requires both DDL
		// locks to be available. Revalidate the later capture under authority.
		if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
			return err
		}
		unlockMutation := c.lockVectorIndexMutation()
		defer unlockMutation()
		c.nativeJSON.mu.Lock()
		defer c.nativeJSON.mu.Unlock()
		read, err := c.nativeJSON.Capture(c.name)
		if err != nil {
			return err
		}
		meta = *read.meta.copy()
		if err := read.Close(); err != nil {
			return err
		}
	} else {
		if c.db == nil {
			return errCollectionDBNil
		}
		unlockSchema := c.lockCollectionSchemaWrite()
		defer unlockSchema()
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return backenddb.ErrClosed
		}
		catalog, err := loadCollectionCatalog(snap, c.collectionName())
		if err != nil {
			_ = snap.Close()
			return err
		}
		if catalog == nil {
			_ = snap.Close()
			return errCollectionNotFound
		}
		meta = *catalog.meta.copy()
		if err := snap.Close(); err != nil {
			return err
		}
	}
	vectors, err := validateVectorIndexDDL(meta, definitions, c.nativeJSON != nil)
	if err != nil {
		return err
	}

	token := &CollectionIndexDDL{active: true, collection: c, meta: meta, vectors: vectors}
	defer func() {
		token.mu.Lock()
		token.active = false
		token.collection = nil
		token.vectors = nil
		token.mu.Unlock()
	}()
	return fn(token)
}

// validateVectorIndexDDL checks the complete captured schema without preparing
// or publishing native state. Existing declarations retain owner idempotence.
func validateVectorIndexDDL(meta CollectionMeta, definitions []VectorIndexDefinition, native bool) (map[string]VectorIndexDefinition, error) {
	vectors := make(map[string]VectorIndexDefinition, len(definitions))
	candidate := *meta.copy()
	for _, requested := range definitions {
		def, err := normalizeVectorIndexDefinition(requested)
		if err != nil {
			return nil, err
		}
		if _, duplicate := vectors[def.Name]; duplicate {
			return nil, fmt.Errorf("collections: duplicate index %q", def.Name)
		}
		vectors[def.Name] = def
		if _, found := findIndex(meta.Indexes, def.Name); found {
			return nil, fmt.Errorf("%w: %q", ErrIndexNameConflict, def.Name)
		}
		if existing, found := findVectorIndex(meta.VectorIndexes, def.Name); found {
			if !VectorIndexDefinitionsLogicallyEqual(existing, def) {
				return nil, fmt.Errorf("collections: existing vector index %q has a different definition", def.Name)
			}
			continue
		}
		var errAdd error
		candidate, _, errAdd = addVectorIndexToCollectionMeta(candidate, def)
		if errAdd != nil {
			return nil, errAdd
		}
		// Typed native collections support their original configured vector only;
		// adding an index is unsupported even when its declaration shape is valid.
		if native && meta.Options.ColumnStore != nil {
			return nil, fmt.Errorf("%w: adding vector indexes to an existing ColumnStore collection is unsupported", backenddb.ErrNativeSemanticUnsupported)
		}
	}
	// This check also refuses existing BSON declarations before fn can publish.
	if _, err := normalizeCollectionMeta(candidate); err != nil {
		return nil, err
	}
	if native {
		if _, err := nativeJSONMeta(candidate); err != nil {
			return nil, err
		}
	}
	return vectors, nil
}

// nativeVectorIndexDDLPreflight rejects unsupported requests before producer
// maintenance can change physical state. Its locks and capture are released
// before the caller prepares; publication must revalidate under fresh authority.
func (c *Collection) nativeVectorIndexDDLPreflight(validate func(CollectionMeta) error) error {
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	c.nativeJSON.mu.Lock()
	defer c.nativeJSON.mu.Unlock()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	return errors.Join(validate(read.meta), read.Close())
}

// VectorIndexDefinitionsLogicallyEqual compares declaration identities without
// generated schema generations. Both inputs must be valid modern declarations.
func VectorIndexDefinitionsLogicallyEqual(a, b VectorIndexDefinition) bool {
	aNormalized, err := normalizeVectorIndexDefinition(a)
	if err != nil {
		return false
	}
	bNormalized, err := normalizeVectorIndexDefinition(b)
	if err != nil {
		return false
	}
	aNormalized.SchemaGeneration = 0
	bNormalized.SchemaGeneration = 0
	return vectorIndexDefinitionValuesEqual(aNormalized, bNormalized)
}

// Meta returns the current schema after this owner's accepted publications.
func (owner *CollectionIndexDDL) Meta() CollectionMeta {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active {
		return CollectionMeta{}
	}
	return *owner.meta.copy()
}

func (owner *CollectionIndexDDL) CreateIndex(def IndexDefinition) (*CollectionMeta, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active {
		return nil, errors.New("collections: index DDL owner has expired")
	}
	c := owner.collection
	var meta *CollectionMeta
	var err error
	if c.nativeJSON != nil {
		meta, err = c.nativeJSON.mutateIndexFamiliesLocked(c, c.name, &def, nil, false, nil, "", nil)
	} else {
		if c.db.CommandWALEnabled() {
			return nil, fmt.Errorf("%w: collection catalog index mutation is rejected under command_wal_v2 until catalog index commands are supported", backenddb.ErrCommandWALRejected)
		}
		for attempt := range maxCollectionMutationRetries {
			meta, err = c.createIndexOnceSchemaLocked(def)
			if err == nil || !isRetriableTextIndexMutationError(err) {
				break
			}
			waitBeforeCollectionMutationRetry(attempt)
		}
	}
	if meta != nil {
		owner.meta = *meta.copy()
	}
	return meta, err
}

func (owner *CollectionIndexDDL) CreateVectorIndex(requested VectorIndexDefinition) (*CollectionMeta, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.active {
		return nil, errors.New("collections: index DDL owner has expired")
	}
	def, err := normalizeVectorIndexDefinition(requested)
	if err != nil {
		return nil, err
	}
	declared, ok := owner.vectors[def.Name]
	if !ok || !VectorIndexDefinitionsLogicallyEqual(declared, def) {
		return nil, errors.New("collections: vector definition was not admitted by this DDL owner")
	}
	if existing, found := findVectorIndex(owner.meta.VectorIndexes, def.Name); found {
		if !VectorIndexDefinitionsLogicallyEqual(existing, def) {
			return nil, fmt.Errorf("collections: vector index %q definition changed", def.Name)
		}
		return owner.meta.copy(), nil
	}
	c := owner.collection
	var meta *CollectionMeta
	if c.nativeJSON != nil {
		meta, err = c.nativeJSON.mutateIndexFamiliesLocked(c, c.name, nil, nil, false, &def, "", nil)
	} else {
		meta, err = c.createVectorIndexSchemaLocked(def)
	}
	if meta != nil {
		owner.meta = *meta.copy()
	}
	return meta, err
}
