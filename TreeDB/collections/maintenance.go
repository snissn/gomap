package collections

import (
	"bytes"
	"strconv"
)

type CollectionStats struct {
	Name             string
	MetadataVersion  uint16
	IDMode           IDMode
	StorageMode      CollectionStorageMode
	DocumentCount    uint64
	IndexCount       int
	IndexEntryCounts map[string]uint64
	UserRootPageID   uint64
	SystemRootPageID uint64
}

type ConsistencyReport struct {
	DocumentCount        uint64
	ExpectedIndexEntries uint64
	ActualIndexEntries   uint64
	MissingIndexEntries  uint64
	OrphanIndexEntries   uint64
}

func (c *Collection) ListIndexes() ([]IndexDefinition, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return nil, err
	}
	return append([]IndexDefinition(nil), c.meta.Indexes...), nil
}

func (c *Collection) Stats() (CollectionStats, error) {
	if c == nil {
		return CollectionStats{}, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return CollectionStats{}, err
	}

	stats := CollectionStats{
		Name:             c.meta.Name,
		MetadataVersion:  c.meta.Version,
		IDMode:           c.meta.Options.IDMode,
		StorageMode:      c.meta.Options.StorageMode,
		IndexCount:       len(c.meta.Indexes),
		IndexEntryCounts: make(map[string]uint64, len(c.meta.Indexes)),
	}
	for _, idx := range c.meta.Indexes {
		stats.IndexEntryCounts[idx.Name] = 0
	}

	if err := c.walkDocuments(func(_ []byte, _ []byte) error {
		stats.DocumentCount++
		return nil
	}); err != nil {
		return CollectionStats{}, err
	}

	for _, idx := range c.meta.Indexes {
		rootDesc, _, err := c.secondaryRootDescriptor(idx)
		if err != nil {
			return CollectionStats{}, err
		}
		indexPrefix, err := CollectionIndexPrefix(c.meta.Name, idx.Name)
		if err != nil {
			return CollectionStats{}, err
		}
		count, err := c.countVisibleKeysWithPrefixAtRoot(rootDesc.RootPageID, indexPrefix)
		if err != nil {
			return CollectionStats{}, err
		}
		stats.IndexEntryCounts[idx.Name] = count
	}

	stats.UserRootPageID, stats.SystemRootPageID = c.rootPageIDs()
	return stats, nil
}

func (c *Collection) CheckConsistency() (ConsistencyReport, error) {
	if c == nil {
		return ConsistencyReport{}, errCollectionNil
	}
	if err := c.refreshMeta(); err != nil {
		return ConsistencyReport{}, err
	}

	report := ConsistencyReport{}
	expected := make(map[string]struct{})
	if err := c.walkDocuments(func(documentID, document []byte) error {
		report.DocumentCount++
		indexKeys, err := c.indexEntriesForDocument(documentID, document)
		if err != nil {
			return err
		}
		for _, indexKey := range indexKeys {
			expected[string(indexKey)] = struct{}{}
		}
		return nil
	}); err != nil {
		return ConsistencyReport{}, err
	}
	report.ExpectedIndexEntries = uint64(len(expected))

	actual := make(map[string]struct{}, len(expected))
	for _, idx := range c.meta.Indexes {
		rootDesc, _, err := c.secondaryRootDescriptor(idx)
		if err != nil {
			return ConsistencyReport{}, err
		}
		indexPrefix, err := CollectionIndexPrefix(c.meta.Name, idx.Name)
		if err != nil {
			return ConsistencyReport{}, err
		}
		if err := c.walkVisibleKeysWithPrefixAtRoot(rootDesc.RootPageID, indexPrefix, func(key []byte) error {
			actual[string(key)] = struct{}{}
			return nil
		}); err != nil {
			return ConsistencyReport{}, err
		}
	}
	report.ActualIndexEntries = uint64(len(actual))

	for key := range expected {
		if _, ok := actual[key]; !ok {
			report.MissingIndexEntries++
		}
	}
	for key := range actual {
		if _, ok := expected[key]; !ok {
			report.OrphanIndexEntries++
		}
	}

	return report, nil
}

func (c *Collection) countVisibleKeysWithPrefix(prefix []byte) (uint64, error) {
	var count uint64
	if err := c.walkVisibleKeysWithPrefix(prefix, func(_ []byte) error {
		count++
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Collection) countVisibleKeysWithPrefixAtRoot(rootID uint64, prefix []byte) (uint64, error) {
	var count uint64
	if err := c.walkVisibleKeysWithPrefixAtRoot(rootID, prefix, func(_ []byte) error {
		count++
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Collection) walkDocuments(fn func(documentID, document []byte) error) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	rootDesc, _, err := c.primaryRootDescriptor()
	if err != nil {
		return err
	}
	it, err := c.db.IteratorAtRoot(rootDesc.RootPageID, nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Valid() {
		if !it.IsDeleted() {
			documentID := it.UnsafeKey()
			if err := fn(documentID, it.UnsafeValue()); err != nil {
				return err
			}
		}
		it.Next()
	}
	return it.Error()
}

func (c *Collection) walkVisibleKeysWithPrefix(prefix []byte, fn func(key []byte) error) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	it, err := c.db.Iterator(prefix, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			if err := fn(key); err != nil {
				return err
			}
		}
		it.Next()
	}
	return it.Error()
}

func (c *Collection) walkVisibleKeysWithPrefixAtRoot(rootID uint64, prefix []byte, fn func(key []byte) error) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionManagerNil
	}
	it, err := c.db.IteratorAtRoot(rootID, prefix, nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			if err := fn(key); err != nil {
				return err
			}
		}
		it.Next()
	}
	return it.Error()
}

func (c *Collection) rootPageIDs() (uint64, uint64) {
	type statsProvider interface {
		Stats() map[string]string
	}
	provider, ok := c.db.(statsProvider)
	if !ok {
		return 0, 0
	}
	stats := provider.Stats()
	return parseUint64Stat(stats["treedb.root_page"]), parseUint64Stat(stats["treedb.system_root_page"])
}

func parseUint64Stat(raw string) uint64 {
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}
