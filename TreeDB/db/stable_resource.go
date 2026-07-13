package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// StableValueLogSegmentRegistrar is the producer boundary consumed by root
// candidate construction. Implementations capture the exact writer identity
// and complete record frontier; they must fail instead of reopening a path.
type StableValueLogSegmentRegistrar interface {
	RegisterStableValueLogSegment([]page.ValuePtr, rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)
}

// StableOuterLeafSegmentRegistrar is the raw outer-leaf counterpart of
// StableValueLogSegmentRegistrar.
type StableOuterLeafSegmentRegistrar interface {
	RegisterStableOuterLeafSegment([]page.LogRecordRef, rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)
}

// StableValueLogAppender is the atomic producer boundary used by root
// candidate construction. The returned set owns every segment referenced by
// the returned pointers, including batches split by rotation.
type StableValueLogAppender interface {
	AppendValuesAndRegisterStableResources([][]byte, rootpublication.StableResourceSpec) ([]page.ValuePtr, *rootpublication.StableResourceSet, error)
}

// StableOuterLeafAppender is the raw outer-leaf counterpart of
// StableValueLogAppender.
type StableOuterLeafAppender interface {
	AppendLeafPagesAndRegisterStableResources([][]byte, rootpublication.StableResourceSpec) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

// SetValueLogStableResourcePinRegistry injects the DB-scoped physical-identity
// gate shared with every cached-mode value-log manager. The registry is
// intentionally explicit: two managers that can delete the same segment must
// never coordinate through a process-global path lookup.
func (db *DB) SetValueLogStableResourcePinRegistry(registry *rootpublication.IdentityPinRegistry) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("treedb: value-log manager unavailable for stable resource registry")
	}
	if db.stableResourcePins != nil && db.stableResourcePins != registry {
		return fmt.Errorf("treedb: value-log stable resource registry already installed")
	}
	if err := db.valueLogManager.SetStableResourcePinRegistry(registry); err != nil {
		return err
	}
	db.stableResourcePins = registry
	return nil
}

// StableResourcePinRegistry returns the DB-owned physical identity registry for
// explicit injection into every value-log, manifest, and asset producer or
// deleter. A DB has exactly one such registry.
func (db *DB) StableResourcePinRegistry() *rootpublication.IdentityPinRegistry {
	if db == nil {
		return nil
	}
	return db.stableResourcePins
}

// RegisterStableValueLogSegment routes candidate construction to the installed
// producer. #3679 owns attaching the returned token to a prepared root; this
// issue owns exact producer capture and returns a typed error when unavailable.
func (db *DB) RegisterStableValueLogSegment(ptrs []page.ValuePtr, spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if db == nil {
		return nil, fmt.Errorf("treedb: stable value-log producer unavailable")
	}
	registrar, ok := db.currentValueLogAppender().(StableValueLogSegmentRegistrar)
	if !ok || registrar == nil {
		return nil, fmt.Errorf("treedb: value-log producer does not support stable resource capture")
	}
	return registrar.RegisterStableValueLogSegment(ptrs, spec)
}

// RegisterStableOuterLeafSegment routes candidate construction to the active
// raw outer-leaf producer. #3679 owns candidate attachment and visibility.
func (db *DB) RegisterStableOuterLeafSegment(refs []page.LogRecordRef, spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	if db == nil || db.leafPageLog == nil {
		return nil, fmt.Errorf("treedb: stable outer-leaf producer unavailable")
	}
	registrar, ok := db.leafPageLog.(StableOuterLeafSegmentRegistrar)
	if !ok || registrar == nil {
		return nil, fmt.Errorf("treedb: outer-leaf producer does not support stable resource capture")
	}
	return registrar.RegisterStableOuterLeafSegment(refs, spec)
}

// AppendValuesAndRegisterStableResources appends through the installed value
// producer and returns ownership of every exact segment needed by the result.
// #3679 owns attaching the set to a prepared root and publishing visibility.
func (db *DB) AppendValuesAndRegisterStableResources(values [][]byte, spec rootpublication.StableResourceSpec) ([]page.ValuePtr, *rootpublication.StableResourceSet, error) {
	if db == nil {
		return nil, nil, fmt.Errorf("treedb: stable value-log producer unavailable")
	}
	appender, ok := db.currentValueLogAppender().(StableValueLogAppender)
	if !ok || appender == nil {
		return nil, nil, fmt.Errorf("treedb: value-log producer does not support atomic stable resource capture")
	}
	return appender.AppendValuesAndRegisterStableResources(values, spec)
}

// AppendLeafPagesAndRegisterStableResources appends raw outer-leaf pages and
// returns ownership of every exact segment needed by the result.
func (db *DB) AppendLeafPagesAndRegisterStableResources(leafPages [][]byte, spec rootpublication.StableResourceSpec) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if db == nil || db.leafPageLog == nil {
		return nil, nil, fmt.Errorf("treedb: stable outer-leaf producer unavailable")
	}
	appender, ok := db.leafPageLog.(StableOuterLeafAppender)
	if !ok || appender == nil {
		return nil, nil, fmt.Errorf("treedb: outer-leaf producer does not support atomic stable resource capture")
	}
	return appender.AppendLeafPagesAndRegisterStableResources(leafPages, spec)
}
