package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// DependencyDirectoryV2 owns one admitted index-generation/root lease. The DB
// supplies release after registering the exact root's sequence with its reader
// registry. The directory contains no retained per-obligation index or cache.
type DependencyDirectoryV2 struct {
	ref     DependencyDirectoryRefV2
	tree    *tree.Tree
	refs    atomic.Int64
	release func()
}

func NewDependencyDirectoryV2(p *pager.Pager, ref DependencyDirectoryRefV2, totalPages uint64, release func()) (*DependencyDirectoryV2, error) {
	if p == nil || ref.RootPageID < 2 || ref.RootPageID >= totalPages || totalPages > p.PageCount() || ref.PhysicalCount > ^uint64(0)-ref.LogicalCount || release == nil {
		return nil, ErrDependencyManifestFormat
	}
	directory := &DependencyDirectoryV2{ref: ref, tree: tree.NewWithPageLimit(p, nil, ref.RootPageID, totalPages), release: release}
	directory.refs.Store(1)
	return directory, nil
}

func (directory *DependencyDirectoryV2) Reference() DependencyDirectoryRefV2 {
	if directory == nil {
		return DependencyDirectoryRefV2{}
	}
	return directory.ref
}

func (directory *DependencyDirectoryV2) Retain() error {
	if directory != nil {
		for refs := directory.refs.Load(); refs > 0 && refs < math.MaxInt64; refs = directory.refs.Load() {
			if directory.refs.CompareAndSwap(refs, refs+1) {
				return nil
			}
		}
	}
	return ErrResourceOwnership
}

func (directory *DependencyDirectoryV2) Release() {
	if directory == nil {
		return
	}
	for refs := directory.refs.Load(); refs > 0; refs = directory.refs.Load() {
		if directory.refs.CompareAndSwap(refs, refs-1) {
			if refs == 1 {
				directory.release()
			}
			return
		}
	}
}

// LookupLogical resolves exact identity through the pinned root. Corruption and
// read failures propagate; only the tree's exact not-found result is absence.
// Returned owner bytes are owned by the caller.
func (directory *DependencyDirectoryV2) LookupLogical(obligation StableLogicalObligation) ([]byte, StableLogicalObligation, bool, error) {
	if err := directory.Retain(); err != nil {
		return nil, StableLogicalObligation{}, false, err
	}
	defer directory.Release()
	key := DependencyLogicalKeyV2(obligation)
	value, err := directory.tree.GetAppend(key, nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, StableLogicalObligation{}, false, nil
	}
	if err != nil {
		return nil, StableLogicalObligation{}, false, err
	}
	owner, got, err := DecodeDependencyLogicalV2(key, value)
	if err != nil {
		return nil, StableLogicalObligation{}, false, err
	}
	entry, err := directory.LookupPhysical(owner)
	if err != nil {
		return nil, StableLogicalObligation{}, false, fmt.Errorf("%w: logical owner: %w", ErrDependencyManifestFormat, err)
	}
	if err := dependencyLogicalOwnerV2(entry, got); err != nil {
		return nil, StableLogicalObligation{}, false, err
	}
	return owner, got, true, nil
}

func (directory *DependencyDirectoryV2) LookupPhysical(key []byte) (DependencyManifestEntryV1, error) {
	if err := directory.Retain(); err != nil {
		return DependencyManifestEntryV1{}, err
	}
	defer directory.Release()
	value, err := directory.tree.GetAppend(key, nil)
	if err != nil {
		return DependencyManifestEntryV1{}, err
	}
	return DecodeDependencyPhysicalV2(key, value)
}

// matchesPhysicalRecordV2 compares against bytes just produced by
// EncodeDependencyPhysicalV2; key must come from that same entry. Equality
// proves the persisted record has that
// exact canonical descriptor without allocating a second decoded copy. A
// different record is still decoded and validated before reporting mismatch.
func (directory *DependencyDirectoryV2) matchesPhysicalRecordV2(key, canonical []byte) (bool, error) {
	if err := directory.Retain(); err != nil {
		return false, err
	}
	defer directory.Release()
	value, err := directory.tree.GetAppend(key, nil)
	if err != nil {
		return false, err
	}
	if bytes.Equal(value, canonical) {
		return true, nil
	}
	if _, err := DecodeDependencyPhysicalV2(key, value); err != nil {
		return false, err
	}
	return false, nil
}

// Walk validates and streams the complete directory with iterator-height plus
// one-record scratch space. Startup cost is O(records * log(physical records)):
// each logical record verifies its exact physical owner/frontier. The visitor
// may retain neither key nor value without copying them.
func (directory *DependencyDirectoryV2) Walk(visit func(key, value []byte) error) error {
	if err := directory.Retain(); err != nil {
		return err
	}
	defer directory.Release()
	it := directory.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{IncludeTombstones: true})
	defer it.Close()
	var physical, logical uint64
	var previous []byte
	for ; it.Valid(); it.Next() {
		key := it.UnsafeKey()
		value, _, flags := it.UnsafeEntry()
		if len(key) == 0 || flags&(node.FlagPointer|node.FlagTombstone) != 0 || (previous != nil && bytes.Compare(previous, key) >= 0) {
			return fmt.Errorf("%w: directory key order or inline entry", ErrDependencyManifestFormat)
		}
		switch key[0] {
		case dependencyPhysicalKeyV2:
			if physical >= directory.ref.PhysicalCount {
				return ErrDependencyManifestFormat
			}
			if _, err := DecodeDependencyPhysicalV2(key, value); err != nil {
				return err
			}
			physical++
		case dependencyLogicalKeyV2:
			if logical >= directory.ref.LogicalCount {
				return ErrDependencyManifestFormat
			}
			owner, obligation, err := DecodeDependencyLogicalV2(key, value)
			if err != nil {
				return err
			}
			entry, err := directory.LookupPhysical(owner)
			if err != nil {
				return fmt.Errorf("%w: logical owner: %w", ErrDependencyManifestFormat, err)
			}
			if err := dependencyLogicalOwnerV2(entry, obligation); err != nil {
				return err
			}
			logical++
		default:
			return ErrDependencyManifestFormat
		}
		if visit != nil {
			if err := visit(key, value); err != nil {
				return err
			}
		}
		previous = append(previous[:0], key...)
	}
	if err := it.Error(); err != nil {
		return err
	}
	if physical != directory.ref.PhysicalCount || logical != directory.ref.LogicalCount {
		return ErrDependencyManifestFormat
	}
	return nil
}

func dependencyLogicalOwnerV2(entry DependencyManifestEntryV1, obligation StableLogicalObligation) error {
	found := false
	for _, field := range entry.Reachability {
		if field == obligation.Reachability {
			found = true
			break
		}
	}
	if !found || obligation.Offset < 0 || obligation.Length <= 0 || uint64(obligation.Offset) > entry.Frontier.Bytes || uint64(obligation.Length) > entry.Frontier.Bytes-uint64(obligation.Offset) {
		return fmt.Errorf("%w: logical obligation outside owner reachability/frontier", ErrDependencyManifestFormat)
	}
	return nil
}
