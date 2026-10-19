package rootpublication

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// One admitted codec workspace serves synchronous directory emissions/lookups.
// Its fixed parser/output capacity is the existing inline directory record
// limit; no growable canonical encoder or generic decoder is reached.
type ownedDirectoryCodecV2 struct {
	parser dependencyBorrowScratch
	key    [page.PageSize]byte
	value  [page.PageSize]byte
	owner  [page.PageSize]byte
}

func withOwnedDirectoryCodecV2(owner *retainedalloc.Owner, visit func(*ownedDirectoryCodecV2) error) error {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ownedDirectoryCodecV2{}))) + 2*retainedalloc.AllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	scratch := new(ownedDirectoryCodecV2)
	defer func() { *scratch = ownedDirectoryCodecV2{}; scratch = nil; owner.RemovePending(charge) }()
	return visit(scratch)
}

func (s *ownedDirectoryCodecV2) physical(entry *stableResourceEntry) (DependencyManifestEntryV1, []byte, []byte, error) {
	t := entry.token
	n := entry.reachability.reachableCount()
	if n == 0 || n > len(s.parser.fields) {
		return DependencyManifestEntryV1{}, nil, nil, ErrDependencyManifestFormat
	}
	i := 0
	for field := range entry.reachability.reachableFields() {
		s.parser.fields[i] = field
		i++
	}
	p := DependencyManifestEntryV1{Kind: t.kind, LogicalLane: entry.logicalLane, ResourceID: entry.resourceID, DiagnosticPath: entry.diagnosticPath, Identity: t.identity, Generation: t.generation, Digest: t.digest, Frontier: entry.frontier, Reachability: s.parser.fields[:n]}
	if namespace, ok := t.diagnosticNamespace(); ok {
		s.parser.namespace = DependencyManifestNamespaceV1{ParentIdentity: namespace.ParentIdentity, Operation: namespace.Operation, OldName: namespace.OldName, NewName: namespace.NewName, DiagnosticPath: namespace.DiagnosticPath}
		p.Namespace = &s.parser.namespace
	}
	var rids []uint64
	if p.Frontier.exactRIDs != nil {
		rids = p.Frontier.exactRIDs.values
	}
	if !canonicalDecodedManifestEntryV1(p) || 21+len(p.Kind)+len(p.LogicalLane)+len(p.ResourceID) > len(s.owner) || dependencyManifestEntrySizeV1(p, rids) > len(s.value) {
		return DependencyManifestEntryV1{}, nil, nil, ErrDependencyManifestFormat
	}
	return p, appendDependencyPhysicalKeyV2(s.owner[:0], p), appendDependencyManifestEntryV1(s.value[:0], p, rids), nil
}

func (s *ownedDirectoryCodecV2) logical(owner []byte, o StableLogicalObligation) ([]byte, []byte, error) {
	if 57+len(o.Class)+len(o.Kind)+len(o.Namespace)+len(o.Reachability) > len(s.key) || 40+len(owner) > len(s.value) {
		return nil, nil, ErrDependencyManifestFormat
	}
	if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
		return nil, nil, err
	}
	key := appendDependencyLogicalKeyV2(s.key[:0], o)
	value := appendU32V1(s.value[:0], uint32(len(owner)))
	value = append(value, owner...)
	value = appendU32V1(value, o.Checksum)
	value = append(value, o.Digest[:]...)
	return key, value, nil
}

// Lookup visits the actual immutable record while its already-admitted owned
// iterator is alive. Only exact absence is reported as found=false.
func (d *DependencyDirectoryV2) lookupBorrowedRecordV2(key []byte, visit func([]byte, []byte) error) (found bool, err error) {
	it := d.tree.OwnedPointerProjectionIterator(key, nil, nil)
	defer it.Close()
	if !it.Valid() || !bytes.Equal(it.UnsafeKey(), key) {
		return false, it.Error()
	}
	value, _, flags := it.UnsafeEntry()
	if flags&(node.FlagPointer|node.FlagTombstone) != 0 {
		return false, ErrDependencyManifestFormat
	}
	return true, visit(it.UnsafeKey(), value)
}

func (s *ownedDirectoryCodecV2) lookupLogical(d *DependencyDirectoryV2, o StableLogicalObligation, visit func([]byte, StableLogicalObligation) error) (bool, error) {
	key, _, err := s.logical(nil, o)
	if err != nil {
		return false, err
	}
	return d.lookupBorrowedRecordV2(key, func(key, value []byte) error {
		owner, got, err := decodeBorrowedLogicalV2(key, value)
		if err != nil {
			return err
		}
		found, err := d.lookupBorrowedRecordV2(owner, func(key, value []byte) error {
			physical, err := s.parser.physical(key, value)
			if err != nil {
				return err
			}
			return dependencyLogicalOwnerV2(physical, got)
		})
		if err != nil {
			return err
		}
		if !found {
			return ErrUnresolvedResource
		}
		return visit(owner, got)
	})
}

type ownedDirectoryDeltaEntryV2 struct {
	key   []byte
	entry *stableResourceEntry
}

// Borrowed identities retain no strings or history: source.mu and the original
// entry/index edges cover them until this synchronous delta walk finishes.
type ownedDirectoryDeltaIdentityV2 struct {
	obligation StableLogicalObligation
	entry      *stableResourceEntry
}

// Requires the source mutex. The physical census owns only physical keys; an
// independently admitted delta-only census detects cross-owner duplicates in
// O(delta log delta). Persisted logical history is checked by exact point lookup.
func walkOwnedDependencyDirectoryChangesV2(source *StableResourceSet, base *DependencyDirectoryV2, visit func([]byte, []byte, bool) error) (physicalCount, logicalCount uint64, retErr error) {
	owner := source.metadata.owner
	if base != nil && base.metadata != owner {
		return 0, 0, ErrResourceOwnership
	}
	var count uint64
	if source.kindViews != nil {
		for _, cell := range source.kindViews.cells {
			if cell.view.count < 0 || uint64(cell.view.count) > ^uint64(0)-count {
				return 0, 0, retainedalloc.ErrCapacity
			}
			count += uint64(cell.view.count)
		}
	}
	if count > uint64(int(^uint(0)>>1))/uint64(unsafe.Sizeof(ownedDirectoryDeltaEntryV2{})) {
		return 0, 0, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(count * uint64(unsafe.Sizeof(ownedDirectoryDeltaEntryV2{})))
	if err := owner.AddPending(charge); err != nil {
		return 0, 0, err
	}
	census := make([]ownedDirectoryDeltaEntryV2, 0, int(count))
	defer func() {
		for i := range census {
			clear(census[i].key)
			census[i] = ownedDirectoryDeltaEntryV2{}
		}
		census = nil
		owner.RemovePending(charge)
	}()
	if base != nil {
		if err := base.Retain(); err != nil {
			return 0, 0, err
		}
		defer func() {
			_, err := base.ReleaseOutcome()
			if err != nil {
				retErr = errors.Join(retErr, err)
			}
		}()
	}
	retErr = withOwnedDirectoryCodecV2(owner, func(s *ownedDirectoryCodecV2) error {
		var buildErr error
		source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			if len(census) == cap(census) || entry.outgoing == nil || entry.outgoing.allocation.owner != owner {
				buildErr = ErrResourceOwnership
				return false
			}
			_, key, _, err := s.physical(entry)
			if err != nil {
				buildErr = err
				return false
			}
			keyCharge := retainedalloc.AllocationCharge(uint64(len(key)))
			if keyCharge > ^uint64(0)-charge {
				buildErr = retainedalloc.ErrCapacity
				return false
			}
			if err := owner.AddPending(keyCharge); err != nil {
				buildErr = err
				return false
			}
			charge += keyCharge
			census = append(census, ownedDirectoryDeltaEntryV2{key: bytes.Clone(key), entry: entry})
			return true
		})
		if buildErr != nil {
			return buildErr
		}
		slices.SortFunc(census, func(a, b ownedDirectoryDeltaEntryV2) int { return bytes.Compare(a.key, b.key) })
		for i := 1; i < len(census); i++ {
			if bytes.Equal(census[i-1].key, census[i].key) {
				return ErrResourceConflict
			}
		}
		var deltaCount uint64
		width := uint64(unsafe.Sizeof(ownedDirectoryDeltaIdentityV2{}))
		maxDelta := uint64(int(^uint(0)>>1)) / width
		for _, item := range census {
			item.entry.logicalObligations.rangeDeltaValues(func(StableLogicalObligation) bool {
				if deltaCount == maxDelta {
					buildErr = retainedalloc.ErrCapacity
					return false
				}
				deltaCount++
				return true
			})
			if buildErr != nil {
				return buildErr
			}
		}
		deltaCharge := retainedalloc.AllocationCharge(deltaCount * width)
		if err := owner.AddPending(deltaCharge); err != nil {
			return err
		}
		delta := make([]ownedDirectoryDeltaIdentityV2, 0, int(deltaCount))
		defer func() {
			clear(delta)
			delta = nil
			owner.RemovePending(deltaCharge)
		}()
		for _, item := range census {
			item.entry.logicalObligations.rangeDeltaValues(func(o StableLogicalObligation) bool {
				if len(delta) == cap(delta) {
					buildErr = ErrResourceConflict
					return false
				}
				delta = append(delta, ownedDirectoryDeltaIdentityV2{obligation: o, entry: item.entry})
				return true
			})
			if buildErr != nil {
				return buildErr
			}
		}
		slices.SortFunc(delta, func(a, b ownedDirectoryDeltaIdentityV2) int {
			return compareResourceObligation(a.obligation, b.obligation)
		})
		for i := 1; i < len(delta); i++ {
			if stableLogicalObligationKey(delta[i-1].obligation) == stableLogicalObligationKey(delta[i].obligation) {
				// Original per-entry indexes are unique; another physical owner
				// cannot claim the same identity, even with identical integrity.
				return ErrResourceConflict
			}
		}
		var added, removed, retained uint64
		for _, item := range census {
			entry := item.entry
			physicalCount++
			if entry.logicalObligations.count < 0 || uint64(entry.logicalObligations.count) > ^uint64(0)-logicalCount {
				return ErrDependencyManifestFormat
			}
			logicalCount += uint64(entry.logicalObligations.count)
			_, _, encoded, err := s.physical(entry)
			if err != nil {
				return err
			}
			changed := true
			if base != nil {
				found, err := base.lookupBorrowedRecordV2(item.key, func(key, value []byte) error {
					changed = !bytes.Equal(encoded, value)
					if changed {
						_, err := s.parser.physical(key, value)
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}
				if found {
					retained++
				}
			}
			if changed {
				if err := visit(item.key, encoded, false); err != nil {
					return err
				}
			}
			if inherited := entry.logicalObligations.directory; inherited != nil && inherited != base {
				return ErrResourceConflict
			}
			for _, o := range entry.logicalObligations.removed {
				if base == nil {
					return ErrResourceConflict
				}
				found, err := s.lookupLogical(base, o, func(key []byte, got StableLogicalObligation) error {
					if got != o || !bytes.Equal(key, item.key) {
						return ErrResourceConflict
					}
					return nil
				})
				if err != nil {
					return err
				}
				if !found {
					return ErrResourceConflict
				}
				key, _, err := s.logical(item.key, o)
				if err != nil {
					return err
				}
				if err := visit(key, nil, true); err != nil {
					return err
				}
				removed++
			}
			entry.logicalObligations.rangeDeltaValues(func(o StableLogicalObligation) bool {
				if base != nil {
					found, err := s.lookupLogical(base, o, func(key []byte, got StableLogicalObligation) error {
						if got != o || !bytes.Equal(key, item.key) {
							return ErrResourceConflict
						}
						return nil
					})
					if err != nil {
						buildErr = err
						return false
					}
					if found {
						return true
					}
				}
				// s.parser is reused by lookups; reconstruct the current descriptor
				// before checking its exact logical owner/frontier.
				physical, _, _, err := s.physical(entry)
				if err == nil {
					err = dependencyLogicalOwnerV2(physical, o)
				}
				if err != nil {
					buildErr = err
					return false
				}
				key, value, err := s.logical(item.key, o)
				if err == nil {
					err = visit(key, value, false)
				}
				if err != nil {
					buildErr = err
					return false
				}
				added++
				return true
			})
			if buildErr != nil {
				return buildErr
			}
		}
		if base == nil {
			if logicalCount != added {
				return ErrDependencyManifestFormat
			}
			return nil
		}
		if retained != base.ref.PhysicalCount {
			isRetained := func(key []byte) bool {
				i := sort.Search(len(census), func(i int) bool { return bytes.Compare(census[i].key, key) >= 0 })
				return i < len(census) && bytes.Equal(census[i].key, key)
			}
			err := base.walkBorrowedRecords(owner, func(key, _ []byte, _ DependencyManifestEntryV1) error {
				if isRetained(key) {
					return nil
				}
				return visit(key, nil, true)
			}, func(key []byte, o StableLogicalObligation) error {
				if isRetained(key) {
					return nil
				}
				encoded, _, err := s.logical(key, o)
				if err != nil {
					return err
				}
				removed++
				return visit(encoded, nil, true)
			}, nil)
			if err != nil {
				return err
			}
		}
		if removed > base.ref.LogicalCount || added > ^uint64(0)-(base.ref.LogicalCount-removed) || logicalCount != base.ref.LogicalCount-removed+added {
			return ErrDependencyManifestFormat
		}
		return nil
	})
	return physicalCount, logicalCount, retErr
}
