package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// MetadataOwner is the original generation/registry admission context. It does
// not convey physical ownership or replace the retained provider lease.
func (d *DependencyDirectoryV2) MetadataOwner() *retainedalloc.Owner {
	if d == nil {
		return nil
	}
	return d.metadata
}

// The census owns only private construction backings. Directory keys are owned
// by the existing entry outgoing allocation, not by a parallel owner map.
type ownedDirectoryCensusV2 struct{ backing *resourceEntryBacking }

func buildOwnedDependencyDirectoryV2(directory *DependencyDirectoryV2, prepare func([]byte, DependencyManifestEntryV1) (*resourceEntryBacking, error), validate func(*os.File, DependencyManifestEntryV1, StableLogicalObligation) error) (_ *StableResourceSet, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("owned dependency directory reconstruction: %w", retErr)
		}
	}()
	owner := directory.metadata
	n := directory.ref.PhysicalCount
	if owner == nil || n > uint64(int(^uint(0)>>1))/uint64(unsafe.Sizeof(ownedDirectoryCensusV2{})) {
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(n * uint64(unsafe.Sizeof(ownedDirectoryCensusV2{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	census := make([]ownedDirectoryCensusV2, 0, int(n))
	defer func() {
		for i := range census {
			b := census[i].backing
			if b == nil {
				continue
			}
			token := b.entries[0].token
			census[i].backing = nil
			if ResourceOwnerState(token.owner.Load()) == ResourceOwnerToken {
				if err := token.ReleaseWithError(); err != nil {
					if !errors.Is(retErr, err) {
						if binding := SelectOriginalCleanupPrior(err, retErr); binding.Valid() && binding.TryLink(retErr) {
							retErr = err
						} else if retErr == nil {
							retErr = err
						} else {
							retErr = errors.Join(retErr, err)
						}
					}
				}
			}
			if err := b.release(); err != nil {
				if !errors.Is(retErr, err) {
					if binding := SelectOriginalCleanupPrior(err, retErr); binding.Valid() && binding.TryLink(retErr) {
						retErr = err
					} else if retErr == nil {
						retErr = err
					} else {
						retErr = errors.Join(retErr, err)
					}
				}
			}
		}
		clear(census)
		census = nil
		owner.RemovePending(charge)
	}()
	err := directory.walkBorrowedRecordsWithPhysical(owner, func(key, _ []byte, physical DependencyManifestEntryV1) error {
		if len(census) == cap(census) {
			return ErrDependencyManifestFormat
		}
		backing, err := prepare(key, physical)
		if backing != nil {
			census = append(census, ownedDirectoryCensusV2{backing: backing})
		}
		if err != nil {
			return err
		}
		if backing == nil {
			return ErrResourceOwnership
		}
		return nil
	}, func(key []byte, obligation StableLogicalObligation, physical DependencyManifestEntryV1) error {
		i := sort.Search(len(census), func(i int) bool {
			return bytes.Compare(census[i].backing.entries[0].logicalObligations.owner, key) >= 0
		})
		if i == len(census) {
			return ErrUnresolvedResource
		}
		entry := &census[i].backing.entries[0]
		if !bytes.Equal(entry.logicalObligations.owner, key) {
			return ErrUnresolvedResource
		}
		if validate != nil {
			if err := entry.token.WithPinnedFile(func(file *os.File) error {
				// The walk has already checked this exact immutable physical
				// record. Its descriptor stays borrowed through this callback.
				return validate(file, physical, obligation)
			}); err != nil {
				return err
			}
		}
		if entry.logicalObligations.count == int(^uint(0)>>1) {
			return ErrDependencyManifestFormat
		}
		for j := range entry.outgoing.fields.cells {
			cell := &entry.outgoing.fields.cells[j]
			if cell.field == obligation.Reachability && cell.reachable {
				cell.commitment.addObligation(obligation)
				entry.logicalObligations.count++
				return nil
			}
		}
		return ErrUnresolvedResource
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("dependency directory physical/logical stream: %w", err)
	}
	if n == 0 {
		return newStableResourceSetWithEmptyDirectory(owner, directory)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, err
	}
	defer func() {
		if builder == nil {
			return
		}
		if err := builder.AbandonWithError(); err != nil {
			if !errors.Is(retErr, err) {
				if binding := SelectOriginalCleanupPrior(err, retErr); binding.Valid() && binding.TryLink(retErr) {
					retErr = err
				} else if retErr == nil {
					retErr = err
				} else {
					retErr = errors.Join(retErr, err)
				}
			}
		}
	}()
	for i := range census {
		backing := census[i].backing
		entry := &backing.entries[0]
		if dependencyKindRequiresLogicalV2(entry.token.kind) && entry.logicalObligations.count == 0 {
			return nil, ErrUnresolvedResource
		}
		token := entry.token
		census[i].backing = nil // the original builder consumes the backing on every exit
		if err := builder.addPreparedAdmittedEntry(backing); err != nil {
			if cleanup := token.ReleaseWithError(); cleanup != nil {
				return nil, fmt.Errorf("dependency directory builder entry kind=%q: %w", token.kind, errors.Join(err, cleanup))
			}
			return nil, fmt.Errorf("dependency directory builder entry kind=%q: %w", token.kind, err)
		}
	}
	bound, err := builder.Freeze()
	if err == nil {
		builder = nil // Freeze transferred the graph and refunded its containing header.
	}
	if err != nil {
		return bound, fmt.Errorf("dependency directory builder Freeze: %w", err)
	}
	return bound, nil
}

func cloneOwnedDirectoryEntryV2(entry *stableResourceEntry, key []byte, physical DependencyManifestEntryV1, directory *DependencyDirectoryV2, exact bool) (_ *resourceEntryBacking, retErr error) {
	token := activeEntryToken(*entry)
	owner := directory.metadata
	if token == nil || token.metadata == nil || token.metadata.owner != owner || entry.outgoing == nil || entry.outgoing.allocation.owner != owner {
		return nil, ErrResourceOwnership
	}
	if token.kind != physical.Kind || entry.logicalLane != physical.LogicalLane || entry.resourceID != physical.ResourceID || entry.diagnosticPath != physical.DiagnosticPath || token.identity != physical.Identity || token.generation != physical.Generation || token.digest != physical.Digest || !durableFrontierCovers(entry.frontier, physical.Frontier) || len(physical.Reachability) != entry.reachability.reachableCount() {
		return nil, fmt.Errorf("%w: directory kind=%q path=%q physical equality kind=%t lane=%t id=%t path=%t identity=%t generation=%t digest=%t frontier=%t fields=%t", ErrResourceConflict, physical.Kind, physical.DiagnosticPath, token.kind == physical.Kind, entry.logicalLane == physical.LogicalLane, entry.resourceID == physical.ResourceID, entry.diagnosticPath == physical.DiagnosticPath, token.identity == physical.Identity, token.generation == physical.Generation, token.digest == physical.Digest, durableFrontierCovers(entry.frontier, physical.Frontier), len(physical.Reachability) == entry.reachability.reachableCount())
	}
	if exact && (!durableFrontierCovers(physical.Frontier, entry.frontier)) {
		return nil, fmt.Errorf("%w: directory kind=%q path=%q exact frontier", ErrResourceConflict, physical.Kind, physical.DiagnosticPath)
	}
	for _, field := range physical.Reachability {
		if !entry.reachability.hasReachable(field) {
			return nil, fmt.Errorf("%w: directory kind=%q path=%q missing reachability=%q", ErrResourceConflict, physical.Kind, physical.DiagnosticPath, field)
		}
	}
	namespace, present := token.diagnosticNamespace()
	if present != (physical.Namespace != nil) {
		return nil, fmt.Errorf("%w: directory kind=%q path=%q namespace presence", ErrResourceConflict, physical.Kind, physical.DiagnosticPath)
	}
	if present && (namespace.ParentIdentity != physical.Namespace.ParentIdentity || namespace.Operation != physical.Namespace.Operation || namespace.OldName != physical.Namespace.OldName || namespace.NewName != physical.Namespace.NewName || namespace.DiagnosticPath != physical.Namespace.DiagnosticPath) {
		return nil, fmt.Errorf("%w: directory kind=%q path=%q namespace descriptor", ErrResourceConflict, physical.Kind, physical.DiagnosticPath)
	}
	if err := token.namespace.validateStable(); err != nil {
		return nil, err
	}
	var registry *IdentityPinRegistry
	if token.identityPin != nil {
		registry = token.identityPin.registry
		if err := registry.Observe(token.identity); err != nil {
			return nil, err
		}
	}
	cloned, err := token.cloneSharedPinnedDirectory(entry.logicalLane, entry.resourceID, entry.diagnosticPath, physical.Frontier, physical.Reachability[0], nil, directory, nil)
	if err != nil {
		if registry != nil {
			err = errors.Join(err, registry.Unobserve(token.identity))
		}
		return nil, err
	}
	cloned.releaseObservation = registry
	var summary *resourceFieldBacking
	var count int
	if exact {
		summary, count = entry.reachability, entry.logicalObligations.count
		if summary == nil {
			return nil, errors.Join(ErrResourceOwnership, cloned.ReleaseWithError())
		}
	}
	backing, err := newOwnedResourceDirectoryEntry(owner, cloned, key, physical.Reachability, summary, count)
	if err != nil {
		return nil, errors.Join(err, cloned.ReleaseWithError())
	}
	return backing, nil
}

func recoverOwnedDependencyDirectoryV2(directory *DependencyDirectoryV2, admit func(DependencyManifestEntryV1) (*StableResourceSet, error), validate func(*os.File, DependencyManifestEntryV1, StableLogicalObligation) error) (*StableResourceSet, error) {
	return buildOwnedDependencyDirectoryV2(directory, func(key []byte, physical DependencyManifestEntryV1) (_ *resourceEntryBacking, retErr error) {
		admitted, err := admit(physical)
		if admitted != nil {
			defer func() {
				if cleanup := admitted.ReleaseWithError(); cleanup != nil {
					if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
						retErr = cleanup
					} else if retErr == nil {
						retErr = cleanup
					} else if !errors.Is(retErr, cleanup) {
						retErr = errors.Join(retErr, cleanup)
					}
				}
			}()
		}
		if err != nil {
			return nil, err
		}
		if admitted == nil {
			return nil, ErrUnresolvedResource
		}
		admitted.mu.Lock()
		defer admitted.mu.Unlock()
		if admitted.metadata == nil || admitted.metadata.owner != directory.metadata || admitted.physicalOnly || admitted.Owner() == ResourceOwnerReleased || admitted.Owner() == ResourceOwnerTransferred {
			return nil, ErrResourceOwnership
		}
		var source *stableResourceEntry
		count := 0
		admitted.rangeEntriesLocked(func(entry *stableResourceEntry) bool { count++; source = entry; return count <= 1 })
		if count != 1 || source.logicalObligations.count != 0 {
			return nil, fmt.Errorf("%w: directory kind=%q path=%q recovered physical census count=%d", ErrResourceConflict, physical.Kind, physical.DiagnosticPath, count)
		}
		return cloneOwnedDirectoryEntryV2(source, key, physical, directory, false)
	}, validate)
}

// Binding consumes the original candidate's immutable summaries. Only recovery
// reconstructs commitments from every persisted logical record.
func bindOwnedDependencyDirectoryV2(source *StableResourceSet, directory *DependencyDirectoryV2) (_ *StableResourceSet, retErr error) {
	if source.metadata == nil || directory.metadata == nil || source.metadata.owner != directory.metadata {
		return nil, ErrResourceOwnership
	}
	var count, logical uint64
	var countErr error
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if count == ^uint64(0) || entry.logicalObligations.count < 0 || uint64(entry.logicalObligations.count) > ^uint64(0)-logical {
			countErr = ErrDependencyManifestFormat
			return false
		}
		count++
		logical += uint64(entry.logicalObligations.count)
		return true
	})
	if countErr != nil {
		return nil, countErr
	}
	if count != directory.ref.PhysicalCount || logical != directory.ref.LogicalCount {
		return nil, fmt.Errorf("%w: directory Bind census physical=%d/%d logical=%d/%d", ErrResourceConflict, count, directory.ref.PhysicalCount, logical, directory.ref.LogicalCount)
	}
	if count == 0 {
		return newStableResourceSetWithEmptyDirectory(directory.metadata, directory)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(directory.metadata)
	if err != nil {
		return nil, err
	}
	defer func() {
		if builder == nil {
			return
		}
		if cleanup := builder.AbandonWithError(); cleanup != nil && !errors.Is(retErr, cleanup) {
			if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
				retErr = cleanup
			} else if retErr == nil {
				retErr = cleanup
			} else {
				retErr = errors.Join(retErr, cleanup)
			}
		}
	}()
	err = withOwnedDirectoryCodecV2(directory.metadata, func(s *ownedDirectoryCodecV2) error {
		var bindErr error
		source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			physical, key, encoded, err := s.physical(entry)
			if err == nil {
				var found bool
				found, err = directory.lookupBorrowedRecordV2(&s.points[0], key, func(key, value []byte) error {
					if bytes.Equal(value, encoded) {
						return nil
					}
					if _, err := s.parser.physical(key, value); err != nil {
						return err
					}
					return fmt.Errorf("%w: directory Bind kind=%q path=%q physical encoding", ErrResourceConflict, physical.Kind, physical.DiagnosticPath)
				})
				if err == nil && !found {
					err = ErrUnresolvedResource
				}
			}
			if err != nil {
				bindErr = err
				return false
			}
			backing, err := cloneOwnedDirectoryEntryV2(entry, key, physical, directory, true)
			if err != nil {
				bindErr = err
				return false
			}
			token := backing.entries[0].token
			// The builder adopts the backing on success and every failure path.
			if err = builder.addPreparedAdmittedEntry(backing); err != nil {
				bindErr = errors.Join(err, token.ReleaseWithError())
				return false
			}
			return true
		})
		return bindErr
	})
	if err != nil {
		return nil, err
	}
	bound, err := builder.Freeze()
	if err == nil {
		builder = nil // Freeze transferred the graph and refunded its containing header.
	}
	return bound, err
}
