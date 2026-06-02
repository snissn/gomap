package db

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const collectionRootDescriptorPrefix = vacuumCollectionRootDescriptorPrefix
const collectionRootOverlayDescriptorPrefix = vacuumCollectionRootOverlayDescriptorPrefix

var collectionRootDescriptorPrefixBytes = vacuumCollectionRootDescriptorPrefixBytes
var collectionRootDescriptorPrefixEndBytes = vacuumCollectionRootDescriptorPrefixEnd()
var collectionRootOverlayDescriptorPrefixBytes = vacuumCollectionRootOverlayDescriptorPrefixBytes
var collectionRootOverlayDescriptorPrefixEndBytes = vacuumCollectionRootOverlayDescriptorPrefixEnd()

func collectionRootDescriptorPrefixEnd() []byte {
	return collectionRootDescriptorPrefixEndBytes
}

func collectionRootOverlayDescriptorPrefixEnd() []byte {
	return collectionRootOverlayDescriptorPrefixEndBytes
}

type maintenanceRootKind uint8

const (
	maintenanceRootUser maintenanceRootKind = iota
	maintenanceRootSystem
	maintenanceRootCollection
)

type maintenanceRoot struct {
	kind          maintenanceRootKind
	rootID        uint64
	descriptorKey []byte
}

type maintenanceRootAccumulator struct {
	roots []maintenanceRoot
	seen  map[maintenanceRootKey]struct{}
}

type maintenanceRootKey struct {
	kind   maintenanceRootKind
	rootID uint64
}

func (a *maintenanceRootAccumulator) add(kind maintenanceRootKind, rootID uint64, descriptorKey []byte) {
	if rootID == 0 {
		return
	}
	if a.seen == nil {
		a.seen = make(map[maintenanceRootKey]struct{}, 4)
	}
	key := maintenanceRootKey{kind: kind, rootID: rootID}
	if _, ok := a.seen[key]; ok {
		return
	}
	a.seen[key] = struct{}{}
	root := maintenanceRoot{
		kind:   kind,
		rootID: rootID,
	}
	if len(descriptorKey) > 0 {
		root.descriptorKey = append([]byte(nil), descriptorKey...)
	}
	a.roots = append(a.roots, root)
}

func (a *maintenanceRootAccumulator) addSystemRootWithDescriptors(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) error {
	if systemRootID == 0 {
		return nil
	}
	a.add(maintenanceRootSystem, systemRootID, nil)
	descriptors, err := vacuumCollectCollectionRootDescriptorsWithContext(ctx, p, reader, systemRootID)
	if err != nil {
		return err
	}
	for _, descriptor := range descriptors {
		a.add(maintenanceRootCollection, descriptor.rootID, descriptor.key)
	}
	return nil
}

func maintenanceRootsForSnapshot(snap *Snapshot) ([]maintenanceRoot, error) {
	return maintenanceRootsForSnapshotWithContext(context.Background(), snap)
}

func maintenanceRootsForSnapshotWithContext(ctx context.Context, snap *Snapshot) ([]maintenanceRoot, error) {
	if snap == nil || snap.state == nil || snap.idx == nil || snap.idx.pager == nil {
		return nil, fmt.Errorf("maintenance roots: missing snapshot state")
	}
	return collectMaintenanceRootsWithContext(ctx, snap.idx.pager, &snap.reader, snap.state)
}

func collectMaintenanceRoots(p *pager.Pager, reader tree.SlabReader, state *DBState) ([]maintenanceRoot, error) {
	return collectMaintenanceRootsWithContext(context.Background(), p, reader, state)
}

func collectMaintenanceRootsWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, state *DBState) ([]maintenanceRoot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if state == nil {
		return nil, fmt.Errorf("maintenance roots: missing db state")
	}
	acc := maintenanceRootAccumulator{
		roots: make([]maintenanceRoot, 0, 2),
	}
	acc.add(maintenanceRootUser, state.RootPageID, nil)
	if err := acc.addSystemRootWithDescriptors(ctx, p, reader, state.SystemRootPageID); err != nil {
		return nil, fmt.Errorf("maintenance roots: collect collection root descriptors: %w", err)
	}
	return acc.roots, nil
}

func collectMaintenanceRootsForSystemRootWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]maintenanceRoot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	acc := maintenanceRootAccumulator{
		roots: make([]maintenanceRoot, 0, 2),
	}
	if err := acc.addSystemRootWithDescriptors(ctx, p, reader, systemRootID); err != nil {
		return nil, fmt.Errorf("maintenance roots: collect collection root descriptors for system root %d: %w", systemRootID, err)
	}
	return acc.roots, nil
}

// CollectMaintenanceRootIDs returns the deduplicated root IDs that storage
// maintenance must preserve for the supplied state: the active user and system
// roots plus collection roots referenced by system descriptors.
func CollectMaintenanceRootIDs(p *pager.Pager, reader tree.SlabReader, state *DBState) ([]uint64, error) {
	return CollectMaintenanceRootIDsWithContext(context.Background(), p, reader, state)
}

// CollectMaintenanceRootIDsWithContext is the context-aware form of
// CollectMaintenanceRootIDs.
func CollectMaintenanceRootIDsWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, state *DBState) ([]uint64, error) {
	roots, err := collectMaintenanceRootsWithContext(ctx, p, reader, state)
	if err != nil {
		return nil, err
	}
	return maintenanceRootIDs(roots), nil
}

// CollectMaintenanceRootIDsForSystemRoot returns the deduplicated root IDs for
// a system root plus collection roots referenced by that system root's
// descriptors.
func CollectMaintenanceRootIDsForSystemRoot(p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]uint64, error) {
	return CollectMaintenanceRootIDsForSystemRootWithContext(context.Background(), p, reader, systemRootID)
}

// CollectMaintenanceRootIDsForSystemRootWithContext is the context-aware form
// of CollectMaintenanceRootIDsForSystemRoot.
func CollectMaintenanceRootIDsForSystemRootWithContext(ctx context.Context, p *pager.Pager, reader tree.SlabReader, systemRootID uint64) ([]uint64, error) {
	roots, err := collectMaintenanceRootsForSystemRootWithContext(ctx, p, reader, systemRootID)
	if err != nil {
		return nil, err
	}
	return maintenanceRootIDs(roots), nil
}

func maintenanceRootIDs(roots []maintenanceRoot) []uint64 {
	out := make([]uint64, 0, len(roots))
	for _, root := range dedupeMaintenanceRootsByRootID(roots) {
		if root.rootID != 0 {
			out = append(out, root.rootID)
		}
	}
	return out
}

func dedupeMaintenanceRootsByRootID(roots []maintenanceRoot) []maintenanceRoot {
	if len(roots) <= 1 {
		if len(roots) == 1 && roots[0].rootID == 0 {
			return roots[:0]
		}
		return roots
	}
	if len(roots) <= 8 {
		out := roots[:0]
		for _, root := range roots {
			if root.rootID == 0 {
				continue
			}
			seen := false
			for _, existing := range out {
				if existing.rootID == root.rootID {
					seen = true
					break
				}
			}
			if !seen {
				out = append(out, root)
			}
		}
		return out
	}
	seen := make(map[uint64]struct{}, len(roots))
	out := roots[:0]
	for _, root := range roots {
		if root.rootID == 0 {
			continue
		}
		if _, ok := seen[root.rootID]; ok {
			continue
		}
		seen[root.rootID] = struct{}{}
		out = append(out, root)
	}
	return out
}
