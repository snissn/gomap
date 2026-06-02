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

func maintenanceRootsForSnapshot(snap *Snapshot) ([]maintenanceRoot, error) {
	if snap == nil || snap.state == nil || snap.idx == nil || snap.idx.pager == nil {
		return nil, fmt.Errorf("maintenance roots: missing snapshot state")
	}
	return collectMaintenanceRoots(snap.idx.pager, &snap.reader, snap.state)
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
	roots := make([]maintenanceRoot, 0, 2)
	var seen map[uint64]struct{}
	addRoot := func(kind maintenanceRootKind, rootID uint64, descriptorKey []byte) {
		if rootID == 0 {
			return
		}
		if seen == nil {
			seen = make(map[uint64]struct{}, 4)
		}
		if _, ok := seen[rootID]; ok {
			return
		}
		seen[rootID] = struct{}{}
		root := maintenanceRoot{
			kind:   kind,
			rootID: rootID,
		}
		if len(descriptorKey) > 0 {
			root.descriptorKey = append([]byte(nil), descriptorKey...)
		}
		roots = append(roots, root)
	}

	addRoot(maintenanceRootUser, state.RootPageID, nil)
	addRoot(maintenanceRootSystem, state.SystemRootPageID, nil)
	if state.SystemRootPageID == 0 {
		return roots, nil
	}

	descriptors, err := vacuumCollectCollectionRootDescriptorsWithContext(ctx, p, reader, state.SystemRootPageID)
	if err != nil {
		return nil, fmt.Errorf("maintenance roots: collect collection root descriptors: %w", err)
	}
	for _, descriptor := range descriptors {
		addRoot(maintenanceRootCollection, descriptor.rootID, descriptor.key)
	}
	return roots, nil
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
	out := make([]uint64, 0, len(roots))
	for _, root := range roots {
		out = append(out, root.rootID)
	}
	return out, nil
}
