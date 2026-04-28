package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

const collectionRootDescriptorPrefix = vacuumCollectionRootDescriptorPrefix

var collectionRootDescriptorPrefixBytes = vacuumCollectionRootDescriptorPrefixBytes

func collectionRootDescriptorPrefixEnd() []byte {
	return vacuumCollectionRootDescriptorPrefixEnd()
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

	descriptors, err := vacuumCollectCollectionRootDescriptors(p, reader, state.SystemRootPageID)
	if err != nil {
		return nil, fmt.Errorf("maintenance roots: collect collection root descriptors: %w", err)
	}
	for _, descriptor := range descriptors {
		addRoot(maintenanceRootCollection, descriptor.rootID, descriptor.key)
	}
	return roots, nil
}
