package nativewire

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// FixedPeerQuiescedANNMoveDestinationV1 admits a trusted dormant, empty node.
// It grants no serving, mutation, transfer, install or cutover authority.
// The initial checkpoint supports exactly one destination voter and one domain.
type FixedPeerQuiescedANNMoveDestinationV1 struct {
	Identity                                raftplacement.VectorPartitionLifecycleIdentityV1
	SourceGroup, ANNGroup, DestinationGroup raftcluster.GroupID
	MoveID                                  string
}

func validateQuiescedANNMoveDestinationV1(c FixedPeerTCPConfigV1, localGroups map[raftcluster.GroupID]bool) error {
	a := c.QuiescedANNMoveDestination
	if a == nil {
		return nil
	}
	v := c.Vector
	invalid := func() error { return errors.New("invalid quiesced ANN move destination admission") }
	if c.Credentials == nil || v == nil || a.Identity != v.Identity || !fixedPeerIdentityV1(a.MoveID) ||
		v.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) ||
		v.Identity.SourceFormat != 0 || v.Identity.SourceV2 != (raftplacement.VectorPartitionLifecycleSourceIdentityV2{}) ||
		v.Manifest.Format != collections.VectorPartitionManifestFormatV1 || v.Manifest.PagedRootV2 != nil || v.Manifest.DomainCount != 1 || v.Manifest.BalancePolicy != "disjoint_v1" {
		return invalid()
	}
	resolved, err := raftplacement.Validate(v.Catalog)
	if err != nil {
		return invalid()
	}
	source, ok := resolved.Placement(v.Collection)
	owners := fixedPeerVectorOwnerGroupsV1(v.Placement)
	if !ok || source.Mode != raftplacement.PlacementModeCollectionV1 || source.GroupID != a.SourceGroup ||
		len(owners) != 1 || owners[0] != a.ANNGroup || a.SourceGroup == a.ANNGroup ||
		a.DestinationGroup == a.SourceGroup || a.DestinationGroup == a.ANNGroup ||
		!localGroups[a.DestinationGroup] || !localGroups[c.Catalog.ID] {
		return invalid()
	}
	localCount := 0
	destinationFound := false
	for _, group := range c.Groups {
		if localGroups[group.ID] {
			localCount++
		}
		if group.ID == a.DestinationGroup {
			destinationFound = len(group.Peers) == 1 && group.Peers[0].ID == c.NodeID
		}
	}
	if localCount != 1 || !destinationFound {
		return invalid()
	}
	// Another collection's source placement cannot be adopted as an empty ANN destination.
	for _, placement := range v.Catalog.Placements {
		if placement.GroupID == a.DestinationGroup {
			return invalid()
		}
		for _, partition := range placement.TokenPartitions {
			if partition.GroupID == a.DestinationGroup {
				return invalid()
			}
		}
	}
	return nil
}

// Open and validate the destination once, before any Raft or serving listener.
// Keep ownership in r.data immediately so the ordinary failure/Close path owns
// it even when a root check fails. openDataGroupV1 subsequently reuses this DB.
func (r *FixedPeerTCPRuntimeV1) openEmptyQuiescedANNMoveDestinationV1() error {
	a := r.config.QuiescedANNMoveDestination
	if a == nil {
		return nil
	}
	database, err := backenddb.Open(backenddb.Options{
		Dir:        filepath.Join(r.config.DataRoot, string(a.DestinationGroup)),
		CommandWAL: true, CommandWALStatsScan: true,
	})
	if err != nil {
		return err
	}
	r.data[a.DestinationGroup] = &fixedPeerDataV1{db: database}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	defer snapshot.Close()
	state := snapshot.State()
	if state == nil || state.AppliedCommandLSN != 0 {
		return fmt.Errorf("%w: destination contains command-WAL coverage", raftcluster.ErrInvalidConfig)
	}
	for _, root := range []uint64{state.RootPageID, state.SystemRootPageID} {
		if root == 0 {
			continue
		}
		it, err := snapshot.IteratorAtRoot(root, nil, nil)
		if err != nil {
			return err
		}
		nonempty := it.Valid()
		err = errors.Join(it.Error(), it.Close())
		if err != nil {
			return err
		}
		if nonempty {
			return fmt.Errorf("%w: destination contains user or system state", raftcluster.ErrInvalidConfig)
		}
	}
	return nil
}
