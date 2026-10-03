package nativewire

import (
	"errors"
	"maps"
	"net/netip"
	"slices"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// FixedPeerTCPVectorInitializationV1 is an immutable first-boot intent, not
// prepared assets, an accepted manifest, or permission to serve vectors.
// V1 admits one RF3 or RF4 group; every data voter is also a catalog voter.
// Collection must use default/default, the existing consensus command scope.
// MaxSourceRows bounds the future preparation input; it is not an ingest quota.
type FixedPeerTCPVectorInitializationV1 struct {
	SourceGroupID                           raftcluster.GroupID
	Collection                              raftplacement.CollectionRefV1
	IndexDefinition                         collections.VectorIndexDefinition
	CatalogEpoch, Generation, MaxSourceRows uint64
	PublicAddresses                         map[raftcluster.NodeID]string
	ShardAddresses                          map[raftcluster.GroupID]map[raftcluster.NodeID]string
}

const FixedPeerVectorPhaseInitializingV1 = "initializing"

func cloneFixedPeerVectorInitializationV1(intent *FixedPeerTCPVectorInitializationV1) *FixedPeerTCPVectorInitializationV1 {
	if intent == nil {
		return nil
	}
	copy := *intent
	// Quantized definitions are refused before cloning; the admitted index has
	// no nested mutable containers. All address maps belong to the runtime.
	copy.IndexDefinition.QuantizedIndexes = nil
	copy.PublicAddresses = maps.Clone(intent.PublicAddresses)
	copy.ShardAddresses = maps.Clone(intent.ShardAddresses)
	for group, members := range copy.ShardAddresses {
		copy.ShardAddresses[group] = maps.Clone(members)
	}
	return &copy
}

func fixedPeerVectorInitializationCatalogV1(config FixedPeerTCPConfigV1) raftplacement.CatalogV1 {
	// Placement catalogs and Raft runtime configurations have distinct feature
	// inventories. Only the placement floor and lifecycle requirement belong
	// in the catalog replicated through the placement meta authority.
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{
		Name:    raftcluster.FeatureVectorPartitionLifecycle,
		Version: raftplacement.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
	})
	catalog := raftplacement.CatalogV1{Features: features,
		Placements: []raftplacement.CollectionPlacementV1{{Collection: config.VectorInitialization.Collection, GroupID: config.VectorInitialization.SourceGroupID}}}
	for _, group := range config.Groups {
		members := make([]raftcluster.NodeID, len(group.Peers))
		for i, peer := range group.Peers {
			members[i] = peer.ID
		}
		catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group.ID, Members: members})
	}
	return catalog
}

func validateFixedPeerVectorInitializationV1(config *FixedPeerTCPConfigV1, addressOK func(string) bool) error {
	intent := config.VectorInitialization
	if intent == nil {
		return nil
	}
	// Generic routed consensus commands currently derive only default/default.
	// Refuse an unreachable initialization scope before persistent roots open.
	if intent.Collection.Database != raftplacement.DefaultDatabase || intent.Collection.Catalog != raftplacement.DefaultCatalog {
		return errors.New("vector initialization requires default database and catalog")
	}
	// Prepare and serving activation support one group with exactly three or
	// four colocated catalog/data voters; no spare or second group is admitted.
	invalid := errors.New("invalid bounded RF3/RF4 vector initialization intent")
	if config.Vector != nil || config.Credentials == nil || len(config.Groups) != 1 || (len(config.Nodes) != 3 && len(config.Nodes) != 4) || len(config.Catalog.Peers) != len(config.Nodes) || len(config.RaftListen) != 2 || intent.CatalogEpoch != 1 || intent.Generation == 0 || intent.MaxSourceRows == 0 || intent.MaxSourceRows > commitlog.VectorPrepareMaxSourceRowsV1 {
		return invalid
	}
	// No spare/dormant destination or differing catalog/data voter roster.
	members := func(peers []raftcluster.Peer) []raftcluster.NodeID {
		out := make([]raftcluster.NodeID, len(peers))
		for i, peer := range peers {
			out[i] = peer.ID
		}
		slices.Sort(out)
		return out
	}
	roster := make([]raftcluster.NodeID, len(config.Nodes))
	for i, node := range config.Nodes {
		roster[i] = node.ID
	}
	slices.Sort(roster)
	if !slices.Equal(roster, members(config.Catalog.Peers)) {
		return invalid
	}
	assigned := map[raftcluster.NodeID]bool{}
	source := false
	for _, group := range config.Groups {
		if len(group.Peers) != len(config.Nodes) {
			return invalid
		}
		source = source || group.ID == intent.SourceGroupID
		for _, peer := range group.Peers {
			if assigned[peer.ID] {
				return invalid
			}
			assigned[peer.ID] = true
		}
	}
	if !source || len(assigned) != len(roster) {
		return invalid
	}
	def, err := collections.NormalizeVectorIndexDefinitionV1(intent.IndexDefinition)
	if err != nil {
		return err
	}
	if def.Strategy != collections.VectorIndexStrategyColumnGraph || def.Metric != collections.VectorMetricCosine || def.Encoding != collections.VectorIndexEncodingFloat32 || def.Representation != "" || def.SchemaGeneration != 0 || len(def.QuantizedIndexes) != 0 || def.Dimensions > 4096 || def.M > 64 || def.EfConstruction > 4096 || def.EfSearch > 4096 {
		return invalid
	}
	intent.IndexDefinition = def
	// The production catalog validator supplies the collection/scope grammar and
	// canonical membership. No collection or apply progress is installed here.
	if _, err := raftplacement.NewCatalogMetaRecordV1(intent.CatalogEpoch, fixedPeerVectorInitializationCatalogV1(*config)); err != nil {
		return err
	}
	if len(intent.PublicAddresses) != len(config.Nodes) || len(intent.ShardAddresses) != len(config.Groups) {
		return invalid
	}
	checkAddress := func(address string) bool {
		endpoint, err := netip.ParseAddrPort(address)
		return err == nil && endpoint.Addr().IsLoopback() && addressOK(address)
	}
	for _, node := range config.Nodes {
		if !checkAddress(intent.PublicAddresses[node.ID]) {
			return invalid
		}
	}
	for _, group := range config.Groups {
		if len(intent.ShardAddresses[group.ID]) != len(group.Peers) {
			return invalid
		}
		for _, peer := range group.Peers {
			address := intent.ShardAddresses[group.ID][peer.ID]
			if !peerPrivateEndpointV1(address) || !addressOK(address) {
				return invalid
			}
		}
	}
	return nil
}
