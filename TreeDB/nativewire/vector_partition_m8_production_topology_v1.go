package nativewire

// M8 production topology is an integration/benchmark-only loopback topology:
// real persisted assets, real HashiCorp Raft proofs, and a serialized TCP M5
// boundary. It is not a deployment or membership-management API.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type VectorPartitionM8ProductionMultiGroupOptionsV1 struct {
	Collection           *collections.Collection
	Manifest             collections.VectorPartitionManifestV1
	RouterSource         VectorPartitionCoordinatorRouterSourceV1
	GroupAssetSetDigests map[string]string
	Database, Catalog    string
}

type VectorPartitionM8ProductionMultiGroupEvidenceV1 struct {
	Network                        string
	Groups                         []VectorPartitionM8ProductionGroupEvidenceV1
	LifecycleState, ReadySetDigest string
	MetaGroup                      string
	MetaNodes                      []string
	MetaLeader                     string
}
type VectorPartitionM8ProductionGroupEvidenceV1 struct {
	GroupID, LeaderID, Endpoint          string
	NodeIDs                              []string
	CommitIndex, ReadIndex, AppliedIndex uint64
	ReadEvidenceKind                     string
	ProvesProductionConsensus            bool
	EndpointHits                         uint64
}

type VectorPartitionM8ProductionMultiGroupV1 struct {
	coordinator *VectorPartitionCoordinatorV1
	data        map[raftcluster.GroupID]*raftcluster.ThreeNodeHarness
	meta        *raftplacement.CatalogMetaLifecycleHarnessV1
	listeners   map[raftcluster.GroupID]net.Listener
	services    map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1
	sources     map[raftcluster.GroupID]*CollectionVectorPartitionGenerationSourceV1
	endpoints   map[raftcluster.GroupID]string
	conns       map[net.Conn]raftcluster.GroupID
	mu          sync.Mutex
	hits        map[raftcluster.GroupID]uint64
	lifecycle   raftplacement.VectorPartitionLifecycleRecordV1
	wg          sync.WaitGroup
	closed      bool
	closeOnce   sync.Once
	closeErr    error
	evidence    map[raftcluster.GroupID]VectorPartitionM8ProductionGroupEvidenceV1
}

func NewVectorPartitionM8ProductionMultiGroupV1(ctx context.Context, opts VectorPartitionM8ProductionMultiGroupOptionsV1) (_ *VectorPartitionM8ProductionMultiGroupV1, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Database == "" {
		opts.Database = "default"
	}
	if opts.Catalog == "" {
		opts.Catalog = "default"
	}
	if opts.Collection == nil || opts.RouterSource == nil || opts.Manifest.State != "ready" || len(opts.Manifest.Placements) < 4 {
		return nil, errors.New("nativewire: M8 production topology requires ready persistent assets")
	}
	groups, err := vectorPartitionM8ValidateAssetsV1(opts.Manifest, opts.GroupAssetSetDigests)
	if err != nil {
		return nil, err
	}
	h := &VectorPartitionM8ProductionMultiGroupV1{data: map[raftcluster.GroupID]*raftcluster.ThreeNodeHarness{}, listeners: map[raftcluster.GroupID]net.Listener{}, services: map[raftcluster.GroupID]*VectorPartitionShardSearchServiceV1{}, sources: map[raftcluster.GroupID]*CollectionVectorPartitionGenerationSourceV1{}, endpoints: map[raftcluster.GroupID]string{}, conns: map[net.Conn]raftcluster.GroupID{}, hits: map[raftcluster.GroupID]uint64{}, evidence: map[raftcluster.GroupID]VectorPartitionM8ProductionGroupEvidenceV1{}}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: raftplacement.CollectionRefV1{Database: opts.Database, Catalog: opts.Catalog, Collection: opts.Manifest.Collection}, IndexName: opts.Manifest.IndexName, IndexDefinitionDigest: opts.Manifest.IndexDefinitionDigest, SourceGeneration: opts.Manifest.SourceGeneration, SourceChecksum: opts.Manifest.SourceChecksum, SourceSchemaHash: opts.Manifest.SourceSchemaHash, SourceRowCount: opts.Manifest.SourceRowCount, PartitionGeneration: opts.Manifest.Generation, PartitionCount: opts.Manifest.PartitionCount}
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	catalog := raftplacement.CatalogV1{Features: features}
	for _, group := range groups {
		data, openErr := raftcluster.OpenThreeNodeHarness(ctx, group)
		if openErr != nil {
			return nil, openErr
		}
		h.data[group] = data
		nodes := data.NodeIDs()
		members := make([]raftcluster.NodeID, len(nodes))
		copy(members, nodes)
		catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group, Members: members, LeaderHint: data.LeaderID()})
	}
	catalog.Placements = []raftplacement.CollectionPlacementV1{{Collection: placement.Collection, GroupID: groups[0], Mode: raftplacement.PlacementModeCollectionV1}}
	for _, p := range opts.Manifest.Placements {
		placement.Partitions = append(placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: p.PartitionID, GroupID: raftcluster.GroupID(p.GroupID)})
	}
	resolved, err := raftplacement.Validate(catalog)
	if err != nil {
		return nil, err
	}
	entry, err := vectorPartitionM8ProofEntryV1()
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		proof, commitErr := h.data[group].CommitAndProve(ctx, entry)
		if commitErr != nil {
			return nil, commitErr
		}
		if !proof.Evidence.ProvesProductionConsensus() {
			return nil, errors.New("nativewire: M8 data commit lacks production evidence")
		}
		h.evidence[group] = VectorPartitionM8ProductionGroupEvidenceV1{GroupID: string(group), LeaderID: string(h.data[group].LeaderID()), CommitIndex: proof.Evidence.Index, ProvesProductionConsensus: true}
	}
	h.meta, err = raftplacement.OpenCatalogMetaLifecycleHarnessV1(ctx, raftplacement.CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalog})
	if err != nil {
		return nil, err
	}
	status, ok := h.meta.LeaderAuthority().Status()
	if !ok {
		return nil, errors.New("nativewire: M8 catalog meta unavailable")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: placement.Collection, CollectionIncarnation: 1, IndexName: placement.IndexName, IndexDefinitionDigest: placement.IndexDefinitionDigest, IndexEpoch: 1, CatalogEpoch: status.Epoch, CatalogDigest: status.Digest}, Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: placement.SourceGeneration, Checksum: placement.SourceChecksum, SchemaHash: placement.SourceSchemaHash, RowCount: placement.SourceRowCount}, Generation: placement.PartitionGeneration}
	lifecycle := h.meta.LifecycleCoordinator()
	if _, err = lifecycle.BeginBuildV1(ctx, identity, groups, 0, 1); err != nil {
		return nil, err
	}
	for _, group := range groups {
		proof, progress, readErr := h.data[group].ReadCoordinator().CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: h.data[group].LeaderID(), GroupID: group})
		if readErr != nil {
			return nil, readErr
		}
		if _, err = lifecycle.RecordGroupReadyV1(ctx, identity, raftplacement.VectorPartitionLifecycleGroupReadyV1{GroupID: group, AppliedIndex: progress.Index, AssetSetDigest: opts.GroupAssetSetDigests[string(group)]}); err != nil {
			return nil, err
		}
		evidence := h.evidence[group]
		evidence.ReadIndex, evidence.AppliedIndex, evidence.ReadEvidenceKind = proof.Index, progress.Index, proof.EvidenceKind.String()
		evidence.ProvesProductionConsensus = evidence.ProvesProductionConsensus && proof.EvidenceKind == raftcluster.ReadIndexEvidenceProduction && proof.HasQuorum && proof.Index != 0
		h.evidence[group] = evidence
	}
	if _, err = lifecycle.PrepareV1(ctx, identity); err != nil {
		return nil, err
	}
	active, err := lifecycle.ActivateV1(ctx, identity)
	if err != nil {
		return nil, err
	}
	h.lifecycle = active
	replicated, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(h.meta.LeaderAuthority(), h.meta.LeaderFence())
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		source, sourceErr := NewCollectionVectorPartitionGenerationSourceForReplicatedLifecycleV1(opts.Collection, placement.Collection, replicated)
		if sourceErr != nil {
			return nil, sourceErr
		}
		service, serviceErr := NewVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{Catalog: resolved, Placement: placement, LocalNodeID: h.data[group].LeaderID(), LocalGroupID: group, ReadCoordinator: h.data[group].ReadCoordinator(), GenerationSource: source})
		if serviceErr != nil {
			return nil, serviceErr
		}
		h.services[group] = service
		h.sources[group] = source
		listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			return nil, listenErr
		}
		h.listeners[group] = listener
		h.endpoints[group] = listener.Addr().String()
		h.serve(group, listener, service)
	}
	dispatcher, err := NewVectorPartitionShardSearchTCPDispatcherV1(h.endpoints)
	if err != nil {
		return nil, err
	}
	counting := VectorPartitionShardSearchDispatcherFuncV1(func(callCtx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
		h.mu.Lock()
		h.hits[request.TargetGroupID]++
		h.mu.Unlock()
		return dispatcher.DispatchVectorPartitionShardSearchV1(callCtx, request)
	})
	h.coordinator, err = NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{Catalog: resolved, Placement: placement, RouterSource: opts.RouterSource, Dispatcher: counting, ReplicatedLifecycle: replicated, RequireReplicatedLifecycle: true, Limits: DefaultVectorPartitionCoordinatorLimitsV1()})
	if err != nil {
		return nil, err
	}
	_ = active
	return h, nil
}

func vectorPartitionM8ValidateAssetsV1(m collections.VectorPartitionManifestV1, digests map[string]string) ([]raftcluster.GroupID, error) {
	if err := m.Validate(collections.DefaultVectorPartitionManifestLimits()); err != nil {
		return nil, fmt.Errorf("nativewire: M8 manifest is not canonical ready state: %w", err)
	}
	if m.State != "ready" || m.RouterGeneration != m.Generation || m.RouterAsset.ID == "" || m.RouterAsset.Checksum == "" || m.RouterAsset.Bytes == 0 {
		return nil, errors.New("nativewire: M8 requires a ready manifest with router asset")
	}
	placementSeen := map[string]bool{}
	groupOwners := map[string]bool{}
	asset := map[uint32]bool{}
	for _, a := range m.Assets {
		if asset[a.PartitionID] {
			return nil, fmt.Errorf("nativewire: M8 duplicate asset for partition %d", a.PartitionID)
		}
		asset[a.PartitionID] = true
	}
	for _, p := range m.Placements {
		key := fmt.Sprintf("%s/%d", p.GroupID, p.PartitionID)
		if placementSeen[key] {
			return nil, fmt.Errorf("nativewire: M8 duplicate placement for partition %d", p.PartitionID)
		}
		placementSeen[key] = true
		if !asset[p.PartitionID] {
			return nil, fmt.Errorf("nativewire: M8 partition %d has no asset", p.PartitionID)
		}
		groupOwners[p.GroupID] = true
	}
	if len(groupOwners) < 2 {
		return nil, errors.New("nativewire: M8 requires two owners")
	}
	out := make([]raftcluster.GroupID, 0, len(groupOwners))
	for owner := range groupOwners {
		if digests[owner] == "" {
			return nil, fmt.Errorf("nativewire: M8 missing digest for %s", owner)
		}
		if got, want := digests[owner], vectorPartitionM8GroupAssetSetDigestV1(owner, m); got != want {
			return nil, fmt.Errorf("nativewire: M8 asset digest mismatch for %s", owner)
		}
		out = append(out, raftcluster.GroupID(owner))
	}
	if len(digests) != len(groupOwners) {
		return nil, errors.New("nativewire: M8 asset digest coverage has extras or omissions")
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}
func vectorPartitionM8GroupAssetSetDigestV1(group string, manifest collections.VectorPartitionManifestV1) string {
	var fields []string
	for _, asset := range manifest.Assets {
		for _, placement := range manifest.Placements {
			if asset.PartitionID == placement.PartitionID && placement.GroupID == group {
				fields = append(fields, fmt.Sprintf("%d/%s/%d/%s", asset.PartitionID, asset.ID, asset.Bytes, asset.Checksum))
			}
		}
	}
	fields = append(fields, fmt.Sprintf("router/%s/%d/%s/%d/%d", manifest.RouterAsset.ID, manifest.RouterAsset.Bytes, manifest.RouterAsset.Checksum, manifest.RouterGeneration, manifest.Generation))
	sort.Strings(fields)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s", group, fields)))
	return hex.EncodeToString(sum[:])
}
func vectorPartitionM8ProofEntryV1() ([]byte, error) {
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("m8-production-proof-v1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "m8_production_proof"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("proof"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"proof":true}`))},
	}
	command, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, fmt.Errorf("nativewire: M8 proof command validation: %w", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, command)
	if err != nil {
		return nil, fmt.Errorf("nativewire: M8 proof entry encoding: %w", err)
	}
	return entry, nil
}
func (h *VectorPartitionM8ProductionMultiGroupV1) serve(group raftcluster.GroupID, listener net.Listener, service *VectorPartitionShardSearchServiceV1) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			h.mu.Lock()
			if h.closed || h.listeners[group] != listener {
				h.mu.Unlock()
				_ = conn.Close()
				return
			}
			h.conns[conn] = group
			h.wg.Add(1) // synchronized with Close before it may call Wait.
			h.mu.Unlock()
			go func() {
				defer h.wg.Done()
				defer func() { h.mu.Lock(); delete(h.conns, conn); h.mu.Unlock() }()
				(VectorPartitionShardSearchTCPServerV1{Service: service, InitialTimeout: 2 * time.Second}).ServeConn(context.Background(), conn)
			}()
		}
	}()
}
func (h *VectorPartitionM8ProductionMultiGroupV1) Coordinator() *VectorPartitionCoordinatorV1 {
	if h == nil {
		return nil
	}
	return h.coordinator
}
func (h *VectorPartitionM8ProductionMultiGroupV1) StopGroup(group string) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	listener := h.listeners[raftcluster.GroupID(group)]
	delete(h.listeners, raftcluster.GroupID(group))
	var conns []net.Conn
	for conn, owner := range h.conns {
		if owner == raftcluster.GroupID(group) {
			conns = append(conns, conn)
		}
	}
	h.mu.Unlock()
	if listener == nil {
		return fmt.Errorf("nativewire: M8 group %s endpoint unavailable", group)
	}
	errs := []error{listener.Close()}
	for _, conn := range conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}
func (h *VectorPartitionM8ProductionMultiGroupV1) Evidence() VectorPartitionM8ProductionMultiGroupEvidenceV1 {
	e := VectorPartitionM8ProductionMultiGroupEvidenceV1{Network: "tcp_loopback_serialized_m5_v1"}
	if h == nil {
		return e
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for group, data := range h.data {
		nodes := data.NodeIDs()
		ids := make([]string, len(nodes))
		for i, id := range nodes {
			ids[i] = string(id)
		}
		hits := h.hits[group]
		endpoint := h.endpoints[group]
		groupEvidence := h.evidence[group]
		groupEvidence.GroupID, groupEvidence.LeaderID, groupEvidence.NodeIDs, groupEvidence.Endpoint, groupEvidence.EndpointHits = string(group), string(data.LeaderID()), ids, endpoint, hits
		e.Groups = append(e.Groups, groupEvidence)
	}
	sort.Slice(e.Groups, func(i, j int) bool { return e.Groups[i].GroupID < e.Groups[j].GroupID })
	if h.meta != nil {
		e.MetaGroup = string(h.meta.GroupID())
		for _, id := range h.meta.NodeIDs() {
			e.MetaNodes = append(e.MetaNodes, string(id))
		}
		e.MetaLeader = string(h.meta.LeaderID())
		e.LifecycleState = string(h.lifecycle.State)
		e.ReadySetDigest = h.lifecycle.ReadySetDigest
	}
	return e
}
func (h *VectorPartitionM8ProductionMultiGroupV1) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		var errs []error
		h.mu.Lock()
		h.closed = true
		for _, l := range h.listeners {
			errs = append(errs, l.Close())
		}
		for conn := range h.conns {
			errs = append(errs, conn.Close())
		}
		h.listeners = map[raftcluster.GroupID]net.Listener{}
		h.mu.Unlock()
		h.wg.Wait()
		// Sources own mapped persistent search assets and must retire before their DB.
		for _, source := range h.sources {
			errs = append(errs, source.Close())
		}
		// Evidence reads data/meta identities under this lock, so retire their
		// providers only after an in-flight Evidence snapshot completes.
		h.mu.Lock()
		for _, data := range h.data {
			errs = append(errs, data.Close())
		}
		meta := h.meta
		h.meta = nil
		h.mu.Unlock()
		if meta != nil {
			errs = append(errs, meta.Close())
		}
		h.closeErr = errors.Join(errs...)
	})
	return h.closeErr
}
