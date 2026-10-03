package nativewire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	"strings"
	"time"
)

type fixedPeerVectorPrepareStatusV1 struct {
	Source     collections.VectorPartitionSourceIdentityV1
	Completion *collections.VectorPartitionPrepareCompletionV1
	RequestID  string
}

func (r *FixedPeerTCPRuntimeV1) servingVectorConfigV1() *FixedPeerTCPVectorConfigV1 {
	if r == nil {
		return nil
	}
	if r.preparedVector != nil {
		return r.preparedVector
	}
	return r.config.Vector
}

// A later snapshot/root replacement invalidates the retained mutable runtime.
// A clean reopen derives a new authenticated serving handle.
func (r *FixedPeerTCPRuntimeV1) requirePreparedVectorCurrentDBV1() error {
	if r == nil || r.preparedVector == nil {
		return nil
	}
	if r.vector == nil || r.vector.boundDB == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	data := r.data[r.vector.dataGroup]
	if data == nil || data.fsm == nil || !data.fsm.HasCurrentDBV1(r.vector.boundDB) {
		return ErrFixedPeerVectorProofStaleV1
	}
	return nil
}

// Expected startup configuration is not an applied catalog or a serving grant.
func (r *FixedPeerTCPRuntimeV1) requirePreparedVectorCatalogV1() error {
	if r == nil || r.preparedVector == nil {
		return nil
	}
	actual, ok := r.authority.Status()
	expected := r.preparedVector.Identity.Index
	if !ok || actual.Epoch != expected.CatalogEpoch || actual.Digest != expected.CatalogDigest {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	return nil
}

func (r *FixedPeerTCPRuntimeV1) vectorInitializationPhaseV1() string {
	if r == nil || r.config.VectorInitialization == nil {
		return ""
	}
	if r.preparedVector != nil && r.vector != nil && r.requirePreparedVectorCurrentDBV1() == nil && r.requirePreparedVectorCatalogV1() == nil {
		record, ok := r.authority.VectorPartitionLifecycleRecordV1(r.preparedVector.Identity)
		if ok && record.State == raftplacement.VectorPartitionLifecycleActiveV1 {
			return "active"
		}
	}
	return FixedPeerVectorPhaseInitializingV1
}

// This authenticated control read proves local durable preparation without
// installing a serving config, changing roots, or manufacturing progress.
func (r *FixedPeerTCPRuntimeV1) vectorPrepareStatusV1(ctx context.Context) (*fixedPeerVectorPrepareStatusV1, error) {
	intent := r.config.VectorInitialization
	if intent == nil || len(r.config.Groups) != 1 {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	data := r.data[intent.SourceGroupID]
	if data == nil || data.db == nil || data.fsm == nil {
		return nil, ErrFixedPeerVectorUnavailableV1
	}
	c, db, err := data.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: intent.SourceGroupID}, intent.Collection.Collection)
	if err != nil {
		return nil, err
	}
	completion, present, err := c.VectorPartitionPrepareCompletionV1(intent.IndexDefinition.Name)
	if err != nil {
		return nil, err
	}
	if !present {
		source, err := c.VectorPartitionSourceIdentityV1(intent.IndexDefinition.Name)
		if err != nil {
			return nil, err
		}
		return &fixedPeerVectorPrepareStatusV1{Source: source}, nil
	}
	v := completion.Command
	if v.Collection != intent.Collection.Collection || v.Index != intent.IndexDefinition.Name || v.IndexDefinitionDigest != collections.VectorIndexDefinitionDigestV1(intent.IndexDefinition) || v.Group != string(intent.SourceGroupID) || v.Generation != intent.Generation || v.MaxSourceRows != intent.MaxSourceRows {
		return nil, errors.New("nativewire: durable prepare does not match immutable initialization intent")
	}
	result, ok, err := data.fsm.LookupCoveredApplyResultV1(raftentry.ApplyEntryID{Term: v.Term, Index: v.IndexPosition})
	if err != nil {
		return nil, err
	}
	if !ok || result.CommandDigest.Hex() != v.CommandDigest {
		return nil, errors.New("nativewire: prepare has no matching covered durable FSM result")
	}
	store, err := collections.OpenExistingVectorPartitionStoreV1(db.Dir())
	if err != nil {
		return nil, err
	}
	m, err := store.Open(v.Collection, v.Index, v.Generation)
	if err != nil {
		return nil, err
	}
	if m.PrepareOrigin == nil || *m.PrepareOrigin != (collections.VectorPartitionPrepareOriginV1{Term: v.Term, Index: v.IndexPosition, CommandDigest: v.CommandDigest}) ||
		m.State != "ready" || m.IntegrityDigest != completion.ManifestDigest || m.ReadySetDigest != completion.ReadySetDigest ||
		m.SourceGeneration != v.SourceGeneration || m.SourceChecksum != v.SourceChecksum || m.SourceSchemaHash != v.SourceSchemaHash || m.SourceRowCount != v.SourceRowCount ||
		m.PartitionCount != 1 || len(m.Placements) != 1 || m.Placements[0].GroupID != v.Group || collections.VectorPartitionLogicalAssetSetDigestV1(v.Group, m) != completion.AssetSetDigest {
		return nil, errors.New("nativewire: authenticated prepare closure differs")
	}
	// Validation-only open proves exact persisted carrier and retained bytes.
	_, err = c.NewPreparedVectorPartitionGenerationReplicatedLiveSearchOpenPlanWithContextV1(ctx, m)
	if err != nil {
		return nil, err
	}
	return &fixedPeerVectorPrepareStatusV1{Source: collections.VectorPartitionSourceIdentityV1{Generation: v.SourceGeneration, Checksum: v.SourceChecksum, SchemaHash: v.SourceSchemaHash, RowCount: v.SourceRowCount}, Completion: &completion, RequestID: string(result.IdempotencyKey)}, nil
}

func (r *FixedPeerTCPRuntimeV1) derivePreparedVectorInitializationV1(ctx context.Context) (*FixedPeerTCPVectorConfigV1, error) {
	intent := r.config.VectorInitialization
	if intent == nil || len(r.config.Groups) != 1 {
		return nil, nil
	}
	data := r.data[intent.SourceGroupID]
	if data == nil || data.db == nil {
		return nil, nil
	}
	c, db, err := data.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: intent.SourceGroupID}, intent.Collection.Collection)
	if errors.Is(err, collections.ErrCollectionNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_, present, err := c.VectorPartitionPrepareCompletionV1(intent.IndexDefinition.Name)
	if err != nil || !present {
		return nil, err
	}
	status, err := r.vectorPrepareStatusV1(ctx)
	if err != nil {
		return nil, err
	}
	if status == nil || status.Completion == nil {
		return nil, errors.New("nativewire: local voter has no validated prepared generation")
	}
	store, err := collections.OpenExistingVectorPartitionStoreV1(db.Dir())
	if err != nil {
		return nil, err
	}
	m, err := store.Open(intent.Collection.Collection, intent.IndexDefinition.Name, intent.Generation)
	if err != nil {
		return nil, err
	}
	catalog := fixedPeerVectorInitializationCatalogV1(r.config)
	record, err := raftplacement.NewCatalogMetaRecordV1(intent.CatalogEpoch, catalog)
	if err != nil {
		return nil, err
	}
	actual, ok := r.authority.Status()
	// Retained catalog logs replay only after peers open and elect a leader.
	// Authenticate the local prepare now; actual catalog authority is still
	// required at readiness, backend admission, and lifecycle publication.
	if ok && (actual.Epoch != record.Epoch || actual.Digest != record.Digest) {
		return nil, raftplacement.ErrCatalogMetaUnavailable
	}
	source := status.Source
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index:  raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: intent.Collection, CollectionIncarnation: 1, IndexName: m.IndexName, IndexDefinitionDigest: m.IndexDefinitionDigest, IndexEpoch: 1, CatalogEpoch: record.Epoch, CatalogDigest: record.Digest},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: source.Generation, Checksum: source.Checksum, SchemaHash: source.SchemaHash, RowCount: source.RowCount}, Generation: intent.Generation,
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Collection: intent.Collection, IndexName: m.IndexName, IndexDefinitionDigest: m.IndexDefinitionDigest, SourceGeneration: source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount, PartitionGeneration: m.Generation, PartitionCount: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: intent.SourceGroupID}}}
	resolved, err := raftplacement.Validate(catalog)
	if err != nil {
		return nil, err
	}
	if err := resolved.ValidateVectorPartitionPlacementV1(placement); err != nil {
		return nil, err
	}
	routerBudget := len(m.Representatives)
	if routerBudget == 0 || routerBudget > min(DefaultVectorPartitionCoordinatorLimitsV1().MaxRouterScoreCalls, collections.MaxVectorPartitionRouterScoreBudgetV3) {
		return nil, errors.New("nativewire: prepared exact router representative budget is invalid")
	}
	base := VectorPartitionCoordinatorRequestV1{Version: VectorPartitionCoordinatorVersionV1, RequestID: "fixed-initialized-vector", CancellationID: "fixed-initialized-vector-cancel", Database: intent.Collection.Database, Catalog: intent.Collection.Catalog, Collection: intent.Collection.Collection, IndexName: m.IndexName, IndexDefinitionDigest: m.IndexDefinitionDigest, Metric: VectorPartitionShardSearchMetricCosineV1, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: routerBudget, PartitionProbes: 1, Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1, TopK: 8, EfSearch: 8, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 8}
	return cloneFixedPeerVectorConfigV1(&FixedPeerTCPVectorConfigV1{Collection: intent.Collection, Catalog: catalog, Manifest: m, Placement: placement, Identity: identity, PublicAddresses: intent.PublicAddresses, ShardAddresses: intent.ShardAddresses, RequestBase: base, IndexedThrough: status.Completion.Command.IndexPosition}), nil
}

func (r *FixedPeerTCPRuntimeV1) validatePreparedVectorAllVotersV1(ctx context.Context) error {
	if r.preparedVector == nil {
		return ErrFixedPeerVectorUnavailableV1
	}
	local, err := r.vectorPrepareStatusV1(ctx)
	if err != nil {
		return err
	}
	if local == nil || local.Completion == nil {
		return errors.New("nativewire: local voter has no validated prepared generation")
	}
	for _, peer := range r.config.Groups[0].Peers {
		var state *fixedPeerVectorPrepareStatusV1
		if peer.ID == r.config.NodeID {
			state = local
		} else {
			reply, err := r.client.call(ctx, peer.ID, "vector-prepare-status", fixedPeerRequestV1{}, false)
			if err != nil {
				return err
			}
			state = reply.VectorPreparation
		}
		if state == nil || state.Completion == nil || state.Source != local.Source || state.Completion.Command != local.Completion.Command || state.Completion.AssetSetDigest != local.Completion.AssetSetDigest {
			return errors.New("nativewire: all voters have not validated the same prepared generation")
		}
	}
	return nil
}

// Resume uses the original guard only from a covered durable result. Restoring
// that guard must reconstruct the entire original digest; a reused key cannot
// authorize a changed source, payload, operation, or command header.
func (r *FixedPeerTCPRuntimeV1) vectorPrepareResumeEntryV1(ctx context.Context, raw []byte, metadata raftentry.RequestMetadataV1) ([]byte, error) {
	// Match the FSM's bounded scheduling selector: ordinary entries take no
	// additional decode or allocation. This prefix grants no command authority.
	magic := iwire.DeterministicEntryMagic
	if r.config.VectorInitialization == nil || len(raw) < len(magic) || string(raw[:len(magic)]) != magic {
		return raw, nil
	}
	version, n := binary.Uvarint(raw[len(magic):])
	if n <= 0 || version != iwire.DeterministicEntryVersion {
		return raw, nil
	}
	command, n := binary.Uvarint(raw[len(magic)+n:])
	if n <= 0 || iwire.CommandID(command) != iwire.CommandVectorPrepareV1 {
		return raw, nil
	}
	entry, err := raftentry.DecodeCommandEntryV1(raw, raftentry.DecodeOptions{})
	if err != nil {
		return nil, err
	}
	decoded := entry.Decoded
	var payload []byte
	for _, section := range decoded.Sections {
		if section.ID == iwire.SectionVectorPrepareV1 {
			payload = section.Bytes
		}
	}
	v, err := commitlog.DecodeVectorPreparePayloadV1(payload)
	if err != nil {
		return nil, err
	}
	suffix := "/prepare"
	if v.Operation == "rebuild" {
		suffix = "/source"
	}
	key := string(entry.IdempotencyKey)
	requestID, shaped := strings.CutSuffix(key, suffix)
	// Other producers retain their ordinary exact-entry replay contract.
	if !shaped || requestID == "" || len(requestID) > 512 {
		return raw, nil
	}
	intent := r.config.VectorInitialization
	if len(r.config.Groups) != 1 || metadata.ClusterRouteGroupID != string(intent.SourceGroupID) ||
		v.Group != string(intent.SourceGroupID) || v.Collection != intent.Collection.Collection ||
		v.Index != intent.IndexDefinition.Name || v.IndexDefinitionDigest != collections.VectorIndexDefinitionDigestV1(intent.IndexDefinition) ||
		v.Generation != intent.Generation || v.MaxSourceRows != intent.MaxSourceRows || v.Term != 0 || v.IndexPosition != 0 || v.CommandDigest != "" {
		return nil, errors.New("nativewire: prepare resume differs from immutable initialization")
	}
	data := r.localDataV1(intent.SourceGroupID)
	if data == nil || data.fsm == nil {
		// An ingress without the group forwards unchanged to the actual owner.
		return raw, nil
	}
	guard, digest, known, err := data.fsm.AppliedIdempotencyGuardV1(ctx, entry.IdempotencyKey)
	if err != nil || !known {
		return raw, err
	}
	sections := []iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: decoded.CommandID, Version: decoded.CommandVersion, Flags: decoded.CommandFlags})}}
	for _, section := range decoded.Sections {
		if section.ID == iwire.SectionExpectedCatalogVersion {
			section.Bytes = binary.AppendUvarint(nil, guard)
		}
		sections = append(sections, section)
	}
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	replay, err := iwire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		return nil, err
	}
	if raftentry.CommandDigestV1ForBytes(replay, raftentry.DecodeOptions{}) != digest {
		return nil, errors.New("nativewire: prepare resume idempotency key conflicts with original command")
	}
	return replay, nil
}

// PrepareVectorInitializationV1 performs real routed Raft source rebuild then
// prepare. Every source voter must agree on the frozen source and completion.
// Successful return requires a clean restart for serving; immutable config is
// never rewritten. Reuse requestID only for the exact same invocation.
func (c *FixedPeerTCPClientV1) PrepareVectorInitializationV1(ctx context.Context, node raftcluster.NodeID, requestID string) (collections.VectorPartitionPrepareCompletionV1, error) {
	var zero collections.VectorPartitionPrepareCompletionV1
	intent := c.config.VectorInitialization
	if intent == nil || len(c.config.Groups) != 1 || requestID == "" || len(requestID) > 512 {
		return zero, ErrFixedPeerVectorUnavailableV1
	}
	group := c.config.Groups[0]
	var prior *fixedPeerVectorPrepareStatusV1
	allPrior := true
	for _, peer := range group.Peers {
		reply, err := c.call(ctx, peer.ID, "vector-prepare-status", fixedPeerRequestV1{}, false)
		state := reply.VectorPreparation
		if err != nil || state == nil || state.Completion == nil {
			allPrior = false
			break
		}
		if state.RequestID != requestID+"/prepare" {
			return zero, errors.New("nativewire: initialization already belongs to another request")
		}
		if prior == nil {
			prior = state
		} else if state.Completion.Command != prior.Completion.Command || state.Completion.AssetSetDigest != prior.Completion.AssetSetDigest {
			return zero, errors.New("nativewire: prepared voters disagree")
		}
	}
	if allPrior && prior != nil {
		return *prior.Completion, nil
	}
	v := commitlog.VectorPrepareV1{Version: 1, Operation: "rebuild", Collection: intent.Collection.Collection, Index: intent.IndexDefinition.Name, Group: string(intent.SourceGroupID), IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(intent.IndexDefinition), Generation: intent.Generation, MaxSourceRows: intent.MaxSourceRows}
	submit := func(v commitlog.VectorPrepareV1, suffix string) error {
		owner, err := c.leader(ctx, group)
		if err != nil {
			return err
		}
		status, err := c.Status(ctx, owner)
		if err != nil {
			return err
		}
		var version uint64
		for _, item := range status.Groups {
			if item.GroupID == group.ID {
				version = item.CatalogVersion
			}
		}
		payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
		if err != nil {
			return err
		}
		sections := []iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandVectorPrepareV1, Version: 1})}, collectionNameRef(v.Collection), {ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, version)}, {ID: iwire.SectionIdempotencyKey, Bytes: []byte(requestID + "/" + suffix)}, {ID: iwire.SectionVectorPrepareV1, Bytes: payload}}
		validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			return err
		}
		entry, err := iwire.AppendDeterministicEntry(nil, validated)
		if err != nil {
			return err
		}
		request := ClusterRouteRequest{Database: intent.Collection.Database, Catalog: intent.Collection.Catalog, Collection: v.Collection, Shape: ClusterRouteShapeCollection}
		route, err := c.Route(ctx, node, request)
		if err != nil {
			return err
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		result, err := c.Submit(ctx, node, entry, metadata)
		if err != nil {
			return err
		}
		if !result.CommittedRecoverable || !result.CommittedApplied {
			return errors.New("nativewire: vector prepare did not complete real committed apply")
		}
		return nil
	}
	if err := submit(v, "source"); err != nil {
		return zero, err
	}
	poll := func(wantCompletion bool) (*fixedPeerVectorPrepareStatusV1, error) {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var first *fixedPeerVectorPrepareStatusV1
			accepted := true
			for _, peer := range group.Peers {
				reply, err := c.call(ctx, peer.ID, "vector-prepare-status", fixedPeerRequestV1{}, false)
				state := reply.VectorPreparation
				if err != nil || state == nil || state.Source.RowCount == 0 || state.Source.RowCount > intent.MaxSourceRows || wantCompletion && state.Completion == nil {
					accepted = false
					break
				}
				if first == nil {
					first = state
				} else if !wantCompletion && state.Source != first.Source {
					// Committed rebuild apply on the submitting owner does not
					// imply every follower has reached that source yet.
					accepted = false
					break
				} else if state.Source != first.Source || wantCompletion && (state.Completion.Command != first.Completion.Command || state.Completion.AssetSetDigest != first.Completion.AssetSetDigest) {
					return nil, errors.New("nativewire: vector prepare voters disagree")
				}
			}
			if accepted && first != nil {
				return first, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	frozen, err := poll(false)
	if err != nil {
		return zero, err
	}
	v.Operation = "prepare"
	v.SourceGeneration = frozen.Source.Generation
	v.SourceChecksum = frozen.Source.Checksum
	v.SourceSchemaHash = frozen.Source.SchemaHash
	v.SourceRowCount = frozen.Source.RowCount
	if err := submit(v, "prepare"); err != nil {
		return zero, err
	}
	prepared, err := poll(true)
	if err != nil {
		return zero, err
	}
	if prepared.Completion.Command.Generation != v.Generation || prepared.Completion.Command.SourceChecksum != v.SourceChecksum {
		return zero, fmt.Errorf("nativewire: prepare completion differs")
	}
	return *prepared.Completion, nil
}
