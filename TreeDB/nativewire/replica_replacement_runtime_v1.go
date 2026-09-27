package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"time"

	hraft "github.com/hashicorp/raft"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func (r *FixedPeerTCPRuntimeV1) localDataV1(group raftcluster.GroupID) *fixedPeerDataV1 {
	r.groupsMu.RLock()
	defer r.groupsMu.RUnlock()
	return r.data[group]
}

func (r *FixedPeerTCPRuntimeV1) openDataGroupV1(cfg raftcluster.Config, transport hraft.Transport, raftConfig *hraft.Config, bootstrap bool, admission *peerNodeAdmissionV1, receiver *replacementReceiverOwnerV1) (*fixedPeerDataV1, error) {
	d := &fixedPeerDataV1{transport: transport, replacementReceiver: receiver}
	providerReady := make(chan struct{})
	defer close(providerReady)

	var err error
	d.db, err = backenddb.Open(backenddb.Options{Dir: cfg.Dir, CommandWAL: true, CommandWALStatsScan: true})
	if err != nil {
		return d, err
	}
	d.fsm, err = raftfsm.Open(raftfsm.Options{DB: d.db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
	if err != nil {
		return d, err
	}
	if receiver != nil {
		receiver.mu.Lock()
		seed, phase := receiver.record.Seed, receiver.record.Phase
		receiver.mu.Unlock()
		d.prejoin, err = newReplacementPrejoinTransportV1(transport, seed, phase, receiver.persistPhase, func(seed raftcluster.ReplacementSnapshotSeedV1) error {
			// Native completion owns this verification even after its transport
			// response expires. Stop only at its own bounded operation lifetime.
			limits, err := d.fsm.SnapshotOperationLimitsV1()
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(context.Background(), limits.Lifetime)
			defer cancel()
			select {
			case <-providerReady:
			default:
				return raftcluster.ErrAdmissionUnavailable
			}
			if d.provider == nil {
				return raftcluster.ErrAdmissionUnavailable
			}
			if err := d.provider.VerifyReplacementSnapshotInstalledV1(ctx, seed); err != nil {
				return err
			}
			return d.fsm.VerifyInstalledSnapshotManifestWithContextV1(ctx, seed.Manifest)
		})
		if err != nil {
			return d, err
		}
		transport = d.prejoin
	}
	d.provider, err = raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: d.fsm, Transport: transport, RaftConfig: raftConfig, Bootstrap: bootstrap, ApplyTimeout: r.config.RequestTimeout})
	if err != nil {
		return d, err
	}
	submitter, err := raftcluster.NewSingleGroupSubmitter(raftcluster.SingleGroupSubmitterOptions{Cluster: cfg, AdmissionProvider: d.provider, CommitSource: d.provider, Preflight: d.fsm, Applier: d.fsm, CatalogVersionProvider: d.fsm})
	if err != nil {
		return d, err
	}
	d.submitter = submitter
	if admission != nil {
		d.submitter = peerBudgetSubmitterV1{SingleGroupSubmitter: submitter, admission: admission, scope: "raft:" + string(cfg.GroupID)}
	}
	return d, nil
}

func (r *FixedPeerTCPRuntimeV1) validateReplacementBeginV1(command raftplacement.ReplicaReplacementBeginV1) (FixedPeerTCPGroupV1, error) {
	if command.ConfigDigest != r.client.digest {
		return FixedPeerTCPGroupV1{}, raftcluster.ErrInvalidConfig
	}
	if _, err := raftplacement.EncodeReplicaReplacementBeginV1(command); err != nil {
		return FixedPeerTCPGroupV1{}, err
	}
	if r.client.addresses[command.NewPeer.ID] == "" {
		return FixedPeerTCPGroupV1{}, fmt.Errorf("%w: replacement node is not preauthorized", raftcluster.ErrInvalidConfig)
	}
	for _, group := range r.config.Groups {
		if group.ID != command.GroupID {
			continue
		}
		if raftcluster.FeatureSetRequiresV1(group.Features, raftcluster.FeatureVectorPartitionLifecycle) {
			return FixedPeerTCPGroupV1{}, raftcluster.ErrUnsupportedFeature
		}
		old := false
		for _, peer := range group.Peers {
			old = old || peer.ID == command.OldNodeID
			if peer.ID == command.NewPeer.ID || peer.Address == command.NewPeer.Address {
				return FixedPeerTCPGroupV1{}, raftcluster.ErrInvalidConfig
			}
		}
		if !old || len(group.Peers) >= fixedPeerMaxPeersV1 {
			return FixedPeerTCPGroupV1{}, raftcluster.ErrInvalidConfig
		}
		group.Peers = append(slices.Clone(group.Peers), command.NewPeer)
		return group, nil
	}
	return FixedPeerTCPGroupV1{}, raftcluster.ErrRouteTargetUnknown
}

func (r *FixedPeerTCPRuntimeV1) replacementAuthorityV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	_, err := r.replacementStateAuthorityV1(ctx, command)
	return err
}
func (r *FixedPeerTCPRuntimeV1) replacementStateAuthorityV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) (raftplacement.ReplicaReplacementStateV1, error) {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return raftplacement.ReplicaReplacementStateV1{}, err
	}
	reply, err := r.catalogConsumerCall(ctx, "replacement-read", fixedPeerRequestV1{Entry: raw})
	if err != nil {
		return raftplacement.ReplicaReplacementStateV1{}, err
	}
	if reply.ReplacementState == nil {
		return raftplacement.ReplicaReplacementStateV1{}, raftplacement.ErrCatalogMetaUnavailable
	}
	actual, err := raftplacement.EncodeReplicaReplacementBeginV1(reply.ReplacementState.Begin)
	if err != nil {
		return raftplacement.ReplicaReplacementStateV1{}, err
	}
	if string(actual) != string(raw) {
		return raftplacement.ReplicaReplacementStateV1{}, raftplacement.ErrCatalogMetaConflict
	}
	return *reply.ReplacementState, nil
}

// prepareReplacementV1 opens a fresh non-bootstrap target or updates an existing
// group's transport identity view after a committed BEGIN. The unchanged fixed
// manifest remains the global node/security anchor; it is never edited to make
// an otherwise unknown node admissible. No operation here can create a voter.
func (r *FixedPeerTCPRuntimeV1) prepareReplacementV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	group, err := r.validateReplacementBeginV1(command)
	if err != nil {
		return err
	}
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	r.groupsMu.Lock()
	defer r.groupsMu.Unlock()
	if r.draining.Load() || r.closed.Load() {
		return raftcluster.ErrAdmissionUnavailable
	}
	if current := r.data[group.ID]; current != nil {
		if current.startErr != nil {
			return current.startErr
		}
		if r.config.NodeID == command.NewPeer.ID && current.replacementID != command.OperationID {
			return raftplacement.ErrCatalogMetaConflict
		}
		return current.stream.replaceAuthorizedPeersV1(group.Peers)
	}
	if r.config.NodeID != command.NewPeer.ID {
		return raftcluster.ErrRouteTargetUnknown
	}
	if len(r.data) >= fixedPeerMaxHostedDataGroupsV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	if state.Seed == nil || state.Phase == raftplacement.ReplicaReplacementBegunV1 {
		return raftcluster.ErrAdmissionUnavailable
	}
	var admission *peerNodeAdmissionV1
	if r.client.peerTransport != nil {
		admission = r.client.peerTransport.admission
	}
	if err := admission.admitReplacementRaftGroupV1(group.ID); err != nil {
		return err
	}
	cfg := raftcluster.Config{Dir: filepath.Join(r.config.DataRoot, string(group.ID)), ClusterDir: r.config.RaftRoot, DisableSideStores: true, NodeID: r.config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features}
	resolved, err := raftcluster.Validate(cfg)
	if err != nil {
		return err
	}
	receiver, err := r.openAuthorizedReplacementReceiverV1(resolved, state)
	if err != nil {
		return err
	}
	keepReceiver := false
	defer func() {
		if !keepReceiver {
			_ = receiver.Close()
		}
	}()
	addr, err := net.ResolveTCPAddr("tcp", command.NewPeer.Address)
	if err != nil {
		return err
	}
	transport, stream, err := newFixedPeerTCPTransportOwnedV1(command.NewPeer.Address, addr, r.config.RequestTimeout, r.client.security, group.Peers, admission, "raft:"+string(group.ID))
	if err != nil {
		return err
	}
	var providerTransport hraft.Transport = transport
	var closer interface {
		Close() error
		CloseStreams()
	} = transport
	if admission != nil {
		bounded := newPeerRaftTransportV1(transport, admission, "raft:"+string(group.ID), false)
		providerTransport, closer = bounded, bounded
	}

	raftConfig := hraft.DefaultConfig()
	raftConfig.HeartbeatTimeout = r.config.RaftTimeout
	raftConfig.ElectionTimeout = r.config.RaftTimeout
	raftConfig.LeaderLeaseTimeout = r.config.RaftTimeout
	raftConfig.LogOutput = io.Discard
	if admission != nil {
		raftConfig.MaxAppendEntries = 1
	}
	d, err := r.openDataGroupV1(cfg, providerTransport, raftConfig, false, admission, receiver)
	keepReceiver = true
	if err != nil {
		d.startErr = err
		d.stream = stream
		r.data[group.ID] = d
		r.transports = append(r.transports, closer)
		return err
	}
	d.stream, d.replacementID = stream, command.OperationID
	entries := make([]raftcluster.GroupSubmitterV1, 0, len(r.data)+1)
	for id, current := range r.data {
		entries = append(entries, raftcluster.GroupSubmitterV1{GroupID: id, Submitter: current.submitter})
	}
	entries = append(entries, raftcluster.GroupSubmitterV1{GroupID: group.ID, Submitter: d.submitter})
	registry, err := raftcluster.NewGroupSubmitterRegistryV1(entries)
	if err == nil {
		var local *raftcluster.GroupRoutedSubmitter
		local, err = raftcluster.NewCatalogMetaGroupRoutedSubmitter(registry, r)
		if err == nil {
			r.local = local
		}
	}
	if err != nil {
		d.startErr = err
		r.data[group.ID] = d
		r.transports = append(r.transports, closer)
		return err
	}
	r.data[group.ID] = d
	r.transports = append(r.transports, closer)
	return nil
}

func (r *FixedPeerTCPRuntimeV1) replacementReadV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) (*raftplacement.ReplicaReplacementBeginV1, error) {
	if _, err := r.validateReplacementBeginV1(command); err != nil {
		return nil, err
	}
	if _, err := r.localCatalogFence(ctx); err != nil {
		return nil, err
	}
	records, err := r.authority.ReplicaReplacementBeginsV1()
	if err != nil {
		return nil, err
	}
	wanted, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.GroupID != command.GroupID {
			continue
		}
		actual, err := raftplacement.EncodeReplicaReplacementBeginV1(record)
		if err != nil {
			return nil, err
		}
		if string(actual) != string(wanted) {
			return nil, raftplacement.ErrCatalogMetaConflict
		}
		return &record, nil
	}
	return nil, raftplacement.ErrCatalogMetaUnavailable
}

func (r *FixedPeerTCPRuntimeV1) handleReplacementV1(ctx context.Context, operation string, raw []byte, reply *fixedPeerReplyV1) error {
	if operation == "/v1/replacement-advance" {
		return r.advanceReplacementV1(ctx, raw, reply)
	}
	command, err := raftplacement.DecodeReplicaReplacementBeginV1(raw)
	if err != nil {
		return err
	}
	if _, err := r.validateReplacementBeginV1(command); err != nil {
		return err
	}
	switch operation {
	case "/v1/replacement-read":
		reply.Replacement, err = r.replacementReadV1(ctx, command)
		if err == nil {
			state, stateErr := r.authority.ReplicaReplacementStateV1(command.GroupID)
			err = stateErr
			reply.ReplacementState = &state
		}
		return err
	case "/v1/replacement-begin":
		if r.meta == nil {
			return raftplacement.ErrCatalogMetaUnavailable
		}
		if _, err := r.localCatalogFence(ctx); err != nil {
			return err
		}
		var work peerWorkLeaseV1
		if r.client.peerTransport != nil {
			work, err = r.client.peerTransport.admission.work("raft:"+string(r.config.Catalog.ID), peerProposalsV1, int64(len(raw))*4)
			if err != nil {
				return err
			}
			defer work.release()
		}
		if _, _, err := r.meta.SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
			return err
		}
		reply.Replacement, err = r.replacementReadV1(ctx, command)
		return err
	case "/v1/replacement-seed", "/v1/replacement-install":
		state, err := r.replacementStateAuthorityV1(ctx, command)
		if err != nil {
			return err
		}
		d := r.localDataV1(command.GroupID)
		if d == nil || d.provider == nil || d.prejoin != nil {
			return raftcluster.ErrRouteTargetUnknown
		}
		if operation == "/v1/replacement-seed" {
			if state.Seed != nil && state.Seed.SourceNodeID != r.config.NodeID {
				return raftcluster.ErrRouteTargetUnknown
			}
			return d.replacementWorkV1(command.OperationID, "seed", func(workCtx context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
				return r.deriveReplacementSeedV1(workCtx, command, d)
			}, reply)
		}
		if state.Phase != raftplacement.ReplicaReplacementSeededV1 || state.Seed == nil || state.Seed.SourceNodeID != r.config.NodeID {
			return raftcluster.ErrAdmissionUnavailable
		}
		return d.replacementWorkV1(command.OperationID, "install", func(workCtx context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
			return r.runReplacementInstallV1(workCtx, command, d)
		}, reply)
	case "/v1/replacement-cutoff":
		return r.replacementReceiverCutoffV1(ctx, command, reply)
	case "/v1/replacement-receiver":
		return r.replacementReceiverStatusV1(ctx, command, reply)
	case "/v1/replacement-allow":
		state, err := r.replacementStateAuthorityV1(ctx, command)
		if err != nil {
			return err
		}
		d := r.localDataV1(command.GroupID)
		if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || d == nil || d.prejoin == nil || state.Seed == nil || !raftcluster.SameReplacementSnapshotSeedV1(d.prejoin.seed, *state.Seed) {
			return raftcluster.ErrAdmissionUnavailable
		}
		return d.prejoin.allowEnrollment()
	case "/v1/replacement-prepare":
		return r.prepareReplacementV1(ctx, command)
	case "/v1/replacement-enroll":
		state, err := r.replacementStateAuthorityV1(ctx, command)
		if err != nil {
			return err
		}
		if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
			return raftcluster.ErrAdmissionUnavailable
		}
		local := r.localDataV1(command.GroupID)
		if local == nil || local.startErr != nil {
			return raftcluster.ErrRouteTargetUnknown
		}
		// The native mutation boundary independently observes the authenticated
		// target's durable cutoff. Coordinator ordering alone is not evidence.
		cutoff, err := r.client.call(ctx, command.NewPeer.ID, "replacement-cutoff", fixedPeerRequestV1{Entry: raw}, false)
		if err != nil {
			return err
		}
		if cutoff.ReplacementSeed == nil || !raftcluster.SameReplacementSnapshotSeedV1(*cutoff.ReplacementSeed, *state.Seed) {
			return raftcluster.ErrAdmissionUnavailable
		}
		configuration, err := local.provider.CommittedConfigurationV1(ctx)
		if err != nil {
			return err
		}
		// Only this exact operation's old voter and absent/nonvoting target may be
		// changed. The provider uses the returned committed index as a CAS fence.
		enrolled, err := local.provider.AddReplacementNonvoterV1(ctx, command.OldNodeID, command.NewPeer, configuration.ConfigurationIndex)
		if err != nil {
			return err
		}
		reply.Membership = &enrolled
		return nil
	default:
		return raftcluster.ErrRouteTargetUnsupported
	}
}

// PrepareReplicaReplacementV1 drives the committed preparation phases through
// actual native snapshot installation and nonvoter enrollment. Promotion and
// durable tail readiness remain separate; success never creates a new voter.
func (c *FixedPeerTCPClientV1) PrepareReplicaReplacementV1(ctx context.Context, catalogLeader raftcluster.NodeID, command raftplacement.ReplicaReplacementBeginV1) (raftcluster.CommittedRaftConfigurationV1, error) {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if command.ConfigDigest != c.digest {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	if _, err := c.call(ctx, catalogLeader, "replacement-begin", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement begin: %w", err)
	}
	var group FixedPeerTCPGroupV1
	for _, candidate := range c.config.Groups {
		if candidate.ID == command.GroupID {
			group = candidate
			break
		}
	}
	if group.ID == "" {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrRouteTargetUnknown
	}
	read, err := c.call(ctx, catalogLeader, "replacement-read", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil || read.ReplacementState == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, errors.Join(err, raftplacement.ErrCatalogMetaUnavailable)
	}
	state := *read.ReplacementState
	if state.Phase == raftplacement.ReplicaReplacementBegunV1 {
		leader, err := c.leader(ctx, group)
		if err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement source leader: %w", err)
		}
		seedReply, err := c.pollReplacementV1(ctx, leader, "replacement-seed", raw)
		if err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement retained seed: %w", err)
		}
		if seedReply.ReplacementSeed == nil {
			return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidSnapshotManifest
		}
		state.Seed = seedReply.ReplacementSeed
		state.Phase = raftplacement.ReplicaReplacementSeededV1
		if err := c.commitReplacementPhaseV1(ctx, catalogLeader, state); err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
	}
	if state.Seed == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidSnapshotManifest
	}
	if _, err := c.call(ctx, command.NewPeer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement target prepare: %w", err)
	}
	for _, peer := range group.Peers {
		if _, err := c.call(ctx, peer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, true); err != nil && peer.ID != command.OldNodeID {
			return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement peer prepare %s: %w", peer.ID, err)
		}
	}
	if state.Phase == raftplacement.ReplicaReplacementSeededV1 {
		// Reconcile before sending: an earlier native response may have been lost.
		receiver, err := c.pollReplacementV1(ctx, command.NewPeer.ID, "replacement-receiver", raw)
		if err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement receiver status: %w", err)
		}
		if !receiver.ReplacementInstalled {
			_, installErr := c.pollReplacementV1(ctx, state.Seed.SourceNodeID, "replacement-install", raw)
			receiver, err = c.pollReplacementV1(ctx, command.NewPeer.ID, "replacement-receiver", raw)
			if err != nil || !receiver.ReplacementInstalled {
				return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement native install: %w", errors.Join(installErr, err, raftcluster.ErrAdmissionUnavailable))
			}
		}
		state.Phase = raftplacement.ReplicaReplacementInstalledV1
		if err := c.commitReplacementPhaseV1(ctx, catalogLeader, state); err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
	}
	if state.Phase == raftplacement.ReplicaReplacementInstalledV1 {
		state.Phase = raftplacement.ReplicaReplacementAddIntentV1
		if err := c.commitReplacementPhaseV1(ctx, catalogLeader, state); err != nil {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrAdmissionUnavailable
	}
	// Permanent receiver cutoff is durable before the native configuration CAS.
	if _, err := c.call(ctx, command.NewPeer.ID, "replacement-allow", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement allow replication: %w", err)
	}
	leader, err := c.leader(ctx, group)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement enrollment leader: %w", err)
	}
	reply, err := c.call(ctx, leader, "replacement-enroll", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, fmt.Errorf("replacement enroll: %w", err)
	}
	if reply.Membership == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	return *reply.Membership, nil
}

func (c *FixedPeerTCPClientV1) pollReplacementV1(ctx context.Context, node raftcluster.NodeID, operation string, raw []byte) (fixedPeerReplyV1, error) {
	for {
		reply, err := c.call(ctx, node, operation, fixedPeerRequestV1{Entry: raw}, true)
		if err != nil {
			return reply, err
		}
		if !reply.ReplacementPending {
			return reply, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return reply, ctx.Err()
		case <-timer.C:
		}
	}
}
func (c *FixedPeerTCPClientV1) commitReplacementPhaseV1(ctx context.Context, node raftcluster.NodeID, state raftplacement.ReplicaReplacementStateV1) error {
	raw, err := raftplacement.EncodeReplicaReplacementStateV1(state)
	if err != nil {
		return err
	}
	_, err = c.call(ctx, node, "replacement-advance", fixedPeerRequestV1{Entry: raw}, true)
	return err
}
