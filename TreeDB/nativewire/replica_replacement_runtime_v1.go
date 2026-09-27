package nativewire

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"

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

func (r *FixedPeerTCPRuntimeV1) openDataGroupV1(cfg raftcluster.Config, transport hraft.Transport, raftConfig *hraft.Config, bootstrap bool, admission *peerNodeAdmissionV1) (*fixedPeerDataV1, error) {
	d := &fixedPeerDataV1{}

	var err error
	d.db, err = backenddb.Open(backenddb.Options{Dir: cfg.Dir, CommandWAL: true, CommandWALStatsScan: true})
	if err != nil {
		return d, err
	}
	d.fsm, err = raftfsm.Open(raftfsm.Options{DB: d.db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
	if err != nil {
		return d, err
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
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	reply, err := r.catalogConsumerCall(ctx, "replacement-read", fixedPeerRequestV1{Entry: raw})
	if err != nil {
		return err
	}
	if reply.Replacement == nil {
		return raftplacement.ErrCatalogMetaUnavailable
	}
	actual, err := raftplacement.EncodeReplicaReplacementBeginV1(*reply.Replacement)
	if err != nil {
		return err
	}
	if string(actual) != string(raw) {
		return raftplacement.ErrCatalogMetaConflict
	}
	return nil
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
	if err := r.replacementAuthorityV1(ctx, command); err != nil {
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
	addr, err := net.ResolveTCPAddr("tcp", command.NewPeer.Address)
	if err != nil {
		return err
	}
	var admission *peerNodeAdmissionV1
	if r.client.peerTransport != nil {
		admission = r.client.peerTransport.admission
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
	cfg := raftcluster.Config{Dir: filepath.Join(r.config.DataRoot, string(group.ID)), ClusterDir: r.config.RaftRoot, DisableSideStores: true, NodeID: r.config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features}
	raftConfig := hraft.DefaultConfig()
	raftConfig.HeartbeatTimeout = r.config.RaftTimeout
	raftConfig.ElectionTimeout = r.config.RaftTimeout
	raftConfig.LeaderLeaseTimeout = r.config.RaftTimeout
	raftConfig.LogOutput = io.Discard
	if admission != nil {
		raftConfig.MaxAppendEntries = 1
	}
	d, err := r.openDataGroupV1(cfg, providerTransport, raftConfig, false, admission)
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
	case "/v1/replacement-prepare":
		return r.prepareReplacementV1(ctx, command)
	case "/v1/replacement-enroll":
		if err := r.replacementAuthorityV1(ctx, command); err != nil {
			return err
		}
		local := r.localDataV1(command.GroupID)
		if local == nil || local.startErr != nil {
			return raftcluster.ErrRouteTargetUnknown
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

// PrepareReplicaReplacementV1 commits the bounded operation, prepares the
// preauthorized fresh target and live peer transports, then enrolls only a
// nonvoter. A successful result is not serving, promotion, or recovery proof.
func (c *FixedPeerTCPClientV1) PrepareReplicaReplacementV1(ctx context.Context, catalogLeader raftcluster.NodeID, command raftplacement.ReplicaReplacementBeginV1) (raftcluster.CommittedRaftConfigurationV1, error) {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if command.ConfigDigest != c.digest {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	if _, err := c.call(ctx, catalogLeader, "replacement-begin", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
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
	if _, err := c.call(ctx, command.NewPeer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, true); err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	for _, peer := range group.Peers {
		// The failed old replica need not be reachable in order to replace it.
		if _, err := c.call(ctx, peer.ID, "replacement-prepare", fixedPeerRequestV1{Entry: raw}, true); err != nil && peer.ID != command.OldNodeID {
			return raftcluster.CommittedRaftConfigurationV1{}, err
		}
	}
	leader, err := c.leader(ctx, group)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	reply, err := c.call(ctx, leader, "replacement-enroll", fixedPeerRequestV1{Entry: raw}, true)
	if err != nil {
		return raftcluster.CommittedRaftConfigurationV1{}, err
	}
	if reply.Membership == nil {
		return raftcluster.CommittedRaftConfigurationV1{}, raftcluster.ErrInvalidConfig
	}
	return *reply.Membership, nil
}
