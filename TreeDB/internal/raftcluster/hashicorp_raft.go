package raftcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const (
	hashicorpRaftCommandEntryFormatV1  = "treedb.raftcluster.hashicorp.command-entry-v1"
	hashicorpRaftDefaultApplyTimeout   = 5 * time.Second
	hashicorpRaftDefaultSnapshotRetain = 2
)

var (
	ErrInvalidHashicorpRaftProvider = errors.New("raftcluster: invalid hashicorp raft provider")
	ErrHashicorpRaftUnavailable     = errors.New("raftcluster: hashicorp raft unavailable")
	ErrHashicorpRaftLogEntry        = errors.New("raftcluster: invalid hashicorp raft log entry")
	ErrRaftSnapshotUnsupported      = errors.New("raftcluster: raft snapshots unsupported")
)

// HashicorpRaftProviderOptions configures the first production single-group
// consensus adapter. The provider owns the log/stable stores it opens from the
// raftcluster storage layout; caller-supplied stores and transports remain
// caller-owned.
type HashicorpRaftProviderOptions struct {
	Cluster Config
	Applier CommittedCommandApplierV1

	Transport     hraft.Transport
	RaftConfig    *hraft.Config
	LogStore      hraft.LogStore
	StableStore   hraft.StableStore
	SnapshotStore hraft.SnapshotStore

	Bootstrap      bool
	ApplyTimeout   time.Duration
	SnapshotRetain int
}

// HashicorpRaftProvider implements AdmissionProvider and CommitSource on top
// of github.com/hashicorp/raft for one TreeDB Raft group.
type HashicorpRaftProvider struct {
	cluster      ResolvedConfig
	raft         *hraft.Raft
	applyTimeout time.Duration
	owned        []io.Closer
}

func OpenHashicorpRaftProvider(opts HashicorpRaftProviderOptions) (*HashicorpRaftProvider, error) {
	cluster, err := Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, err)
	}
	if opts.Applier == nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("committed command applier is required"))
	}
	if opts.Transport == nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("transport is required"))
	}
	localAddr, ok := localPeerAddress(cluster)
	if !ok {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("local peer %q missing from resolved config", cluster.NodeID))
	}
	if got := string(opts.Transport.LocalAddr()); got != localAddr {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("transport local address %q does not match local peer address %q", got, localAddr))
	}

	stores, err := openHashicorpRaftStores(cluster.Layout, opts)
	if err != nil {
		return nil, err
	}
	defer stores.closeOnError(&err)

	conf := hashicorpRaftConfig(cluster.NodeID, opts.RaftConfig)
	if opts.Bootstrap {
		hasState, hasStateErr := hraft.HasExistingState(stores.log, stores.stable, stores.snapshots)
		if hasStateErr != nil {
			err = errors.Join(ErrInvalidHashicorpRaftProvider, hasStateErr)
			return nil, err
		}
		if !hasState {
			if bootErr := hraft.BootstrapCluster(conf, stores.log, stores.stable, stores.snapshots, opts.Transport, hashicorpRaftConfiguration(cluster)); bootErr != nil {
				err = errors.Join(ErrInvalidHashicorpRaftProvider, bootErr)
				return nil, err
			}
		}
	}
	r, err := hraft.NewRaft(conf, hashicorpRaftFSM{
		groupID: cluster.GroupID,
		applier: opts.Applier,
	}, stores.log, stores.stable, stores.snapshots, opts.Transport)
	if err != nil {
		err = errors.Join(ErrInvalidHashicorpRaftProvider, err)
		return nil, err
	}
	applyTimeout := opts.ApplyTimeout
	if applyTimeout <= 0 {
		applyTimeout = hashicorpRaftDefaultApplyTimeout
	}
	return &HashicorpRaftProvider{
		cluster:      cluster,
		raft:         r,
		applyTimeout: applyTimeout,
		owned:        stores.owned,
	}, nil
}

func (p *HashicorpRaftProvider) Config() ResolvedConfig {
	if p == nil {
		return ResolvedConfig{}
	}
	return p.cluster
}

func (p *HashicorpRaftProvider) Close() error {
	if p == nil {
		return nil
	}
	var errs []error
	if p.raft != nil {
		errs = append(errs, p.raft.Shutdown().Error())
	}
	for _, closer := range p.owned {
		if closer != nil {
			errs = append(errs, closer.Close())
		}
	}
	p.owned = nil
	return errors.Join(errs...)
}

func (p *HashicorpRaftProvider) ClusterAdmissionStatus(ctx context.Context) (AdmissionStatus, error) {
	if p == nil || p.raft == nil {
		return AdmissionStatus{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return AdmissionStatus{}, ctx.Err()
	default:
	}
	state := p.raft.State()
	if state == hraft.Leader {
		return LeaderAdmission(), nil
	}
	_, leaderID := p.raft.LeaderWithID()
	reason := "hashicorp raft state " + state.String()
	if state == hraft.Shutdown {
		return UnavailableAdmission(reason), nil
	}
	if leaderID != "" {
		return FollowerAdmission(NodeID(leaderID), reason), nil
	}
	return UnavailableAdmission(reason + " without known leader"), nil
}

func (p *HashicorpRaftProvider) CommitCommandEntryV1(ctx context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
	if p == nil || p.raft == nil {
		return CommitCommandEntryV1Result{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if req.GroupID != "" && req.GroupID != p.cluster.GroupID {
		return CommitCommandEntryV1Result{}, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("request group %q does not match local group %q", req.GroupID, p.cluster.GroupID))
	}
	if req.NodeID != "" && req.NodeID != p.cluster.NodeID {
		return CommitCommandEntryV1Result{}, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("request node %q does not match local node %q", req.NodeID, p.cluster.NodeID))
	}
	payload, err := encodeHashicorpRaftCommandEntryV1(req.Clone(), p.cluster)
	if err != nil {
		return CommitCommandEntryV1Result{}, err
	}
	future := p.raft.Apply(payload, p.applyTimeout)
	if err := waitHashicorpRaftFuture(ctx, future); err != nil {
		return CommitCommandEntryV1Result{}, mapHashicorpRaftApplyError(err)
	}
	response, ok := future.Response().(hashicorpRaftApplyResponseV1)
	if !ok {
		if responseErr, ok := future.Response().(error); ok {
			return CommitCommandEntryV1Result{}, errors.Join(ErrHashicorpRaftLogEntry, responseErr)
		}
		return CommitCommandEntryV1Result{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("unexpected FSM response %T", future.Response()))
	}
	if response.Entry.Term == 0 || response.Entry.Index == 0 {
		if response.ApplyErr != nil {
			return CommitCommandEntryV1Result{}, errors.Join(ErrHashicorpRaftLogEntry, response.ApplyErr)
		}
		return CommitCommandEntryV1Result{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("FSM response missing committed entry id"))
	}
	if !bytes.Equal(response.Entry.Bytes, req.EntryBytes) {
		return CommitCommandEntryV1Result{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("committed entry bytes differ from request"))
	}
	_, leaderID := p.raft.LeaderWithID()
	if leaderID == "" {
		leaderID = hraft.ServerID(p.cluster.NodeID)
	}
	return CommitCommandEntryV1Result{
		Entry: response.Entry.Clone(),
		Evidence: CommitEvidenceV1{
			Kind:                CommitEvidenceProductionConsensusV1,
			GroupID:             p.cluster.GroupID,
			NodeID:              p.cluster.NodeID,
			LeaderID:            NodeID(leaderID),
			Term:                response.Entry.Term,
			Index:               response.Entry.Index,
			Committed:           true,
			ProductionConsensus: true,
		},
	}, nil
}

func localPeerAddress(cluster ResolvedConfig) (string, bool) {
	for _, peer := range cluster.Peers {
		if peer.ID == cluster.NodeID {
			return peer.Address, true
		}
	}
	return "", false
}

type hashicorpRaftStores struct {
	log       hraft.LogStore
	stable    hraft.StableStore
	snapshots hraft.SnapshotStore
	owned     []io.Closer
}

func openHashicorpRaftStores(layout StorageLayout, opts HashicorpRaftProviderOptions) (hashicorpRaftStores, error) {
	for _, dir := range []string{layout.LogDir, layout.StableDir, layout.SnapshotDir} {
		if dir == "" {
			return hashicorpRaftStores{}, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("missing storage directory"))
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return hashicorpRaftStores{}, errors.Join(ErrInvalidHashicorpRaftProvider, err)
		}
	}
	stores := hashicorpRaftStores{
		log:       opts.LogStore,
		stable:    opts.StableStore,
		snapshots: opts.SnapshotStore,
	}
	var err error
	if stores.log == nil {
		stores.log, err = raftboltdb.NewBoltStore(filepath.Join(layout.LogDir, "raft-log.bolt"))
		if err != nil {
			return hashicorpRaftStores{}, errors.Join(ErrInvalidHashicorpRaftProvider, err)
		}
		stores.owned = append(stores.owned, stores.log.(io.Closer))
	}
	if stores.stable == nil {
		stores.stable, err = raftboltdb.NewBoltStore(filepath.Join(layout.StableDir, "raft-stable.bolt"))
		if err != nil {
			stores.closeOwned()
			return hashicorpRaftStores{}, errors.Join(ErrInvalidHashicorpRaftProvider, err)
		}
		stores.owned = append(stores.owned, stores.stable.(io.Closer))
	}
	if stores.snapshots == nil {
		retain := opts.SnapshotRetain
		if retain <= 0 {
			retain = hashicorpRaftDefaultSnapshotRetain
		}
		stores.snapshots, err = hraft.NewFileSnapshotStore(layout.SnapshotDir, retain, io.Discard)
		if err != nil {
			stores.closeOwned()
			return hashicorpRaftStores{}, errors.Join(ErrInvalidHashicorpRaftProvider, err)
		}
	}
	return stores, nil
}

func (s *hashicorpRaftStores) closeOnError(errp *error) {
	if errp != nil && *errp != nil {
		s.closeOwned()
		s.owned = nil
	}
}

func (s *hashicorpRaftStores) closeOwned() {
	for _, closer := range s.owned {
		if closer != nil {
			_ = closer.Close()
		}
	}
}

func hashicorpRaftConfig(nodeID NodeID, src *hraft.Config) *hraft.Config {
	var conf hraft.Config
	if src == nil {
		conf = *hraft.DefaultConfig()
		conf.HeartbeatTimeout = 100 * time.Millisecond
		conf.ElectionTimeout = 100 * time.Millisecond
		conf.CommitTimeout = 10 * time.Millisecond
		conf.LeaderLeaseTimeout = 50 * time.Millisecond
		conf.SnapshotInterval = 24 * time.Hour
		conf.SnapshotThreshold = ^uint64(0) / 2
	} else {
		conf = *src
	}
	conf.LocalID = hraft.ServerID(nodeID)
	if conf.LogOutput == nil && conf.Logger == nil {
		conf.LogOutput = io.Discard
	}
	if conf.LogLevel == "" {
		conf.LogLevel = "ERROR"
	}
	conf.NoLegacyTelemetry = true
	if conf.SnapshotInterval == 0 {
		conf.SnapshotInterval = 24 * time.Hour
	}
	if conf.SnapshotThreshold == 0 {
		conf.SnapshotThreshold = ^uint64(0) / 2
	}
	return &conf
}

func hashicorpRaftConfiguration(cluster ResolvedConfig) hraft.Configuration {
	servers := make([]hraft.Server, 0, len(cluster.Peers))
	for _, peer := range cluster.Peers {
		servers = append(servers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(peer.ID),
			Address:  hraft.ServerAddress(peer.Address),
		})
	}
	return hraft.Configuration{Servers: servers}
}

func waitHashicorpRaftFuture(ctx context.Context, future hraft.Future) error {
	if future == nil {
		return ErrHashicorpRaftUnavailable
	}
	errc := make(chan error, 1)
	go func() {
		errc <- future.Error()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mapHashicorpRaftApplyError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, hraft.ErrNotLeader), errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return errors.Join(ErrNotLeader, err)
	case errors.Is(err, hraft.ErrLeadershipLost),
		errors.Is(err, hraft.ErrAbortedByRestore),
		errors.Is(err, hraft.ErrRaftShutdown),
		errors.Is(err, hraft.ErrEnqueueTimeout):
		return errors.Join(ErrAdmissionUnavailable, err)
	default:
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	}
}

type hashicorpRaftCommandEntryV1 struct {
	Format                   string                      `json:"format"`
	Version                  uint16                      `json:"version"`
	GroupID                  GroupID                     `json:"group_id"`
	NodeID                   NodeID                      `json:"node_id"`
	EntryBytes               []byte                      `json:"entry_bytes"`
	CurrentCatalogVersion    uint64                      `json:"current_catalog_version"`
	HasCurrentCatalogVersion bool                        `json:"has_current_catalog_version"`
	SyncLocalCommandWAL      bool                        `json:"sync_local_command_wal"`
	RequestMetadata          raftentry.RequestMetadataV1 `json:"request_metadata"`
	ExpectedTarget           *raftentry.TargetIdentityV1 `json:"expected_target,omitempty"`
}

func encodeHashicorpRaftCommandEntryV1(req CommitCommandEntryV1Request, cluster ResolvedConfig) ([]byte, error) {
	payload := hashicorpRaftCommandEntryV1{
		Format:                   hashicorpRaftCommandEntryFormatV1,
		Version:                  1,
		GroupID:                  cluster.GroupID,
		NodeID:                   req.NodeID,
		EntryBytes:               bytes.Clone(req.EntryBytes),
		CurrentCatalogVersion:    req.CurrentCatalogVersion,
		HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(req.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(req.ExpectedTarget),
	}
	if payload.NodeID == "" {
		payload.NodeID = cluster.NodeID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Join(ErrHashicorpRaftLogEntry, err)
	}
	return encoded, nil
}

func decodeHashicorpRaftCommandEntryV1(src []byte) (hashicorpRaftCommandEntryV1, error) {
	var payload hashicorpRaftCommandEntryV1
	if err := json.Unmarshal(src, &payload); err != nil {
		return hashicorpRaftCommandEntryV1{}, errors.Join(ErrHashicorpRaftLogEntry, err)
	}
	if payload.Format != hashicorpRaftCommandEntryFormatV1 || payload.Version != 1 {
		return hashicorpRaftCommandEntryV1{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("unsupported format/version %q/%d", payload.Format, payload.Version))
	}
	if payload.GroupID == "" {
		return hashicorpRaftCommandEntryV1{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("missing group id"))
	}
	if len(payload.EntryBytes) == 0 {
		return hashicorpRaftCommandEntryV1{}, errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("missing command entry bytes"))
	}
	payload.EntryBytes = bytes.Clone(payload.EntryBytes)
	payload.RequestMetadata = cloneRequestMetadataV1(payload.RequestMetadata)
	payload.ExpectedTarget = cloneExpectedTargetV1(payload.ExpectedTarget)
	return payload, nil
}

type hashicorpRaftApplyResponseV1 struct {
	Entry       CommittedCommandEntryV1
	ApplyResult raftentry.ApplyResultV1
	ApplyErr    error
}

type hashicorpRaftFSM struct {
	groupID GroupID
	applier CommittedCommandApplierV1
}

func (f hashicorpRaftFSM) Apply(log *hraft.Log) interface{} {
	if log == nil || log.Type != hraft.LogCommand {
		return hashicorpRaftApplyResponseV1{ApplyErr: errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("unsupported log"))}
	}
	payload, err := decodeHashicorpRaftCommandEntryV1(log.Data)
	if err != nil {
		return hashicorpRaftApplyResponseV1{ApplyErr: err}
	}
	if f.groupID != "" && payload.GroupID != f.groupID {
		return hashicorpRaftApplyResponseV1{ApplyErr: errors.Join(ErrHashicorpRaftLogEntry, fmt.Errorf("entry group %q does not match local group %q", payload.GroupID, f.groupID))}
	}
	entry := CommittedCommandEntryV1{
		Term:                     log.Term,
		Index:                    log.Index,
		Bytes:                    bytes.Clone(payload.EntryBytes),
		CurrentCatalogVersion:    payload.CurrentCatalogVersion,
		HasCurrentCatalogVersion: payload.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      payload.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(payload.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(payload.ExpectedTarget),
	}
	result, applyErr := f.applier.ApplyCommittedCommandEntryV1(context.Background(), entry.Clone())
	return hashicorpRaftApplyResponseV1{
		Entry:       entry,
		ApplyResult: result,
		ApplyErr:    applyErr,
	}
}

func (f hashicorpRaftFSM) Snapshot() (hraft.FSMSnapshot, error) {
	return nil, ErrRaftSnapshotUnsupported
}

func (f hashicorpRaftFSM) Restore(io.ReadCloser) error {
	return ErrRaftSnapshotUnsupported
}
