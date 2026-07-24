package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"
)

// CatalogMetaCommittedStateV1 is implemented by raftplacement's authority.
// The dependency inversion keeps raftcluster independent of catalog schema
// while ensuring only committed Raft Apply and Restore can publish state.
type CatalogMetaCommittedStateV1 interface {
	ApplyCatalogMetaCommittedV1(CatalogMetaApplyCapabilityV1, []byte, uint64) error
	ExportCatalogMetaSnapshotBytesV1() ([]byte, error)
	InstallCatalogMetaSnapshotBytesV1([]byte) error
}

type CatalogMetaApplyCapabilityV1 struct{ granted bool }

func catalogMetaApplyCapabilityV1() CatalogMetaApplyCapabilityV1 {
	return CatalogMetaApplyCapabilityV1{granted: true}
}
func (c CatalogMetaApplyCapabilityV1) Granted() bool { return c.granted }

type CatalogMetaRaftProviderOptionsV1 struct {
	Cluster        Config
	State          CatalogMetaCommittedStateV1
	Transport      hraft.Transport
	RaftConfig     *hraft.Config
	LogStore       hraft.LogStore
	StableStore    hraft.StableStore
	SnapshotStore  hraft.SnapshotStore
	Bootstrap      bool
	ApplyTimeout   time.Duration
	SnapshotRetain int
}

// CatalogMetaRaftProviderV1 is the declared single Raft authority for catalog
// commands. It has no local Set API: a generation becomes visible only when
// the committed log invokes State.ApplyCatalogMetaCommittedV1.
type CatalogMetaRaftProviderV1 struct {
	cluster      ResolvedConfig
	raft         *hraft.Raft
	owned        []io.Closer
	applyTimeout time.Duration
}

func OpenCatalogMetaRaftProviderV1(opts CatalogMetaRaftProviderOptionsV1) (*CatalogMetaRaftProviderV1, error) {
	cluster, err := Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, err)
	}
	if opts.State == nil || opts.Transport == nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("catalog meta state and transport are required"))
	}
	address, ok := localPeerAddress(cluster)
	if !ok || string(opts.Transport.LocalAddr()) != address {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("local peer address does not match transport"))
	}
	stores, err := openHashicorpRaftStores(cluster.Layout, HashicorpRaftProviderOptions{LogStore: opts.LogStore, StableStore: opts.StableStore, SnapshotStore: opts.SnapshotStore, SnapshotRetain: opts.SnapshotRetain})
	if err != nil {
		return nil, err
	}
	defer stores.closeOnError(&err)
	conf := hashicorpRaftConfig(cluster.NodeID, opts.RaftConfig)
	if opts.Bootstrap {
		has, e := hraft.HasExistingState(stores.log, stores.stable, stores.snapshots)
		if e != nil {
			return nil, e
		}
		if !has {
			if e = hraft.BootstrapCluster(conf, stores.log, stores.stable, stores.snapshots, opts.Transport, hashicorpRaftConfiguration(cluster)); e != nil {
				return nil, e
			}
		}
	}
	r, err := hraft.NewRaft(conf, catalogMetaRaftFSMV1{state: opts.State}, stores.log, stores.stable, stores.snapshots, opts.Transport)
	if err != nil {
		return nil, err
	}
	timeout := opts.ApplyTimeout
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	return &CatalogMetaRaftProviderV1{cluster: cluster, raft: r, owned: stores.owned, applyTimeout: timeout}, nil
}

func (p *CatalogMetaRaftProviderV1) ClusterAdmissionStatus(ctx context.Context) (AdmissionStatus, error) {
	if p == nil || p.raft == nil {
		return AdmissionStatus{}, ErrInvalidHashicorpRaftProvider
	}
	state := p.raft.State()
	if state == hraft.Leader {
		return LeaderAdmission(), nil
	}
	_, leader := p.raft.LeaderWithID()
	if state == hraft.Shutdown {
		return UnavailableAdmission("catalog meta raft shutdown"), nil
	}
	if leader != "" {
		return FollowerAdmission(NodeID(leader), "catalog meta raft follower"), nil
	}
	return UnavailableAdmission("catalog meta leader unavailable"), nil
}
func (p *CatalogMetaRaftProviderV1) Close() error {
	if p == nil {
		return nil
	}
	var es []error
	if p.raft != nil {
		es = append(es, p.raft.Shutdown().Error())
	}
	for _, c := range p.owned {
		es = append(es, c.Close())
	}
	return errors.Join(es...)
}
func (p *CatalogMetaRaftProviderV1) SubmitCatalogMetaCommandV1(ctx context.Context, command []byte) (uint64, uint64, error) {
	if p == nil || p.raft == nil {
		return 0, 0, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	status, err := p.ClusterAdmissionStatus(ctx)
	if err != nil {
		return 0, 0, err
	}
	if status.Unavailable {
		return 0, 0, ErrAdmissionUnavailable
	}
	if !status.Leader {
		return 0, 0, ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		// Nothing has been enqueued yet; this is an ordinary caller
		// cancellation, not an ambiguous commit outcome.
		return 0, 0, err
	}
	f := p.raft.Apply(bytes.Clone(command), p.applyTimeout)
	if err := waitHashicorpRaftFuture(ctx, f); err != nil {
		return 0, 0, mapHashicorpRaftApplyError(err)
	}
	response, ok := f.Response().(catalogMetaRaftApplyResponseV1)
	if !ok {
		return 0, 0, ErrHashicorpRaftLogEntry
	}
	if response.err != nil {
		return 0, 0, response.err
	}
	return response.term, response.index, nil
}
func (p *CatalogMetaRaftProviderV1) SnapshotCatalogMetaV1(ctx context.Context) error {
	if p == nil || p.raft == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return waitHashicorpRaftFuture(ctx, p.raft.Snapshot())
}

type catalogMetaRaftApplyResponseV1 struct {
	term, index uint64
	err         error
}
type catalogMetaRaftFSMV1 struct{ state CatalogMetaCommittedStateV1 }

func (f catalogMetaRaftFSMV1) Apply(log *hraft.Log) interface{} {
	if log == nil || log.Type != hraft.LogCommand || f.state == nil {
		return catalogMetaRaftApplyResponseV1{err: ErrHashicorpRaftLogEntry}
	}
	return catalogMetaRaftApplyResponseV1{term: log.Term, index: log.Index, err: f.state.ApplyCatalogMetaCommittedV1(catalogMetaApplyCapabilityV1(), bytes.Clone(log.Data), log.Index)}
}
func (f catalogMetaRaftFSMV1) Snapshot() (hraft.FSMSnapshot, error) {
	if f.state == nil {
		return nil, ErrRaftSnapshotUnsupported
	}
	b, e := f.state.ExportCatalogMetaSnapshotBytesV1()
	if e != nil {
		return nil, e
	}
	return catalogMetaRaftSnapshotV1{bytes: b}, nil
}
func (f catalogMetaRaftFSMV1) Restore(src io.ReadCloser) error {
	if src == nil || f.state == nil {
		return ErrRaftSnapshotUnsupported
	}
	defer src.Close()
	b, e := io.ReadAll(io.LimitReader(src, 2<<20))
	if e != nil {
		return e
	}
	return f.state.InstallCatalogMetaSnapshotBytesV1(b)
}

type catalogMetaRaftSnapshotV1 struct{ bytes []byte }

func (s catalogMetaRaftSnapshotV1) Persist(sink hraft.SnapshotSink) error {
	if sink == nil {
		return ErrRaftSnapshotUnsupported
	}
	if _, e := sink.Write(s.bytes); e != nil {
		_ = sink.Cancel()
		return e
	}
	return sink.Close()
}
func (s catalogMetaRaftSnapshotV1) Release() {}
