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
	hashicorpRaftSnapshotCheckInterval = 24 * time.Hour
	hashicorpRaftMinTrailingLogs       = 1
	hashicorpRaftReadIndexInitialPoll  = 2 * time.Millisecond
	hashicorpRaftReadIndexMaxPoll      = 25 * time.Millisecond
	hashicorpRaftSnapshotCopyBuffer    = 64 << 10
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

	// ApplyFailureHandler is invoked before the FSM returns a local apply error.
	// Nil panics, which fails the Raft node closed so followers cannot advance
	// their applied index past an unapplied TreeDB command.
	ApplyFailureHandler func(error)
}

// HashicorpRaftProvider implements AdmissionProvider, CommitSource, and
// ReadIndexProvider on top of github.com/hashicorp/raft for one TreeDB Raft
// group.
type HashicorpRaftProvider struct {
	cluster         ResolvedConfig
	raft            *hraft.Raft
	logStore        hraft.LogStore
	appliedProgress AppliedProgressReader
	applyTimeout    time.Duration
	owned           []io.Closer
}

func OpenHashicorpRaftProvider(opts HashicorpRaftProviderOptions) (*HashicorpRaftProvider, error) {
	cluster, err := Validate(opts.Cluster)
	if err != nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, err)
	}
	if opts.Applier == nil {
		return nil, errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("committed command applier is required"))
	}
	if err := validateHashicorpRaftApplier(opts.Applier); err != nil {
		return nil, err
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
	applyFailureHandler := opts.ApplyFailureHandler
	if applyFailureHandler == nil {
		applyFailureHandler = panicHashicorpRaftApplyFailure
	}
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
		groupID:             cluster.GroupID,
		applier:             opts.Applier,
		applyFailureHandler: applyFailureHandler,
	}, stores.log, stores.stable, stores.snapshots, opts.Transport)
	if err != nil {
		err = errors.Join(ErrInvalidHashicorpRaftProvider, err)
		return nil, err
	}
	applyTimeout := opts.ApplyTimeout
	if applyTimeout <= 0 {
		applyTimeout = hashicorpRaftDefaultApplyTimeout
	}
	progressReader, _ := opts.Applier.(AppliedProgressReader)
	return &HashicorpRaftProvider{
		cluster:         cluster,
		raft:            r,
		logStore:        stores.log,
		appliedProgress: progressReader,
		applyTimeout:    applyTimeout,
		owned:           stores.owned,
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

func (p *HashicorpRaftProvider) ReadIndex(ctx context.Context, target ReadIndexBarrier) (ReadIndexProof, error) {
	if p == nil || p.raft == nil {
		return ReadIndexProof{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if target.NodeID != "" && target.NodeID != p.cluster.NodeID {
		return ReadIndexProof{}, fmt.Errorf("%w: read-index target node %q does not match local node %q", ErrReadBarrierTargetMismatch, target.NodeID, p.cluster.NodeID)
	}
	if target.GroupID != "" && target.GroupID != p.cluster.GroupID {
		return ReadIndexProof{}, fmt.Errorf("%w: read-index target group %q does not match local group %q", ErrReadBarrierTargetMismatch, target.GroupID, p.cluster.GroupID)
	}
	if err := ctx.Err(); err != nil {
		return ReadIndexProof{}, err
	}
	if err := p.requireHashicorpReadIndexLeader(); err != nil {
		return ReadIndexProof{}, err
	}
	if err := waitHashicorpRaftFuture(ctx, p.raft.VerifyLeader()); err != nil {
		return ReadIndexProof{}, p.mapHashicorpRaftReadIndexError(err)
	}
	if err := p.requireHashicorpReadIndexLeader(); err != nil {
		return ReadIndexProof{}, err
	}
	readTerm, readCommitIndex, err := p.waitHashicorpReadIndexCurrentTermCommit(ctx)
	if err != nil {
		return ReadIndexProof{}, err
	}
	progress, err := p.waitTreeDBAppliedReadPrefix(ctx, readCommitIndex)
	if err != nil {
		return ReadIndexProof{}, err
	}
	if err := p.requireHashicorpReadIndexLeaderTerm(readTerm); err != nil {
		return ReadIndexProof{}, err
	}
	// HashiCorp Raft v1.7 has no native ReadIndex API. VerifyLeader sends an
	// immediate heartbeat to voting peers. Before trusting CommitIndex, wait for
	// the leader's startup no-op/current-term entry to be committed; otherwise a
	// newly elected leader can under-report the committed prefix learned from the
	// previous term. Then wait for the local TreeDB applier to consume command
	// entries in that prefix without appending LogBarrier entries. If TreeDB
	// applied progress is behind, the provider inspects the committed log gap and
	// accepts only command-free gaps (no-op/configuration/barrier entries do not
	// mutate TreeDB state).
	proof := ReadIndexProof{
		NodeID:       p.cluster.NodeID,
		GroupID:      p.cluster.GroupID,
		Term:         readTerm,
		Index:        progress.Index,
		HasQuorum:    true,
		EvidenceKind: ReadIndexEvidenceProduction,
	}
	if err := target.Check(proof); err != nil {
		return ReadIndexProof{}, err
	}
	return proof, nil
}

func (p *HashicorpRaftProvider) readIndexAppliedProgress(ctx context.Context) (AppliedProgress, error) {
	if p == nil {
		return AppliedProgress{}, ErrInvalidHashicorpRaftProvider
	}
	if p.appliedProgress == nil {
		return AppliedProgress{}, fmt.Errorf("%w: applied progress reader is not configured", ErrReadBarrierNotSatisfied)
	}
	progress, err := p.appliedProgress.AppliedProgress(ctx)
	if err != nil {
		return AppliedProgress{}, err
	}
	if err := p.validateReadIndexAppliedProgress(progress); err != nil {
		return AppliedProgress{}, err
	}
	return progress, nil
}

func (p *HashicorpRaftProvider) readIndexAppliedProgressSnapshot(ctx context.Context) (AppliedProgress, error) {
	if p == nil {
		return AppliedProgress{}, ErrInvalidHashicorpRaftProvider
	}
	if p.appliedProgress == nil {
		return AppliedProgress{}, fmt.Errorf("%w: applied progress reader is not configured", ErrReadBarrierNotSatisfied)
	}
	progress, err := p.appliedProgress.AppliedProgress(ctx)
	if err != nil {
		return AppliedProgress{}, err
	}
	if err := p.validateReadIndexAppliedProgressIdentity(progress); err != nil {
		return AppliedProgress{}, err
	}
	if !progress.HasApplied {
		progress.Index = 0
		progress.Term = 0
	}
	return progress, nil
}

func (p *HashicorpRaftProvider) waitTreeDBAppliedReadPrefix(ctx context.Context, minIndex uint64) (AppliedProgress, error) {
	if p == nil || p.raft == nil {
		return AppliedProgress{}, ErrInvalidHashicorpRaftProvider
	}
	if err := p.requireHashicorpReadIndexLeader(); err != nil {
		return AppliedProgress{}, err
	}
	progress, err := p.readIndexAppliedProgressSnapshot(ctx)
	if err != nil {
		return AppliedProgress{}, err
	}
	if progress.HasApplied && progress.Index != 0 && (minIndex == 0 || progress.Index >= minIndex) {
		return progress, nil
	}
	firstIndex, err := readIndexGapFirstIndexAfterApplied(progress.Index)
	if err != nil {
		return AppliedProgress{}, err
	}
	noCommands, firstCmd, err := p.readIndexGapHasNoCommands(firstIndex, minIndex)
	if err != nil {
		return AppliedProgress{}, err
	}
	if noCommands {
		if progress.HasApplied && progress.Index != 0 {
			return progress, nil
		}
		return AppliedProgress{}, fmt.Errorf("%w: no applied TreeDB command index", ErrReadBarrierNotSatisfied)
	}
	timeout := p.applyTimeout
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	delay := hashicorpRaftReadIndexInitialPoll
	pollTimer := time.NewTimer(delay)
	defer pollTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return AppliedProgress{}, ctx.Err()
		case <-timer.C:
			return AppliedProgress{}, fmt.Errorf("%w: TreeDB applied index %d below read-index commit prefix %d", ErrReadBarrierNotSatisfied, progress.Index, minIndex)
		case <-pollTimer.C:
		}
		if delay < hashicorpRaftReadIndexMaxPoll {
			delay *= 2
			if delay > hashicorpRaftReadIndexMaxPoll {
				delay = hashicorpRaftReadIndexMaxPoll
			}
		}
		pollTimer.Reset(delay)
		if err := p.requireHashicorpReadIndexLeader(); err != nil {
			return AppliedProgress{}, err
		}
		progress, err = p.readIndexAppliedProgressSnapshot(ctx)
		if err != nil {
			return AppliedProgress{}, err
		}
		if progress.HasApplied && progress.Index != 0 && progress.Index >= minIndex {
			return progress, nil
		}
		// Skip the gap rescan if the previously found command is still ahead of
		// applied progress: the gap still has commands and no log I/O is needed.
		if firstCmd != 0 && progress.Index < firstCmd {
			continue
		}
		firstIndex, err := readIndexGapFirstIndexAfterApplied(progress.Index)
		if err != nil {
			return AppliedProgress{}, err
		}
		noCommands, firstCmd, err = p.readIndexGapHasNoCommands(firstIndex, minIndex)
		if err != nil {
			return AppliedProgress{}, err
		}
		if noCommands {
			if progress.HasApplied && progress.Index != 0 {
				return progress, nil
			}
			return AppliedProgress{}, fmt.Errorf("%w: no applied TreeDB command index", ErrReadBarrierNotSatisfied)
		}
	}
}

func (p *HashicorpRaftProvider) waitHashicorpReadIndexCurrentTermCommit(ctx context.Context) (uint64, uint64, error) {
	if p == nil || p.raft == nil {
		return 0, 0, ErrInvalidHashicorpRaftProvider
	}
	return waitHashicorpCurrentTermCommitV1(ctx, p.raft, p.logStore, p.applyTimeout, p.requireHashicorpReadIndexLeader)
}

func waitHashicorpCurrentTermCommitV1(ctx context.Context, r *hraft.Raft, logStore hraft.LogStore, timeout time.Duration, requireLeader func() error) (uint64, uint64, error) {
	if r == nil || logStore == nil || requireLeader == nil {
		return 0, 0, ErrInvalidHashicorpRaftProvider
	}
	if timeout <= 0 {
		timeout = hashicorpRaftDefaultApplyTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	delay := hashicorpRaftReadIndexInitialPoll
	pollTimer := time.NewTimer(delay)
	defer pollTimer.Stop()
	for {
		if err := requireLeader(); err != nil {
			return 0, 0, err
		}
		term := r.CurrentTerm()
		commitIndex := r.CommitIndex()
		ok, err := raftLogIndexHasTermV1(logStore, commitIndex, term)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			return term, commitIndex, nil
		}
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-timer.C:
			return 0, 0, fmt.Errorf("%w: leader term %d has no committed read-index prefix at commit index %d", ErrReadBarrierNotSatisfied, term, commitIndex)
		case <-pollTimer.C:
		}
		if delay < hashicorpRaftReadIndexMaxPoll {
			delay *= 2
			if delay > hashicorpRaftReadIndexMaxPoll {
				delay = hashicorpRaftReadIndexMaxPoll
			}
		}
		pollTimer.Reset(delay)
	}
}

func (p *HashicorpRaftProvider) readIndexCommitIndexHasCurrentTerm(commitIndex, term uint64) (bool, error) {
	if p == nil {
		return false, ErrInvalidHashicorpRaftProvider
	}
	return raftLogIndexHasTermV1(p.logStore, commitIndex, term)
}

func raftLogIndexHasTermV1(logStore hraft.LogStore, commitIndex, term uint64) (bool, error) {
	if commitIndex == 0 || term == 0 {
		return false, nil
	}
	if logStore == nil {
		return false, fmt.Errorf("%w: hashicorp raft log store is not configured", ErrReadBarrierNotSatisfied)
	}
	var entry hraft.Log
	if err := logStore.GetLog(commitIndex, &entry); err != nil {
		return false, errors.Join(
			ErrReadBarrierNotSatisfied,
			fmt.Errorf("unable to inspect committed raft log %d for read-index term gate: %w", commitIndex, err),
		)
	}
	if entry.Term > term {
		return false, fmt.Errorf("%w: committed raft log %d has future term %d above current term %d", ErrReadBarrierNotSatisfied, commitIndex, entry.Term, term)
	}
	return entry.Term == term, nil
}

func readIndexGapFirstIndexAfterApplied(appliedIndex uint64) (uint64, error) {
	if appliedIndex == ^uint64(0) {
		return 0, fmt.Errorf("%w: applied raft index %d cannot advance read-index gap scan", ErrReadBarrierNotSatisfied, appliedIndex)
	}
	return appliedIndex + 1, nil
}

func (p *HashicorpRaftProvider) readIndexGapHasNoCommands(firstIndex, lastIndex uint64) (bool, uint64, error) {
	if p == nil {
		return false, 0, ErrInvalidHashicorpRaftProvider
	}
	if firstIndex == 0 {
		firstIndex = 1
	}
	if lastIndex < firstIndex {
		return true, 0, nil
	}
	if p.logStore == nil {
		return false, 0, fmt.Errorf("%w: hashicorp raft log store is not configured", ErrReadBarrierNotSatisfied)
	}
	var entry hraft.Log
	for index := firstIndex; ; index++ {
		entry = hraft.Log{}
		if err := p.logStore.GetLog(index, &entry); err != nil {
			return false, 0, errors.Join(
				ErrReadBarrierNotSatisfied,
				fmt.Errorf("unable to inspect committed raft log %d for read-index gap: %w", index, err),
			)
		}
		if entry.Type == hraft.LogCommand {
			return false, index, nil
		}
		if index == lastIndex {
			break
		}
	}
	return true, 0, nil
}

func (p *HashicorpRaftProvider) requireHashicorpReadIndexLeader() error {
	if p == nil || p.raft == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if state := p.raft.State(); state != hraft.Leader {
		return p.hashicorpReadIndexNotLeader(state)
	}
	return nil
}

func (p *HashicorpRaftProvider) requireHashicorpReadIndexLeaderTerm(term uint64) error {
	if err := p.requireHashicorpReadIndexLeader(); err != nil {
		return err
	}
	if got := p.raft.CurrentTerm(); got != term {
		return fmt.Errorf("%w: read-index leader term changed from %d to %d before proof", ErrReadBarrierNotSatisfied, term, got)
	}
	return nil
}

func (p *HashicorpRaftProvider) validateReadIndexAppliedProgress(progress AppliedProgress) error {
	if p == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if err := p.validateReadIndexAppliedProgressIdentity(progress); err != nil {
		return err
	}
	if !progress.HasApplied || progress.Index == 0 {
		return fmt.Errorf("%w: no applied TreeDB command index", ErrReadBarrierNotSatisfied)
	}
	return nil
}

func (p *HashicorpRaftProvider) validateReadIndexAppliedProgressIdentity(progress AppliedProgress) error {
	if p == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if progress.NodeID != p.cluster.NodeID {
		return fmt.Errorf("%w: applied progress node %q does not match local node %q", ErrReadBarrierTargetMismatch, progress.NodeID, p.cluster.NodeID)
	}
	if progress.GroupID != p.cluster.GroupID {
		return fmt.Errorf("%w: applied progress group %q does not match local group %q", ErrReadBarrierTargetMismatch, progress.GroupID, p.cluster.GroupID)
	}
	return nil
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
	if err := ctx.Err(); err != nil {
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
	if response.ApplyErr != nil {
		return CommitCommandEntryV1Result{}, hashicorpRaftLogEntryError(response.ApplyErr)
	}
	if response.Entry.Term == 0 || response.Entry.Index == 0 {
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

func (p *HashicorpRaftProvider) hashicorpReadIndexNotLeader(state hraft.RaftState) error {
	reason := "read-index requires leader; hashicorp raft state " + state.String()
	if p != nil && p.raft != nil {
		if _, leaderID := p.raft.LeaderWithID(); leaderID != "" {
			reason += "; leader_hint=" + string(leaderID)
		}
	}
	if state == hraft.Shutdown {
		return errors.Join(ErrHashicorpRaftUnavailable, errors.New(reason))
	}
	return errors.Join(ErrNotLeader, errors.New(reason))
}

func validateHashicorpRaftApplier(applier CommittedCommandApplierV1) error {
	if gapSupport, ok := applier.(InitialIndexGapSupportV1); ok && !gapSupport.AllowsInitialIndexGapV1() {
		return errors.Join(ErrInvalidHashicorpRaftProvider, fmt.Errorf("committed command applier must allow initial Raft log index gaps for HashiCorp Raft"))
	}
	return nil
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
	stores.snapshots = wrapHashicorpRaftSnapshotStoreV1(stores.snapshots)
	return stores, nil
}

func (s *hashicorpRaftStores) closeOnError(errp *error) {
	if errp != nil && *errp != nil {
		s.closeOwned()
		s.owned = nil
	}
}

// HashicorpRaftSnapshotResultV1 reports the persisted HashiCorp snapshot and
// local log range observed around the snapshot operation. Log compaction is
// performed by HashiCorp Raft only after the FSM snapshot is durably persisted.
type HashicorpRaftSnapshotResultV1 struct {
	ID                string
	LastIncludedTerm  uint64
	LastIncludedIndex uint64
	SizeBytes         int64

	FirstLogIndexBefore uint64
	LastLogIndexBefore  uint64
	FirstLogIndexAfter  uint64
	LastLogIndexAfter   uint64
	LogCompacted        bool

	Manifest SnapshotManifestV1
}

// Snapshot asks HashiCorp Raft to persist an FSM snapshot and compact logs
// according to the active Raft configuration.
func (p *HashicorpRaftProvider) Snapshot(ctx context.Context) (HashicorpRaftSnapshotResultV1, error) {
	if p == nil || p.raft == nil || p.logStore == nil {
		return HashicorpRaftSnapshotResultV1{}, ErrInvalidHashicorpRaftProvider
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return HashicorpRaftSnapshotResultV1{}, err
	}
	firstBefore, firstBeforeErr := p.logStore.FirstIndex()
	lastBefore, lastBeforeErr := p.logStore.LastIndex()
	if firstBeforeErr != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, firstBeforeErr)
	}
	if lastBeforeErr != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, lastBeforeErr)
	}
	future := p.raft.Snapshot()
	if err := waitHashicorpRaftFuture(ctx, future); err != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, err)
	}
	meta, snapshot, err := future.Open()
	if err != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, err)
	}
	defer snapshot.Close()
	manifest, err := DecodeSnapshotManifestV1FromArchiveReader(snapshot)
	if err != nil {
		return HashicorpRaftSnapshotResultV1{}, err
	}
	if err := validateHashicorpRaftSnapshotManifestV1(meta, manifest); err != nil {
		return HashicorpRaftSnapshotResultV1{}, err
	}
	firstAfter, firstAfterErr := p.logStore.FirstIndex()
	lastAfter, lastAfterErr := p.logStore.LastIndex()
	if firstAfterErr != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, firstAfterErr)
	}
	if lastAfterErr != nil {
		return HashicorpRaftSnapshotResultV1{}, errors.Join(ErrHashicorpRaftUnavailable, lastAfterErr)
	}
	result := HashicorpRaftSnapshotResultV1{
		ID:                  meta.ID,
		LastIncludedTerm:    meta.Term,
		LastIncludedIndex:   meta.Index,
		SizeBytes:           meta.Size,
		FirstLogIndexBefore: firstBefore,
		LastLogIndexBefore:  lastBefore,
		FirstLogIndexAfter:  firstAfter,
		LastLogIndexAfter:   lastAfter,
		LogCompacted:        firstAfter > firstBefore || (firstAfter == 0 && lastAfter == 0 && lastBefore > 0),
		Manifest:            manifest,
	}
	return result, nil
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
		conf.SnapshotInterval = hashicorpRaftSnapshotCheckInterval
		conf.SnapshotThreshold = ^uint64(0)
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
	if conf.SnapshotInterval < 5*time.Millisecond {
		conf.SnapshotInterval = hashicorpRaftSnapshotCheckInterval
	}
	if conf.TrailingLogs < hashicorpRaftMinTrailingLogs {
		conf.TrailingLogs = hashicorpRaftMinTrailingLogs
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

func (p *HashicorpRaftProvider) mapHashicorpRaftReadIndexError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, hraft.ErrNotLeader),
		errors.Is(err, hraft.ErrLeadershipLost),
		errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		state := hraft.Shutdown
		if p != nil && p.raft != nil {
			state = p.raft.State()
		}
		return errors.Join(p.hashicorpReadIndexNotLeader(state), err)
	case errors.Is(err, hraft.ErrAbortedByRestore),
		errors.Is(err, hraft.ErrRaftShutdown),
		errors.Is(err, hraft.ErrEnqueueTimeout):
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	default:
		return errors.Join(ErrHashicorpRaftUnavailable, err)
	}
}

func mapHashicorpRaftApplyError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return errors.Join(ErrCommitAmbiguous, err)
	case errors.Is(err, hraft.ErrNotLeader), errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return errors.Join(ErrNotLeader, err)
	case errors.Is(err, hraft.ErrLeadershipLost):
		return errors.Join(ErrCommitAmbiguous, err)
	case errors.Is(err, hraft.ErrAbortedByRestore),
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
	groupID             GroupID
	applier             CommittedCommandApplierV1
	applyFailureHandler func(error)
}

func (f hashicorpRaftFSM) Apply(log *hraft.Log) interface{} {
	if log == nil || log.Type != hraft.LogCommand {
		return f.applyFailureResponse(CommittedCommandEntryV1{}, raftentry.ApplyResultV1{}, fmt.Errorf("unsupported log"))
	}
	payload, err := decodeHashicorpRaftCommandEntryV1(log.Data)
	if err != nil {
		return f.applyFailureResponse(CommittedCommandEntryV1{}, raftentry.ApplyResultV1{}, err)
	}
	if f.groupID != "" && payload.GroupID != f.groupID {
		return f.applyFailureResponse(CommittedCommandEntryV1{}, raftentry.ApplyResultV1{}, fmt.Errorf("entry group %q does not match local group %q", payload.GroupID, f.groupID))
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
	if applyErr != nil {
		return f.applyFailureResponse(entry, result, applyErr)
	}
	return hashicorpRaftApplyResponseV1{
		Entry:       entry,
		ApplyResult: result,
	}
}

func (f hashicorpRaftFSM) applyFailureResponse(entry CommittedCommandEntryV1, result raftentry.ApplyResultV1, err error) hashicorpRaftApplyResponseV1 {
	err = hashicorpRaftLogEntryError(err)
	handler := f.applyFailureHandler
	if handler == nil {
		handler = panicHashicorpRaftApplyFailure
	}
	handler(err)
	return hashicorpRaftApplyResponseV1{
		Entry:       entry,
		ApplyResult: result,
		ApplyErr:    err,
	}
}

func panicHashicorpRaftApplyFailure(err error) {
	panic(err)
}

func hashicorpRaftLogEntryError(err error) error {
	if err == nil {
		return ErrHashicorpRaftLogEntry
	}
	if errors.Is(err, ErrHashicorpRaftLogEntry) {
		return err
	}
	return errors.Join(ErrHashicorpRaftLogEntry, err)
}

func (f hashicorpRaftFSM) Snapshot() (hraft.FSMSnapshot, error) {
	exporter, ok := f.applier.(RaftSnapshotExporterV1)
	if !ok {
		return nil, ErrRaftSnapshotUnsupported
	}
	snapshot, err := exporter.ExportRaftSnapshotV1()
	if err != nil {
		return nil, err
	}
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	return hashicorpRaftSnapshotV1{snapshot: snapshot}, nil
}

func (f hashicorpRaftFSM) Restore(src io.ReadCloser) error {
	if src == nil {
		return ErrRaftSnapshotUnsupported
	}
	defer src.Close()
	installer, ok := f.applier.(RaftSnapshotInstallerV1)
	if !ok {
		return ErrRaftSnapshotUnsupported
	}
	return installer.InstallRaftSnapshotV1(src)
}

type hashicorpRaftSnapshotV1 struct {
	snapshot RaftSnapshotV1
}

func (s hashicorpRaftSnapshotV1) Persist(sink hraft.SnapshotSink) error {
	if sink == nil {
		return ErrInvalidSnapshotManifest
	}
	if err := s.snapshot.Validate(); err != nil {
		_ = sink.Cancel()
		return err
	}
	if boundary, ok := hashicorpRaftSnapshotBoundaryFromSinkV1(sink); ok {
		if err := validateHashicorpRaftSnapshotBoundaryV1(boundary, s.snapshot.Manifest); err != nil {
			_ = sink.Cancel()
			return err
		}
	}
	src, err := s.snapshot.OpenArchive()
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	copyErr := copyHashicorpRaftSnapshotArchiveV1(sink, src)
	closeSrcErr := src.Close()
	if copyErr != nil {
		_ = sink.Cancel()
		return copyErr
	}
	if closeSrcErr != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raftcluster: close raft snapshot archive source: %w", closeSrcErr)
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}

func copyHashicorpRaftSnapshotArchiveV1(dst io.Writer, src io.Reader) error {
	buf := make([]byte, hashicorpRaftSnapshotCopyBuffer)
	_, err := io.CopyBuffer(dst, src, buf)
	return err
}

func (s hashicorpRaftSnapshotV1) Release() {
	_ = s.snapshot.Release()
}

type hashicorpRaftSnapshotBoundaryV1 struct {
	Term  uint64
	Index uint64
}

type hashicorpRaftSnapshotBoundarySinkV1 interface {
	hashicorpRaftSnapshotBoundaryV1() (hashicorpRaftSnapshotBoundaryV1, bool)
}

type hashicorpRaftSnapshotStoreV1 struct {
	inner hraft.SnapshotStore
}

func wrapHashicorpRaftSnapshotStoreV1(inner hraft.SnapshotStore) hraft.SnapshotStore {
	if inner == nil {
		return nil
	}
	if _, ok := inner.(hashicorpRaftSnapshotStoreV1); ok {
		return inner
	}
	return hashicorpRaftSnapshotStoreV1{inner: inner}
}

func (s hashicorpRaftSnapshotStoreV1) Create(version hraft.SnapshotVersion, index, term uint64, configuration hraft.Configuration, configurationIndex uint64, trans hraft.Transport) (hraft.SnapshotSink, error) {
	sink, err := s.inner.Create(version, index, term, configuration, configurationIndex, trans)
	if err != nil || sink == nil {
		return sink, err
	}
	return hashicorpRaftSnapshotSinkV1{
		SnapshotSink: sink,
		boundary: hashicorpRaftSnapshotBoundaryV1{
			Term:  term,
			Index: index,
		},
	}, nil
}

func (s hashicorpRaftSnapshotStoreV1) List() ([]*hraft.SnapshotMeta, error) {
	return s.inner.List()
}

func (s hashicorpRaftSnapshotStoreV1) Open(id string) (*hraft.SnapshotMeta, io.ReadCloser, error) {
	return s.inner.Open(id)
}

type hashicorpRaftSnapshotSinkV1 struct {
	hraft.SnapshotSink
	boundary hashicorpRaftSnapshotBoundaryV1
}

func (s hashicorpRaftSnapshotSinkV1) hashicorpRaftSnapshotBoundaryV1() (hashicorpRaftSnapshotBoundaryV1, bool) {
	return s.boundary, true
}

func hashicorpRaftSnapshotBoundaryFromSinkV1(sink hraft.SnapshotSink) (hashicorpRaftSnapshotBoundaryV1, bool) {
	if sink == nil {
		return hashicorpRaftSnapshotBoundaryV1{}, false
	}
	boundarySink, ok := sink.(hashicorpRaftSnapshotBoundarySinkV1)
	if !ok {
		return hashicorpRaftSnapshotBoundaryV1{}, false
	}
	return boundarySink.hashicorpRaftSnapshotBoundaryV1()
}

func validateHashicorpRaftSnapshotManifestV1(meta *hraft.SnapshotMeta, manifest SnapshotManifestV1) error {
	if meta == nil {
		return fmt.Errorf("%w: missing hashicorp snapshot metadata", ErrInvalidSnapshotManifest)
	}
	return validateHashicorpRaftSnapshotBoundaryV1(hashicorpRaftSnapshotBoundaryV1{
		Term:  meta.Term,
		Index: meta.Index,
	}, manifest)
}

func validateHashicorpRaftSnapshotBoundaryV1(boundary hashicorpRaftSnapshotBoundaryV1, manifest SnapshotManifestV1) error {
	if boundary.Term != manifest.LastIncludedTerm {
		return fmt.Errorf("%w: hashicorp snapshot term %d does not match manifest term %d", ErrInvalidSnapshotManifest, boundary.Term, manifest.LastIncludedTerm)
	}
	if boundary.Index != manifest.LastIncludedIndex {
		return fmt.Errorf("%w: hashicorp snapshot index %d does not match manifest index %d", ErrInvalidSnapshotManifest, boundary.Index, manifest.LastIncludedIndex)
	}
	return nil
}
