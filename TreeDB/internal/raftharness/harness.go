package raftharness

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

type EvidenceKindV1 string

const (
	// EvidenceInjectedCommittedEntryReplayV1 means the harness applied entries
	// that the test injected as already committed. It is not production Raft
	// consensus evidence.
	EvidenceInjectedCommittedEntryReplayV1 EvidenceKindV1 = "injected-committed-entry-replay-v1"
	// EvidenceInjectedSnapshotInstallV1 means the harness reconstructed a
	// logical snapshot cut from injected committed entries. It is not production
	// Raft snapshot-transfer evidence.
	EvidenceInjectedSnapshotInstallV1 EvidenceKindV1 = "injected-snapshot-install-v1"
)

var (
	ErrNodeNotFound         = errors.New("raftharness: node not found")
	ErrNodeClosed           = errors.New("raftharness: node closed")
	ErrHarnessClosed        = errors.New("raftharness: harness closed")
	ErrCommittedLogGap      = errors.New("raftharness: committed log gap")
	ErrCommittedLogConflict = errors.New("raftharness: committed log conflict")
)

type Options struct {
	RootDir      string
	GroupID      raftcluster.GroupID
	Nodes        []NodeConfig
	StoreOptions raftapply.DurableApplyStoreOptions
}

type NodeConfig struct {
	ID      raftcluster.NodeID
	Address string
}

type Harness struct {
	mu           sync.Mutex
	rootDir      string
	groupID      raftcluster.GroupID
	nodeConfigs  []NodeConfig
	storeOptions raftapply.DurableApplyStoreOptions
	nodes        map[raftcluster.NodeID]*Node
	committed    []raftfsm.CommittedEntryV1
	closed       bool
}

type Node struct {
	id     raftcluster.NodeID
	db     *backenddb.DB
	fsm    *raftfsm.FSM
	closed bool
}

type CommitEvidenceV1 struct {
	Kind                EvidenceKindV1
	Committed           bool
	ProductionConsensus bool
	EntryIDs            []raftentry.ApplyEntryID
	Applied             map[raftcluster.NodeID][]raftentry.ApplyResultV1
}

type SnapshotInstallEvidenceV1 struct {
	Kind               EvidenceKindV1
	Installed          bool
	ProductionSnapshot bool
	Manifest           raftcluster.SnapshotManifestV1
	EntryIDs           []raftentry.ApplyEntryID
	Applied            []raftentry.ApplyResultV1
}

func (e SnapshotInstallEvidenceV1) ProvesProductionSnapshot() bool {
	return e.ProductionSnapshot
}

func (e CommitEvidenceV1) ProvesProductionConsensus() bool {
	return e.ProductionConsensus
}

func (e CommitEvidenceV1) HasCommittedSuccess() bool {
	if !e.Committed {
		return false
	}
	for _, results := range e.Applied {
		for _, result := range results {
			if result.Status == raftentry.ApplyStatusApplied || result.Status == raftentry.ApplyStatusAlreadyApplied {
				return true
			}
		}
	}
	return false
}

func Open(opts Options) (*Harness, error) {
	if opts.RootDir == "" {
		return nil, fmt.Errorf("raftharness: missing root dir")
	}
	groupID := opts.GroupID
	if groupID == "" {
		groupID = "default"
	}
	nodeConfigs, err := normalizeNodeConfigs(opts.Nodes)
	if err != nil {
		return nil, err
	}
	h := &Harness{
		rootDir:      opts.RootDir,
		groupID:      groupID,
		nodeConfigs:  nodeConfigs,
		storeOptions: opts.StoreOptions,
		nodes:        make(map[raftcluster.NodeID]*Node, len(nodeConfigs)),
	}
	for _, cfg := range nodeConfigs {
		node, err := h.openNode(cfg)
		if err != nil {
			_ = h.Close()
			return nil, err
		}
		h.nodes[cfg.ID] = node
	}
	return h, nil
}

func (h *Harness) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	var err error
	for _, node := range h.nodes {
		err = errors.Join(err, node.close())
	}
	return err
}

func (h *Harness) Node(id raftcluster.NodeID) (*Node, bool) {
	if h == nil {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	node, ok := h.nodes[id]
	return node, ok
}

func (h *Harness) CloseNode(id raftcluster.NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	return node.close()
}

func (h *Harness) ReopenNode(id raftcluster.NodeID) (*Node, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHarnessClosed
	}
	for _, cfg := range h.nodeConfigs {
		if cfg.ID != id {
			continue
		}
		if existing := h.nodes[id]; existing != nil {
			if err := existing.close(); err != nil {
				return nil, err
			}
		}
		node, err := h.openNode(cfg)
		if err != nil {
			return nil, err
		}
		h.nodes[id] = node
		return node, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
}

func (h *Harness) PreCommitEvidence(entries ...raftfsm.CommittedEntryV1) CommitEvidenceV1 {
	return CommitEvidenceV1{
		Kind:                EvidenceInjectedCommittedEntryReplayV1,
		Committed:           false,
		ProductionConsensus: false,
		EntryIDs:            applyEntryIDs(entries),
	}
}

func (h *Harness) Commit(entries ...raftfsm.CommittedEntryV1) (CommitEvidenceV1, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	evidence := CommitEvidenceV1{
		Kind:                EvidenceInjectedCommittedEntryReplayV1,
		Committed:           false,
		ProductionConsensus: false,
		EntryIDs:            applyEntryIDs(entries),
	}
	if h.closed {
		return evidence, ErrHarnessClosed
	}
	staged := make([]raftfsm.CommittedEntryV1, len(h.committed), len(h.committed)+len(entries))
	copy(staged, h.committed)
	for _, entry := range entries {
		next, err := appendCommitted(staged, entry)
		if err != nil {
			return evidence, err
		}
		staged = next
	}
	h.committed = staged
	evidence.Committed = len(entries) > 0
	return evidence, nil
}

func (h *Harness) CommitAndApply(nodeIDs []raftcluster.NodeID, entries ...raftfsm.CommittedEntryV1) (CommitEvidenceV1, error) {
	evidence, err := h.Commit(entries...)
	if err != nil {
		return evidence, err
	}
	evidence.Applied = make(map[raftcluster.NodeID][]raftentry.ApplyResultV1, len(nodeIDs))
	for _, id := range nodeIDs {
		results, err := h.ApplyCommittedEntriesToNode(id, entries...)
		evidence.Applied[id] = results
		if err != nil {
			return evidence, err
		}
	}
	return evidence, nil
}

func (h *Harness) ApplyCommittedEntriesToNode(id raftcluster.NodeID, entries ...raftfsm.CommittedEntryV1) ([]raftentry.ApplyResultV1, error) {
	node, committed, err := h.committedEntriesForApply(id, entries)
	if err != nil {
		return nil, err
	}
	return node.ApplyCommittedEntriesV1(committed...)
}

func (h *Harness) committedEntriesForApply(id raftcluster.NodeID, entries []raftfsm.CommittedEntryV1) (*Node, []raftfsm.CommittedEntryV1, error) {
	if h == nil {
		return nil, nil, ErrHarnessClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	committed := make([]raftfsm.CommittedEntryV1, 0, len(entries))
	var previous raftentry.ApplyEntryID
	for i, entry := range entries {
		entryID := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
		if entryID.Term == 0 || entryID.Index == 0 {
			return nil, nil, fmt.Errorf("%w: invalid committed id %+v", ErrCommittedLogGap, entryID)
		}
		if i > 0 {
			if entryID.Index <= previous.Index {
				return nil, nil, fmt.Errorf("%w: non-increasing apply index %d after %d", ErrCommittedLogConflict, entryID.Index, previous.Index)
			}
			if entryID.Term < previous.Term {
				return nil, nil, fmt.Errorf("%w: term regression at index %d: term %d after term %d", ErrCommittedLogConflict, entryID.Index, entryID.Term, previous.Term)
			}
		}
		want, _, ok := committedEntryByIndex(h.committed, entryID.Index)
		if !ok {
			return nil, nil, fmt.Errorf("%w: apply entry index %d is not committed", ErrCommittedLogGap, entryID.Index)
		}
		if !committedEntriesEqual(want, entry) {
			return nil, nil, fmt.Errorf("%w: apply entry does not match committed log at index %d", ErrCommittedLogConflict, entryID.Index)
		}
		committed = append(committed, cloneCommittedEntry(want))
		previous = entryID
	}
	return node, committed, nil
}

func (h *Harness) CatchUpNode(id raftcluster.NodeID) ([]raftentry.ApplyResultV1, error) {
	return h.catchUpNodeThrough(id, 0)
}

// CatchUpNodeThrough catches the node up only through maxIndex. A maxIndex of
// zero catches up through the full committed log.
func (h *Harness) CatchUpNodeThrough(id raftcluster.NodeID, maxIndex uint64) ([]raftentry.ApplyResultV1, error) {
	return h.catchUpNodeThrough(id, maxIndex)
}

func (h *Harness) InstallSnapshotPrefixToNodeV1(id raftcluster.NodeID, manifest raftcluster.SnapshotManifestV1) (SnapshotInstallEvidenceV1, error) {
	evidence := SnapshotInstallEvidenceV1{
		Kind:               EvidenceInjectedSnapshotInstallV1,
		ProductionSnapshot: false,
		Manifest:           manifest,
	}
	if h == nil {
		return evidence, ErrHarnessClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return evidence, ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		h.mu.Unlock()
		return evidence, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	if err := rejectNonEmptySnapshotInstallTarget(node); err != nil {
		h.mu.Unlock()
		return evidence, err
	}
	prefix, ok := committedPrefixThroughIndex(h.committed, manifest.LastIncludedIndex)
	if !ok {
		h.mu.Unlock()
		return evidence, fmt.Errorf("%w: snapshot index %d is not committed", ErrCommittedLogGap, manifest.LastIncludedIndex)
	}
	last := prefix[len(prefix)-1]
	if last.Term != manifest.LastIncludedTerm || last.Index != manifest.LastIncludedIndex {
		h.mu.Unlock()
		return evidence, fmt.Errorf("%w: snapshot last included %d/%d does not match committed log %d/%d", ErrCommittedLogConflict, manifest.LastIncludedTerm, manifest.LastIncludedIndex, last.Term, last.Index)
	}
	evidence.EntryIDs = applyEntryIDs(prefix)
	rootDir := h.rootDir
	groupID := h.groupID
	nodeConfigs := append([]NodeConfig(nil), h.nodeConfigs...)
	storeOptions := h.storeOptions
	h.mu.Unlock()

	if err := verifySnapshotPrefix(rootDir, groupID, id, nodeConfigs, storeOptions, manifest, prefix); err != nil {
		return evidence, err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return evidence, ErrHarnessClosed
	}
	node, ok = h.nodes[id]
	if !ok {
		h.mu.Unlock()
		return evidence, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	if err := rejectNonEmptySnapshotInstallTarget(node); err != nil {
		h.mu.Unlock()
		return evidence, err
	}
	if err := verifyCommittedPrefixLocked(h.committed, prefix, manifest); err != nil {
		h.mu.Unlock()
		return evidence, err
	}
	results, err := node.ApplyCommittedEntriesV1(prefix...)
	evidence.Applied = results
	if err != nil {
		h.mu.Unlock()
		return evidence, err
	}
	if err := node.fsm.VerifyInstalledSnapshotManifestV1(manifest); err != nil {
		h.mu.Unlock()
		return evidence, err
	}
	evidence.Installed = true
	h.mu.Unlock()
	return evidence, nil
}

// ReplaySnapshotTailToNodeV1 verifies that id contains the manifest boundary,
// then replays only committed entries after LastIncludedIndex.
func (h *Harness) ReplaySnapshotTailToNodeV1(id raftcluster.NodeID, manifest raftcluster.SnapshotManifestV1) ([]raftentry.ApplyResultV1, error) {
	if h == nil {
		return nil, ErrHarnessClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	prefix, ok := committedPrefixThroughIndex(h.committed, manifest.LastIncludedIndex)
	if !ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: snapshot index %d is not committed", ErrCommittedLogGap, manifest.LastIncludedIndex)
	}
	last := prefix[len(prefix)-1]
	if last.Term != manifest.LastIncludedTerm || last.Index != manifest.LastIncludedIndex {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: snapshot last included %d/%d does not match committed log %d/%d", ErrCommittedLogConflict, manifest.LastIncludedTerm, manifest.LastIncludedIndex, last.Term, last.Index)
	}
	tail := committedEntriesFromIndexThrough(h.committed, manifest.LastIncludedIndex, 0, false)
	h.mu.Unlock()

	if err := node.fsm.VerifyInstalledSnapshotManifestV1(manifest); err != nil {
		return nil, err
	}
	return node.ApplyCommittedEntriesV1(tail...)
}

func rejectNonEmptySnapshotInstallTarget(node *Node) error {
	if node == nil || node.db == nil || node.closed {
		return fmt.Errorf("%w: closed node", ErrNodeClosed)
	}
	if _, ok := node.LastApplied(); ok {
		return fmt.Errorf("%w: node %s already has local applied progress", ErrCommittedLogConflict, node.id)
	}
	if state, ok := node.db.StateToken(); ok && state.AppliedCommandLSN != 0 {
		lsn := state.AppliedCommandLSN
		return fmt.Errorf("%w: node %s has local AppliedCommandLSN coverage %d without apply progress", ErrCommittedLogConflict, node.id, lsn)
	}
	return nil
}

func verifyCommittedPrefixLocked(committed, prefix []raftfsm.CommittedEntryV1, manifest raftcluster.SnapshotManifestV1) error {
	wantPrefix, ok := committedPrefixThroughIndex(committed, manifest.LastIncludedIndex)
	if !ok {
		return fmt.Errorf("%w: snapshot index %d is not committed", ErrCommittedLogGap, manifest.LastIncludedIndex)
	}
	if len(prefix) != len(wantPrefix) {
		return fmt.Errorf("%w: snapshot prefix length %d does not match committed prefix length %d through index %d", ErrCommittedLogGap, len(prefix), len(wantPrefix), manifest.LastIncludedIndex)
	}
	for i, entry := range prefix {
		if !committedEntriesEqual(wantPrefix[i], entry) {
			return fmt.Errorf("%w: snapshot prefix diverged at entry offset %d", ErrCommittedLogConflict, i)
		}
	}
	last := wantPrefix[len(wantPrefix)-1]
	if last.Term != manifest.LastIncludedTerm || last.Index != manifest.LastIncludedIndex {
		return fmt.Errorf("%w: snapshot last included %d/%d does not match committed log %d/%d", ErrCommittedLogConflict, manifest.LastIncludedTerm, manifest.LastIncludedIndex, last.Term, last.Index)
	}
	return nil
}

func (h *Harness) catchUpNodeThrough(id raftcluster.NodeID, maxIndex uint64) ([]raftentry.ApplyResultV1, error) {
	if h == nil {
		return nil, ErrHarnessClosed
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		h.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	var startIndex uint64
	includeStart := false
	var prefix []raftfsm.CommittedEntryV1
	if last, ok := node.LastApplied(); ok {
		if last.Index == 0 {
			h.mu.Unlock()
			return nil, fmt.Errorf("%w: node %s has invalid last applied index 0", ErrCommittedLogConflict, id)
		}
		var ok bool
		prefix, ok = committedPrefixThroughIndex(h.committed, last.Index)
		if !ok {
			h.mu.Unlock()
			return nil, fmt.Errorf("%w: node %s last applied index %d is not committed", ErrCommittedLogConflict, id, last.Index)
		}
		lastCommitted := prefix[len(prefix)-1]
		if lastCommitted.Term != last.Term {
			h.mu.Unlock()
			return nil, fmt.Errorf("%w: node %s last applied %d/%d does not match committed log term %d", ErrCommittedLogConflict, id, last.Term, last.Index, lastCommitted.Term)
		}
		startIndex = last.Index
		includeStart = true
	}
	entries := committedEntriesFromIndexThrough(h.committed, startIndex, maxIndex, includeStart)
	h.mu.Unlock()
	if len(prefix) > 0 {
		result, err := node.fsm.ValidateAppliedPrefixV1(prefix)
		if err != nil {
			return []raftentry.ApplyResultV1{result}, err
		}
	}
	return node.ApplyCommittedEntriesV1(entries...)
}

func verifySnapshotPrefix(rootDir string, groupID raftcluster.GroupID, targetID raftcluster.NodeID, nodeConfigs []NodeConfig, storeOptions raftapply.DurableApplyStoreOptions, manifest raftcluster.SnapshotManifestV1, prefix []raftfsm.CommittedEntryV1) error {
	probeRoot, err := os.MkdirTemp(rootDir, "snapshot-install-probe-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(probeRoot) }()
	dbDir := filepath.Join(probeRoot, "nodes", string(targetID), "db")
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dbDir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB: db,
		Cluster: raftcluster.Config{
			Dir:     dbDir,
			NodeID:  targetID,
			GroupID: groupID,
			Peers:   peersForNodeConfigs(nodeConfigs),
		},
		StoreOptions: storeOptions,
	})
	if err != nil {
		return err
	}
	defer func() { _ = fsm.Close() }()
	if _, err := fsm.ApplyCommittedEntriesV1(prefix); err != nil {
		return err
	}
	return fsm.VerifyInstalledSnapshotManifestV1(manifest)
}

func (h *Harness) openNode(cfg NodeConfig) (*Node, error) {
	dbDir := filepath.Join(h.rootDir, "nodes", string(cfg.ID), "db")
	db, err := backenddb.Open(backenddb.Options{
		Dir:                          dbDir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		return nil, err
	}
	cluster := raftcluster.Config{
		Dir:     dbDir,
		NodeID:  cfg.ID,
		GroupID: h.groupID,
		Peers:   peersForNodeConfigs(h.nodeConfigs),
	}
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:           db,
		Cluster:      cluster,
		StoreOptions: h.storeOptions,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Node{id: cfg.ID, db: db, fsm: fsm}, nil
}

func appendCommitted(committed []raftfsm.CommittedEntryV1, entry raftfsm.CommittedEntryV1) ([]raftfsm.CommittedEntryV1, error) {
	id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	if id.Term == 0 || id.Index == 0 {
		return nil, fmt.Errorf("%w: invalid committed id %+v", ErrCommittedLogGap, id)
	}
	if existing, _, ok := committedEntryByIndex(committed, id.Index); ok {
		if !committedEntriesEqual(existing, entry) {
			return nil, fmt.Errorf("%w: divergent entry at index %d", ErrCommittedLogConflict, id.Index)
		}
		return committed, nil
	}
	if len(committed) > 0 {
		previous := committed[len(committed)-1]
		if id.Index <= previous.Index {
			return nil, fmt.Errorf("%w: non-increasing committed index %d after %d", ErrCommittedLogConflict, id.Index, previous.Index)
		}
		if id.Term < previous.Term {
			return nil, fmt.Errorf("%w: term regression at index %d: term %d after term %d", ErrCommittedLogConflict, id.Index, id.Term, previous.Term)
		}
	}
	return append(committed, cloneCommittedEntry(entry)), nil
}

func committedEntryByIndex(committed []raftfsm.CommittedEntryV1, index uint64) (raftfsm.CommittedEntryV1, int, bool) {
	if index == 0 {
		return raftfsm.CommittedEntryV1{}, -1, false
	}
	for i, entry := range committed {
		switch {
		case entry.Index == index:
			return entry, i, true
		case entry.Index > index:
			return raftfsm.CommittedEntryV1{}, -1, false
		}
	}
	return raftfsm.CommittedEntryV1{}, -1, false
}

func committedPrefixThroughIndex(committed []raftfsm.CommittedEntryV1, index uint64) ([]raftfsm.CommittedEntryV1, bool) {
	_, pos, ok := committedEntryByIndex(committed, index)
	if !ok {
		return nil, false
	}
	prefix := make([]raftfsm.CommittedEntryV1, 0, pos+1)
	for _, entry := range committed[:pos+1] {
		prefix = append(prefix, cloneCommittedEntry(entry))
	}
	return prefix, true
}

func committedEntriesFromIndexThrough(committed []raftfsm.CommittedEntryV1, startIndex, maxIndex uint64, includeStart bool) []raftfsm.CommittedEntryV1 {
	entries := make([]raftfsm.CommittedEntryV1, 0, len(committed))
	for _, entry := range committed {
		if maxIndex != 0 && entry.Index > maxIndex {
			break
		}
		if startIndex != 0 {
			if entry.Index < startIndex || (!includeStart && entry.Index == startIndex) {
				continue
			}
		}
		entries = append(entries, cloneCommittedEntry(entry))
	}
	return entries
}

func (n *Node) ID() raftcluster.NodeID {
	if n == nil {
		return ""
	}
	return n.id
}

func (n *Node) DB() *backenddb.DB {
	if n == nil {
		return nil
	}
	return n.db
}

func (n *Node) LastApplied() (raftentry.ApplyEntryID, bool) {
	if n == nil || n.fsm == nil {
		return raftentry.ApplyEntryID{}, false
	}
	return n.fsm.LastApplied()
}

func (n *Node) LogicalDigestV1(opts raftapply.LogicalDigestOptionsV1) (raftapply.LogicalDigestV1, error) {
	if n == nil || n.fsm == nil {
		return raftapply.LogicalDigestV1{}, fmt.Errorf("%w: nil node", ErrNodeNotFound)
	}
	return n.fsm.LogicalDigestV1(opts)
}

func (n *Node) ApplyCommittedEntriesV1(entries ...raftfsm.CommittedEntryV1) ([]raftentry.ApplyResultV1, error) {
	if n == nil || n.fsm == nil || n.closed {
		return nil, fmt.Errorf("%w: closed node", ErrNodeNotFound)
	}
	return n.fsm.ApplyCommittedEntriesV1(entries)
}

func (n *Node) close() error {
	if n == nil || n.closed {
		return nil
	}
	n.closed = true
	return errors.Join(n.fsm.Close(), n.db.Close())
}

func normalizeNodeConfigs(configs []NodeConfig) ([]NodeConfig, error) {
	if len(configs) == 0 {
		configs = []NodeConfig{
			{ID: "node-a"},
			{ID: "node-b"},
			{ID: "node-c"},
		}
	}
	out := make([]NodeConfig, 0, len(configs))
	seen := make(map[raftcluster.NodeID]struct{}, len(configs))
	for _, cfg := range configs {
		if err := validateNodeIDPathSegment(cfg.ID); err != nil {
			return nil, err
		}
		if _, ok := seen[cfg.ID]; ok {
			return nil, fmt.Errorf("raftharness: duplicate node id %s", cfg.ID)
		}
		seen[cfg.ID] = struct{}{}
		if cfg.Address == "" {
			cfg.Address = "inprocess://" + string(cfg.ID)
		}
		out = append(out, cfg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func validateNodeIDPathSegment(id raftcluster.NodeID) error {
	raw := string(id)
	if raw == "" {
		return fmt.Errorf("raftharness: missing node id")
	}
	if strings.TrimSpace(raw) != raw {
		return fmt.Errorf("raftharness: invalid node id %q: leading or trailing whitespace", raw)
	}
	if raw == "." || raw == ".." {
		return fmt.Errorf("raftharness: invalid node id %q: not a valid path segment", raw)
	}
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return fmt.Errorf("raftharness: invalid node id %q: unsupported character %q", raw, r)
		}
	}
	return nil
}

func peersForNodeConfigs(configs []NodeConfig) []raftcluster.Peer {
	peers := make([]raftcluster.Peer, 0, len(configs))
	for _, cfg := range configs {
		peers = append(peers, raftcluster.Peer{ID: cfg.ID, Address: cfg.Address})
	}
	return peers
}

func applyEntryIDs(entries []raftfsm.CommittedEntryV1) []raftentry.ApplyEntryID {
	ids := make([]raftentry.ApplyEntryID, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index})
	}
	return ids
}

func cloneCommittedEntry(entry raftfsm.CommittedEntryV1) raftfsm.CommittedEntryV1 {
	entry.Bytes = bytes.Clone(entry.Bytes)
	entry.RequestMetadata.TraceContext = bytes.Clone(entry.RequestMetadata.TraceContext)
	entry.RequestMetadata.ClusterRouteMembers = append([]string(nil), entry.RequestMetadata.ClusterRouteMembers...)
	if entry.ExpectedTarget != nil {
		target := entry.ExpectedTarget.Clone()
		entry.ExpectedTarget = &target
	}
	return entry
}

func committedEntriesEqual(a, b raftfsm.CommittedEntryV1) bool {
	if a.Type != b.Type ||
		a.Term != b.Term ||
		a.Index != b.Index ||
		a.CurrentCatalogVersion != b.CurrentCatalogVersion ||
		a.HasCurrentCatalogVersion != b.HasCurrentCatalogVersion ||
		a.SyncLocalCommandWAL != b.SyncLocalCommandWAL ||
		a.RequestMetadata.RequestID != b.RequestMetadata.RequestID ||
		a.RequestMetadata.AckPolicy != b.RequestMetadata.AckPolicy ||
		a.RequestMetadata.DeadlineUnixNanos != b.RequestMetadata.DeadlineUnixNanos ||
		a.RequestMetadata.Compression != b.RequestMetadata.Compression ||
		a.RequestMetadata.OmitResultIDs != b.RequestMetadata.OmitResultIDs ||
		a.RequestMetadata.OmitResponseMeta != b.RequestMetadata.OmitResponseMeta ||
		a.RequestMetadata.ClusterRouteKnown != b.RequestMetadata.ClusterRouteKnown ||
		a.RequestMetadata.ClusterRouteDatabase != b.RequestMetadata.ClusterRouteDatabase ||
		a.RequestMetadata.ClusterRouteCatalog != b.RequestMetadata.ClusterRouteCatalog ||
		a.RequestMetadata.ClusterRouteCollection != b.RequestMetadata.ClusterRouteCollection ||
		a.RequestMetadata.ClusterRouteShape != b.RequestMetadata.ClusterRouteShape ||
		a.RequestMetadata.ClusterRouteGroupID != b.RequestMetadata.ClusterRouteGroupID ||
		a.RequestMetadata.ClusterRouteLeaderHint != b.RequestMetadata.ClusterRouteLeaderHint ||
		a.RequestMetadata.ClusterRoutePlacementMode != b.RequestMetadata.ClusterRoutePlacementMode ||
		a.RequestMetadata.ClusterRouteKey != b.RequestMetadata.ClusterRouteKey ||
		a.RequestMetadata.ClusterRouteTokenKnown != b.RequestMetadata.ClusterRouteTokenKnown ||
		a.RequestMetadata.ClusterRouteToken != b.RequestMetadata.ClusterRouteToken ||
		a.RequestMetadata.ClusterRoutePartitionID != b.RequestMetadata.ClusterRoutePartitionID ||
		!equalStringSlices(a.RequestMetadata.ClusterRouteMembers, b.RequestMetadata.ClusterRouteMembers) ||
		!bytes.Equal(a.RequestMetadata.TraceContext, b.RequestMetadata.TraceContext) ||
		!bytes.Equal(a.Bytes, b.Bytes) {
		return false
	}
	switch {
	case a.ExpectedTarget == nil && b.ExpectedTarget == nil:
		return true
	case a.ExpectedTarget == nil || b.ExpectedTarget == nil:
		return false
	default:
		return a.ExpectedTarget.Equal(*b.ExpectedTarget)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
