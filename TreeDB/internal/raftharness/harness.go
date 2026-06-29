package raftharness

import (
	"bytes"
	"errors"
	"fmt"
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
)

var (
	ErrNodeNotFound         = errors.New("raftharness: node not found")
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
	node, err := h.nodeForApply(id)
	if err != nil {
		return nil, err
	}
	return node.ApplyCommittedEntriesV1(entries...)
}

func (h *Harness) nodeForApply(id raftcluster.NodeID) (*Node, error) {
	if h == nil {
		return nil, ErrHarnessClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrHarnessClosed
	}
	node, ok := h.nodes[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	return node, nil
}

func (h *Harness) CatchUpNode(id raftcluster.NodeID) ([]raftentry.ApplyResultV1, error) {
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
	start := 0
	if last, ok := node.LastApplied(); ok {
		if last.Index == 0 {
			h.mu.Unlock()
			return nil, fmt.Errorf("%w: node %s has invalid last applied index 0", ErrCommittedLogConflict, id)
		}
		if last.Index > uint64(len(h.committed)) {
			h.mu.Unlock()
			return nil, fmt.Errorf("%w: node %s last applied index %d beyond committed log length %d", ErrCommittedLogConflict, id, last.Index, len(h.committed))
		}
		start = int(last.Index - 1)
	}
	entries := make([]raftfsm.CommittedEntryV1, 0, len(h.committed)-start)
	for _, entry := range h.committed[start:] {
		entries = append(entries, cloneCommittedEntry(entry))
	}
	h.mu.Unlock()
	return node.ApplyCommittedEntriesV1(entries...)
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
	wantNext := uint64(len(committed) + 1)
	if id.Index > wantNext {
		return nil, fmt.Errorf("%w: got index %d after %d", ErrCommittedLogGap, id.Index, len(committed))
	}
	if id.Index < wantNext {
		existing := committed[id.Index-1]
		if !committedEntriesEqual(existing, entry) {
			return nil, fmt.Errorf("%w: divergent entry at index %d", ErrCommittedLogConflict, id.Index)
		}
		return committed, nil
	}
	if len(committed) > 0 {
		previous := committed[len(committed)-1]
		if id.Term < previous.Term {
			return nil, fmt.Errorf("%w: term regression at index %d: term %d after term %d", ErrCommittedLogConflict, id.Index, id.Term, previous.Term)
		}
	}
	return append(committed, cloneCommittedEntry(entry)), nil
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
