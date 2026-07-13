package rootpublication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrNamespacePersistenceUnsupported is returned when an adapter cannot make
// creation or rename durable at a namespace boundary. It must never be
// converted into a successful file-only token.
var ErrNamespacePersistenceUnsupported = errors.New("root publication: namespace persistence unsupported")

// StableResourceKind names a concrete resource family. New authoritative
// resource kinds must also be added to STABLE_RESOURCE_INVENTORY.md.
type StableResourceKind string

const (
	StableResourceValueLog    StableResourceKind = "value_log"
	StableResourceOuterLeaf   StableResourceKind = "outer_leaf"
	StableResourceDictionary  StableResourceKind = "dictionary"
	StableResourceTemplate    StableResourceKind = "template"
	StableResourceColumn      StableResourceKind = "column"
	StableResourceTypedColumn StableResourceKind = "typed_column"
	StableResourceVector      StableResourceKind = "vector"
	StableResourceText        StableResourceKind = "text"
	StableResourceCommandWAL  StableResourceKind = "command_wal"
	StableResourceQueryReady  StableResourceKind = "query_ready"
)

// StableNamespaceToken binds a token to the namespace operation that made the
// resource reachable. Identity, not DiagnosticPath, is the durable key.
type StableNamespaceToken struct {
	ParentIdentity string
	Identity       string
	Generation     uint64
	Establish      func(context.Context) error
}

func (n StableNamespaceToken) validate() error {
	if n.Identity == "" || n.ParentIdentity == "" {
		return fmt.Errorf("%w: missing parent or namespace identity", ErrNamespacePersistenceUnsupported)
	}
	if n.Establish == nil {
		return fmt.Errorf("%w: no namespace persistence primitive for %q", ErrNamespacePersistenceUnsupported, n.Identity)
	}
	return nil
}

func (n StableNamespaceToken) establish(ctx context.Context) error {
	if err := n.validate(); err != nil {
		return err
	}
	if err := n.Establish(ctx); err != nil {
		return fmt.Errorf("root publication: establish namespace %q: %w", n.Identity, err)
	}
	return nil
}

// StableResourceLease is a single idempotent ownership hand-off. A producer
// retains it until a durable consumer releases it or transfers it onward.
type StableResourceLease struct {
	once    sync.Once
	release func()
}

func joinStableResourceLeases(left, right *StableResourceLease) *StableResourceLease {
	if left == right {
		return left
	}
	return NewStableResourceLease(func() {
		left.Release()
		right.Release()
	})
}

func NewStableResourceLease(release func()) *StableResourceLease {
	return &StableResourceLease{release: release}
}

func (l *StableResourceLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.release != nil {
			l.release()
		}
	})
}

// StableResourceToken is candidate-scoped, immutable resource debt. Callback
// closures are captured at registration and must operate on the pinned handle
// or immutable generation, never reopen DiagnosticPath.
type StableResourceToken struct {
	Kind           StableResourceKind
	Namespace      string
	Identity       string
	Generation     uint64
	DiagnosticPath string
	Frontier       uint64
	Digest         string
	ReachableBy    string
	// PinnedOperationID identifies the captured handle/generation operation.
	// Duplicate tokens with different operations are rejected rather than
	// accidentally selecting one callback and leaking the other producer pin.
	PinnedOperationID string
	MutableAppend     bool
	NamespaceToken    StableNamespaceToken
	FlushThrough      func(context.Context, uint64) error
	SyncThrough       func(context.Context, uint64) error
	Lease             *StableResourceLease
}

func (t StableResourceToken) key() string {
	return string(t.Kind) + "\x00" + t.Namespace + "\x00" + t.Identity + "\x00" + fmt.Sprintf("%020d", t.Generation)
}

func (t StableResourceToken) validate() error {
	if t.Kind == "" || t.Namespace == "" || t.Identity == "" || t.Frontier == 0 || t.ReachableBy == "" || t.PinnedOperationID == "" {
		return fmt.Errorf("%w: incomplete stable resource token kind=%q identity=%q", ErrInvalidCandidate, t.Kind, t.Identity)
	}
	if err := t.NamespaceToken.validate(); err != nil {
		return err
	}
	if t.FlushThrough == nil || t.SyncThrough == nil {
		return fmt.Errorf("%w: resource %q has no pinned flush/sync operation", ErrInvalidCandidate, t.Identity)
	}
	if t.Lease == nil {
		return fmt.Errorf("%w: resource %q has no ownership lease", ErrInvalidCandidate, t.Identity)
	}
	if t.MutableAppend {
		return nil
	}
	if t.Digest == "" {
		return fmt.Errorf("%w: immutable resource %q is missing digest", ErrInvalidCandidate, t.Identity)
	}
	return nil
}

// StableResourceSet is an immutable deterministic token union.
type StableResourceSet struct{ tokens []StableResourceToken }

func NewStableResourceSet(tokens []StableResourceToken) (StableResourceSet, error) {
	copyTokens := append([]StableResourceToken(nil), tokens...)
	sort.Slice(copyTokens, func(i, j int) bool { return copyTokens[i].key() < copyTokens[j].key() })
	out := copyTokens[:0]
	for _, token := range copyTokens {
		if err := token.validate(); err != nil {
			return StableResourceSet{}, err
		}
		if len(out) == 0 || out[len(out)-1].key() != token.key() {
			out = append(out, token)
			continue
		}
		merged, err := mergeStableResourceToken(out[len(out)-1], token)
		if err != nil {
			return StableResourceSet{}, err
		}
		out[len(out)-1] = merged
	}
	return StableResourceSet{tokens: append([]StableResourceToken(nil), out...)}, nil
}

func mergeStableResourceToken(left, right StableResourceToken) (StableResourceToken, error) {
	if left.Digest != right.Digest ||
		left.ReachableBy != right.ReachableBy ||
		left.MutableAppend != right.MutableAppend ||
		left.PinnedOperationID != right.PinnedOperationID ||
		left.NamespaceToken.Identity != right.NamespaceToken.Identity ||
		left.NamespaceToken.ParentIdentity != right.NamespaceToken.ParentIdentity ||
		left.NamespaceToken.Generation != right.NamespaceToken.Generation {
		return StableResourceToken{}, fmt.Errorf("%w: conflicting stable resource identity %q", ErrInvalidCandidate, left.Identity)
	}
	left.Lease = joinStableResourceLeases(left.Lease, right.Lease)
	if right.Frontier > left.Frontier {
		left.Frontier, left.FlushThrough, left.SyncThrough = right.Frontier, right.FlushThrough, right.SyncThrough
	}
	return left, nil
}

func (s StableResourceSet) Tokens() []StableResourceToken {
	return append([]StableResourceToken(nil), s.tokens...)
}
func (s StableResourceSet) Len() int { return len(s.tokens) }

func (s StableResourceSet) union(other StableResourceSet) (StableResourceSet, error) {
	return NewStableResourceSet(append(s.Tokens(), other.Tokens()...))
}

// FlushAndSync flushes and syncs data through every captured frontier before
// establishing creation/rename namespace durability. Callbacks are pinned at
// registration and never reopen DiagnosticPath.
func (s StableResourceSet) FlushAndSync(ctx context.Context) error {
	for _, token := range s.tokens {
		if err := token.FlushThrough(ctx, token.Frontier); err != nil {
			return fmt.Errorf("root publication: flush %s/%s: %w", token.Kind, token.Identity, err)
		}
		if err := token.SyncThrough(ctx, token.Frontier); err != nil {
			return fmt.Errorf("root publication: sync %s/%s: %w", token.Kind, token.Identity, err)
		}
		if err := token.NamespaceToken.establish(ctx); err != nil {
			return err
		}
	}
	return nil
}

// FlushAndSyncWithMetrics is the activation-facing variant. Keeping the
// recorder explicit prevents hidden process-global registration state.
func (s StableResourceSet) FlushAndSyncWithMetrics(ctx context.Context, metrics *StableResourceMetricsRecorder) error {
	for _, token := range s.tokens {
		flushStarted := time.Now()
		if err := token.FlushThrough(ctx, token.Frontier); err != nil {
			return fmt.Errorf("root publication: flush %s/%s: %w", token.Kind, token.Identity, err)
		}
		if metrics != nil {
			metrics.record(token.Kind, stableResourceFlush, time.Since(flushStarted))
		}
		syncStarted := time.Now()
		if err := token.SyncThrough(ctx, token.Frontier); err != nil {
			return fmt.Errorf("root publication: sync %s/%s: %w", token.Kind, token.Identity, err)
		}
		if metrics != nil {
			metrics.record(token.Kind, stableResourceSync, time.Since(syncStarted))
		}
		namespaceStarted := time.Now()
		if err := token.NamespaceToken.establish(ctx); err != nil {
			if metrics != nil {
				metrics.reject()
			}
			return err
		}
		if metrics != nil {
			metrics.record(token.Kind, stableResourceNamespace, time.Since(namespaceStarted))
		}
	}
	return nil
}

func (s StableResourceSet) Release() {
	for _, token := range s.tokens {
		token.Lease.Release()
	}
}

// StableResourceDebt owns a candidate's leases across outcome transitions.
// Retry and partial durable failure retain pins; all terminal paths release
// exactly once. #3679/#3718 choose the production transition point.
type StableResourceDebt struct {
	set  StableResourceSet
	once sync.Once
}

func NewStableResourceDebt(set StableResourceSet) *StableResourceDebt {
	return &StableResourceDebt{set: set}
}
func (d *StableResourceDebt) Retry()      {}
func (d *StableResourceDebt) Success()    { d.release() }
func (d *StableResourceDebt) Superseded() { d.release() }
func (d *StableResourceDebt) Abandon()    { d.release() }
func (d *StableResourceDebt) Poison()     { d.release() }
func (d *StableResourceDebt) release() {
	if d != nil {
		d.once.Do(d.set.Release)
	}
}

type stableResourceExtension struct {
	set StableResourceSet
	err error
}

func (e stableResourceExtension) union(other immutableExtension) immutableExtension {
	right, ok := other.(stableResourceExtension)
	if !ok {
		return stableResourceExtension{err: fmt.Errorf("%w: resource slot has incompatible extension", ErrInvalidCandidate)}
	}
	if e.err != nil {
		return e
	}
	if right.err != nil {
		return right
	}
	set, err := e.set.union(right.set)
	return stableResourceExtension{set: set, err: err}
}

// NewPreparedRootCandidateWithStableResources is the #3677 construction seam.
// Nested producers must pass their complete transitive child union here before
// installing a root or catalog identifier into the candidate.
func NewPreparedRootCandidateWithStableResources(spec CandidateSpec, tokens []StableResourceToken) (*PreparedRootCandidate, error) {
	set, err := NewStableResourceSet(tokens)
	if err != nil {
		return nil, err
	}
	return newPreparedRootCandidateWithExtensions(spec, extensionSlots{resourceSet: stableResourceExtension{set: set}})
}

func (c *PreparedRootCandidate) StableResourceSet() (StableResourceSet, error) {
	if c == nil || c.extensions.resourceSet == nil {
		return StableResourceSet{}, nil
	}
	extension, ok := c.extensions.resourceSet.(stableResourceExtension)
	if !ok {
		return StableResourceSet{}, fmt.Errorf("%w: resource slot has incompatible extension", ErrInvalidCandidate)
	}
	if extension.err != nil {
		return StableResourceSet{}, extension.err
	}
	return extension.set, nil
}

func (c *PreparedRootCandidate) StableResourceTokens() ([]StableResourceToken, error) {
	set, err := c.StableResourceSet()
	return set.Tokens(), err
}

// StableResourceMetrics is deliberately independent of coordinator Stats so
// #3679 can wire it into production without changing this dormant scheduler.
type StableResourceMetrics struct {
	PendingTokensHighWater uint64
	PendingBytesHighWater  uint64
	DescriptorHighWater    uint64
	PinHighWater           uint64
	Flushes                uint64
	Syncs                  uint64
	NamespaceSyncs         uint64
	Coalesced              uint64
	Conflicts              uint64
	Rejected               uint64
	OldestPendingAge       time.Duration
	ByKind                 map[StableResourceKind]StableResourceKindMetrics
}

type StableResourceKindMetrics struct {
	Flushes, Syncs, NamespaceSyncs                 uint64
	FlushDuration, SyncDuration, NamespaceDuration time.Duration
}

type stableResourceMetricEvent uint8

const (
	stableResourceFlush stableResourceMetricEvent = iota
	stableResourceSync
	stableResourceNamespace
)

// StableResourceMetricsRecorder is candidate-local instrumentation for the
// later production publisher. It deliberately has no global registry.
type StableResourceMetricsRecorder struct {
	mu                                            sync.Mutex
	pendingAt                                     time.Time
	pendingTokens, pendingBytes                   uint64
	pendingTokensHighWater, pendingBytesHighWater uint64
	descriptorHighWater, pinHighWater             uint64
	coalesced, conflicts, rejected                uint64
	byKind                                        map[StableResourceKind]StableResourceKindMetrics
}

func (r *StableResourceMetricsRecorder) ObservePending(tokens, bytes, descriptors, pins uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if tokens > 0 && r.pendingTokens == 0 {
		r.pendingAt = time.Now()
	}
	if tokens == 0 {
		r.pendingAt = time.Time{}
	}
	r.pendingTokens, r.pendingBytes = tokens, bytes
	if tokens > r.pendingTokensHighWater {
		r.pendingTokensHighWater = tokens
	}
	if bytes > r.pendingBytesHighWater {
		r.pendingBytesHighWater = bytes
	}
	if descriptors > r.descriptorHighWater {
		r.descriptorHighWater = descriptors
	}
	if pins > r.pinHighWater {
		r.pinHighWater = pins
	}
}

func (r *StableResourceMetricsRecorder) ObserveCoalesced(count uint64) {
	if r != nil {
		r.mu.Lock()
		r.coalesced += count
		r.mu.Unlock()
	}
}

func (r *StableResourceMetricsRecorder) ObserveConflict() {
	if r != nil {
		r.mu.Lock()
		r.conflicts++
		r.mu.Unlock()
	}
}

func (r *StableResourceMetricsRecorder) ObserveRejected() { r.reject() }

func (r *StableResourceMetricsRecorder) record(kind StableResourceKind, event stableResourceMetricEvent, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byKind == nil {
		r.byKind = make(map[StableResourceKind]StableResourceKindMetrics)
	}
	m := r.byKind[kind]
	switch event {
	case stableResourceFlush:
		m.Flushes++
		m.FlushDuration += d
	case stableResourceSync:
		m.Syncs++
		m.SyncDuration += d
	case stableResourceNamespace:
		m.NamespaceSyncs++
		m.NamespaceDuration += d
	}
	r.byKind[kind] = m
}
func (r *StableResourceMetricsRecorder) reject() { r.mu.Lock(); r.rejected++; r.mu.Unlock() }

func (r *StableResourceMetricsRecorder) Snapshot() StableResourceMetrics {
	if r == nil {
		return StableResourceMetrics{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := StableResourceMetrics{PendingTokensHighWater: r.pendingTokensHighWater, PendingBytesHighWater: r.pendingBytesHighWater, DescriptorHighWater: r.descriptorHighWater, PinHighWater: r.pinHighWater, Coalesced: r.coalesced, Conflicts: r.conflicts, Rejected: r.rejected, ByKind: make(map[StableResourceKind]StableResourceKindMetrics, len(r.byKind))}
	for kind, metrics := range r.byKind {
		result.ByKind[kind] = metrics
		result.Flushes += metrics.Flushes
		result.Syncs += metrics.Syncs
		result.NamespaceSyncs += metrics.NamespaceSyncs
	}
	if !r.pendingAt.IsZero() {
		result.OldestPendingAge = time.Since(r.pendingAt)
	}
	return result
}
