// Package rootpublication provides the dormant scheduling primitive for
// coalescing prepared TreeDB roots before stable publication.
//
// The package now owns exact stable resource and namespace dependencies plus
// their candidate-to-coordinator transfer contract. It deliberately has no
// connection to TreeDB's current commit, pager, meta, recovery, maintenance,
// or public-profile paths; later durability tickets activate publication and
// deletion consumers.
package rootpublication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

const (
	SoftPendingBytes   = uint64(64 << 20)
	HardPendingBytes   = uint64(256 << 20)
	HardPendingCommits = uint64(65_536)

	minPublishDelay = 10 * time.Millisecond
	maxPublishDelay = 100 * time.Millisecond
)

var (
	ErrClosed             = errors.New("root publication coordinator closed")
	ErrRecoveryRequired   = errors.New("root publication recovery required")
	ErrInvalidCandidate   = errors.New("invalid root publication candidate")
	ErrPublisherProtocol  = errors.New("root publication publisher protocol error")
	ErrPublicationStopped = errors.New("root publication stopped before debt drained")
)

// Frontier is the scalar, immutable visible/durable boundary associated with a
// prepared root. Root page identities may change freely; the sequence and
// dependency frontiers must only advance.
type Frontier struct {
	commitSeq         uint64
	userRootPageID    uint64
	systemRootPageID  uint64
	appliedCommandLSN uint64
	maxEntryRevision  uint64
}

func NewFrontier(commitSeq, userRootPageID, systemRootPageID, appliedCommandLSN, maxEntryRevision uint64) Frontier {
	return Frontier{
		commitSeq: commitSeq, userRootPageID: userRootPageID,
		systemRootPageID: systemRootPageID, appliedCommandLSN: appliedCommandLSN,
		maxEntryRevision: maxEntryRevision,
	}
}

func (f Frontier) CommitSeq() uint64         { return f.commitSeq }
func (f Frontier) UserRootPageID() uint64    { return f.userRootPageID }
func (f Frontier) SystemRootPageID() uint64  { return f.systemRootPageID }
func (f Frontier) AppliedCommandLSN() uint64 { return f.appliedCommandLSN }
func (f Frontier) MaxEntryRevision() uint64  { return f.maxEntryRevision }

func (f Frontier) Dominates(older Frontier) bool {
	return f.commitSeq > older.commitSeq &&
		f.appliedCommandLSN >= older.appliedCommandLSN &&
		f.maxEntryRevision >= older.maxEntryRevision
}

// CandidateSpec is copied into a PreparedRootCandidate. DependencyBytes and
// IndexBytes represent owned unpublished debt, not a path or resource identity.
type CandidateSpec struct {
	Frontier        Frontier
	FreelistHeadID  uint64
	TotalPages      uint64
	DependencyBytes uint64
	IndexBytes      uint64
	Obligations     []ObligationID
	ResourceSet     *StableResourceSet
}

// ObligationID is an opaque, path-free identity for dependency ownership. Its
// concrete resource interpretation belongs to #3677. This foundation only
// needs a comparable value so supersession can retain a deterministic union.
type ObligationID [16]byte

// extensionSlots reserve package-private typed ownership points for #3677,
// #3678, and #3679. Keeping them private prevents this foundation from
// inventing a path-based identity or prematurely exposing a public contract.
type immutableExtension interface {
	union(immutableExtension) (immutableExtension, error)
}

type extensionSlots struct {
	resourceSet       immutableExtension
	cowFreelist       immutableExtension
	durableRootRecord immutableExtension
}

// PreparedRootCandidate is immutable after construction. All fields are
// private and coalescing creates a new value rather than mutating an enqueued
// candidate.
type PreparedRootCandidate struct {
	frontier        Frontier
	freelistHeadID  uint64
	totalPages      uint64
	dependencyBytes uint64
	indexBytes      uint64
	obligations     []ObligationID
	extensions      extensionSlots
}

func NewPreparedRootCandidate(spec CandidateSpec) (*PreparedRootCandidate, error) {
	return newPreparedRootCandidateWithExtensions(spec, extensionSlots{})
}

func newPreparedRootCandidateWithExtensions(spec CandidateSpec, extensions extensionSlots) (*PreparedRootCandidate, error) {
	if spec.Frontier.commitSeq == 0 {
		return nil, fmt.Errorf("%w: commit sequence is zero", ErrInvalidCandidate)
	}
	if spec.ResourceSet != nil {
		if extensions.resourceSet != nil {
			return nil, fmt.Errorf("%w: duplicate resource-set extension", ErrInvalidCandidate)
		}
		if err := spec.ResourceSet.validateResolved(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCandidate, err)
		}
		if err := spec.ResourceSet.transfer(ResourceOwnerBuilder, ResourceOwnerCandidate); err != nil {
			return nil, fmt.Errorf("%w: transfer resource set: %w", ErrInvalidCandidate, err)
		}
		extensions.resourceSet = spec.ResourceSet
	}
	return &PreparedRootCandidate{
		frontier: spec.Frontier, freelistHeadID: spec.FreelistHeadID,
		totalPages: spec.TotalPages, dependencyBytes: spec.DependencyBytes,
		indexBytes: spec.IndexBytes, obligations: normalizeObligations(spec.Obligations), extensions: extensions,
	}, nil
}

func (c *PreparedRootCandidate) Frontier() Frontier      { return c.frontier }
func (c *PreparedRootCandidate) FreelistHeadID() uint64  { return c.freelistHeadID }
func (c *PreparedRootCandidate) TotalPages() uint64      { return c.totalPages }
func (c *PreparedRootCandidate) DependencyBytes() uint64 { return c.dependencyBytes }
func (c *PreparedRootCandidate) IndexBytes() uint64      { return c.indexBytes }
func (c *PreparedRootCandidate) Obligations() []ObligationID {
	return append([]ObligationID(nil), c.obligations...)
}
func (c *PreparedRootCandidate) OwnedBytes() uint64 {
	return saturatingAdd(c.dependencyBytes, c.indexBytes)
}

func (c *PreparedRootCandidate) resourceSet() *StableResourceSet {
	if c == nil || c.extensions.resourceSet == nil {
		return nil
	}
	set, _ := c.extensions.resourceSet.(*StableResourceSet)
	return set
}

// Resources exposes the immutable candidate-scoped token union to the
// publisher. The returned set does not permit mutation or ownership transfer.
func (c *PreparedRootCandidate) Resources() *StableResourceSet { return c.resourceSet() }

// AbandonResources releases a prepared candidate that failed before Enqueue.
// It is idempotent and cannot release coordinator-owned resources.
func (c *PreparedRootCandidate) AbandonResources() {
	if set := c.resourceSet(); set != nil {
		set.releaseFrom(ResourceOwnerCandidate)
	}
}

func coalesceCandidates(candidates []*PreparedRootCandidate) (*PreparedRootCandidate, error) {
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	latest := candidates[len(candidates)-1]
	coalesced := *latest
	coalesced.dependencyBytes = 0
	coalesced.indexBytes = 0
	coalesced.obligations = nil
	coalesced.extensions = extensionSlots{}
	for _, candidate := range candidates {
		coalesced.dependencyBytes = saturatingAdd(coalesced.dependencyBytes, candidate.dependencyBytes)
		coalesced.indexBytes = saturatingAdd(coalesced.indexBytes, candidate.indexBytes)
		coalesced.obligations = append(coalesced.obligations, candidate.obligations...)
		var err error
		coalesced.extensions, err = unionExtensionSlots(coalesced.extensions, candidate.extensions)
		if err != nil {
			return nil, err
		}
	}
	coalesced.obligations = normalizeObligations(coalesced.obligations)
	return &coalesced, nil
}

func unionExtensionSlots(older, newer extensionSlots) (extensionSlots, error) {
	resourceSet, err := unionExtensionValue(older.resourceSet, newer.resourceSet)
	if err != nil {
		return extensionSlots{}, err
	}
	cowFreelist, err := unionExtensionValue(older.cowFreelist, newer.cowFreelist)
	if err != nil {
		return extensionSlots{}, err
	}
	durableRootRecord, err := unionExtensionValue(older.durableRootRecord, newer.durableRootRecord)
	if err != nil {
		return extensionSlots{}, err
	}
	return extensionSlots{resourceSet: resourceSet, cowFreelist: cowFreelist, durableRootRecord: durableRootRecord}, nil
}

func unionExtensionValue(older, newer immutableExtension) (immutableExtension, error) {
	if older == nil {
		return newer, nil
	}
	if newer == nil {
		return older, nil
	}
	return older.union(newer)
}

func normalizeObligations(obligations []ObligationID) []ObligationID {
	normalized := append([]ObligationID(nil), obligations...)
	sort.Slice(normalized, func(i, j int) bool {
		return bytes.Compare(normalized[i][:], normalized[j][:]) < 0
	})
	if len(normalized) < 2 {
		return normalized
	}
	out := normalized[:1]
	for _, obligation := range normalized[1:] {
		if obligation != out[len(out)-1] {
			out = append(out, obligation)
		}
	}
	return out
}

func saturatingAdd(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

type PublishOutcome uint8

const (
	PublishSucceeded PublishOutcome = iota
	PublishRetryableFailure
	PublishAmbiguous
)

// PublishResult classifies whether the target meta could have changed. A zero
// durable sequence on success means the candidate's exact sequence.
type PublishResult struct {
	Outcome                    PublishOutcome
	DurableCommitSeq           uint64
	OldestRecoverableCommitSeq uint64
	Err                        error
}

// Publisher performs synchronous stable I/O. The coordinator always invokes
// it without holding its state lock or an admission/build token. Implementations
// must observe ctx so Stop can terminate a stalled callback deterministically.
type Publisher interface {
	Publish(ctx context.Context, candidate *PreparedRootCandidate) PublishResult
}

type PublisherFunc func(context.Context, *PreparedRootCandidate) PublishResult

func (f PublisherFunc) Publish(ctx context.Context, c *PreparedRootCandidate) PublishResult {
	return f(ctx, c)
}

type WakeReason string

const (
	WakeNone          WakeReason = "none"
	WakeTimer         WakeReason = "timer"
	WakeWaiter        WakeReason = "durability_waiter"
	WakeSoftBytes     WakeReason = "soft_bytes"
	WakeHardAdmission WakeReason = "hard_admission"
	WakeRetry         WakeReason = "retry"
	WakeDrain         WakeReason = "drain"
)

type Stats struct {
	VisibleCommitSeq           uint64
	DurableCommitSeq           uint64
	OldestRecoverableCommitSeq uint64
	PendingCommits             uint64
	PendingBytes               uint64
	PendingAge                 time.Duration
	LastGroupSize              uint64
	LastServiceDuration        time.Duration
	EWMAServiceDuration        time.Duration
	PublishDelay               time.Duration
	AdmissionWaits             uint64
	ActiveBuilders             uint64
	WaiterCount                uint64
	PreMetaFailures            uint64
	Retries                    uint64
	Poisoned                   bool
	WakeReason                 WakeReason
	PublishCalls               uint64
	ResourceCoalesces          uint64
	ResourceConflicts          uint64
	RejectedCandidates         uint64
	Resources                  []ResourceKindStats
}
