package raftcluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	hraft "github.com/hashicorp/raft"
)

// ReplacementSnapshotSeedV1 identifies one native, persisted snapshot selected
// before learner admission. SnapshotID belongs to SourceNodeID's native store;
// losing that retained artifact is an explicit retry failure, never authority
// to substitute a newer snapshot for the same operation.
type ReplacementSnapshotSeedV1 struct {
	SourceNodeID        NodeID                `json:"source_node_id"`
	SnapshotID          string                `json:"snapshot_id"`
	Version             hraft.SnapshotVersion `json:"version"`
	Term                uint64                `json:"term"`
	Index               uint64                `json:"index"`
	ConfigurationIndex  uint64                `json:"configuration_index"`
	ConfigurationSHA256 string                `json:"configuration_sha256"`
	ArchiveSHA256       string                `json:"archive_sha256"`
	SizeBytes           int64                 `json:"size_bytes"`
	Manifest            SnapshotManifestV1    `json:"manifest"`
}

func (s ReplacementSnapshotSeedV1) Validate() error {
	if err := s.Manifest.Validate(s.Manifest.Scope); err != nil {
		return err
	}
	if s.SourceNodeID == "" || s.SourceNodeID != s.Manifest.NodeID || !replacementSnapshotIDValidV1(s.SnapshotID) || s.Version != hraft.SnapshotVersionMax || s.Term != s.Manifest.LastIncludedTerm || s.Index != s.Manifest.LastIncludedIndex || s.ConfigurationIndex == 0 || s.ConfigurationIndex > s.Index || s.SizeBytes <= 0 {
		return ErrInvalidSnapshotManifest
	}
	for _, digest := range []string{s.ConfigurationSHA256, s.ArchiveSHA256} {
		sum, err := hex.DecodeString(digest)
		if err != nil || len(sum) != sha256.Size || hex.EncodeToString(sum) != digest {
			return ErrInvalidSnapshotManifest
		}
	}
	return nil
}

// Native FileSnapshotStore.Open joins this ID to its store path. List reads
// the embedded metadata ID, so membership in List alone is not a basename proof.
func replacementSnapshotIDValidV1(id string) bool {
	return id != "" && len(id) <= 256 && id != "." && id != ".." && !strings.ContainsAny(id, "/\\\x00")
}

func replacementSnapshotConfigurationDigestV1(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// MatchesRequestV1 checks immutable snapshot coordinates before the native
// installer can consume any bytes. Sender term/leader may change on retry;
// authenticated group membership and the operation gate authorize that sender.
func (s ReplacementSnapshotSeedV1) MatchesRequestV1(request *hraft.InstallSnapshotRequest) bool {
	return request != nil && s.Validate() == nil && request.SnapshotVersion == s.Version && request.LastLogTerm == s.Term && request.LastLogIndex == s.Index && request.ConfigurationIndex == s.ConfigurationIndex && request.Size == s.SizeBytes && replacementSnapshotConfigurationDigestV1(request.Configuration) == s.ConfigurationSHA256
}

func replacementSnapshotSeedFromMetaV1(meta *hraft.SnapshotMeta, manifest SnapshotManifestV1, archiveSHA256 string) (ReplacementSnapshotSeedV1, error) {
	if err := validateHashicorpRaftSnapshotManifestV1(meta, manifest); err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	seed := ReplacementSnapshotSeedV1{SourceNodeID: manifest.NodeID, SnapshotID: meta.ID, Version: meta.Version, Term: meta.Term, Index: meta.Index, ConfigurationIndex: meta.ConfigurationIndex, ConfigurationSHA256: replacementSnapshotConfigurationDigestV1(hraft.EncodeConfiguration(meta.Configuration)), SizeBytes: meta.Size, ArchiveSHA256: archiveSHA256, Manifest: manifest}
	return seed, seed.Validate()
}

func (s ReplacementSnapshotSeedV1) matchesMetaV1(meta *hraft.SnapshotMeta) error {
	if meta == nil || meta.ID != s.SnapshotID || meta.Version != s.Version || meta.Term != s.Term || meta.Index != s.Index || meta.Size != s.SizeBytes || meta.ConfigurationIndex != s.ConfigurationIndex || replacementSnapshotConfigurationDigestV1(hraft.EncodeConfiguration(meta.Configuration)) != s.ConfigurationSHA256 {
		return fmt.Errorf("%w: retained replacement snapshot changed", ErrInvalidSnapshotManifest)
	}
	return nil
}

// Snapshot configuration membership is checked separately from configured peer
// identities. A fresh seed must exclude the target entirely, including a
// pending/nonvoting entry; callers also fence the latest committed configuration.
func validateReplacementSeedConfigurationV1(configuration hraft.Configuration, old, target NodeID, address string) error {
	oldVoter := false
	for _, server := range configuration.Servers {
		if NodeID(server.ID) == target || string(server.Address) == address {
			return ErrInvalidConfig
		}
		oldVoter = oldVoter || NodeID(server.ID) == old && server.Suffrage == hraft.Voter
	}
	if !oldVoter {
		return ErrInvalidConfig
	}
	return nil
}

func replacementSnapshotSeedsEqualV1(a, b ReplacementSnapshotSeedV1) bool {
	// time.Time carries optional monotonic state in memory; the established
	// manifest comparator uses the canonical persisted representation.
	return a.SourceNodeID == b.SourceNodeID && a.SnapshotID == b.SnapshotID && a.Version == b.Version && a.Term == b.Term && a.Index == b.Index && a.ConfigurationIndex == b.ConfigurationIndex && a.ConfigurationSHA256 == b.ConfigurationSHA256 && a.SizeBytes == b.SizeBytes && a.ArchiveSHA256 == b.ArchiveSHA256 && snapshotManifestV1Equal(a.Manifest, b.Manifest)
}

// SameReplacementSnapshotSeedV1 compares the full immutable seed binding.
func SameReplacementSnapshotSeedV1(a, b ReplacementSnapshotSeedV1) bool {
	return replacementSnapshotSeedsEqualV1(a, b)
}

// replacementSnapshotCleanupDebtV1 retains opaque native ownership when the
// pinned library cannot prove cleanup. FileSnapshotSink marks itself closed
// before Flush/Sync/Close; another Cancel returning nil is not a cleanup proof.
// This failure requires process restart, not an in-process retry/release.
type replacementSnapshotCleanupDebtV1 struct {
	cause  error
	future hraft.Future
	sink   hraft.SnapshotSink
}

func (e *replacementSnapshotCleanupDebtV1) Error() string {
	return "raftcluster: replacement snapshot cleanup requires process restart: " + e.cause.Error()
}
func (e *replacementSnapshotCleanupDebtV1) Unwrap() error { return e.cause }

// ReplacementSnapshotCleanupRequiredV1 distinguishes unresolved native resource
// ownership from ordinary cancellation after positively completed native work.
func ReplacementSnapshotCleanupRequiredV1(err error) bool {
	var debt *replacementSnapshotCleanupDebtV1
	return errors.As(err, &debt)
}

// RetainReplacementSnapshotSeedV1 copies an already selected native snapshot
// into the caller's one-operation FileSnapshotStore. The caller serializes the
// operation, admits source+copy+temporary disk bytes, and retains its namespace
// and any failed-cleanup debt. The ordinary Raft store may reap old snapshots;
// this separate native store receives no automatic snapshots and preserves the
// selected artifact across process restart. No catalog authority is published
// until this method succeeds.
//
// Native SnapshotStore.Open verifies its CRC synchronously before returning;
// cancellation is checked before/after Open and between streaming reads, but
// cannot interrupt that library-owned CRC pass.
func (p *HashicorpRaftProvider) RetainReplacementSnapshotSeedV1(ctx context.Context, selected HashicorpRaftSnapshotResultV1, destination hraft.SnapshotStore, transport hraft.Transport, old, target NodeID, address string, maxBytes int64) (seed ReplacementSnapshotSeedV1, err error) {
	if p == nil || p.snapshotStore == nil || destination == nil || transport == nil || maxBytes <= 0 || !replacementSnapshotIDValidV1(selected.ID) || selected.SizeBytes <= 0 || selected.SizeBytes > maxBytes {
		return seed, ErrInvalidConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return seed, err
	}
	available, err := p.snapshotStore.List()
	if err != nil {
		return seed, err
	}
	selectedFound := false
	for _, candidate := range available {
		if candidate != nil && candidate.ID == selected.ID {
			if candidate.Size != selected.SizeBytes || candidate.Index != selected.LastIncludedIndex || candidate.Term != selected.LastIncludedTerm {
				return seed, ErrInvalidSnapshotManifest
			}
			selectedFound = true
		}
	}
	if !selectedFound {
		return seed, ErrInvalidSnapshotManifest
	}
	meta, source, err := p.snapshotStore.Open(selected.ID)
	if err != nil {
		return seed, err
	}
	defer source.Close()
	if err := ctx.Err(); err != nil {
		return seed, err
	}
	if meta.Size != selected.SizeBytes || meta.Index != selected.LastIncludedIndex || meta.Term != selected.LastIncludedTerm {
		return seed, ErrInvalidSnapshotManifest
	}
	if err := validateHashicorpRaftSnapshotManifestV1(meta, selected.Manifest); err != nil {
		return seed, err
	}
	if err := validateReplacementSeedConfigurationV1(meta.Configuration, old, target, address); err != nil {
		return seed, err
	}
	metas, err := destination.List()
	if err != nil {
		return seed, err
	}
	// Retry reopens the persisted selected ID through OpenReplacementSnapshotSeedV1.
	// Never replace an existing seed implicitly with a newer captured snapshot.
	if len(metas) != 0 {
		return seed, ErrInvalidConfig
	}
	sink, err := destination.Create(meta.Version, meta.Index, meta.Term, meta.Configuration, meta.ConfigurationIndex, transport)
	if err != nil {
		return seed, err
	}
	closed := false
	defer func() {
		if !closed {
			if cleanupErr := sink.Cancel(); cleanupErr != nil {
				err = &replacementSnapshotCleanupDebtV1{cause: errors.Join(err, cleanupErr), sink: sink}
			}
		}
	}()
	hash := sha256.New()
	reader := replacementSnapshotContextReaderV1{ctx: ctx, reader: source}
	n, err := io.CopyBuffer(io.MultiWriter(sink, hash), io.LimitReader(reader, meta.Size+1), make([]byte, hashicorpRaftSnapshotCopyBuffer))
	if err != nil {
		return seed, err
	}
	if n != meta.Size {
		return seed, ErrInvalidSnapshotManifest
	}
	if err := ctx.Err(); err != nil {
		return seed, err
	}
	// A failed native Close has already changed its closed bit. Retain that
	// opaque owner; deferred Cancel must not mistake idempotent nil for cleanup.
	closed = true
	if err := sink.Close(); err != nil {
		return seed, &replacementSnapshotCleanupDebtV1{cause: err, sink: sink}
	}
	copyMeta := *meta
	copyMeta.ID = sink.ID()
	return replacementSnapshotSeedFromMetaV1(&copyMeta, selected.Manifest, hex.EncodeToString(hash.Sum(nil)))
}

type replacementSnapshotContextReaderV1 struct {
	ctx    context.Context
	reader io.Reader
}

func (r replacementSnapshotContextReaderV1) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// OpenReplacementSnapshotSeedV1 reopens exactly the operation-owned native ID.
// Missing or changed metadata refuses; there is no fallback to the newest seed.
// The receiver verifies the full archive digest before granting install proof.
func OpenReplacementSnapshotSeedV1(ctx context.Context, store hraft.SnapshotStore, seed ReplacementSnapshotSeedV1) (io.ReadCloser, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil {
		return nil, ErrInvalidConfig
	}
	if err := seed.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	available, err := store.List()
	if err != nil {
		return nil, err
	}
	found := false
	for _, candidate := range available {
		if candidate != nil && candidate.ID == seed.SnapshotID {
			if err := seed.matchesMetaV1(candidate); err != nil {
				return nil, err
			}
			found = true
		}
	}
	if !found {
		return nil, ErrInvalidSnapshotManifest
	}
	meta, reader, err := store.Open(seed.SnapshotID)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(ctx.Err(), seed.matchesMetaV1(meta)); err != nil {
		return nil, errors.Join(err, reader.Close())
	}
	return reader, nil
}

// VerifyReplacementSnapshotInstalledV1 checks persisted native snapshot bytes
// and the native applied boundary. The transport calls this only after the
// native InstallSnapshot RPC completed successfully; it is also the restart
// reconciliation check while ordinary RPCs remain quarantined. The caller must
// additionally verify the recovered FSM command/WAL/result state. AppliedIndex
// by itself is never durable-tail proof.
func (p *HashicorpRaftProvider) VerifyReplacementSnapshotInstalledV1(ctx context.Context, seed ReplacementSnapshotSeedV1) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.raft == nil || p.snapshotStore == nil {
		return ErrInvalidHashicorpRaftProvider
	}
	if err := seed.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.raft.AppliedIndex() != seed.Index {
		return ErrReadBarrierNotSatisfied
	}
	metas, err := p.snapshotStore.List()
	if err != nil {
		return err
	}
	for _, candidate := range metas {
		if candidate == nil || candidate.Index != seed.Index || candidate.Term != seed.Term {
			continue
		}
		copySeed := seed
		copySeed.SnapshotID = candidate.ID
		reader, err := OpenReplacementSnapshotSeedV1(ctx, p.snapshotStore, copySeed)
		if err != nil {
			return err
		}
		hash := sha256.New()
		n, readErr := io.CopyBuffer(hash, io.LimitReader(replacementSnapshotContextReaderV1{ctx: ctx, reader: reader}, seed.SizeBytes+1), make([]byte, hashicorpRaftSnapshotCopyBuffer))
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
			return err
		}
		if n != seed.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != seed.ArchiveSHA256 {
			return ErrInvalidSnapshotManifest
		}
		if p.raft.AppliedIndex() != seed.Index {
			return ErrReadBarrierNotSatisfied
		}
		return nil
	}
	return fmt.Errorf("%w: exact installed replacement snapshot absent", ErrInvalidSnapshotManifest)
}

// InstallReplacementSeedV1 invokes the ordinary native installer only for an
// absent prejoin target whose receiver is durably quarantined. The caller owns
// that operation's single install attempt until this synchronous transport call
// returns, even if its caller context expires. A lost response is ambiguous and
// must be reconciled against the receiver receipt, never resent after Add intent.
func (p *HashicorpRaftProvider) InstallReplacementSeedV1(ctx context.Context, seed ReplacementSnapshotSeedV1, store hraft.SnapshotStore, transport hraft.Transport, old NodeID, target Peer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if transport == nil || p == nil {
		return ErrInvalidConfig
	}
	configuration, err := p.CommittedConfigurationV1(ctx)
	if err != nil {
		return err
	}
	if configuration.ConfigurationIndex != seed.ConfigurationIndex {
		return ErrInvalidConfig
	}
	oldVoter := false
	for _, member := range configuration.Members {
		if member.ID == target.ID || member.Address == target.Address {
			return ErrInvalidConfig
		}
		oldVoter = oldVoter || member.ID == old && member.Voter
	}
	if !oldVoter {
		return ErrInvalidConfig
	}
	// Read exact native metadata after bounded List validation, retaining the
	// same reader through transfer. Open's synchronous CRC is not cancellable.
	reader, err := OpenReplacementSnapshotSeedV1(ctx, store, seed)
	if err != nil {
		return err
	}
	defer reader.Close()
	metas, err := store.List()
	if err != nil {
		return err
	}
	var meta *hraft.SnapshotMeta
	for _, candidate := range metas {
		if candidate != nil && candidate.ID == seed.SnapshotID {
			meta = candidate
			break
		}
	}
	if err := seed.matchesMetaV1(meta); err != nil {
		return err
	}
	if err := validateReplacementSeedConfigurationV1(meta.Configuration, old, target.ID, target.Address); err != nil {
		return err
	}
	if err := p.requireHashicorpReadIndexLeaderTerm(configuration.Term); err != nil {
		return err
	}
	id := hraft.ServerID(p.cluster.NodeID)
	address := transport.EncodePeer(id, transport.LocalAddr())
	request := hraft.InstallSnapshotRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax, ID: []byte(id), Addr: address}, SnapshotVersion: meta.Version, Term: configuration.Term, Leader: address, LastLogIndex: meta.Index, LastLogTerm: meta.Term, Peers: meta.Peers, Size: meta.Size, Configuration: hraft.EncodeConfiguration(meta.Configuration), ConfigurationIndex: meta.ConfigurationIndex}
	var response hraft.InstallSnapshotResponse
	if err := transport.InstallSnapshot(hraft.ServerID(target.ID), hraft.ServerAddress(target.Address), &request, &response, replacementSnapshotContextReaderV1{ctx: ctx, reader: reader}); err != nil {
		return errors.Join(ErrCommitAmbiguous, err)
	}
	if !response.Success {
		return ErrAdmissionUnavailable
	}
	if err := p.requireHashicorpReadIndexLeaderTerm(configuration.Term); err != nil {
		return err
	}
	return ctx.Err()
}
