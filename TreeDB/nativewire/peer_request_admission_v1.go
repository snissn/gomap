package nativewire

import (
	"context"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type peerWorkLeaseV1 struct {
	request peerResourceLeaseV1
	bytes peerResourceLeaseV1
}

func (a *peerNodeAdmissionV1) work(scope string, kind int, bytes int64) (peerWorkLeaseV1, error) {
	var work peerWorkLeaseV1
	var err error
	work.request, err = a.acquire(scope, kind, 1)
	if err != nil { return work, err }
	work.bytes, err = a.acquire(scope, peerBytesV1, bytes)
	if err != nil { work.request.release(); return work, err }
	return work, nil
}

func (w *peerWorkLeaseV1) release() { w.bytes.release(); w.request.release() }

func peerControlScopeV1(operation string) string {
	switch strings.TrimPrefix(operation, "/v1/") {
	case "status", "catalog-read", "catalog-route", "catalog-validate": return "control-read"
	case "forward": return "control-forward"
	default: return "control-write"
	}
}

// Account conservatively for request copies and the bounded inventory/status
// envelope. Submit replies also carry the decoded and committed command.
func peerControlBytesV1(requestBytes int64) int64 { return 4<<20 + 6*requestBytes }

// Bound JSON expansion before json.Marshal allocates. String escaping may
// expand to six bytes; byte slices use base64 and uint64 tokens need 21 bytes.
func preflightPeerRequestBytesV1(body fixedPeerRequestV1) (int64, error) {
	remaining := int64(fixedPeerMaxRPCBytesV1-4096)
	charge := func(length int, multiplier int64) bool {
		if length < 0 || int64(length) > remaining/multiplier { return false }
		remaining -= int64(length)*multiplier
		return true
	}
	if !charge(len(body.Entry), 2) || !charge(len(body.Metadata.TraceContext), 2) || !charge(len(body.Route.Tokens), 22) || len(body.Metadata.ClusterRouteMembers) > fixedPeerMaxPeersV1 { return 0, raftcluster.ErrRouteTargetUnsupported }
	m, r := body.Metadata, body.Route
	for _, value := range []string{m.Compression, m.ClusterRouteDatabase, m.ClusterRouteCatalog, m.ClusterRouteCollection, m.ClusterRouteShape, m.ClusterRouteGroupID, m.ClusterRouteLeaderHint, m.ClusterRoutePlacementMode, m.ClusterRouteKey, m.ClusterRoutePartitionID, m.CatalogMetaDigest, r.Database, r.Catalog, r.Collection, r.CommandName, string(r.Shape)} {
		if !charge(len(value), 6) { return 0, raftcluster.ErrRouteTargetUnsupported }
	}
	for _, value := range m.ClusterRouteMembers { if !charge(len(value)+1, 6) { return 0, raftcluster.ErrRouteTargetUnsupported } }
	return int64(fixedPeerMaxRPCBytesV1)-remaining, nil
}

// Embed the existing submitter to preserve its admission/capability methods.
// The lease spans the actual preflight, Raft commit and deterministic apply.
type peerBudgetSubmitterV1 struct {
	*raftcluster.SingleGroupSubmitter
	admission *peerNodeAdmissionV1
	scope string
}

func (s peerBudgetSubmitterV1) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (raftcluster.SubmitResultV1, error) {
	return s.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, nil)
}

func (s peerBudgetSubmitterV1) SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1, before func(context.Context) error) (raftcluster.SubmitResultV1, error) {
	if len(entry) > fixedPeerMaxRPCBytesV1 { return raftcluster.SubmitResultV1{}, raftcluster.ErrRouteTargetUnsupported }
	work, err := s.admission.work(s.scope, peerProposalsV1, int64(len(entry))*4)
	if err != nil { return raftcluster.SubmitResultV1{}, err }
	defer work.release()
	if before != nil { return s.SingleGroupSubmitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, before) }
	return s.SingleGroupSubmitter.SubmitCommandEntryV1(ctx, entry, metadata)
}
