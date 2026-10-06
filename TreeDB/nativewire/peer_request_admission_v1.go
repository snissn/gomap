package nativewire

import (
	"context"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// Authenticated native traffic starts with a 1 MiB frame profile; legacy
// native-wire framing remains unchanged. Applications can set a server limit
// explicitly within their declared node byte capacity.
const peerNativeDefaultFrameV1 = 1 << 20

type peerWorkLeaseV1 struct {
	request  peerResourceLeaseV1
	bytes    peerResourceLeaseV1
	lifetime *peerRequestLifetimeV1
	ctx      context.Context
}

func (a *peerNodeAdmissionV1) work(scope string, kind int, bytes int64) (peerWorkLeaseV1, error) {
	var work peerWorkLeaseV1
	var err error
	work.request, err = a.acquire(scope, kind, 1)
	if err != nil {
		return work, err
	}
	work.bytes, err = a.acquire(scope, peerBytesV1, bytes)
	if err != nil {
		work.request.release()
		return work, err
	}
	return work, nil
}

func (w *peerWorkLeaseV1) release() {
	w.bytes.release()
	w.request.release()
	w.lifetime.release()
	w.lifetime = nil
}

func peerControlScopeV1(operation string) string {
	switch strings.TrimPrefix(operation, "/v1/") {
	case "status", "replacement-read", "replacement-cutoff", "replacement-tail-check", "catalog-read", "vector-catalog-read", "vector-prepare-status", "catalog-route", "catalog-validate", "group-read-proof":
		return "control-read"
	case "vector-split-source-proof", "vector-split-receipt":
		return "control-proof"
	case "readiness", "diagnostics":
		return "control-diagnostics"
	case "forward":
		return "control-forward"
	default:
		return "control-write"
	}
}

// Read and diagnostic control operations stay available as dependencies while
// draining. Forwarding and writes require a live local originating request.
func peerControlRequestModeV1(operation string, ordinary peerRequestModeV1) peerRequestModeV1 {
	switch peerControlScopeV1(operation) {
	case "control-read", "control-proof", "control-diagnostics":
		return peerRequestInternalV1
	default:
		return ordinary
	}
}

// Account conservatively for request copies and the bounded inventory/status
// envelope. Submit replies also carry the decoded and committed command.
func peerControlBytesV1(requestBytes int64) int64 { return 4<<20 + 6*requestBytes }

// Bound JSON expansion before json.Marshal allocates. String escaping may
// expand to six bytes; byte slices use base64 and uint64 tokens need 21 bytes.
func preflightPeerRequestBytesV1(body fixedPeerRequestV1) (int64, error) {
	remaining := int64(fixedPeerMaxRPCBytesV1 - 4096)
	charge := func(length int, multiplier int64) bool {
		if length < 0 || int64(length) > remaining/multiplier {
			return false
		}
		remaining -= int64(length) * multiplier
		return true
	}
	if !charge(len(body.VectorMutationVisibility), 2) || !charge(len(body.Entry), 2) || !charge(len(body.Metadata.TraceContext), 2) || !charge(len(body.Route.Tokens), 22) || len(body.Metadata.ClusterRouteMembers) > fixedPeerMaxPeersV1 {
		return 0, raftcluster.ErrRouteTargetUnsupported
	}
	m, r := body.Metadata, body.Route
	for _, value := range []string{m.Compression, m.ClusterRouteDatabase, m.ClusterRouteCatalog, m.ClusterRouteCollection, m.ClusterRouteShape, m.ClusterRouteGroupID, m.ClusterRouteLeaderHint, m.ClusterRoutePlacementMode, m.ClusterRouteKey, m.ClusterRoutePartitionID, m.CatalogMetaDigest, r.Database, r.Catalog, r.Collection, r.CommandName, string(r.Shape)} {
		if !charge(len(value), 6) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
	}
	for _, value := range m.ClusterRouteMembers {
		if !charge(len(value)+1, 6) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
	}
	if body.VectorSearch != nil {
		if !charge(len(body.VectorSearch.Query), 24) || !charge(len(body.VectorSearch.VisibilityToken), 2) || !charge(len(body.VectorSearch.Generation.Index), 6) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
	}
	chargeInsert := func(r public.InsertRequestV1) bool {
		return charge(len(r.ID), 2) && charge(len(r.Document), 2) && charge(len(r.IdempotencyKey), 2) && charge(len(r.Vector), 24) && charge(len(r.Generation.Index), 6)
	}
	routed := [2]*VectorPartitionRoutedInsertV1{body.VectorInsert}
	if body.VectorMutation != nil {
		routed[1] = &body.VectorMutation.VectorPartitionRoutedInsertV1
	}
	for _, v := range routed {
		if v == nil {
			continue
		}
		if !chargeInsert(v.Request) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
		i, s := v.Identity.Index, v.Identity.SourceV2
		for _, value := range []string{string(v.SourceGroup), string(v.OwnerGroup), v.ReadySetDigest, v.RouterModelDigest, v.CatalogProof.Digest, i.Collection.Database, i.Collection.Catalog, i.Collection.Collection, i.IndexName, i.IndexDefinitionDigest, i.CatalogDigest, v.Identity.Immutable.ManifestDigest, v.Identity.Immutable.PlacementDigest, s.SourceMapDigest, s.SnapshotSetDigest, s.GraphProfileDigest, s.PlacementDigest} {
			if !charge(len(value), 6) {
				return 0, raftcluster.ErrRouteTargetUnsupported
			}
		}
	}
	if p := body.ColocatedAudit; p != nil {
		// Each bounded write includes fixed request/response fields, numbers,
		// deadlines and JSON punctuation; final states have their own envelope.
		populationOnly := p.Population != nil && len(p.Writes) == 0 && len(p.Final) == 0 && p.HighestNewCommitIndex == 0 && p.RequiredAppliedIndex > 0
		if !populationOnly && (len(p.Writes) < 6 || len(p.Writes) > 63 || len(p.Final) < 1 || len(p.Final) > len(p.Writes)) || !charge(len(p.RunID), 6) || !charge(len(p.Writes), 2048) || !charge(len(p.Final), 256) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
		// All new numeric fields and punctuation fit in this fixed allowance;
		// charge the sole dynamic string before any JSON allocation.
		if p.Population != nil && (!charge(1, 512) || !charge(len(p.Population.SHA256), 6)) {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
		for _, w := range p.Writes {
			if (w.Replace == nil) == (w.Delete == nil) {
				return 0, raftcluster.ErrRouteTargetUnsupported
			}
			if w.Replace != nil && !chargeInsert(*w.Replace) {
				return 0, raftcluster.ErrRouteTargetUnsupported
			}
			if w.Delete != nil {
				r := w.Delete
				if !charge(len(r.ID), 2) || !charge(len(r.IdempotencyKey), 2) || !charge(len(r.Generation.Index), 6) {
					return 0, raftcluster.ErrRouteTargetUnsupported
				}
			}
			r := w.Response
			if !charge(len(r.VisibilityToken), 2) || !charge(len(r.Generation.Index), 6) || !charge(len(r.OwnerGroup), 6) {
				return 0, raftcluster.ErrRouteTargetUnsupported
			}
		}
		for _, f := range p.Final {
			if !charge(len(f.ID), 2) || !charge(len(f.Document), 2) {
				return 0, raftcluster.ErrRouteTargetUnsupported
			}
		}
	}
	if body.VectorLifecycle != nil && !charge(len(body.VectorLifecycle.Action), 6) {
		return 0, raftcluster.ErrRouteTargetUnsupported
	}
	return int64(fixedPeerMaxRPCBytesV1) - remaining, nil
}

// Embed the existing submitter to preserve its admission/capability methods.
// The lease spans the actual preflight, Raft commit and deterministic apply.
type peerBudgetSubmitterV1 struct {
	*raftcluster.SingleGroupSubmitter
	admission *peerNodeAdmissionV1
	scope     string
}

func (s peerBudgetSubmitterV1) SubmitCommandEntryV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (raftcluster.SubmitResultV1, error) {
	return s.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, nil)
}

func (s peerBudgetSubmitterV1) SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1, before func(context.Context) error) (raftcluster.SubmitResultV1, error) {
	if len(entry) > fixedPeerMaxRPCBytesV1 {
		return raftcluster.SubmitResultV1{}, raftcluster.ErrRouteTargetUnsupported
	}
	work, err := s.admission.work(s.scope, peerProposalsV1, int64(len(entry))*4)
	if err != nil {
		return raftcluster.SubmitResultV1{}, err
	}
	defer work.release()
	if before != nil {
		return s.SingleGroupSubmitter.SubmitCommandEntryWithPreCommitV1(ctx, entry, metadata, before)
	}
	return s.SingleGroupSubmitter.SubmitCommandEntryV1(ctx, entry, metadata)
}
