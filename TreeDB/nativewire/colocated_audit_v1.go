package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const ColocatedAuditPlanMaxBytesV1 = 512 << 10

// This is an operator observation plan, never a command or serving capability.
// Requests have zero deadlines, retaining their original logical bytes.
type ColocatedAuditWriteV1 struct {
	Replace  *public.ReplaceRequestV1 `json:",omitempty"`
	Delete   *public.DeleteRequestV1  `json:",omitempty"`
	Response public.MutationResponseV1
}
type ColocatedAuditStateV1 struct {
	ID, Document []byte
	Absent       bool
}
type ColocatedAuditPlanV1 struct {
	Version               int
	RunID                 string
	HighestNewCommitIndex uint64
	RequiredAppliedIndex  uint64
	Writes                []ColocatedAuditWriteV1
	Final                 []ColocatedAuditStateV1
	Population            *collections.VectorSourcePopulationExpectationV1 `json:",omitempty"`
}
type ColocatedAuditWitnessV1 struct {
	AttemptSHA256 string
	Outcome       commitlog.ColocatedVectorMutationOutcomeV1
}
type ColocatedAuditProofV1 struct {
	ID                     []byte
	ExpectedDocumentSHA256 string `json:",omitempty"`
	Absent                 bool
	Live                   collections.VectorIndexPartitionLiveStatusV1
}
type ColocatedAuditReceiptV1 struct {
	Version                                      int
	RunID, PlanSHA256, NodeID, OwnerGroup, Scope string
	AppliedTerm, AppliedIndex                    uint64
	PhysicalState                                backenddb.StateToken
	CommandWALNextLSN                            uint64
	RetainedCount, RetainedBytes                 uint64
	RetainedChain                                string
	Witnesses                                    []ColocatedAuditWitnessV1
	Final                                        []ColocatedAuditProofV1
	Population                                   *collections.VectorSourcePopulationProofV1 `json:",omitempty"`
}

func DecodeColocatedAuditPlanV1(ctx context.Context, input io.Reader) (ColocatedAuditPlanV1, error) {
	var p ColocatedAuditPlanV1
	raw, err := io.ReadAll(io.LimitReader(input, ColocatedAuditPlanMaxBytesV1+1))
	if err != nil {
		return p, err
	}
	if len(raw) > ColocatedAuditPlanMaxBytesV1 {
		return p, errors.New("colocated audit plan exceeds bound")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&p); err != nil {
		return p, err
	}
	if d.Decode(new(any)) != io.EOF {
		return p, errors.New("trailing audit plan")
	}
	return p, ValidateColocatedAuditPlanV1(ctx, p)
}
func ValidateColocatedAuditPlanV1(ctx context.Context, p ColocatedAuditPlanV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	populationOnly := p.Population != nil && len(p.Writes) == 0 && len(p.Final) == 0 && p.HighestNewCommitIndex == 0 && p.RequiredAppliedIndex > 0
	if p.Version != 1 || len(p.RunID) == 0 || len(p.RunID) > 64 || strings.Trim(p.RunID, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" || !populationOnly && (len(p.Writes) != 6 || len(p.Final) < 1 || len(p.Final) > 6 || p.HighestNewCommitIndex == 0 || p.RequiredAppliedIndex < p.HighestNewCommitIndex) {
		return errors.New("audit requires version1, bounded run ID, six original outcomes and final known IDs")
	}
	if p.Population != nil {
		if err := collections.ValidateVectorSourcePopulationExpectationV1(*p.Population); err != nil {
			return err
		}
	}
	if _, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{ColocatedAudit: &p}); err != nil {
		return err
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > ColocatedAuditPlanMaxBytesV1 {
		return errors.New("audit plan encoded bound")
	}
	if populationOnly {
		return ctx.Err()
	}
	keys := make(map[string]bool, 6)
	last := make(map[string]ColocatedAuditStateV1, 6)
	var scope commitlog.ColocatedVectorMutationScopeV1
	var previous uint64
	for i, w := range p.Writes {
		if (w.Replace == nil) == (w.Delete == nil) {
			return errors.New("audit operation must be exactly replace or delete")
		}
		var id, key []byte
		var g public.GenerationIDV1
		if w.Replace != nil {
			r := w.Replace
			if !r.Deadline.IsZero() {
				return errors.New("audit logical request has deadline")
			}
			if err := public.ValidateReplaceRequestV1(ctx, *r); err != nil {
				return err
			}
			id, key, g = r.ID, r.IdempotencyKey, r.Generation
			last[string(id)] = ColocatedAuditStateV1{ID: id, Document: r.Document}
		} else {
			r := w.Delete
			if !r.Deadline.IsZero() {
				return errors.New("audit logical request has deadline")
			}
			if err := public.ValidateDeleteRequestV1(ctx, *r); err != nil {
				return err
			}
			id, key, g = r.ID, r.IdempotencyKey, r.Generation
			last[string(id)] = ColocatedAuditStateV1{ID: id, Absent: true}
		}
		if keys[string(key)] {
			return errors.New("audit repeated original attempt")
		}
		keys[string(key)] = true
		if w.Response.AppliedIndex > p.RequiredAppliedIndex {
			return errors.New("audit applied floor below original ACK")
		}
		if err := public.ValidateMutationResponseV1(g, w.Delete != nil, w.Response); err != nil {
			return err
		}
		token, err := decodeColocatedVectorVisibilityV1(w.Response.VisibilityToken)
		if err != nil || token.Scope.ValidateV1() != nil || token.Outcome.AppliedCommandLSN != 0 || token.Outcome.CommandDigest == ([32]byte{}) || !bytes.Equal(token.Attempt, key) || token.Scope.Index != g.Index || token.Scope.Generation != g.Generation || token.Scope.OwnerGroup != w.Response.OwnerGroup {
			return errors.New("audit request/token identity mismatch")
		}
		if i == 0 {
			scope = token.Scope
		}
		if scope != token.Scope || w.Response.CommitIndex <= previous || !colocatedAuditOutcomeMatchesV1(token.Outcome, w.Response, w.Delete != nil) {
			return errors.New("audit original outcome/scope/position mismatch")
		}
		previous = w.Response.CommitIndex
	}
	if previous != p.HighestNewCommitIndex || len(last) != len(p.Final) {
		return errors.New("audit highest new commit/final set mismatch")
	}
	seen := make(map[string]bool, len(last))
	for _, f := range p.Final {
		want, ok := last[string(f.ID)]
		if !ok || seen[string(f.ID)] || want.Absent != f.Absent || !bytes.Equal(want.Document, f.Document) {
			return errors.New("audit final state differs from complete acknowledged ledger")
		}
		seen[string(f.ID)] = true
	}
	return ctx.Err()
}
func colocatedAuditOutcomeMatchesV1(o commitlog.ColocatedVectorMutationOutcomeV1, r public.MutationResponseV1, deletion bool) bool {
	affected := r.Modified
	if deletion {
		affected = r.Deleted
	}
	return o.Term == r.CommitTerm && o.Index == r.CommitIndex && o.Coverage == r.Coverage && o.Revision == r.LiveRevision && o.Matched == r.Matched && o.Affected == affected
}
func (r *FixedPeerTCPRuntimeV1) DiagnosticsWithColocatedAuditV1(ctx context.Context, p ColocatedAuditPlanV1) (FixedPeerDiagnosticsV1, error) {
	if r == nil {
		return FixedPeerDiagnosticsV1{}, raftcluster.ErrAdmissionUnavailable
	}
	if err := ValidateColocatedAuditPlanV1(ctx, p); err != nil {
		return FixedPeerDiagnosticsV1{}, err
	}
	select {
	case r.diagnostics <- struct{}{}:
		defer func() { <-r.diagnostics }()
	default:
		return FixedPeerDiagnosticsV1{}, raftcluster.ErrAdmissionUnavailable
	}
	report, err := r.diagnosticsV1(ctx)
	if err != nil {
		return report, err
	}
	audit, err := r.colocatedAuditV1(ctx, p)
	if err == nil {
		report.ColocatedAudit = &audit
	}
	return report, err
}
func (c *FixedPeerTCPClientV1) DiagnosticsWithColocatedAuditV1(ctx context.Context, node raftcluster.NodeID, p ColocatedAuditPlanV1) (FixedPeerDiagnosticsV1, error) {
	if err := ValidateColocatedAuditPlanV1(ctx, p); err != nil {
		return FixedPeerDiagnosticsV1{}, err
	}
	reply, err := c.call(ctx, node, "diagnostics", fixedPeerRequestV1{ColocatedAudit: &p}, false)
	if reply.Diagnostics == nil || reply.Diagnostics.ColocatedAudit == nil {
		return FixedPeerDiagnosticsV1{}, errors.Join(err, ErrFixedPeerVectorProofStaleV1)
	}
	return *reply.Diagnostics, err
}
func (r *FixedPeerTCPRuntimeV1) colocatedAuditV1(ctx context.Context, p ColocatedAuditPlanV1) (ColocatedAuditReceiptV1, error) {
	var out ColocatedAuditReceiptV1
	if err := ValidateColocatedAuditPlanV1(ctx, p); err != nil {
		return out, err
	}
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return out, err
	}
	if r == nil || r.preparedVector == nil || r.vector == nil {
		return out, ErrFixedPeerVectorUnavailableV1
	}
	// Fresh network/quorum authority is obtained while ALL voters remain live.
	// No outcome submit, repair, retry or new endpoint is used by this operation.
	ready, err := r.readinessV1(ctx)
	if err != nil || !ready.Ready || ready.Draining || ready.VectorPhase != "active" {
		return out, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	if err := r.requirePreparedVectorCurrentDBV1(); err != nil {
		return out, err
	}
	if r.preparedVector == nil || r.vector == nil || r.vector.collection == nil {
		return out, ErrFixedPeerVectorUnavailableV1
	}
	vector := r.servingVectorConfigV1()
	owner, err := vectorPartitionColocatedOwnerV1(vector.Manifest, vector.Placement)
	if err != nil {
		return out, err
	}
	for _, w := range p.Writes {
		// Route only the fresh token/quorum proof. The audit below still reads
		// this voter's own current FSM, covered witnesses and source/live roots.
		if e := r.requireColocatedVectorVisibilityV1(ctx, public.SearchRequestV1{Version: 1, Generation: w.Response.Generation, VisibilityToken: w.Response.VisibilityToken}); e != nil {
			return out, e
		}
	}
	if err = r.vector.lockMutationAdmissionV1(ctx); err != nil {
		return out, err
	}
	defer r.vector.mutationMu.Unlock()
	data := r.data[owner]
	if data == nil || data.fsm == nil {
		return out, ErrFixedPeerVectorUnavailableV1
	}
	_, db, err := data.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: owner, MinAppliedIndex: p.RequiredAppliedIndex}, vector.Collection.Collection)
	if err != nil || db != r.vector.boundDB {
		return out, errors.Join(ErrFixedPeerVectorProofStaleV1, err)
	}
	before, ok := db.StateToken()
	if !ok {
		return out, ErrFixedPeerVectorProofStaleV1
	}
	nextLSN := db.CommandWALNextLSN()
	// No allocated frame may await publication at this observation boundary.
	if nextLSN == 0 || before.AppliedCommandLSN == ^uint64(0) || nextLSN != before.AppliedCommandLSN+1 {
		return out, ErrFixedPeerVectorProofStaleV1
	}
	if r.vector.manager == nil {
		return out, ErrFixedPeerVectorUnavailableV1
	}
	pending := r.vector.manager.StatsSnapshot()
	if pending.PendingDocuments != 0 || pending.PendingBytes != 0 || pending.PendingRootRuns != 0 || pending.PendingIndexedFlushUnits != 0 || pending.OverlayMutableDocuments != 0 || pending.OverlayQueuedIndexedFlushUnits != 0 || pending.OverlayActiveIndexedFlushUnits != 0 || pending.IndexedAsyncFlushRunning != 0 {
		return out, ErrFixedPeerVectorProofStaleV1
	}
	status, err := r.Status(ctx)
	if err != nil {
		return out, err
	}
	var term, index uint64
	for _, g := range status.Groups {
		if g.GroupID == owner {
			term, index = g.Applied.Term, g.Applied.Index
		}
	}
	if index < p.RequiredAppliedIndex {
		return out, ErrFixedPeerVectorProofStaleV1
	}
	c := r.vector.collection
	raw, _ := json.Marshal(p)
	digest := sha256.Sum256(raw)
	out = ColocatedAuditReceiptV1{Version: 1, RunID: p.RunID, PlanSHA256: hex.EncodeToString(digest[:]), NodeID: string(r.config.NodeID), OwnerGroup: string(owner), Scope: "six retained original outcomes and known-ID canonical source/absence plus exact live membership only; no entire population proof", AppliedTerm: term, AppliedIndex: index, PhysicalState: before, CommandWALNextLSN: nextLSN}
	if p.Population != nil {
		out.Scope = "complete current canonical source vector population at this voter's current FSM applied position, at or above requested floor; no full-document or reverse live-graph equality"
		if len(p.Writes) > 0 {
			out.Scope += "; six retained original outcomes and known-ID canonical source/absence plus exact live membership"
		}
	}
	// Raft apply holds the FSM lock before acquiring shared collection admission.
	// Never acquire that lock from the prepared callback: bracket admission with
	// identity checks instead, and retain physical/WAL checks while admitted.
	if !data.fsm.HasCurrentDBV1(db) {
		return ColocatedAuditReceiptV1{}, ErrFixedPeerVectorProofStaleV1
	}
	err = c.WithPreparedCommandWALMutation(func(admitted *collections.CommandWALAdmittedCollection) error {
		// Admission may flush pending writes: any state change makes this audit fail.
		state, ok := db.StateToken()
		if !ok || state != before || db.CommandWALNextLSN() != nextLSN {
			return ErrFixedPeerVectorProofStaleV1
		}
		logical, _, err := c.VerifyVectorPartitionColocatedMutationLogicalStateV1(ctx)
		if err != nil || len(logical) != 0 && (len(logical) != 2 || len(logical[1]) != 56 || binary.LittleEndian.Uint64(logical[1]) != 1) {
			return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		if len(logical) == 2 {
			out.RetainedCount = binary.LittleEndian.Uint64(logical[1][8:])
			out.RetainedBytes = binary.LittleEndian.Uint64(logical[1][16:])
			out.RetainedChain = hex.EncodeToString(logical[1][24:])
		}
		if len(p.Writes) > 0 && out.RetainedCount != 6 {
			return errors.New("audit requires exactly six retained outcomes in this fresh collection checkpoint")
		}
		for _, w := range p.Writes {
			token, _ := decodeColocatedVectorVisibilityV1(w.Response.VisibilityToken)
			request := public.InsertRequestV1{}
			deletion := w.Delete != nil
			if deletion {
				v := w.Delete
				request = public.InsertRequestV1{Version: v.Version, Generation: v.Generation, ID: v.ID, IdempotencyKey: v.IdempotencyKey}
			} else {
				request = *w.Replace
				format, e := fixedPeerVectorDocumentFormatV1(c)
				if e != nil {
					return e
				}
				decoded, e := c.ValidatedVectorFromDocumentV1(vector.Manifest.IndexName, format, request.Document)
				if e != nil || !sameFloat32BitsV1(decoded, request.Vector) {
					return errors.Join(ErrFixedPeerVectorDocumentV1, e)
				}
			}
			routed := VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: request}, Delete: deletion}
			entry, err := fixedPeerVectorColocatedEntryV1(vector.Collection.Collection, token.Outcome.ExpectedCatalogVersion, routed, token.Scope)
			if err != nil {
				return err
			}
			command := raftentry.CommandDigestV1ForBytes(entry, raftentry.DecodeOptions{})
			if [32]byte(command) != token.Outcome.CommandDigest {
				return ErrFixedPeerVectorProofStaleV1
			}
			actual, found, err := c.ReadVectorPartitionColocatedOutcomeV1(token.Scope, token.Attempt, [32]byte(command))
			if err != nil || !found || actual.AppliedCommandLSN == 0 || actual.AppliedCommandLSN > before.AppliedCommandLSN || actual.Index > index {
				return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
			}
			compare := actual
			compare.AppliedCommandLSN = 0
			if compare != token.Outcome || !colocatedAuditOutcomeMatchesV1(actual, w.Response, deletion) {
				return ErrFixedPeerVectorProofStaleV1
			}
			hash := sha256.Sum256(token.Attempt)
			out.Witnesses = append(out.Witnesses, ColocatedAuditWitnessV1{AttemptSHA256: hex.EncodeToString(hash[:]), Outcome: actual})
		}
		for _, f := range p.Final {
			matched := int64(1)
			if f.Absent {
				matched = 0
			}
			live, err := admitted.ProveVectorPartitionColocatedMutationV1(ctx, vector.Manifest, f.ID, f.Document, f.Absent, matched, 1)
			if err != nil {
				return err
			}
			item := ColocatedAuditProofV1{ID: bytes.Clone(f.ID), Absent: f.Absent, Live: live}
			if !f.Absent {
				sum := sha256.Sum256(f.Document)
				item.ExpectedDocumentSHA256 = hex.EncodeToString(sum[:])
			}
			out.Final = append(out.Final, item)
		}
		if p.Population != nil {
			proof, e := admitted.ProveVectorSourcePopulationV1(ctx, vector.Manifest, *p.Population)
			if e != nil {
				return e
			}
			out.Population = &proof
		}
		after, err := c.VectorPartitionColocatedMutationLogicalStateV1(ctx)
		if err != nil || len(after) != len(logical) || len(logical) == 2 && (!bytes.Equal(after[0], logical[0]) || !bytes.Equal(after[1], logical[1])) {
			return errors.Join(ErrFixedPeerVectorProofStaleV1, err)
		}
		state, ok = db.StateToken()
		if !ok || state != before || db.CommandWALNextLSN() != nextLSN {
			return ErrFixedPeerVectorProofStaleV1
		}
		return ctx.Err()
	})
	if err != nil {
		return ColocatedAuditReceiptV1{}, err
	}
	if !data.fsm.HasCurrentDBV1(db) {
		return ColocatedAuditReceiptV1{}, ErrFixedPeerVectorProofStaleV1
	}
	readyAfter, e := r.readinessV1(ctx)
	if e != nil || !readyAfter.Ready || readyAfter.Draining || readyAfter.VectorPhase != "active" {
		return ColocatedAuditReceiptV1{}, errors.Join(ErrFixedPeerVectorProofStaleV1, e)
	}
	after, err := r.Status(ctx)
	if err != nil {
		return ColocatedAuditReceiptV1{}, err
	}
	foundOwner := false
	for _, g := range after.Groups {
		if g.GroupID == owner {
			foundOwner = true
			if g.Applied.Term != term || g.Applied.Index != index {
				return ColocatedAuditReceiptV1{}, fmt.Errorf("audit applied state changed")
			}
		}
	}
	if !foundOwner {
		return ColocatedAuditReceiptV1{}, ErrFixedPeerVectorProofStaleV1
	}
	state, ok := db.StateToken()
	if !ok || state != before || db.CommandWALNextLSN() != nextLSN || !data.fsm.HasCurrentDBV1(db) {
		return ColocatedAuditReceiptV1{}, ErrFixedPeerVectorProofStaleV1
	}
	if err = r.requirePreparedVectorCatalogV1(); err != nil {
		return ColocatedAuditReceiptV1{}, err
	}
	return out, ctx.Err()
}
