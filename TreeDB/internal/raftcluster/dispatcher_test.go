package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type recordingGroupSubmitter struct {
	groupID  GroupID
	features FeatureSet
	status   AdmissionStatus
	err      error
	calls    []recordingGroupSubmitCall
}

type recordingGroupSubmitCall struct {
	entry    []byte
	metadata raftentry.RequestMetadataV1
}

type submitOnlyGroupSubmitter struct {
	groupID GroupID
}

type recordingCatalogRouteValidator struct {
	calls      []raftentry.RequestMetadataV1
	nilContext bool
	err        error
}

func (v *recordingCatalogRouteValidator) ValidateCatalogRouteMetadata(ctx context.Context, metadata raftentry.RequestMetadataV1) error {
	v.calls = append(v.calls, cloneRequestMetadataV1(metadata))
	v.nilContext = ctx == nil
	return v.err
}

func (s *submitOnlyGroupSubmitter) Config() ResolvedConfig {
	return ResolvedConfig{GroupID: s.groupID}
}

func (s *submitOnlyGroupSubmitter) SubmitCommandEntryV1(context.Context, []byte, raftentry.RequestMetadataV1) (SubmitResultV1, error) {
	panic("unexpected submit")
}

func (s *recordingGroupSubmitter) Config() ResolvedConfig {
	return ResolvedConfig{GroupID: s.groupID, Features: s.features}
}

func (s *recordingGroupSubmitter) ClusterAdmissionStatus(context.Context) (AdmissionStatus, error) {
	if s.status == (AdmissionStatus{}) {
		return LeaderAdmission(), nil
	}
	return s.status, nil
}

func (s *recordingGroupSubmitter) SubmitCommandEntryV1(_ context.Context, entry []byte, metadata raftentry.RequestMetadataV1) (SubmitResultV1, error) {
	s.calls = append(s.calls, recordingGroupSubmitCall{
		entry:    bytes.Clone(entry),
		metadata: cloneRequestMetadataV1(metadata),
	})
	if s.err != nil {
		return SubmitResultV1{}, s.err
	}
	return SubmitResultV1{
		ActualAck: metadata.AckPolicy,
		CommittedEntry: CommittedCommandEntryV1{
			Term:            1,
			Index:           uint64(len(s.calls)),
			Bytes:           bytes.Clone(entry),
			RequestMetadata: cloneRequestMetadataV1(metadata),
		},
		Evidence: CommitEvidenceV1{
			Kind:                CommitEvidenceProductionConsensusV1,
			GroupID:             s.groupID,
			Term:                1,
			Index:               uint64(len(s.calls)),
			Committed:           true,
			ProductionConsensus: true,
		},
	}, nil
}

func (s *recordingGroupSubmitter) SubmitCommandEntryWithPreCommitV1(ctx context.Context, entry []byte, metadata raftentry.RequestMetadataV1, preCommit func(context.Context) error) (SubmitResultV1, error) {
	if err := preCommit(ctx); err != nil {
		return SubmitResultV1{}, err
	}
	return s.SubmitCommandEntryV1(ctx, entry, metadata)
}

func (s *recordingGroupSubmitter) snapshot() []recordingGroupSubmitCall {
	out := make([]recordingGroupSubmitCall, len(s.calls))
	for i := range s.calls {
		out[i] = recordingGroupSubmitCall{
			entry:    bytes.Clone(s.calls[i].entry),
			metadata: cloneRequestMetadataV1(s.calls[i].metadata),
		}
	}
	return out
}

func TestGroupRoutedSubmitterRoutesCollectionAndTokenTargets(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	groupB := &recordingGroupSubmitter{groupID: "group-b"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA, groupB)
	entry := testClusterCommandEntry(t, 7)

	collectionMeta := routeMetadata("group-b", iwire.AckVisible)
	collectionMeta.RequestID = 101
	collectionMeta.TraceContext = []byte("trace-b")
	collectionResult, err := dispatcher.SubmitCommandEntryV1(context.Background(), entry, collectionMeta)
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1 collection route: %v", err)
	}
	if calls := groupA.snapshot(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0 before token route", len(calls))
	}
	groupBCalls := groupB.snapshot()
	if len(groupBCalls) != 1 {
		t.Fatalf("group-b calls=%d want 1", len(groupBCalls))
	}
	if groupBCalls[0].metadata.ClusterRouteGroupID != "group-b" || groupBCalls[0].metadata.RequestID != 101 || string(groupBCalls[0].metadata.TraceContext) != "trace-b" {
		t.Fatalf("group-b metadata=%+v want group-b request metadata preserved", groupBCalls[0].metadata)
	}
	if collectionResult.CommittedEntry.RequestMetadata.ClusterRouteGroupID != "group-b" {
		t.Fatalf("committed collection route group=%q want group-b", collectionResult.CommittedEntry.RequestMetadata.ClusterRouteGroupID)
	}

	tokenMeta := routeMetadata("group-a", iwire.AckRaftCommitted)
	tokenMeta.ClusterRouteShape = clusterRouteShapeTokenV1
	tokenMeta.ClusterRoutePlacementMode = clusterRoutePlacementTokenV1
	tokenMeta.ClusterRouteKey = clusterRouteKeyDocumentIDV1
	tokenMeta.ClusterRouteTokenKnown = true
	tokenMeta.ClusterRouteToken = 42
	tokenMeta.ClusterRoutePartitionID = "p0"
	tokenMeta.ClusterRouteLeaderHint = "node-a"
	tokenMeta.TraceContext = []byte("trace-a")
	tokenResult, err := dispatcher.SubmitCommandEntryV1(context.Background(), entry, tokenMeta)
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1 token route: %v", err)
	}
	groupACalls := groupA.snapshot()
	if len(groupACalls) != 1 {
		t.Fatalf("group-a calls=%d want 1", len(groupACalls))
	}
	if got := groupACalls[0].metadata; got.ClusterRouteShape != clusterRouteShapeTokenV1 || !got.ClusterRouteTokenKnown || got.ClusterRouteToken != 42 || got.ClusterRoutePartitionID != "p0" {
		t.Fatalf("group-a token metadata=%+v want token route p0 token=42", got)
	}
	if tokenResult.ActualAck != iwire.AckRaftCommitted || tokenResult.CommittedEntry.RequestMetadata.ClusterRouteToken != 42 {
		t.Fatalf("token result ack/token=%d/%d want raft_committed/42", tokenResult.ActualAck, tokenResult.CommittedEntry.RequestMetadata.ClusterRouteToken)
	}
}

func TestCatalogMetaGroupRoutedSubmitterValidatesBeforeOwnerLookup(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	registry, err := NewGroupSubmitterRegistryV1([]GroupSubmitterV1{{GroupID: "group-a", Submitter: groupA}})
	if err != nil {
		t.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	if _, err := NewCatalogMetaGroupRoutedSubmitter(registry, nil); !errors.Is(err, ErrInvalidSubmitter) {
		t.Fatalf("nil validator error=%v want ErrInvalidSubmitter", err)
	}
	validator := &recordingCatalogRouteValidator{err: errors.New("catalog route mismatch")}
	dispatcher, err := NewCatalogMetaGroupRoutedSubmitter(registry, validator)
	if err != nil {
		t.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	metadata := routeMetadata("group-a", iwire.AckRaftCommitted)
	metadata.CatalogMetaEpoch = 3
	metadata.CatalogMetaDigest = "digest"
	if _, err := dispatcher.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), metadata); err == nil {
		t.Fatal("catalog validator rejection unexpectedly admitted")
	}
	if len(validator.calls) != 1 ||
		validator.calls[0].CatalogMetaEpoch != 3 ||
		validator.calls[0].CatalogMetaDigest != "digest" {
		t.Fatalf("validator calls=%+v want one call with epoch 3 and digest %q", validator.calls, "digest")
	}
	if got := len(groupA.snapshot()); got != 0 {
		t.Fatalf("owner submit calls=%d want 0 before catalog validation", got)
	}
}

func TestCatalogMetaGroupRoutedSubmitterNormalizesNilContextBeforeValidation(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	registry, err := NewGroupSubmitterRegistryV1([]GroupSubmitterV1{{GroupID: "group-a", Submitter: groupA}})
	if err != nil {
		t.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	validator := &recordingCatalogRouteValidator{}
	dispatcher, err := NewCatalogMetaGroupRoutedSubmitter(registry, validator)
	if err != nil {
		t.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	if _, err := dispatcher.SubmitCommandEntryV1(nil, testClusterCommandEntry(t, 8), routeMetadata("group-a", iwire.AckRaftCommitted)); err != nil {
		t.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	if validator.nilContext {
		t.Fatal("catalog validator received nil context")
	}
	if len(validator.calls) != 1 || len(groupA.snapshot()) != 1 {
		t.Fatalf("validator/owner calls=%d/%d want 1/1", len(validator.calls), len(groupA.snapshot()))
	}
}

func TestGroupRoutedSubmitterRejectsUnsupportedRoutesBeforeSubmit(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA)
	entry := testClusterCommandEntry(t, 7)

	valid := routeMetadata("group-a", iwire.AckVisible)
	token := valid
	token.ClusterRouteShape = clusterRouteShapeTokenV1
	token.ClusterRoutePlacementMode = clusterRoutePlacementTokenV1
	token.ClusterRouteKey = clusterRouteKeyDocumentIDV1
	token.ClusterRouteTokenKnown = true
	token.ClusterRouteToken = 11
	token.ClusterRoutePartitionID = "p0"

	tests := []struct {
		name    string
		meta    raftentry.RequestMetadataV1
		want    error
		wantMsg string
	}{
		{name: "missing_route", meta: raftentry.RequestMetadataV1{AckPolicy: iwire.AckVisible}, want: ErrRouteTargetMissing},
		{name: "missing_identity", meta: func() raftentry.RequestMetadataV1 {
			meta := valid
			meta.ClusterRouteCollection = ""
			return meta
		}(), want: ErrRouteTargetMissing},
		{name: "missing_group", meta: routeMetadata("", iwire.AckVisible), want: ErrRouteTargetMissing},
		{name: "unknown_group", meta: routeMetadata("group-z", iwire.AckVisible), want: ErrRouteTargetUnknown},
		{name: "fanout_required", meta: func() raftentry.RequestMetadataV1 {
			meta := valid
			meta.ClusterRouteShape = clusterRouteShapeTokenBatchV1
			meta.ClusterRoutePlacementMode = clusterRoutePlacementTokenV1
			return meta
		}(), want: ErrRouteFanoutRequired},
		{name: "collection_mode_mismatch", meta: func() raftentry.RequestMetadataV1 {
			meta := valid
			meta.ClusterRoutePlacementMode = clusterRoutePlacementTokenV1
			return meta
		}(), want: ErrRouteGroupMismatch},
		{name: "token_missing_token", meta: func() raftentry.RequestMetadataV1 {
			meta := token
			meta.ClusterRouteTokenKnown = false
			return meta
		}(), want: ErrRouteTargetMissing},
		{name: "token_missing_partition", meta: func() raftentry.RequestMetadataV1 {
			meta := token
			meta.ClusterRoutePartitionID = ""
			return meta
		}(), want: ErrRouteTargetMissing},
		{name: "token_missing_route_key", meta: func() raftentry.RequestMetadataV1 {
			meta := token
			meta.ClusterRouteKey = ""
			return meta
		}(), want: ErrRouteTargetUnsupported, wantMsg: "missing route key"},
		{name: "token_unsupported_route_key", meta: func() raftentry.RequestMetadataV1 {
			meta := token
			meta.ClusterRouteKey = "tenant_id"
			return meta
		}(), want: ErrRouteTargetUnsupported, wantMsg: "unsupported route key \"tenant_id\""},
		{name: "unsupported_shape", meta: func() raftentry.RequestMetadataV1 {
			meta := valid
			meta.ClusterRouteShape = "scatter"
			return meta
		}(), want: ErrRouteTargetUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(groupA.snapshot())
			_, err := dispatcher.SubmitCommandEntryV1(context.Background(), entry, tt.meta)
			if !errors.Is(err, tt.want) {
				t.Fatalf("SubmitCommandEntryV1 err=%v want %v", err, tt.want)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("SubmitCommandEntryV1 err=%q want message containing %q", err, tt.wantMsg)
			}
			if after := len(groupA.snapshot()); after != before {
				t.Fatalf("group-a calls after rejection=%d want %d", after, before)
			}
		})
	}
}

func TestGroupRoutedSubmitterRejectsUnknownRemoteOwnerWithStableHintBeforeSubmit(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	groupB := &recordingGroupSubmitter{groupID: "group-b"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA, groupB)

	meta := routeMetadata("group-z", iwire.AckRaftCommitted)
	meta.ClusterRouteShape = clusterRouteShapeTokenV1
	meta.ClusterRoutePlacementMode = clusterRoutePlacementRingV1
	meta.ClusterRouteKey = clusterRouteKeyDocumentIDV1
	meta.ClusterRouteTokenKnown = true
	meta.ClusterRouteToken = 99
	meta.ClusterRoutePartitionID = "p9"
	meta.ClusterRouteMembers = []string{"node-z", "node-y"}
	meta.ClusterRouteLeaderHint = "node-z"
	_, err := dispatcher.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), meta)
	if !errors.Is(err, ErrRouteTargetUnknown) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want route target unknown", err)
	}
	if !strings.Contains(err.Error(), `route group "group-z" is not configured locally`) {
		t.Fatalf("SubmitCommandEntryV1 err=%q want stable unknown-owner text", err)
	}
	if !strings.Contains(err.Error(), "leader_hint=node-z") {
		t.Fatalf("SubmitCommandEntryV1 err=%q want leader hint", err)
	}
	route, ok := RouteErrorMetadataOf(err)
	if !ok {
		t.Fatalf("RouteErrorMetadataOf ok=false err=%v", err)
	}
	if route.Class != RouteErrorClassRemoteOwnerRedirect ||
		route.GroupID != "group-z" ||
		route.LeaderHint != "node-z" ||
		route.Database != "default" ||
		route.Catalog != "default" ||
		route.Collection != "users" ||
		route.Shape != clusterRouteShapeTokenV1 ||
		route.PlacementMode != clusterRoutePlacementRingV1 ||
		route.RouteKey != clusterRouteKeyDocumentIDV1 ||
		!route.TokenKnown ||
		route.Token != 99 ||
		route.PartitionID != "p9" {
		t.Fatalf("route metadata=%+v want remote owner redirect for group-z/node-z token p9", route)
	}
	if len(route.Members) != 2 || route.Members[0] != "node-z" || route.Members[1] != "node-y" {
		t.Fatalf("route members=%v want [node-z node-y]", route.Members)
	}
	if calls := groupA.snapshot(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
	if calls := groupB.snapshot(); len(calls) != 0 {
		t.Fatalf("group-b calls=%d want 0", len(calls))
	}
}

func TestGroupRoutedSubmitterUnknownOwnerHasStableRouteClassBeforeSubmit(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA)

	meta := routeMetadata("group-z", iwire.AckVisible)
	meta.ClusterRouteLeaderHint = ""
	meta.ClusterRouteMembers = nil
	_, err := dispatcher.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), meta)
	if !errors.Is(err, ErrRouteTargetUnknown) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want route target unknown", err)
	}
	route, ok := RouteErrorMetadataOf(err)
	if !ok {
		t.Fatalf("RouteErrorMetadataOf ok=false err=%v", err)
	}
	if route.Class != RouteErrorClassUnknownOwner || route.GroupID != "group-z" || route.LeaderHint != "" {
		t.Fatalf("route metadata=%+v want unknown owner group-z without leader hint", route)
	}
	if calls := groupA.snapshot(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
}

func TestGroupRoutedSubmitterMissingOwnerHasStableRouteClassBeforeSubmit(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA)

	meta := routeMetadata("", iwire.AckVisible)
	_, err := dispatcher.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), meta)
	if !errors.Is(err, ErrRouteTargetMissing) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want route target missing", err)
	}
	route, ok := RouteErrorMetadataOf(err)
	if !ok {
		t.Fatalf("RouteErrorMetadataOf ok=false err=%v", err)
	}
	if route.Class != RouteErrorClassMissingOwner || route.GroupID != "" || route.Collection != "users" {
		t.Fatalf("route metadata=%+v want missing owner for users route", route)
	}
	if calls := groupA.snapshot(); len(calls) != 0 {
		t.Fatalf("group-a calls=%d want 0", len(calls))
	}
}

func TestGroupSubmitterRegistryRejectsMismatchedConfiguredGroup(t *testing.T) {
	_, err := NewGroupSubmitterRegistryV1([]GroupSubmitterV1{
		{GroupID: "group-a", Submitter: &recordingGroupSubmitter{groupID: "group-b"}},
	})
	if !errors.Is(err, ErrInvalidSubmitter) {
		t.Fatalf("NewGroupSubmitterRegistryV1 err=%v want invalid submitter", err)
	}
}

func TestGroupRoutedSubmitterAdmissionMissingProviderFailsClosed(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a", status: LeaderAdmission()}
	registry, err := NewGroupSubmitterRegistryV1([]GroupSubmitterV1{
		{GroupID: "group-a", Submitter: groupA},
		{GroupID: "group-b", Submitter: &submitOnlyGroupSubmitter{groupID: "group-b"}},
	})
	if err != nil {
		t.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := NewCatalogMetaGroupRoutedSubmitter(registry, &recordingCatalogRouteValidator{})
	if err != nil {
		t.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	status, err := dispatcher.ClusterAdmissionStatus(context.Background())
	if err != nil {
		t.Fatalf("ClusterAdmissionStatus: %v", err)
	}
	if !status.Unavailable || !strings.Contains(status.Reason, "group \"group-b\" admission provider is unavailable") {
		t.Fatalf("ClusterAdmissionStatus=%+v want unavailable missing group-b admission provider", status)
	}
}

func TestGroupRoutedSubmitterRequiresFeatureWhenAnyGroupEnablesIt(t *testing.T) {
	features := DefaultFeatureSet()
	features.Required = append(features.Required, RequiredFeature{
		Name:    FeatureVectorPartitionLifecycle,
		Version: SupportedFeatureFloors[FeatureVectorPartitionLifecycle],
	})
	dispatcher := newTestGroupRoutedSubmitter(t,
		&recordingGroupSubmitter{groupID: "group-a"},
		&recordingGroupSubmitter{groupID: "group-b", features: features},
	)
	required, err := dispatcher.RequiresFeatureV1(FeatureVectorPartitionLifecycle)
	if err != nil {
		t.Fatalf("RequiresFeatureV1: %v", err)
	}
	if !required {
		t.Fatal("vector partition lifecycle requirement=false want true from group-b config")
	}
}

func TestGroupRoutedSubmitterDelegatesSerializedPreCommitV1(t *testing.T) {
	groupA := &recordingGroupSubmitter{groupID: "group-a"}
	dispatcher := newTestGroupRoutedSubmitter(t, groupA)
	called := false
	if _, err := dispatcher.SubmitCommandEntryWithPreCommitV1(context.Background(), testClusterCommandEntry(t, 7), routeMetadata("group-a", iwire.AckRaftCommitted), func(context.Context) error {
		called = true
		if calls := groupA.snapshot(); len(calls) != 0 {
			t.Fatalf("data submit ran before pre-commit callback: calls=%d", len(calls))
		}
		return nil
	}); err != nil {
		t.Fatalf("SubmitCommandEntryWithPreCommitV1: %v", err)
	}
	if !called || len(groupA.snapshot()) != 1 {
		t.Fatalf("callback/submits=%v/%d want true/1", called, len(groupA.snapshot()))
	}
}

func TestGroupRoutedSubmitterRejectsMissingSerializedPreCommitV1(t *testing.T) {
	groupA := &submitOnlyGroupSubmitter{groupID: "group-a"}
	registry, err := NewGroupSubmitterRegistryV1([]GroupSubmitterV1{{GroupID: "group-a", Submitter: groupA}})
	if err != nil {
		t.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := NewCatalogMetaGroupRoutedSubmitter(registry, &recordingCatalogRouteValidator{})
	if err != nil {
		t.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	called := false
	_, err = dispatcher.SubmitCommandEntryWithPreCommitV1(context.Background(), testClusterCommandEntry(t, 7), routeMetadata("group-a", iwire.AckRaftCommitted), func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrInvalidSubmitter) {
		t.Fatalf("SubmitCommandEntryWithPreCommitV1 err=%v want invalid submitter", err)
	}
	if called {
		t.Fatal("pre-commit callback ran without a serialized target submitter")
	}
}

func newTestGroupRoutedSubmitter(tb testing.TB, submitters ...*recordingGroupSubmitter) *GroupRoutedSubmitter {
	tb.Helper()
	entries := make([]GroupSubmitterV1, len(submitters))
	for i, submitter := range submitters {
		entries[i] = GroupSubmitterV1{GroupID: submitter.groupID, Submitter: submitter}
	}
	registry, err := NewGroupSubmitterRegistryV1(entries)
	if err != nil {
		tb.Fatalf("NewGroupSubmitterRegistryV1: %v", err)
	}
	dispatcher, err := NewCatalogMetaGroupRoutedSubmitter(registry, &recordingCatalogRouteValidator{})
	if err != nil {
		tb.Fatalf("NewCatalogMetaGroupRoutedSubmitter: %v", err)
	}
	return dispatcher
}
