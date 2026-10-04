package raftcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// This submit-boundary stand-in proves only gate ordering and byte preservation.
// The RF4 native and production FSM tests prove actual covered witness authority.
func TestSingleGroupSubmitterStaleColocatedRequiresKnownReplayV1(t *testing.T) {
	conflict, staleOwner := errors.New("atomic witness command/scope conflict"), errors.New("current owner authority stale")
	for _, command := range []iwire.CommandID{iwire.CommandReplaceBatch, iwire.CommandDeleteBatch} {
		for _, tc := range []struct {
			name                      string
			scope, callback, known    bool
			shape                     string
			preErr, ownerErr, wantErr error
			preflight, owner, commit  int
		}{
			{name: "exact_original", scope: true, callback: true, known: true, shape: "vector_partition_exact_id", preflight: 1, owner: 1, commit: 1},
			{name: "unknown_first_write", scope: true, callback: true, shape: "vector_partition_exact_id", preflight: 1, wantErr: ErrCatalogVersionMismatch},
			{name: "wrong_command_or_scope", scope: true, callback: true, shape: "vector_partition_exact_id", preErr: conflict, preflight: 1, wantErr: conflict},
			{name: "changed_current_authority", scope: true, callback: true, known: true, shape: "vector_partition_exact_id", ownerErr: staleOwner, preflight: 1, owner: 1, wantErr: staleOwner},
			{name: "ordinary_no_scope", callback: true, known: true, shape: "vector_partition_exact_id", wantErr: ErrCatalogVersionMismatch},
			{name: "missing_owner_callback", scope: true, known: true, shape: "vector_partition_exact_id", wantErr: ErrCatalogVersionMismatch},
			{name: "generic_route", scope: true, callback: true, known: true, shape: "collection", wantErr: ErrCatalogVersionMismatch},
		} {
			t.Run(fmt.Sprint(command)+"/"+tc.name, func(t *testing.T) {
				sections := []iwire.Section{
					{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})},
					{ID: iwire.SectionIdempotencyKey, Bytes: []byte("original")},
					{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 7)},
					{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "users"...)},
					{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
				}
				if command == iwire.CommandReplaceBatch {
					sections = append(sections,
						iwire.Section{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
						iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"embedding":[0,1]}`))},
						iwire.Section{ID: iwire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)})
				}
				if tc.scope {
					scope, err := commitlog.EncodeColocatedVectorMutationScopeV1(commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: "embedding", Generation: 1, OwnerGroup: "group-a", Digest: [32]byte{1}})
					if err != nil {
						t.Fatal(err)
					}
					sections = append(sections, iwire.Section{ID: iwire.SectionColocatedVectorMutationScopeV1, Bytes: scope})
				}
				validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
				if err != nil {
					t.Fatal(err)
				}
				entry, err := iwire.AppendDeterministicEntry(nil, validated)
				if err != nil {
					t.Fatal(err)
				}
				preflightCalls, ownerCalls, commitCalls := 0, 0, 0
				applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}}
				submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
					AdmissionProvider:      StaticAdmissionProvider{Status: LeaderAdmission()},
					CatalogVersionProvider: staticCatalogVersion(8),
					Preflight: CommandEntryPreflightFunc(func(_ context.Context, req CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
						preflightCalls++
						if req.CurrentCatalogVersion != 8 || req.DecodedEntry.Target.CommandID != command || !bytes.Equal(req.EntryBytes, entry) {
							t.Fatal("preflight changed original identity or current guard")
						}
						return CommandEntryPreflightResultV1{KnownIdempotencyReplay: tc.known}, tc.preErr
					}),
					CommitSource: CommitSourceFunc(func(_ context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
						commitCalls++
						if !bytes.Equal(req.EntryBytes, entry) {
							t.Fatal("commit changed original bytes")
						}
						return productionCommittedResult(req, 3, 1), nil
					}),
					Applier: applier,
				})
				metadata := raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted, ClusterRouteShape: tc.shape}
				var result SubmitResultV1
				if tc.callback {
					result, err = submitter.SubmitCommandEntryWithPreCommitV1(t.Context(), entry, metadata, func(context.Context) error { ownerCalls++; return tc.ownerErr })
				} else {
					result, err = submitter.SubmitCommandEntryV1(t.Context(), entry, metadata)
				}
				if tc.wantErr == nil {
					if err != nil || !result.CommittedRecoverable || !result.CommittedApplied || result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
						t.Fatalf("retry=%+v err=%v", result, err)
					}
				} else if !errors.Is(err, tc.wantErr) {
					t.Fatalf("refusal=%v want=%v", err, tc.wantErr)
				}
				if preflightCalls != tc.preflight || ownerCalls != tc.owner || commitCalls != tc.commit || len(applier.snapshot()) != tc.commit {
					t.Fatalf("preflight/owner/commit/apply=%d/%d/%d/%d want=%d/%d/%d/%d", preflightCalls, ownerCalls, commitCalls, len(applier.snapshot()), tc.preflight, tc.owner, tc.commit, tc.commit)
				}
			})
		}
	}
}
