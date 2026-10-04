package raftapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// The APIs/scoped witness were absent on the construction base (the issue's
// compile-capability exception). These semantic regressions additionally pin
// the crash window that an external result-only witness cannot close.
func TestColocatedVectorMutationAtomicRecoveryV1(t *testing.T) {
	for _, operation := range []string{"replace", "delete", "same-content", "missing-delete"} {
		for _, point := range []FaultPointV1{FaultAfterLocalWALAppendBeforeVisibleV1, FaultAfterVisibleBeforeResultRecordV1, FaultAfterResultRecordBeforeProgressV1, FaultPointV1("result-store-write-failure")} {
			t.Run(fmt.Sprintf("%s/%s", operation, point), func(t *testing.T) {
				dir, database, c, manifest := newSplitApplyRecoveryFixtureV1(t)
				defer func() { _ = database.Close() }()
				scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: "group-b", Digest: sha256.Sum256([]byte("colocated-scope"))}
				id, document, deletion := []byte("base-x"), []byte(`{"embedding":[0,1],"kind":"replacement"}`), false
				matched, affected := uint64(1), uint64(1)
				switch operation {
				case "delete":
					deletion, matched = true, 0
				case "same-content":
					// Same column/retained content, with reordered fields and numbers.
					document = []byte(` {"kind":"base-x","embedding":[1e0,0.0]} `)
					affected = 0
				case "missing-delete":
					deletion, matched, affected, id = true, 0, 0, []byte("missing")
				}
				beforeSource, err := c.Get(id)
				if err != nil {
					t.Fatal(err)
				}
				beforePin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				beforeLive := beforePin.StatusV1()
				beforePin.Release()
				raw := colocatedApplyEntryV1(t, scope, id, document, deletion, []byte("original"))
				meta := applyMeta(3, 1)
				meta.GroupID, meta.SyncLocalCommandWAL = scope.OwnerGroup, true
				beforeLSN, frames := database.State().AppliedCommandLSN, len(readCommandWALFrames(t, dir))
				if _, err := PreflightCommandEntryV1(database, raw, meta, Options{}); err != nil {
					t.Fatal(err)
				}
				wrong := meta
				wrong.GroupID = "forged"
				if _, err := PreflightCommandEntryV1(database, raw, wrong, Options{}); err == nil {
					t.Fatal("wrong actual FSM group admitted")
				}
				if len(readCommandWALFrames(t, dir)) != frames {
					t.Fatal("preflight appended")
				}
				results, progress := NewMemoryApplyResultStore(16), NewMemoryApplyProgressStore(16, 16)
				fault := &colocatedRecordedFaultV1{singlePointFaultInjector: singlePointFaultInjector{point: point}}
				failedStore := &colocatedRecordedResultFailureV1{}
				options := Options{ResultStore: results, ProgressStore: progress, FaultInjector: fault}
				if point == FaultPointV1("result-store-write-failure") {
					options.ResultStore = failedStore
					options.FaultInjector = nil
				}
				result, err := ApplyCommittedEntryV1(database, raw, meta, options)
				if err == nil || result.Status != raftentry.ApplyStatusRecoveryRequired || (point == FaultPointV1("result-store-write-failure") && !failedStore.reached) || (point != FaultPointV1("result-store-write-failure") && !fault.reached) {
					t.Fatalf("fault=%+v err=%v", result, err)
				}
				digest := [32]byte(raftentry.CommandDigestV1ForBytes(raw, raftentry.DecodeOptions{}))
				original, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
				if point == FaultAfterLocalWALAppendBeforeVisibleV1 {
					if known || err != nil || database.State().AppliedCommandLSN != beforeLSN {
						t.Fatalf("unpublished original=%+v known=%v err=%v", original, known, err)
					}
				} else if err != nil || !known || original.Matched != matched || original.Affected != affected || original.Term != 3 || original.Index != 1 {
					t.Fatalf("visible original=%+v known=%v err=%v", original, known, err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = openApplyHarnessDBWithOptions(t, dir, backenddb.Options{ResolvedProfile: backenddb.ProfileCommandWALDurable})
				c, err = collections.NewCollectionManager(database).OpenCollection(manifest.Collection)
				if err != nil {
					t.Fatal(err)
				}
				recovered, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
				if err != nil || !known || recovered.Matched != matched || recovered.Affected != affected || recovered.Term != 3 || recovered.Index != 1 || (original.AppliedCommandLSN != 0 && recovered != original) {
					t.Fatalf("reopen original=%+v recovered=%+v known=%v err=%v", original, recovered, known, err)
				}
				if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
					t.Fatal(err)
				}
				pin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				status := pin.StatusV1()
				pin.Release()
				if status.Coverage < recovered.Coverage || status.Revision < recovered.Revision {
					t.Fatalf("recovered live floor=%+v witness=%+v", status, recovered)
				}
				if affected == 0 && (recovered.Coverage != beforeLive.Coverage || recovered.Revision != beforeLive.Revision || status.Coverage != beforeLive.Coverage || status.Revision != beforeLive.Revision) {
					t.Fatalf("no-op changed live/source floor before=%+v live=%+v witness=%+v", beforeLive, status, recovered)
				}
				if affected != 0 && (recovered.Coverage <= beforeLive.Coverage || recovered.Revision <= beforeLive.Revision) {
					t.Fatalf("changed content failed to advance before=%+v witness=%+v", beforeLive, recovered)
				}
				afterSource, err := c.Get(id)
				if err != nil || (affected == 0 && !bytes.Equal(beforeSource, afterSource)) {
					t.Fatalf("no-op changed source before=%q after=%q err=%v", beforeSource, afterSource, err)
				}
				// A later operation is deliberately visible before the lost original reply is
				// retried. Even missing/no-op originals must not be reinterpreted or reapplied.
				laterDocument := []byte(`{"embedding":[-1,0],"kind":"later"}`)
				if deletion {
					if _, err := c.Insert(id, laterDocument); err != nil {
						t.Fatal(err)
					}
				} else {
					results, err := c.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: id, Update: func([]byte) ([]byte, bool, error) { return laterDocument, true, nil }}})
					if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
						t.Fatalf("later replacement results=%+v err=%v", results, err)
					}
				}
				before, err := c.Get(id)
				if err != nil {
					t.Fatal(err)
				}
				beforeRetry := database.State().AppliedCommandLSN
				// Fresh Harness progress starts at 1; retry the same committed entry.
				meta.EntryID.Index = 1
				result, err = ApplyCommittedEntryV1(database, raw, meta, Options{ResultStore: NewMemoryApplyResultStore(16), ProgressStore: NewMemoryApplyProgressStore(16, 16)})
				if err != nil || result.Status != raftentry.ApplyStatusAlreadyApplied || result.MatchedCount != 0 || result.AffectedCount != 0 {
					t.Fatalf("original retry=%+v err=%v", result, err)
				}
				after, err := c.Get(id)
				if err != nil || !bytes.Equal(before, after) || database.State().AppliedCommandLSN != beforeRetry {
					t.Fatalf("old retry reapplied later source before=%q after=%q err=%v", before, after, err)
				}
				got, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
				if err != nil || !known || got != recovered {
					t.Fatalf("old outcome changed: %+v %+v %v", got, recovered, err)
				}
				wrongScope := scope
				wrongScope.Digest[0] ^= 1
				if _, _, err := c.ReadVectorPartitionColocatedOutcomeV1(wrongScope, []byte("original"), digest); err == nil {
					t.Fatal("wrong scope accepted retained outcome")
				}
				wrongDigest := digest
				wrongDigest[0] ^= 1
				if _, _, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), wrongDigest); err == nil {
					t.Fatal("wrong command accepted retained outcome")
				}
			})
		}
	}
}

func colocatedApplyEntryV1(t testing.TB, scope commitlog.ColocatedVectorMutationScopeV1, id, document []byte, deletion bool, key []byte) []byte {
	t.Helper()
	raw, err := commitlog.EncodeColocatedVectorMutationScopeV1(scope)
	if err != nil {
		t.Fatal(err)
	}
	command := nativewire.CommandReplaceBatch
	if deletion {
		command = nativewire.CommandDeleteBatch
	}
	sections := []nativewire.Section{{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: command, Version: 1})}, {ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef("docs")}, {ID: nativewire.SectionIdempotencyKey, Bytes: key}, {ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)}, {ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, id)}, {ID: nativewire.SectionColocatedVectorMutationScopeV1, Bytes: raw}}
	if !deletion {
		sections = append(sections, nativewire.Section{ID: nativewire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(nativewire.DocumentFormatJSON))}, nativewire.Section{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, document)}, nativewire.Section{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)})
	}
	validated, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

// An ordinary decoded command carries no new retained state and takes the
// absent-scope branch before JSON decoding, lookup or source lowering.
func TestColocatedVectorMutationOrdinaryAdmissionAllocationV1(t *testing.T) {
	var entry raftentry.CommandEntryV1
	entry.Decoded.Sections = []nativewire.Section{{ID: nativewire.SectionDocumentIDs, Bytes: []byte("ordinary")}}
	if allocations := testing.AllocsPerRun(100, func() {
		_, scoped, err := colocatedVectorMutationInputsV1(entry, ApplyMetadataV1{})
		if scoped || err != nil {
			t.Fatal("ordinary command entered scoped admission")
		}
	}); allocations != 0 {
		t.Fatalf("ordinary scope probe allocs=%g", allocations)
	}
}

func TestColocatedVectorMutationApplyErrorClassificationV1(t *testing.T) {
	_, database, c, manifest := newSplitApplyRecoveryFixtureV1(t)
	defer func() { _ = database.Close() }()
	scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: "group-b", Digest: sha256.Sum256([]byte("colocated-scope"))}
	id, document, attempt := []byte("base-x"), []byte(`{"embedding":[0,1],"kind":"replacement"}`), []byte("classification")
	raw := colocatedApplyEntryV1(t, scope, id, document, false, attempt)
	meta := applyMeta(3, 1)
	meta.SyncLocalCommandWAL = true
	before, err := c.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	beforeLSN := database.State().AppliedCommandLSN
	for _, group := range []string{"", "forged"} {
		meta.GroupID = group
		result, err := ApplyCommittedEntryV1(database, raw, meta, Options{})
		assertRejected(t, result, err, raftentry.ApplyStatusDeterministicGuardFailure, raftentry.ErrorTargetMismatchV1)
		after, err := c.Get(id)
		if err != nil || !bytes.Equal(before, after) || database.State().AppliedCommandLSN != beforeLSN {
			t.Fatalf("input rejection mutated source: err=%v before=%q after=%q", err, before, after)
		}
	}
	meta.GroupID = scope.OwnerGroup
	result, err := ApplyCommittedEntryV1(database, raw, meta, Options{})
	if err != nil || result.Status != raftentry.ApplyStatusApplied {
		t.Fatalf("valid original result=%+v err=%v", result, err)
	}
	// An unreadable/conflicting covered witness is persistent-state uncertainty,
	// which must keep the recovery path rather than look like an input rejection.
	scope.Digest[0] ^= 1
	conflict := colocatedApplyEntryV1(t, scope, id, document, false, attempt)
	result, err = ApplyCommittedEntryV1(database, conflict, meta, Options{})
	assertRecoveryRequired(t, result, err, raftentry.ErrorUnsafeDurabilityModeV1)
}

// Record arrival at the intended cut so a preapply error cannot masquerade as
// a crash-window regression.
type colocatedRecordedFaultV1 struct {
	singlePointFaultInjector
	reached bool
}

func (f *colocatedRecordedFaultV1) InjectApplyFault(point FaultPointV1, ctx ApplyFaultContextV1) error {
	if point == f.point {
		f.reached = true
	}
	return f.singlePointFaultInjector.InjectApplyFault(point, ctx)
}

type colocatedRecordedResultFailureV1 struct {
	recordApplyResultStoreFailAfterPreflight
	reached bool
}

func (s *colocatedRecordedResultFailureV1) RecordApplyResult(record ApplyResultRecordV1) error {
	s.reached = true
	return s.recordApplyResultStoreFailAfterPreflight.RecordApplyResult(record)
}
