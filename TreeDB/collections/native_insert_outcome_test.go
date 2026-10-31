package collections

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Exercise the same final-row unique rejection with and without an external
// apply owner. This seam returns before intent construction and CommitNative:
// neither journal assignment nor logical installation may occur. Apply owners
// retain their prior authority and must never receive split/replay permission.
func TestNativeInsertFinalPreparationOutcomeBoundary(t *testing.T) {
	for _, applyOwner := range []bool{false, true} {
		t.Run(fmt.Sprintf("apply_%t", applyOwner), func(t *testing.T) {
			backend, _, col := openNativeProducerAdmissionFixture(t, &CollectionMeta{Name: "users", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}}})
			before, ok := backend.StateToken()
			if !ok {
				t.Fatal("missing state token")
			}
			read, err := col.nativeJSON.Capture("users")
			if err != nil {
				t.Fatal(err)
			}
			sequence := read.capture.LogicalSequence()
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			next := backend.CommandWALNextLSN()
			frames := collectionCommandWALFrames(t, backend.Dir())
			var appends, installs atomic.Int32
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyAppend {
					appends.Add(1)
				}
				if e.Point == durabilitycut.BeforeCOWCutSwap || e.Point == durabilitycut.AfterLogicalMVCCStaging {
					installs.Add(1)
				}
				return nil
			})
			defer func() {
				if restore != nil {
					restore()
				}
			}()
			tx, err := col.nativeJSON.Begin("users")
			if err != nil {
				t.Fatal(err)
			}
			doc, err := bson.Marshal(bson.D{{Key: "email", Value: "same@example.com"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{doc, doc}); err != nil {
				t.Fatalf("expected final-row conflict, staging returned %v", err)
			}
			var apply *nativeCommandWALApplyCommit
			if applyOwner {
				apply = &nativeCommandWALApplyCommit{}
			}
			err = tx.commitNativeApply(false, apply)
			if !errors.Is(err, ErrUniqueIndexConflict) || errors.Is(err, ErrMutationNotAccepted) == applyOwner || errors.Is(err, ErrCommitAmbiguous) {
				t.Fatalf("final preparation err=%v apply=%t", err, applyOwner)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			restore()
			restore = nil
			if now, ok := backend.StateToken(); !ok || now != before {
				t.Fatalf("checkpoint authority changed: before=%+v after=%+v", before, now)
			}
			if appends.Load() != 0 || installs.Load() != 0 || backend.CommandWALNextLSN() != next || !reflect.DeepEqual(frames, collectionCommandWALFrames(t, backend.Dir())) {
				t.Fatalf("private rejection published: appends=%d installs=%d next=%d want=%d", appends.Load(), installs.Load(), backend.CommandWALNextLSN(), next)
			}
			read, err = col.nativeJSON.Capture("users")
			if err != nil {
				t.Fatal(err)
			}
			if read.capture.LogicalSequence() != sequence {
				t.Fatalf("logical sequence advanced: %d want=%d", read.capture.LogicalSequence(), sequence)
			}
			if err := read.Close(); err != nil {
				t.Fatal(err)
			}
			rows, _, err := col.ScanDocuments(10)
			if err != nil || len(rows) != 0 {
				t.Fatalf("rejected rows=%v err=%v", rows, err)
			}
			indexed, err := col.FindByIndex("email", "same@example.com")
			if err != nil || len(indexed) != 0 {
				t.Fatalf("rejected index=%q err=%v", indexed, err)
			}
		})
	}
}
