package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

func cowOrderedAdmissionWrite(db *DB, key, value []byte, input mvccadmission.Input, sync bool) error {
	b := db.NewBatchWithSize(1)
	err := b.SetWithMVCCInput(key, value, input)
	if err == nil {
		if sync {
			err = b.WriteSync()
		} else {
			err = b.Write()
		}
	}
	return errors.Join(err, b.Close())
}

func cowOrderedAdmissionFloor(t *testing.T, db *DB, owner *mvccadmission.Capability) uint64 {
	t.Helper()
	key := []byte{0, 'T', 'D', 'B', 'M', 'V', 'C', 'C', 0, 'M', 1, 'd', 'f'}
	record := make([]byte, 9)
	record[0] = 1
	binary.BigEndian.PutUint64(record[1:], 1)
	if err := cowOrderedAdmissionWrite(db, key, record, owner.FloorInput(key, record, 1), false); err != nil {
		t.Fatal(err)
	}
	observed, err := db.Get(key)
	if err != nil || !bytes.Equal(observed, record) {
		t.Fatalf("actual admitted floor=%x error=%v", observed, err)
	}
	return binary.BigEndian.Uint64(observed[1:])
}

func cowOrderedAdmissionCapture(t *testing.T, owner *mvccadmission.Capability, floor uint64) mvccadmission.Request {
	t.Helper()
	work := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	request, ok, err := owner.CaptureRequest(floor, &work)
	if err != nil || !ok {
		t.Fatalf("original owner capture: available=%t error=%v", ok, err)
	}
	return request
}

func cowOrderedAdmissionValidate(request mvccadmission.Request) (bool, error) {
	work := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	return request.Validate(&work)
}

func TestCOWOrderedOriginalAuthorityConsumedOnlyAtAcceptedCut(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	floor := cowOrderedAdmissionFloor(t, db, owner)
	request := cowOrderedAdmissionCapture(t, owner, floor)
	qualified := cowOrderedLifecycleKey(t, "qualified", 2)
	if err := cowOrderedAdmissionWrite(db, qualified, []byte{1, 'v'}, owner.Input(qualified, 2, floor), false); err != nil {
		t.Fatal(err)
	}
	if ok, err := cowOrderedAdmissionValidate(request); err != nil || !ok {
		t.Fatalf("same original owner changed its input epoch: available=%t error=%v", ok, err)
	}

	refusedKey := []byte("refused-raw")
	injected := errors.New("refuse canonical COW preparation")
	err := func() error {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Root == db.dir && event.Point == durabilitycut.AfterCOWCanonicalPreparation {
				return injected
			}
			return nil
		})
		defer restore()
		return cowOrderedAdmissionWrite(db, refusedKey, []byte("not installed"), mvccadmission.Input{}, false)
	}()
	if !errors.Is(err, injected) {
		t.Fatalf("failed prepare error=%v", err)
	}
	if value, err := db.Get(refusedKey); err != nil || value != nil {
		t.Fatalf("refused raw write became reachable: value=%x error=%v", value, err)
	}
	if ok, err := cowOrderedAdmissionValidate(request); err != nil || !ok {
		t.Fatalf("canceled raw preparation invalidated the original owner: available=%t error=%v", ok, err)
	}

	if err := cowOrderedAdmissionWrite(db, []byte("accepted-raw"), []byte("installed"), mvccadmission.Input{}, false); err != nil {
		t.Fatal(err)
	}
	if ok, err := cowOrderedAdmissionValidate(request); ok || !errors.Is(err, mvccadmission.ErrRequestInvalid) {
		t.Fatalf("accepted raw cut failed to invalidate the original request: available=%t error=%v", ok, err)
	}
	// A legal later write can preserve a newly observed epoch, but cannot repair
	// the old request invalidated by the actual raw publication.
	current := cowOrderedAdmissionCapture(t, owner, floor)
	next := cowOrderedLifecycleKey(t, "qualified", 3)
	if err := cowOrderedAdmissionWrite(db, next, []byte{1, 'w'}, owner.Input(next, 3, floor), false); err != nil {
		t.Fatal(err)
	}
	if ok, err := cowOrderedAdmissionValidate(current); err != nil || !ok {
		t.Fatalf("qualified write changed the current epoch: available=%t error=%v", ok, err)
	}
	if ok, err := cowOrderedAdmissionValidate(request); ok || !errors.Is(err, mvccadmission.ErrRequestInvalid) {
		t.Fatalf("qualified write repaired an invalid old request: available=%t error=%v", ok, err)
	}
}

type cowOrderedAdmissionCheckpointFailure struct {
	*backenddb.DB
	err error
}

func (b *cowOrderedAdmissionCheckpointFailure) Checkpoint() error { return b.err }

func TestCOWOrderedInstalledAuthoritySurvivesCheckpointError(t *testing.T) {
	db, owner := cowOrderedLifecycleFixture(t)
	floor := cowOrderedAdmissionFloor(t, db, owner)
	request := cowOrderedAdmissionCapture(t, owner, floor)
	original := db.backend
	injected := errors.New("report error after logical COW installation")
	db.backend = &cowOrderedAdmissionCheckpointFailure{DB: original.(*backenddb.DB), err: injected}
	defer func() { db.backend = original }()
	checked, invalidBeforePhysical := false, false
	db.testBeforeCheckpointFrontierCapture = func() {
		checked = true
		ok, err := cowOrderedAdmissionValidate(request)
		invalidBeforePhysical = !ok && errors.Is(err, mvccadmission.ErrRequestInvalid)
	}
	defer func() { db.testBeforeCheckpointFrontierCapture = nil }()
	key, value := []byte("raw-with-late-error"), []byte("accepted")
	if err := cowOrderedAdmissionWrite(db, key, value, mvccadmission.Input{}, true); !errors.Is(err, injected) {
		t.Fatalf("expected post-install checkpoint error, got %v", err)
	}
	if !checked || !invalidBeforePhysical {
		t.Fatalf("logical installation did not consume original authority before physical checkpoint: checked=%t invalid=%t", checked, invalidBeforePhysical)
	}
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("accepted error lost installed raw value: value=%x error=%v", got, err)
	}
	if ok, err := cowOrderedAdmissionValidate(request); ok || !errors.Is(err, mvccadmission.ErrRequestInvalid) {
		t.Fatalf("checkpoint error rolled back installed authority: available=%t error=%v", ok, err)
	}
}

func TestCOWOrderedReusedBatchDropsPreviousInputSummary(t *testing.T) {
	for _, publishRaw := range []bool{false, true} {
		name := "reset-unpublished"
		if publishRaw {
			name = "reset-after-publication"
		}
		t.Run(name, func(t *testing.T) {
			db, owner := cowOrderedLifecycleFixture(t)
			floor := cowOrderedAdmissionFloor(t, db, owner)
			b := db.NewBatchWithSize(1)
			defer b.Close()
			if err := b.Set([]byte("previous-raw"), []byte("raw")); err != nil {
				t.Fatal(err)
			}
			if publishRaw {
				// Successful publication resets this SAME batch for reuse.
				if err := b.Write(); err != nil {
					t.Fatal(err)
				}
			} else {
				b.Reset()
			}
			request := cowOrderedAdmissionCapture(t, owner, floor)
			key := cowOrderedLifecycleKey(t, "current-qualified", 2)
			value := []byte{1, 'v'}
			if err := b.SetWithMVCCInput(key, value, owner.Input(key, 2, floor)); err != nil {
				t.Fatal(err)
			}
			if err := b.Write(); err != nil {
				t.Fatal(err)
			}
			if ok, err := cowOrderedAdmissionValidate(request); err != nil || !ok {
				t.Fatalf("previous batch input leaked into current qualified publication: available=%t error=%v", ok, err)
			}
			if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
				t.Fatalf("reused qualified batch value=%x error=%v", got, err)
			}
		})
	}
}
