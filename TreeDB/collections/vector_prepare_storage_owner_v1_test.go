package collections

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestVectorPrepareStorageOwnerIdentityAndExpiryV1(t *testing.T) {
	open := func() *backenddb.DB {
		db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	db, other := open(), open()
	capture, err := AcquireVectorPrepareStableCaptureV1(db)
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Close()
	var retained *VectorPrepareStorageOwnerV1
	var copied VectorPrepareStorageOwnerV1
	injected := errors.New("callback aborted")
	err = capture.WithStorageBarrierV1(context.Background(), func(owner *VectorPrepareStorageOwnerV1) error {
		retained = owner
		copied = *owner
		if err := owner.ValidateDBV1(db); err != nil {
			t.Fatal(err)
		}
		if err := owner.ValidateDBV1(other); err == nil {
			t.Fatal("owner accepted wrong DB")
		}
		// Refusal happens before semantic preflight or any caller Append callback.
		wrong := &Collection{db: other}
		if err := wrong.WithPreparedCommandWALVectorPrepareOwnedV1(context.Background(), commitlog.VectorPrepareV1{}, owner, func(*CommandWALAdmittedCollection) error { t.Fatal("wrong DB reached Append callback"); return nil }); err == nil {
			t.Fatal("wrong DB executor accepted")
		}

		root := owner.state.root
		owner.state.root = root + "/wrong"
		if err := owner.ValidateDBV1(db); err == nil {
			t.Fatal("owner accepted wrong root")
		}
		owner.state.root = root
		if err := (&VectorPrepareStorageOwnerV1{}).ValidateDBV1(db); err == nil {
			t.Fatal("zero owner acquired authority")
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if err := retained.ValidateDBV1(db); err == nil {
		t.Fatal("error callback retained authority")
	}
	if err := copied.ValidateDBV1(db); err == nil {
		t.Fatal("copied owner retained authority")
	}
	if err := retained.withDBV1(db, func(*backenddb.Snapshot) error { t.Fatal("expired snapshot borrowed"); return nil }); err == nil {
		t.Fatal("expired owner accepted")
	}
	// A subsequent callback mints a distinct owner and never revives the old one.
	if err := capture.WithStorageBarrierV1(context.Background(), func(owner *VectorPrepareStorageOwnerV1) error {
		if owner == retained {
			t.Fatal("owner reused")
		}
		if err := retained.ValidateDBV1(db); err == nil {
			t.Fatal("old owner revived")
		}
		return owner.ValidateDBV1(db)
	}); err != nil {
		t.Fatal(err)
	}
	captureCopy := *capture
	captureCopy.Close()
	capture.Close()
	if err := capture.WithStorageBarrierV1(context.Background(), func(*VectorPrepareStorageOwnerV1) error { t.Fatal("closed capture callback ran"); return nil }); err == nil {
		t.Fatal("closed capture accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fresh, err := AcquireVectorPrepareStableCaptureV1(db)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.WithStorageBarrierV1(ctx, func(*VectorPrepareStorageOwnerV1) error { t.Fatal("cancelled callback ran"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
