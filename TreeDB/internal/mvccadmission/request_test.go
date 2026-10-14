package mvccadmission

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"testing"
)

func requestWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}

func TestRequestSameAuthorityOwnedPublications(t *testing.T) {
	var a Authority
	owner := a.Issue()
	r, done, err := owner.CaptureRequest(2, requestWork())
	if !done || err != nil || r.Floor() != 2 {
		t.Fatalf("capture %v %v", done, err)
	}
	original := r
	key, err := mvcckey.Append(nil, []byte("future"), 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 80; i++ {
		cut := a.Begin()
		cut.Observe(key, owner.Input(key, 100, 2))
		cut.Commit()
		if done, err := r.Validate(requestWork()); !done || err != nil {
			t.Fatalf("legal overwrite %d: %v %v", i, done, err)
		}
		if r != original {
			t.Fatal("request changed after legal publication")
		}
	}
	old, err := mvcckey.Append(nil, []byte("selected"), 1)
	if err != nil {
		t.Fatal(err)
	}
	cut := a.Begin()
	if done, err := cut.ObservePrune(r, old, requestWork()); !done || err != nil {
		cut.Abort()
		t.Fatalf("prune observation %v %v", done, err)
	}
	cut.Commit()
	if done, err := r.Validate(requestWork()); !done || err != nil {
		t.Fatalf("own prune invalidated request %v %v", done, err)
	}
	cut = a.Begin()
	cut.Refuse()
	cut.Commit()
	if done, err := r.Validate(requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
		t.Fatalf("foreign publication %v %v", done, err)
	}
	// Later legal writes cannot repair the original request's sticky epoch.
	cut = a.Begin()
	cut.Observe(key, owner.Input(key, 100, 2))
	cut.Commit()
	if done, err := r.Validate(requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
		t.Fatalf("sticky invalidation %v %v", done, err)
	}
}

func TestRequestRefusalOwnerAndPhysicalPredicates(t *testing.T) {
	var a Authority
	owner := a.Issue()
	zero := &iterator.OrdinalScanWork{}
	if r, done, err := owner.CaptureRequest(2, zero); r != (Request{}) || done || err != nil {
		t.Fatal("zero-work capture observed owner")
	}
	r, _, _ := owner.CaptureRequest(2, requestWork())
	if done, err := r.Validate(zero); done || err != nil {
		t.Fatal("zero-work validation consumed owner")
	}
	old, _ := mvcckey.Append(nil, []byte("k"), 1)
	cut := a.Begin()
	if done, err := cut.ObservePrune(r, old, zero); done || err != nil || cut.observed {
		cut.Abort()
		t.Fatal("refusal changed cut")
	}
	cut.Abort()
	for _, key := range [][]byte{nil, discardFloorMetadataKey[:], []byte("raw"), append([]byte(nil), old[:len(old)-1]...)} {
		cut = a.Begin()
		if done, err := cut.ObservePrune(r, key, requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
			cut.Abort()
			t.Fatal("non-version operand accepted")
		}
		cut.Abort()
	}
	future, _ := mvcckey.Append(nil, []byte("k"), 100)
	cut = a.Begin()
	if done, err := cut.ObservePrune(r, future, requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
		cut.Abort()
		t.Fatal("future version accepted")
	}
	cut.Abort()
	var foreign Authority
	foreign.Issue()
	cut = foreign.Begin()
	if done, err := cut.ObservePrune(r, old, requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
		cut.Abort()
		t.Fatal("different DB incarnation accepted")
	}
	cut.Abort()
	a.Issue()
	if done, err := r.Validate(requestWork()); done || !errors.Is(err, ErrRequestInvalid) {
		t.Fatal("replacement owner preserved request")
	}
}

func TestRequestControlZeroAllocations(t *testing.T) {
	var a Authority
	owner := a.Issue()
	r, _, _ := owner.CaptureRequest(2, requestWork())
	key, _ := mvcckey.Append(nil, []byte("selected"), 1)
	w := iterator.OrdinalScanWork{}
	allocs := testing.AllocsPerRun(100, func() {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		captured, done, err := owner.CaptureRequest(2, &w)
		if !done || err != nil || captured != r {
			panic("capture")
		}
		if done, err := r.Validate(&w); !done || err != nil {
			panic("validate")
		}
		cut := a.Begin()
		if done, err := cut.ObservePrune(r, key, &w); !done || err != nil {
			cut.Abort()
			panic("observe")
		}
		cut.Abort()
	})
	if allocs != 0 {
		t.Fatalf("request controls allocate: %g", allocs)
	}
}
