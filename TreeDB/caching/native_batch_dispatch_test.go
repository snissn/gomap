package caching

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

// Embedding the native batch must not let dispatch bypass a decorator's methods.
type dispatchDecorator struct {
	*backenddb.Batch
	calls []string
	err   error
}

func (b *dispatchDecorator) SetView(k, v []byte) error {
	b.calls = append(b.calls, "setView")
	return b.err
}
func (b *dispatchDecorator) DeleteView(k []byte) error {
	b.calls = append(b.calls, "deleteView")
	return b.err
}
func (b *dispatchDecorator) SetPointerView(k []byte, p page.ValuePtr) error {
	b.calls = append(b.calls, "pointerView")
	return b.err
}
func (b *dispatchDecorator) SetViewWithRevision(k, v []byte, r page.EntryRevision) error {
	b.calls = append(b.calls, "setRevisionView")
	return b.err
}
func (b *dispatchDecorator) DeleteViewWithRevision(k []byte, r page.EntryRevision) error {
	b.calls = append(b.calls, "deleteRevisionView")
	return b.err
}
func (b *dispatchDecorator) SetPointerViewWithRevision(k []byte, p page.ValuePtr, r page.EntryRevision) error {
	b.calls = append(b.calls, "pointerRevisionView")
	return b.err
}

type dispatchOpaque struct {
	batch.Interface
	calls []string
	ops   []batch.Entry
	err   error
}

func (b *dispatchOpaque) Set(k, v []byte) error { b.calls = append(b.calls, "set"); return b.err }
func (b *dispatchOpaque) Delete(k []byte) error { b.calls = append(b.calls, "delete"); return b.err }
func (b *dispatchOpaque) SetOps(ops []batch.Entry) error {
	b.calls = append(b.calls, "ops")
	b.ops = append(b.ops, ops...)
	return b.err
}

func dispatchCases() []struct {
	name         string
	op           batch.Entry
	view, opaque string
} {
	return []struct {
		name         string
		op           batch.Entry
		view, opaque string
	}{
		{"put", batch.Entry{Key: []byte("key"), Value: []byte("value")}, "setView", "set"},
		{"delete", batch.Entry{Type: batch.OpDelete, Key: []byte("key")}, "deleteView", "delete"},
		{"pointer", batch.Entry{Key: []byte("key"), IsPtr: true}, "pointerView", "ops"},
		{"revisionPut", batch.Entry{Key: []byte("key"), Value: []byte("value"), Revision: 9}, "setRevisionView", "ops"},
		{"revisionDelete", batch.Entry{Type: batch.OpDelete, Key: []byte("key"), Revision: 9}, "deleteRevisionView", "ops"},
		{"revisionPointer", batch.Entry{Key: []byte("key"), IsPtr: true, Revision: 9}, "pointerRevisionView", "ops"},
	}
}

func TestNativeBatchDispatchWrappedCapabilitiesAndErrors(t *testing.T) {
	sentinel := errors.New("decorator failure")
	for _, tc := range dispatchCases() {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := &dispatchDecorator{err: sentinel}
			if err := appendCanonicalFlushRunChunkToBackendBatch(wrapped, []batch.Entry{tc.op, tc.op}); !errors.Is(err, sentinel) {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(wrapped.calls, []string{tc.view}) {
				t.Fatalf("decorator bypass or continued after error: %v", wrapped.calls)
			}
			opaque := &dispatchOpaque{}
			if err := appendCanonicalFlushRunChunkToBackendBatch(opaque, []batch.Entry{tc.op}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(opaque.calls, []string{tc.opaque}) {
				t.Fatalf("opaque fallback=%v", opaque.calls)
			}
			if tc.opaque == "ops" && !reflect.DeepEqual(opaque.ops, []batch.Entry{tc.op}) {
				t.Fatalf("fallback changed operation: %+v", opaque.ops)
			}
			opaque.err = sentinel
			if err := appendCanonicalFlushRunChunkToBackendBatch(opaque, []batch.Entry{tc.op}); !errors.Is(err, sentinel) {
				t.Fatalf("fallback error=%v", err)
			}
		})
	}
}

func TestNativeBatchDispatchNil(t *testing.T) {
	if err := appendCanonicalFlushRunChunkToBackendBatch(nil, nil); err == nil {
		t.Fatal("nil batch accepted")
	}
	if got := resolveBackendBatchRevisionOps(nil); got != (backendBatchRevisionOps{}) {
		t.Fatal("nil capabilities")
	}
	var native *backenddb.Batch
	// A typed nil retains its dynamic type and capabilities, just as the old assertions did.
	got := resolveBackendBatchRevisionOps(native)
	if got.set != native || got.setView != native || got.del != native || got.delView != native || got.ptr != native || got.ptrView != native {
		t.Fatal("typed nil capabilities changed")
	}
	if err := appendCanonicalFlushRunChunkToBackendBatch(native, nil); err != nil {
		t.Fatal(err)
	}
}

func TestNativeBatchDispatchDurabilityAndViewOwnership(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "wrapped"}[wrapped], func(t *testing.T) {
			dir := t.TempDir()
			opts := backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
			db, err := backenddb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			native, ok := db.NewBatch().(*backenddb.Batch)
			if !ok {
				t.Fatal("fixture did not create native batch")
			}
			defer func() {
				if native != nil {
					_ = native.Close()
				}
			}()
			var target batch.Interface = native
			// This wrapper exposes only the public batch contract and must keep fallback copy semantics.
			if wrapped {
				target = struct{ batch.Interface }{native}
			}
			ops := []batch.Entry{{Key: []byte("put"), Value: []byte("value"), Revision: 9}, {Type: batch.OpDelete, Key: []byte("gone"), Revision: 10}, {Key: []byte("legacy"), Value: []byte("old")}}
			if err := appendCanonicalFlushRunChunkToBackendBatch(target, ops); err != nil {
				t.Fatal(err)
			}
			var seen []batch.Entry
			if err := native.Replay(func(e batch.Entry) error { seen = append(seen, e); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(seen) != 3 {
				t.Fatalf("entries=%d", len(seen))
			}
			for _, e := range seen {
				if string(e.Key) == "legacy" && ((&e.Key[0] == &ops[2].Key[0]) == wrapped || (&e.Value[0] == &ops[2].Value[0]) == wrapped) {
					t.Fatal("legacy view/copy ownership changed")
				}
				if string(e.Key) == "put" && (e.Revision != 9 || &e.Key[0] != &ops[0].Key[0] || &e.Value[0] != &ops[0].Value[0]) {
					t.Fatal("stable revision views lost ownership")
				}
			}
			if err := target.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err := native.Close(); err != nil {
				t.Fatal(err)
			}
			native = nil
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db, err = backenddb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]string{"put": "value", "legacy": "old"} {
				v, err := db.Get([]byte(key))
				if err != nil || string(v) != want {
					t.Fatalf("reopen %s=%q,%v", key, v, err)
				}
			}
		})
	}
}

var dispatchRevisionSink backendBatchRevisionOps

func BenchmarkNativeBatchRevisionDispatch(b *testing.B) {
	native := new(backenddb.Batch)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dispatchRevisionSink = resolveBackendBatchRevisionOps(native)
	}
}
