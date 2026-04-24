package treedbadapter

import (
	"errors"
	"testing"

	treebatch "github.com/snissn/gomap/TreeDB/batch"
)

type stubBatch struct {
	setCalls    int
	deleteCalls int
}

func (s *stubBatch) Set(_, _ []byte) error {
	s.setCalls++
	return nil
}

func (s *stubBatch) Delete(_ []byte) error {
	s.deleteCalls++
	return nil
}

func (s *stubBatch) Write() error                             { return nil }
func (s *stubBatch) WriteSync() error                         { return nil }
func (s *stubBatch) Close() error                             { return nil }
func (s *stubBatch) Replay(func(treebatch.Entry) error) error { return nil }
func (s *stubBatch) GetByteSize() (int, error)                { return 0, nil }

type stubBatchWithView struct {
	stubBatch
	setStealViewCalls int
	setViewCalls      int
	deleteViewCalls   int
	setStealViewErr   error
	setViewErr        error
	deleteViewErr     error
}

func (s *stubBatchWithView) SetStealView(_, _ []byte) error {
	s.setStealViewCalls++
	return s.setStealViewErr
}

func (s *stubBatchWithView) SetView(_, _ []byte) error {
	s.setViewCalls++
	return s.setViewErr
}

func (s *stubBatchWithView) DeleteView(_ []byte) error {
	s.deleteViewCalls++
	return s.deleteViewErr
}

func TestBatchSetDeleteViewFallback(t *testing.T) {
	base := &stubBatch{}
	wrapped := &batch{b: base}
	if err := wrapped.SetStealView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set steal view fallback: %v", err)
	}
	if err := wrapped.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set view fallback: %v", err)
	}
	if err := wrapped.DeleteView([]byte("k")); err != nil {
		t.Fatalf("delete view fallback: %v", err)
	}
	if base.setCalls != 2 {
		t.Fatalf("expected set fallback call count=2, got=%d", base.setCalls)
	}
	if base.deleteCalls != 1 {
		t.Fatalf("expected delete fallback call count=1, got=%d", base.deleteCalls)
	}
}

func TestBatchSetDeleteViewUseOptionalInterfaces(t *testing.T) {
	base := &stubBatchWithView{}
	wrapped := &batch{
		b:          base,
		setSteal:   base,
		setView:    base,
		deleteView: base,
	}
	if err := wrapped.SetStealView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set steal view: %v", err)
	}
	if err := wrapped.SetView([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set view: %v", err)
	}
	if err := wrapped.DeleteView([]byte("k")); err != nil {
		t.Fatalf("delete view: %v", err)
	}
	if base.setStealViewCalls != 1 {
		t.Fatalf("expected set steal view call count=1, got=%d", base.setStealViewCalls)
	}
	if base.setViewCalls != 1 {
		t.Fatalf("expected set view call count=1, got=%d", base.setViewCalls)
	}
	if base.deleteViewCalls != 1 {
		t.Fatalf("expected delete view call count=1, got=%d", base.deleteViewCalls)
	}
	if base.setCalls != 0 {
		t.Fatalf("expected set fallback not called, got=%d", base.setCalls)
	}
	if base.deleteCalls != 0 {
		t.Fatalf("expected delete fallback not called, got=%d", base.deleteCalls)
	}
}

func TestBatchSetDeleteViewPropagateOptionalErrors(t *testing.T) {
	base := &stubBatchWithView{
		setStealViewErr: errors.New("set-steal-view-failed"),
		setViewErr:      errors.New("set-view-failed"),
		deleteViewErr:   errors.New("delete-view-failed"),
	}
	wrapped := &batch{
		b:          base,
		setSteal:   base,
		setView:    base,
		deleteView: base,
	}
	if err := wrapped.SetStealView([]byte("k"), []byte("v")); !errors.Is(err, base.setStealViewErr) {
		t.Fatalf("expected set steal view error propagation, got=%v", err)
	}
	if err := wrapped.SetView([]byte("k"), []byte("v")); !errors.Is(err, base.setViewErr) {
		t.Fatalf("expected set view error propagation, got=%v", err)
	}
	if err := wrapped.DeleteView([]byte("k")); !errors.Is(err, base.deleteViewErr) {
		t.Fatalf("expected delete view error propagation, got=%v", err)
	}
}
