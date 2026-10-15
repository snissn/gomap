package zipper

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// This producer models both legal post-append pauses: the original after-hook
// has not returned, then the wrapper has returned but cache/result admission
// refuses. It retains the actual work pointer and immutable output bytes.
type retainedWorkspaceLeafLogV6 struct {
	t                       *testing.T
	first, pending          *valuelog.LeafAppendWork
	image                   []byte
	outputs                 int
	afterPause, resultPause bool
}

func (l *retainedWorkspaceLeafLogV6) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	l.t.Fatal("unexpected unbounded leaf append")
	return page.LeafLogPtr{}, nil
}
func (l *retainedWorkspaceLeafLogV6) AppendLeafPageWithWork(image []byte, s *valuelog.LeafAppendWork, w *iterator.OrdinalScanWork) (page.LeafLogPtr, bool, error) {
	if s.Phase == 0 {
		if l.pending != nil {
			l.t.Fatal("retargeted live append")
		}
		if l.first == nil {
			l.first = s
		} else if l.first != s {
			l.t.Fatal("serial outputs allocated separate append workspaces")
		}
		if s.RID != 0 || s.Before || s.After || s.Err != nil || s.Value != nil || s.Frame != nil {
			l.t.Fatal("new target inherited completed append state")
		}
		l.pending = s
		l.image = bytes.Clone(image)
		l.outputs++
		s.Phase = 3
		s.RID = uint64(l.outputs)
		s.Before = true
		s.Ptr = page.LeafLogPtr{FileID: 1, Offset: uint64(l.outputs) * 8192}
		l.afterPause = true
		return page.LeafLogPtr{}, false, nil
	}
	if l.pending != s || !bytes.Equal(image, l.image) {
		l.t.Fatal("append target or image changed during refusal")
	}
	if !s.After {
		s.After = true
		l.resultPause = true
		// Exhaust only this call's remaining allowance, after successful wrapper
		// work, so the original caller must pause before cache/result installation.
		for w.Reserve(1, 0) {
		}
		return s.Ptr, true, nil
	}
	l.pending = nil
	l.image = nil
	return s.Ptr, true, nil
}

func TestMaterializedWriteSharesAppendWorkspaceThroughHookAndResultRefusalV6(t *testing.T) {
	t.Run("target-identity", func(t *testing.T) { runMaterializedWorkspaceV6(t, false) })
	t.Run("cancel-refusal-custody", func(t *testing.T) { runMaterializedWorkspaceV6(t, true) })
}
func runMaterializedWorkspaceV6(t *testing.T, checkCancel bool) {
	p, e := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := p.Close(); e != nil {
			t.Error(e)
		}
	}()
	if _, e = p.Alloc(5); e != nil {
		t.Fatal(e)
	}
	for i, key := range []string{"a", "z"} {
		image := make([]byte, page.PageSize)
		b := node.NewBuilder(image, page.PageTypeLeaf)
		b.SetPageID(uint64(i + 2))
		if e = b.AddLeafEntry([]byte(key), []byte("old"), node.FlagInline, page.ValuePtr{}); e != nil {
			t.Fatal(e)
		}
		b.FinishNoNode()
		if e = p.Write(uint64(i+2), image); e != nil {
			t.Fatal(e)
		}
	}
	image := make([]byte, page.PageSize)
	b := node.NewBuilder(image, page.PageTypeInternal)
	b.SetPageID(4)
	if e = b.AddInternalChild(nil, 2); e != nil {
		t.Fatal(e)
	}
	if e = b.AddInternalChild([]byte("z"), 3); e != nil {
		t.Fatal(e)
	}
	b.FinishNoNode()
	if e = p.Write(4, image); e != nil {
		t.Fatal(e)
	}
	a := freelist.New(p, 0)
	if e = a.EnableCOWV1(freelist.MustNewFreelistGenerationV1(1, 5, nil, nil), freelist.NewReservationLedger()); e != nil {
		t.Fatal(e)
	}
	z := New(p, a)
	log := &retainedWorkspaceLeafLogV6{t: t}
	z.SetLeafPageLog(log)
	z.SetOuterLeavesInValueLog(true)
	ops := []batch.Entry{{Key: []byte("a"), Value: []byte("new-a"), Type: batch.OpPut}, {Key: []byte("z"), Value: []byte("new-z"), Type: batch.OpPut}}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	fresh := func() *iterator.OrdinalScanWork {
		return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	}
	s, ready, e := NewMaterializedWriteWork(z, &owner, page.PageChildRef(4), ops, fresh())
	if !ready || e != nil {
		t.Fatal(ready, e)
	}
	testedCancel := !checkCancel
	for calls := 0; calls < 256; calls++ {
		ready, e = s.Step(a, fresh())
		if e != nil {
			t.Fatal(e)
		}
		if log.pending != nil && !testedCancel {
			before := owner.Bytes()
			closed, err := s.Cancel(fresh())
			if closed || !errors.Is(err, ErrMaterializedWriteContinuation) || owner.Bytes() != before || log.pending == nil {
				t.Fatal("cancel discarded unfinished append target or containing custody", closed, err)
			}
			testedCancel = true
		}
		if ready {
			if log.outputs != 2 || !log.afterPause || !log.resultPause || !testedCancel {
				t.Fatal("fixture missed real serial append/refusal paths", log.outputs)
			}
			_, _, _, ok := s.Result()
			if !ok {
				t.Fatal("missing completed output")
			}
			m, c := s.Complete()
			s = nil
			m.RemovePending(c)
			if owner.Bytes() != 0 {
				t.Fatal("output accounting did not detach")
			}
			return
		}
	}
	t.Fatal("retained materializer did not finish")
}
