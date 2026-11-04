package tree

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type appendBindingsTestReader struct{ checksum bool }

func (r *appendBindingsTestReader) Read(page.ValuePtr) ([]byte, error) { return nil, nil }

func (r *appendBindingsTestReader) ReadUnsafe(page.ValuePtr) ([]byte, error) { return nil, nil }
func (r *appendBindingsTestReader) ReadUnsafeAppend(_ page.ValuePtr, dst []byte) ([]byte, error) {
	return dst, nil
}
func (r *appendBindingsTestReader) ReadUnsafeAppendBatch(_ []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	return dst, nil
}
func (r *appendBindingsTestReader) ReadUnsafeTo(_ page.ValuePtr, dst []byte) ([]byte, bool, error) {
	return dst, true, nil
}
func (r *appendBindingsTestReader) ReadLeafLogPageUnsafeTo(_ page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	return dst, true, nil
}
func (r *appendBindingsTestReader) ReadLeafLogPageUnsafeToWithState(_ page.LeafLogPtr, dst []byte) ([]byte, bool, LeafLogPageReadState, error) {
	return dst, true, LeafLogPageReadState{}, nil
}
func (r *appendBindingsTestReader) ReadLeafLogPageUnsafeView(page.LeafLogPtr) ([]byte, LeafLogPageViewLease, bool, error) {
	return nil, nil, false, nil
}
func (r *appendBindingsTestReader) ReadLeafLogPageUnsafeViewWithState(page.LeafLogPtr) ([]byte, LeafLogPageViewLease, bool, LeafLogPageReadState, error) {
	return nil, nil, false, LeafLogPageReadState{}, nil
}
func (r *appendBindingsTestReader) MarkLeafLogPageChecksumVerified(page.LeafLogPtr) {}

func (r *appendBindingsTestReader) ReadChecksumEnabled() bool { return r.checksum }

func appendBindingsForTest(r *appendBindingsTestReader) AppendReadBindings {
	return AppendReadBindings{
		Reader: r, Appender: r, Batcher: r, ToReader: r,
		LeafToReader: r, LeafToState: r, LeafView: r, LeafViewState: r, VerifyMarker: r,
	}
}

type appendBindingsKeyReader struct {
	*appendBindingsTestReader
	enabled bool
	calls   int
}

func (r *appendBindingsKeyReader) KeyAwareEnabled() bool { r.calls++; return r.enabled }
func (r *appendBindingsKeyReader) ReadUnsafeForKey(page.ValuePtr, []byte) ([]byte, error) {
	return nil, nil
}
func (r *appendBindingsKeyReader) ReadUnsafeAppendForKey(_ page.ValuePtr, _ []byte, dst []byte) ([]byte, error) {
	return dst, nil
}
func (r *appendBindingsKeyReader) ReadUnsafeAppendBatchForKeys(_ []page.ValuePtr, _ [][]byte, dst [][]byte) ([][]byte, error) {
	return dst, nil
}

func TestTreeAppendReadBindingsMatchReset(t *testing.T) {
	first := &appendBindingsTestReader{checksum: true}
	second := &appendBindingsTestReader{checksum: false}
	dirty := &appendBindingsKeyReader{appendBindingsTestReader: first, enabled: true}
	original := Tree{}
	original.Reset(&pager.Pager{}, dirty, 41)
	original.negativeFilter = &NegativeFilter{}
	original.pageLimit = 17
	original.pageNamespace = 23
	original.primaryLocal = true
	for _, tc := range []struct {
		name   string
		reader *appendBindingsTestReader
		root   uint64
	}{
		{"first", first, 51},
		{"second", second, 52},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pager.Pager{}
			generic, bound := original, original
			generic.Reset(p, tc.reader, tc.root)
			bindings := appendBindingsForTest(tc.reader)
			bound.ResetAppendReadBindings(p, &bindings, tc.root)
			if bound != generic {
				t.Fatalf("complete Tree state differs: bound=%+v generic=%+v", bound, generic)
			}
			if bound.shouldVerifyLeafRefChecksum() != tc.reader.checksum {
				t.Fatal("current checksum policy lost")
			}
			tc.reader.checksum = !tc.reader.checksum
			if bound.shouldVerifyLeafRefChecksum() != tc.reader.checksum {
				t.Fatal("checksum policy frozen at bind")
			}
			// The actual release-side operation stays the ordinary nil Reset.
			bound.Reset(nil, nil, 0)
			if bound != (Tree{}) {
				t.Fatal("nil Reset retained binding aliases")
			}
		})
	}
	t.Run("nil", func(t *testing.T) {
		p := &pager.Pager{}
		generic, bound := original, original
		generic.Reset(p, nil, 61)
		bound.ResetAppendReadBindings(p, nil, 61)
		if bound != generic {
			t.Fatal("nil bundle differs from ordinary empty-reader Reset")
		}
	})
}

func TestTreeGenericResetRetainsDynamicKeyPolicy(t *testing.T) {
	r := &appendBindingsKeyReader{appendBindingsTestReader: &appendBindingsTestReader{}}
	tr := Tree{}
	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{"enabled", true},
		{"disabled", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.enabled = tc.enabled
			before := r.calls
			tr.Reset(nil, r, 1)
			if r.calls != before+1 {
				t.Fatal("ordinary Reset omitted current key policy call")
			}
			if (tr.slabKeyReader != nil) != tc.enabled ||
				(tr.slabKeyAppender != nil) != tc.enabled ||
				(tr.slabKeyBatcher != nil) != tc.enabled {
				t.Fatal("ordinary Reset ignored changed policy on same reader")
			}
		})
	}
}
