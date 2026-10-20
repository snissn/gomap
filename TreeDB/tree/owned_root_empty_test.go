package tree

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestOwnedUserRootEmptyCapturedDATA(t *testing.T) {
	for _, name := range []string{"plain_empty", "nonempty_base", "internal_base"} {
		t.Run(name, func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Error(err)
				}
			})
			if _, err := p.Alloc(2); err != nil {
				t.Fatal(err)
			}
			makeBase := func(kind page.PageType, nonempty bool) (uint64, []byte) {
				id, err := p.Alloc(1)
				if err != nil {
					t.Fatal(err)
				}
				image, err := p.GetForWrite(id)
				if err != nil {
					t.Fatal(err)
				}
				b := node.NewBuilder(image, kind)
				b.SetPageID(id)
				if nonempty {
					if err := b.AddLeafEntry([]byte("a"), []byte("v"), node.FlagInline, page.ValuePtr{}); err != nil {
						t.Fatal(err)
					}
				}
				b.FinishNoNode()
				return id, image
			}
			kind := page.PageTypeLeaf
			if name == "internal_base" {
				kind = page.PageTypeInternal
			}
			baseID, _ := makeBase(kind, name == "nonempty_base")
			rootID := baseID

			captured := New(p, nil, rootID)
			empty, err := captured.OwnedUserRootEmpty()
			wantError := false
			wantEmpty := name == "plain_empty"
			if (err != nil) != wantError || empty != wantEmpty {
				t.Fatalf("empty=%v err=%v; want empty=%v error=%v", empty, err, wantEmpty, wantError)
			}
			if !wantEmpty {
				return
			}
			if allocs := testing.AllocsPerRun(100, func() {
				empty, err := captured.OwnedUserRootEmpty()
				if err != nil || !empty {
					panic("captured empty root changed")
				}
			}); allocs != 0 {
				t.Fatalf("empty-root proof allocates %g", allocs)
			}
			// A newer published tree must not replace this captured root's proof.
			laterID, _ := makeBase(page.PageTypeLeaf, true)
			if empty, err := New(p, nil, laterID).OwnedUserRootEmpty(); err != nil || empty {
				t.Fatalf("later root empty=%v err=%v", empty, err)
			}
			if empty, err := captured.OwnedUserRootEmpty(); err != nil || !empty {
				t.Fatalf("captured old root empty=%v err=%v", empty, err)
			}
		})
	}
}
