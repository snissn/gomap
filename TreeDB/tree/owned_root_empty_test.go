package tree

import (
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestOwnedUserRootEmptyCapturedPrimary(t *testing.T) {
	for _, name := range []string{"plain_empty", "primary_empty", "nonempty_base", "internal_base", "put", "absence", "directory_corrupt", "base_digest", "missing_base"} {
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
			baseID, baseImage := makeBase(kind, name == "nonempty_base")
			rootID := baseID
			if name != "plain_empty" {
				rootID, err = p.Alloc(1)
				if err != nil {
					t.Fatal(err)
				}
				image, err := p.GetForWrite(rootID)
				if err != nil {
					t.Fatal(err)
				}
				base := node.PrimaryOperand{Ref: page.PageChildRef(baseID), Digest: sha256.Sum256(baseImage)}
				var entries []node.PrimaryDirectoryEntry
				if name == "put" {
					id, component := makeBase(page.PageTypeLeaf, true)
					entries = []node.PrimaryDirectoryEntry{{Key: []byte("a"), Revision: 1, Kind: node.PrimaryPut, Operand: node.PrimaryOperand{Ref: page.PageChildRef(id), Digest: sha256.Sum256(component)}}}
				}
				if name == "absence" {
					entries = []node.PrimaryDirectoryEntry{{Key: []byte("a"), Revision: 1, Kind: node.PrimaryAbsence}}
				}
				if name == "base_digest" {
					base.Digest[0] ^= 1
				}
				if name == "missing_base" {
					base.Ref = page.PageChildRef(999999)
				}
				if err := node.EncodePrimaryDirectory(image, rootID, 1, base, entries); err != nil {
					t.Fatal(err)
				}
				if name == "directory_corrupt" {
					image[len(image)-1] ^= 1
				}
			}
			captured := New(p, nil, rootID)
			empty, err := captured.OwnedUserRootEmpty()
			wantError := name == "directory_corrupt" || name == "base_digest" || name == "missing_base"
			wantEmpty := name == "plain_empty" || name == "primary_empty"
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
