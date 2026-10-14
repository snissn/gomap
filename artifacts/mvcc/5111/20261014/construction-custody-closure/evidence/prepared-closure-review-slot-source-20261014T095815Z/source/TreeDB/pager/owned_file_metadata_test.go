package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"testing"
)

func TestOwnedChildFileContributionActualCompletion(t *testing.T) {
	for _, refusal := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "physical-refusal"}[refusal], func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			const charge = 1024
			if err := owner.AddPending(charge); err != nil {
				t.Fatal(err)
			}
			f, err := os.CreateTemp(t.TempDir(), "original-child")
			if err != nil {
				t.Fatal(err)
			}
			p, err := OpenOwnedFileWithMetadata(f, f.Name(), int64(os.Getpagesize())*page.PageSize, OpenOptions{}, &owner, charge)
			if err != nil {
				t.Fatal(err)
			}
			if owner.Bytes() != charge || p.ownedFileMetadata != &owner || p.file != f {
				t.Fatal("lost admitted original child")
			}
			if refusal {
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			first := p.Close()
			if refusal {
				if !errors.Is(first, os.ErrClosed) || owner.Bytes() != charge || p.file != f || p.path == "" {
					t.Fatal("physical refusal refunded/lost original", first, owner.Bytes())
				}
				for i := 0; i < 3; i++ {
					if p.Close() != first || owner.Bytes() != charge {
						t.Fatal("replayed/refunded failed child")
					}
				}
			} else {
				if first != nil || owner.Bytes() != 0 || p.file != nil || p.path != "" {
					t.Fatal("actual consumption did not scrub/refund", first, owner.Bytes())
				}
				if err := p.Close(); err != nil || owner.Bytes() != 0 {
					t.Fatal("duplicate success", err)
				}
			}
		})
	}
}
