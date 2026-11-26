package node

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestSharedPrefixLenWordAtATime(t *testing.T) {
	for n := 0; n <= 40; n++ {
		base := make([]byte, n)
		for i := range base {
			base[i] = byte('a' + (i % 26))
		}
		if got := sharedPrefixLen(base, base); got != n {
			t.Fatalf("equal len=%d: got %d", n, got)
		}
		for diffAt := 0; diffAt < n; diffAt++ {
			other := append([]byte(nil), base...)
			other[diffAt] ^= 0x01
			if got := sharedPrefixLen(base, other); got != diffAt {
				t.Fatalf("len=%d diffAt=%d: got %d", n, diffAt, got)
			}
			if got := sharedPrefixLen(other, base); got != diffAt {
				t.Fatalf("reversed len=%d diffAt=%d: got %d", n, diffAt, got)
			}
		}
		if n > 0 {
			short := base[:n-1]
			if got := sharedPrefixLen(base, short); got != n-1 {
				t.Fatalf("prefix len=%d: got %d", n-1, got)
			}
			if got := sharedPrefixLen(short, base); got != n-1 {
				t.Fatalf("reversed prefix len=%d: got %d", n-1, got)
			}
		}
	}
}

func TestTryAddLeafEntryWithRevisionReserveParity(t *testing.T) {
	for _, reserve := range []int{0, 256, 1024} {
		for _, withRevs := range []bool{false, true} {
			bufRef := make([]byte, page.PageSize)
			bRef := NewBuilderWithOptions(bufRef, page.PageTypeLeaf, BuilderOptions{
				LeafPrefixCompression: true,
				LeafColumnar:          true,
				EntryRevisions:        withRevs,
			})
			bufFast := make([]byte, page.PageSize)
			bFast := NewBuilderWithOptions(bufFast, page.PageTypeLeaf, BuilderOptions{
				LeafPrefixCompression: true,
				LeafColumnar:          true,
				EntryRevisions:        withRevs,
			})

			for i := 0; i < 200; i++ {
				key := []byte(fmt.Sprintf("tenant-0001/collection/secondary/city-%02d/doc-%06d", i%17, i))
				val := []byte(fmt.Sprintf("payload-%04d", i))
				rev := page.LegacyEntryRevision
				if withRevs {
					rev = page.EntryRevision(i + 1)
				}

				entrySize, prefixLen, suffixLen := bRef.LeafEntrySizeWithPrefix(key, val, FlagInline)
				var errRef error
				if reserve > 0 && bRef.Count() > 0 && bRef.FreeSpace() < entrySize+DirectoryEntrySize+reserve {
					errRef = ErrNodeFull
				} else {
					errRef = bRef.AddLeafEntryWithPrefixRevision(key, val, FlagInline, page.ValuePtr{}, rev, entrySize, prefixLen, suffixLen)
				}
				errFast := bFast.TryAddLeafEntryWithRevisionReserve(key, val, FlagInline, page.ValuePtr{}, rev, reserve)
				if errRef != errFast {
					t.Fatalf("reserve=%d withRevs=%t i=%d: errRef=%v errFast=%v", reserve, withRevs, i, errRef, errFast)
				}
				if errRef == ErrNodeFull {
					break
				}
			}

			bRef.FinishNoNode()
			bFast.FinishNoNode()
			if !bytes.Equal(bufRef, bufFast) {
				t.Fatalf("reserve=%d withRevs=%t: finished page bytes diverged", reserve, withRevs)
			}
		}
	}
}
