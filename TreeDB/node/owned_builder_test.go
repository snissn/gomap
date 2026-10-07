package node

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOwnedBuilderScratchEncodingsAndNoGrowth(t *testing.T) {
	for _, opts := range []BuilderOptions{
		{LeafPrefixCompression: true},
		{LeafColumnar: true},
		{LeafColumnar: true, LeafPrefixCompression: true},
		{InternalBaseDelta: true},
	} {
		t.Run(string(rune('a'+boolInt(opts.LeafPrefixCompression)+2*boolInt(opts.LeafColumnar)+4*boolInt(opts.InternalBaseDelta))), func(t *testing.T) {
			typ := page.PageTypeLeaf
			if opts.InternalBaseDelta {
				typ = page.PageTypeInternal
			}
			var charged uint64
			data := make([]byte, page.PageSize)
			b, err := NewOwnedBuilderWithOptions(data, typ, opts, 180, func(n uint64) error { charged += n; return nil })
			if err != nil {
				t.Fatal(err)
			}
			if charged < uint64(unsafe.Sizeof(Builder{}))+uint64(unsafe.Sizeof(ownedBuilderScratch{})) || !b.OwnedScratch() {
				t.Fatal("uncharged scratch")
			}
			s := b.ownedScratch
			arenaCap := cap(s.keyArena)
			for i := 0; i < 80; i++ {
				key := append(bytes.Repeat([]byte("p"), 120), byte(i))
				if typ == page.PageTypeInternal {
					err = b.AddInternalChild(key, uint64(i+1))
				} else {
					err = b.AddLeafEntry(key, []byte("x"), FlagInline, page.ValuePtr{})
				}
				if errors.Is(err, ErrNodeFull) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if cap(s.keyArena) != arenaCap || b.leafColumnarV2EntriesH != nil || b.leafColumnarPrefixV2EntriesH != nil || b.internalBaseEntriesH != nil || b.internalBaseArenaH != nil || b.leafColumnarV2ArenaH != nil {
				t.Fatal("owned scratch used pool/growth")
			}
			if typ == page.PageTypeInternal && len(b.internalBaseArena) <= page.PageSize {
				t.Fatal("test did not exercise expanded full keys")
			}
			if typ == page.PageTypeInternal {
				b.SetInternalFenceBounds([]byte("lo"), []byte("hi"))
			}
			b.FinishNoNode()
			b.ResetWithOptions(data, typ, opts)
			if typ == page.PageTypeInternal {
				err = b.AddInternalChild(bytes.Repeat([]byte("x"), 181), 1)
			} else {
				err = b.AddLeafEntry(bytes.Repeat([]byte("x"), 181), nil, FlagInline, page.ValuePtr{})
			}
			if !errors.Is(err, ErrOwnedBuilderScratch) {
				t.Fatalf("over-width admission %v", err)
			}
			b.ReleaseScratch()
			for _, v := range s.internalEntries[:cap(s.internalEntries)] {
				if v.key != nil {
					t.Fatal("released pointer tail")
				}
			}
		})
	}
}

func TestOwnedBuilderScratchRejectBeforeAllocationAndReset(t *testing.T) {
	sentinel := errors.New("denied")
	if _, err := NewOwnedBuilderWithOptions(make([]byte, page.PageSize), page.PageTypeLeaf, BuilderOptions{}, 10, func(uint64) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	b, err := NewOwnedBuilderWithOptions(make([]byte, page.PageSize), page.PageTypeLeaf, BuilderOptions{LeafColumnar: true}, 10, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	b.ResetWithOptions(make([]byte, page.PageSize), page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	if err := b.AddLeafEntry([]byte("x"), nil, FlagInline, page.ValuePtr{}); !errors.Is(err, ErrOwnedBuilderScratch) {
		t.Fatalf("different encoding silently restored generic scratch: %v", err)
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestOwnedBuilderScratchCloseCannotRegainPool(t *testing.T) {
	b, err := NewOwnedBuilderWithOptions(make([]byte, page.PageSize), page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true}, 20, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := b.AddInternalChild([]byte("key"), 1); err != nil {
		t.Fatal(err)
	}
	b.CloseOwnedScratch()
	if b.ownedScratch.internalEntries != nil || b.ownedScratch.keyArena != nil || b.internalBaseArena != nil {
		t.Fatal("closed backing retained")
	}
	b.ResetWithOptions(make([]byte, page.PageSize), page.PageTypeInternal, BuilderOptions{InternalBaseDelta: true})
	if err := b.AddInternalChild([]byte("key"), 2); !errors.Is(err, ErrOwnedBuilderScratch) {
		t.Fatalf("closed Reset regained authority: %v", err)
	}
	if b.internalBaseEntriesH != nil || b.internalBaseArenaH != nil {
		t.Fatal("closed Reset acquired pools")
	}
}

func TestOwnedBuilderScratchProducerEntryCreditAndReset(t *testing.T) {
	for _, opts := range []BuilderOptions{{LeafColumnar: true}, {LeafColumnar: true, LeafPrefixCompression: true}, {InternalBaseDelta: true}} {
		typ := page.PageTypeLeaf
		if opts.InternalBaseDelta {
			typ = page.PageTypeInternal
		}
		var charged uint64
		b, err := NewOwnedBuilderWithEntryLimit(make([]byte, page.PageSize), typ, opts, 20, 1, func(n uint64) error { charged += n; return nil })
		if err != nil {
			t.Fatal(err)
		}
		s := b.ownedScratch
		if opts.InternalBaseDelta && (cap(s.internalEntries) != 1 || cap(s.keyArena) != 20) {
			t.Fatal("internal producer credit not exact")
		}
		if opts.LeafColumnar && opts.LeafPrefixCompression && cap(s.prefixEntries) != 1 {
			t.Fatal("prefix producer credit not exact")
		}
		if opts.LeafColumnar && !opts.LeafPrefixCompression && cap(s.leafEntries) != 1 {
			t.Fatal("leaf producer credit not exact")
		}
		add := func(key string) error {
			if typ == page.PageTypeInternal {
				return b.AddInternalChild([]byte(key), 1)
			}
			return b.AddLeafEntry([]byte(key), []byte("v"), FlagInline, page.ValuePtr{})
		}
		if err := add("a"); err != nil {
			t.Fatal(err)
		}
		before := charged
		if err := add("b"); !errors.Is(err, ErrOwnedBuilderScratch) || charged != before {
			t.Fatalf("producer credit grew: %v", err)
		}
		b.ResetWithOptions(b.Data(), typ, opts)
		if err := add("a"); err != nil {
			t.Fatal(err)
		}
		if err := add("b"); !errors.Is(err, ErrOwnedBuilderScratch) || charged != before {
			t.Fatalf("Reset replenished credit: %v", err)
		}
		b.CloseOwnedScratch()
	}
	var reserved bool
	if _, err := NewOwnedBuilderWithEntryLimit(make([]byte, page.PageSize), page.PageTypeLeaf, BuilderOptions{}, 20, -1, func(uint64) error { reserved = true; return nil }); !errors.Is(err, ErrOwnedBuilderScratch) || reserved {
		t.Fatalf("negative credit admitted: %v", err)
	}
}
