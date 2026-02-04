package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func leafKeysPerPage(t *testing.T, prefixCompression bool, prefixBytes int, flags byte, valueSize int) int {
	t.Helper()

	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	var value []byte
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 && valueSize > 0 {
		value = make([]byte, valueSize)
	}

	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: prefixCompression})
	b.SetPageID(1)

	var inserted int
	for i := 0; i < len(keys); i++ {
		err := b.AddLeafEntry(keys[i], value, flags, page.ValuePtr{Offset: uint64(i), Length: 123, FileID: 1})
		if err == ErrNodeFull {
			break
		}
		if err != nil {
			t.Fatalf("AddLeafEntry: %v", err)
		}
		inserted++
	}
	return inserted
}

func TestLeafPrefixCompression_IncreasesPageDensity_PointerEntries(t *testing.T) {
	plain := leafKeysPerPage(t, false, 16, FlagPointer, 0)
	compressed := leafKeysPerPage(t, true, 16, FlagPointer, 0)

	if compressed <= plain {
		t.Fatalf("expected prefix compression to increase keys/page for pointer entries; plain=%d compressed=%d", plain, compressed)
	}
	// Enforce a minimum benefit so future changes don't silently regress density.
	// Target is conservative to avoid flakiness across small format tweaks.
	min := plain + plain/10 // +10%
	if compressed < min {
		t.Fatalf("expected >=10%% density improvement; plain=%d compressed=%d min=%d", plain, compressed, min)
	}
}

func TestLeafPackedValuePtr_IncreasesPageDensity_PointerEntries(t *testing.T) {
	unpacked := leafKeysPerPage(t, true, 16, FlagPointer, 0)

	keys := makeBenchKeys(benchKeyCount, 16)
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		PackedValuePtr:        true,
	})
	b.SetPageID(1)
	ptr := page.ValuePtr{Offset: 1, Length: 123, FileID: 1}

	packed := 0
	for i := 0; i < len(keys); i++ {
		err := b.AddLeafEntry(keys[i], nil, FlagPointer, ptr)
		if err == ErrNodeFull {
			break
		}
		if err != nil {
			t.Fatalf("AddLeafEntry: %v", err)
		}
		packed++
	}

	if packed <= unpacked {
		t.Fatalf("expected packed ValuePtr to increase keys/page for pointer entries; unpacked=%d packed=%d", unpacked, packed)
	}
	min := unpacked + unpacked/20 // +5%
	if packed < min {
		t.Fatalf("expected >=5%% density improvement; unpacked=%d packed=%d min=%d", unpacked, packed, min)
	}
}

func TestLeafPrefixCompression_IncreasesPageDensity_InlineEntries(t *testing.T) {
	plain := leafKeysPerPage(t, false, 16, FlagInline, 128)
	compressed := leafKeysPerPage(t, true, 16, FlagInline, 128)

	if compressed <= plain {
		t.Fatalf("expected prefix compression to increase keys/page for inline entries; plain=%d compressed=%d", plain, compressed)
	}
	min := plain + plain/20 // +5%
	if compressed < min {
		t.Fatalf("expected >=5%% density improvement; plain=%d compressed=%d min=%d", plain, compressed, min)
	}
}

func BenchmarkLeafPageDensity(b *testing.B) {
	type variant struct {
		name              string
		prefixCompression bool
		packedValuePtr    bool
		prefixBytes       int
		flags             byte
		valueSize         int
	}
	variants := []variant{
		{name: "ptr/plain", prefixCompression: false, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/prefix", prefixCompression: true, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/prefix/packed", prefixCompression: true, packedValuePtr: true, prefixBytes: 16, flags: FlagPointer},
		{name: "inline128/plain", prefixCompression: false, prefixBytes: 16, flags: FlagInline, valueSize: 128},
		{name: "inline128/prefix", prefixCompression: true, prefixBytes: 16, flags: FlagInline, valueSize: 128},
	}

	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			keys := makeBenchKeys(benchKeyCount, v.prefixBytes)
			var value []byte
			if v.flags&FlagPointer == 0 && v.flags&FlagTombstone == 0 && v.valueSize > 0 {
				value = make([]byte, v.valueSize)
			}
			buf := make([]byte, page.PageSize)
			ptr := page.ValuePtr{Offset: 1, Length: 123, FileID: 1}

			// Measure density once; report as a benchmark metric.
			buildOnce := func() int {
				builder := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
					LeafPrefixCompression: v.prefixCompression,
					PackedValuePtr:        v.packedValuePtr,
				})
				builder.SetPageID(1)
				inserted := 0
				for i := 0; i < len(keys); i++ {
					if err := builder.AddLeafEntry(keys[i], value, v.flags, ptr); err != nil {
						if err == ErrNodeFull {
							break
						}
						b.Fatalf("AddLeafEntry: %v", err)
					}
					inserted++
				}
				return inserted
			}

			keysPerPage := buildOnce()
			// Also capture encoding throughput/alloc behavior (as a sanity check).
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = buildOnce()
			}
			b.StopTimer()
			b.ReportMetric(float64(keysPerPage), "keys/page")
		})
	}
}
