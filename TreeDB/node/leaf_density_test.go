package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func leafKeysPerPage(t *testing.T, opts BuilderOptions, prefixBytes int, flags byte, valueSize int) int {
	t.Helper()

	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	var value []byte
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 && valueSize > 0 {
		value = make([]byte, valueSize)
	}

	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, opts)
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
	plain := leafKeysPerPage(t, BuilderOptions{}, 16, FlagPointer, 0)
	compressed := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true}, 16, FlagPointer, 0)

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
	unpacked := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true}, 16, FlagPointer, 0)

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
	plain := leafKeysPerPage(t, BuilderOptions{}, 16, FlagInline, 128)
	compressed := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true}, 16, FlagInline, 128)

	if compressed <= plain {
		t.Fatalf("expected prefix compression to increase keys/page for inline entries; plain=%d compressed=%d", plain, compressed)
	}
	min := plain + plain/20 // +5%
	if compressed < min {
		t.Fatalf("expected >=5%% density improvement; plain=%d compressed=%d min=%d", plain, compressed, min)
	}
}

func TestLeafColumnarPrefixCompression_IncreasesPageDensity_PointerEntries(t *testing.T) {
	columnar := leafKeysPerPage(t, BuilderOptions{LeafColumnar: true}, 16, FlagPointer, 0)
	columnarPrefix := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}, 16, FlagPointer, 0)

	if columnarPrefix <= columnar {
		t.Fatalf("expected columnar+prefix to increase keys/page for pointer entries; columnar=%d columnar_prefix=%d", columnar, columnarPrefix)
	}
	min := columnar + columnar/20 // +5%
	if columnarPrefix < min {
		t.Fatalf("expected >=5%% density improvement; columnar=%d columnar_prefix=%d min=%d", columnar, columnarPrefix, min)
	}
}

func TestLeafColumnar_DoesNotReducePageDensity_PointerEntries(t *testing.T) {
	plain := leafKeysPerPage(t, BuilderOptions{}, 16, FlagPointer, 0)
	columnar := leafKeysPerPage(t, BuilderOptions{LeafColumnar: true}, 16, FlagPointer, 0)
	if columnar < plain {
		t.Fatalf("expected columnar leaves to not reduce keys/page for pointer entries; plain=%d columnar=%d", plain, columnar)
	}
}

func TestLeafColumnarPrefixPacked_PointerDensityWithinTolerance(t *testing.T) {
	prefixPacked := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true, PackedValuePtr: true}, 16, FlagPointer, 0)
	columnarPrefixPacked := leafKeysPerPage(t, BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true}, 16, FlagPointer, 0)
	// The combined stream layout pays extra metadata bytes to separate key/value
	// access hot paths. Guard against severe density regressions.
	min := prefixPacked - prefixPacked/10 // allow up to 10% lower density
	if columnarPrefixPacked < min {
		t.Fatalf("expected columnar+prefix+packed density within 10%% of prefix+packed; prefix_packed=%d columnar_prefix_packed=%d min=%d", prefixPacked, columnarPrefixPacked, min)
	}
}

func TestLeafAdaptiveEncoding_DensityFixture_HighPrefixInline(t *testing.T) {
	base := BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}
	keys := makeBenchKeys(128, 16)
	entries := make([]LeafHeuristicEntry, 0, len(keys))
	for i := range keys {
		entries = append(entries, LeafHeuristicEntry{Key: keys[i], Flags: FlagInline})
	}

	adaptive := AdaptiveLeafBuilderOptions(base, entries)
	if adaptive.LeafColumnar {
		t.Fatalf("expected high-prefix inline fixture to disable columnar mode")
	}
	if !adaptive.LeafPrefixCompression {
		t.Fatalf("expected high-prefix inline fixture to keep prefix compression")
	}

	adaptiveDensity := leafKeysPerPage(t, adaptive, 16, FlagInline, 128)
	columnarPrefixDensity := leafKeysPerPage(t, base, 16, FlagInline, 128)
	if adaptiveDensity < columnarPrefixDensity {
		t.Fatalf("expected adaptive high-prefix inline density to be >= fixed columnar+prefix; adaptive=%d fixed=%d", adaptiveDensity, columnarPrefixDensity)
	}
}

func TestLeafAdaptiveEncoding_DensityFixture_PointerLowPrefix(t *testing.T) {
	base := BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}
	keys := makeBenchKeys(128, 1)
	entries := make([]LeafHeuristicEntry, 0, len(keys))
	for i := range keys {
		flags := byte(FlagPointer)
		if i%8 == 0 {
			flags = FlagInline
		}
		entries = append(entries, LeafHeuristicEntry{Key: keys[i], Flags: flags})
	}

	adaptive := AdaptiveLeafBuilderOptions(base, entries)
	if !adaptive.LeafColumnar {
		t.Fatalf("expected low-prefix pointer fixture to keep columnar mode")
	}
	if adaptive.LeafPrefixCompression {
		t.Fatalf("expected low-prefix pointer fixture to disable prefix compression")
	}

	adaptiveDensity := leafKeysPerPage(t, adaptive, 1, FlagPointer, 0)
	columnarPrefixDensity := leafKeysPerPage(t, base, 1, FlagPointer, 0)
	min := columnarPrefixDensity - columnarPrefixDensity/20 // allow up to 5% drop
	if adaptiveDensity < min {
		t.Fatalf("expected adaptive low-prefix pointer density within 5%% of fixed columnar+prefix; adaptive=%d fixed=%d min=%d", adaptiveDensity, columnarPrefixDensity, min)
	}
}

func BenchmarkLeafPageDensity(b *testing.B) {
	type variant struct {
		name              string
		prefixCompression bool
		leafColumnar      bool
		packedValuePtr    bool
		prefixBytes       int
		flags             byte
		valueSize         int
	}
	variants := []variant{
		{name: "ptr/plain", prefixCompression: false, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/prefix", prefixCompression: true, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/prefix/packed", prefixCompression: true, packedValuePtr: true, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/columnar", leafColumnar: true, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/columnar_prefix", prefixCompression: true, leafColumnar: true, prefixBytes: 16, flags: FlagPointer},
		{name: "ptr/columnar_prefix/packed", prefixCompression: true, leafColumnar: true, packedValuePtr: true, prefixBytes: 16, flags: FlagPointer},
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
					LeafColumnar:          v.leafColumnar,
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
