package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func benchmarkFillInternal(b *testing.B, baseDelta bool, prefixBytes int) {
	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	buf := make([]byte, page.PageSize)

	opts := BuilderOptions{}
	if baseDelta {
		opts.InternalBaseDelta = true
	}

	b.ReportAllocs()
	b.ResetTimer()

	totalEntries := 0
	for i := 0; i < b.N; i++ {
		builder := NewBuilderWithOptions(buf, page.PageTypeInternal, opts)
		builder.SetPageID(1)

		entries := 0
		for ; entries < len(keys); entries++ {
			if err := builder.AddInternalChild(keys[entries], uint64(1_000_000+entries)); err != nil {
				if err == ErrNodeFull {
					break
				}
				b.Fatalf("AddInternalChild: %v", err)
			}
		}
		_ = builder.Finish()
		totalEntries += entries
	}

	b.ReportMetric(float64(totalEntries)/float64(b.N), "entries/page")
}

func BenchmarkFillInternal_Plain_PrefixHeavy(b *testing.B) {
	benchmarkFillInternal(b, false, 16)
}

func BenchmarkFillInternal_BaseDelta_PrefixHeavy(b *testing.B) {
	benchmarkFillInternal(b, true, 16)
}

func benchmarkSearchInternal(b *testing.B, baseDelta bool, prefixBytes int) {
	keys := makeBenchKeys(benchKeyCount, prefixBytes)
	buf := make([]byte, page.PageSize)

	opts := BuilderOptions{}
	if baseDelta {
		opts.InternalBaseDelta = true
	}

	builder := NewBuilderWithOptions(buf, page.PageTypeInternal, opts)
	builder.SetPageID(1)

	nKeys := 0
	for ; nKeys < len(keys); nKeys++ {
		if err := builder.AddInternalChild(keys[nKeys], uint64(1_000_000+nKeys)); err != nil {
			if err == ErrNodeFull {
				break
			}
			b.Fatalf("AddInternalChild: %v", err)
		}
	}
	n := builder.Finish()
	queries := keys[:nKeys]

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q := queries[i%len(queries)]
		_, _ = n.SearchInternal(q)
	}
}

func BenchmarkSearchInternal_Plain_PrefixHeavy(b *testing.B) {
	benchmarkSearchInternal(b, false, 16)
}

func BenchmarkSearchInternal_BaseDelta_PrefixHeavy(b *testing.B) {
	benchmarkSearchInternal(b, true, 16)
}
