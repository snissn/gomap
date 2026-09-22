package embedding

import (
	"context"
	"math"
	"runtime"
	"testing"
	"time"
)

func runtimeNumGoroutine() int { return runtime.NumGoroutine() }

// waitGoroutinesSettle waits until goroutine count returns to baseline,
// returning the positive delta if it never settles within maxWait.
func waitGoroutinesSettle(baseline int, maxWait time.Duration) int {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if delta := runtime.NumGoroutine() - baseline; delta <= 0 {
			return 0
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine() - baseline
}

// TestHashingDeterminismBitIdenticalAcrossRuns proves same inputs + config
// produce bit-identical float32 vectors across repeated runs.
func TestHashingDeterminismBitIdenticalAcrossRuns(t *testing.T) {
	texts := [][]byte{
		[]byte("alpha beta gamma"),
		[]byte("the quick brown fox jumps over the lazy dog"),
		[]byte("unicode: café naïve 犬 ねこ"),
		[]byte("repeat repeat repeat repeat"),
		[]byte(""),
	}
	first, err := embedAll(texts)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	for run := range 3 {
		again, err := embedAll(texts)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		for i := range first {
			if len(first[i]) != len(again[i]) {
				t.Fatalf("run %d text %d length drift", run, i)
			}
			for j := range first[i] {
				if math.Float32bits(first[i][j]) != math.Float32bits(again[i][j]) {
					t.Fatalf("run %d text %d elem %d not bit-identical: %08x vs %08x",
						run, i, j, math.Float32bits(first[i][j]), math.Float32bits(again[i][j]))
				}
			}
		}
	}
}

func embedAll(texts [][]byte) ([][]float32, error) {
	emb, err := DefaultRegistry().Create(Config{Provider: ProviderHashing, Dimensions: 16})
	if err != nil {
		return nil, err
	}
	return emb.EmbedBatch(context.Background(), texts)
}

// TestHashingParityFixtures pins the reference embedder output against the
// committed fixture: every element must match bit-for-bit.
func TestHashingParityFixtures(t *testing.T) {
	fixture, err := loadParityFixture(parityFixturePath)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	texts := make([][]byte, len(fixture.Texts))
	for i, s := range fixture.Texts {
		texts[i] = []byte(s)
	}
	got, err := embedAll(texts)
	if err != nil {
		t.Fatalf("embed fixture texts: %v", err)
	}
	if len(got) != len(fixture.VectorBitsHex) {
		t.Fatalf("got %d vectors want %d", len(got), len(fixture.VectorBitsHex))
	}
	for i, vec := range got {
		want := fixture.VectorBitsHex[i]
		if len(vec) != len(want) {
			t.Fatalf("vector %d: got %d elems want %d", i, len(vec), len(want))
		}
		for j, v := range vec {
			if gotHex := float32BitsHex(v); gotHex != want[j] {
				t.Fatalf("vector %d elem %d: got %s want pinned %s", i, j, gotHex, want[j])
			}
		}
	}
}

// TestHashingUnitNorm verifies L2 normalization for non-degenerate inputs.
func TestHashingUnitNorm(t *testing.T) {
	vectors, err := embedAll([][]byte{[]byte("alpha beta gamma delta epsilon zeta eta theta iota kappa")})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	normSq := float64(0)
	for _, v := range vectors[0] {
		normSq += float64(v) * float64(v)
	}
	if math.Abs(normSq-1) > 1e-6 {
		t.Fatalf("norm^2=%v want ~1", normSq)
	}
}

// TestHashingZeroVectorDegenerateInput documents degenerate-input semantics:
// text without tokens yields the all-zero vector, not NaN.
func TestHashingZeroVectorDegenerateInput(t *testing.T) {
	vectors, err := embedAll([][]byte{[]byte("   "), []byte("")})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	for i, vec := range vectors {
		for j, v := range vec {
			if v != 0 || math.Signbit(float64(v)) {
				t.Fatalf("vector %d elem %d = %v want +0", i, j, v)
			}
		}
	}
}

func TestHashingDimensionsMatchConfig(t *testing.T) {
	emb, err := NewHashingEmbedder(Config{Provider: ProviderHashing, Dimensions: 64})
	if err != nil {
		t.Fatalf("NewHashingEmbedder: %v", err)
	}
	if emb.Dimensions() != 64 {
		t.Fatalf("Dimensions()=%d want 64", emb.Dimensions())
	}
}

func TestHashingRejectsDimensionMismatchInFactory(t *testing.T) {
	if _, err := NewHashingEmbedder(Config{Provider: ProviderHashing, Dimensions: 0}); err == nil {
		t.Fatal("zero-dimension hashing config accepted")
	}
}

func TestDefaultRegistryHasHashing(t *testing.T) {
	if _, err := DefaultRegistry().Create(Config{Provider: ProviderHashing, Dimensions: 8}); err != nil {
		t.Fatalf("DefaultRegistry Create(hashing): %v", err)
	}
}

var sink [][]float32

func BenchmarkHashingEmbedBatch10kDocs(b *testing.B) {
	const docs = 10000
	texts := make([][]byte, docs)
	words := []string{"retrieval", "augmented", "generation", "embedding", "benchmark",
		"collection", "vector", "index", "chunk", "document"}
	for i := range texts {
		var buf []byte
		for w := range 40 {
			buf = append(buf, words[(i+w)%len(words)]...)
			buf = append(buf, ' ')
		}
		texts[i] = buf
	}
	emb, err := DefaultRegistry().Create(Config{Provider: ProviderHashing, Dimensions: 256})
	if err != nil {
		b.Fatalf("Create: %v", err)
	}
	ctx := context.Background()
	b.SetBytes(int64(docs))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		vectors, err := emb.EmbedBatch(ctx, texts)
		if err != nil {
			b.Fatalf("EmbedBatch: %v", err)
		}
		sink = vectors
	}
	b.ReportMetric(float64(b.N*docs)/b.Elapsed().Seconds(), "docs/sec")
}
