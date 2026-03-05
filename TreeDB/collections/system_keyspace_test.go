package collections

import (
	"math/rand"
	"testing"
)

func TestSystemKeyspaceCanonicalizesName(t *testing.T) {
	name := string([]byte{'u', 's', 'e', 'r', 0xff, 0xfe})
	if err := ValidateCollectionName(name); err == nil {
		t.Fatalf("expected non-UTF8/unsafe name rejection: %q", name)
	}

	if err := ValidateCollectionName("users/active"); err == nil {
		t.Fatalf("expected slash rejection")
	}
	if err := ValidateCollectionName("users:active"); err == nil {
		t.Fatalf("expected colon rejection")
	}
	if err := ValidateCollectionName(" users "); err == nil {
		t.Fatalf("expected leading/trailing space rejection")
	}
	if err := ValidateCollectionName("orders"); err != nil {
		t.Fatalf("unexpected rejection for valid name: %v", err)
	}
}

func TestSystemKeyspacePrefixIsolation(t *testing.T) {
	metaKey, err := SystemCollectionMetaKey("users")
	if err != nil {
		t.Fatalf("meta key: %v", err)
	}
	idxKey, err := SystemIndexKey("users", "by_name")
	if err != nil {
		t.Fatalf("index key: %v", err)
	}
	docPrefix, err := SystemCollectionPrefix("users")
	if err != nil {
		t.Fatalf("doc prefix: %v", err)
	}
	if string(metaKey) == string(idxKey) || string(metaKey) == string(docPrefix) || string(idxKey) == string(docPrefix) {
		t.Fatalf("system key collision detected")
	}
	if len(metaKey) == 0 || len(idxKey) == 0 || len(docPrefix) == 0 {
		t.Fatalf("empty system key")
	}
}

func TestIndexNameCollisionDetection(t *testing.T) {
	base := "users"
	i1, err := SystemIndexKey(base, "status")
	if err != nil {
		t.Fatalf("index key 1: %v", err)
	}
	i2, err := SystemIndexKey(base, "status_")
	if err != nil {
		t.Fatalf("index key 2: %v", err)
	}
	if string(i1) == string(i2) {
		t.Fatalf("expected distinct keys for distinct index names")
	}
	i3, err := SystemIndexKey(base, "status")
	if err != nil {
		t.Fatalf("index key 3: %v", err)
	}
	if string(i1) != string(i3) {
		t.Fatalf("expected stable key generation for same index name")
	}

	if _, err := SystemIndexKey(base, " status"); err == nil {
		t.Fatalf("expected leading space to be rejected")
	}
}

func BenchmarkCollectionMeta_EncodeLarge(b *testing.B) {
	meta := &CollectionMeta{
		Name:    "bench_collection",
		Indexes: make([]IndexDefinition, 0, 256),
	}
	for i := 0; i < 256; i++ {
		meta.Indexes = append(meta.Indexes, IndexDefinition{
			Name:   randomIndexName(i),
			Field:  "nested.field." + randomIndexName(i),
			Unique: i%2 == 0,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := meta.Encode(); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func randomIndexName(seed int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	rng := rand.New(rand.NewSource(int64(seed)))
	buf := make([]byte, 12)
	for i := range buf {
		buf[i] = letters[rng.Intn(len(letters))]
	}
	return string(buf)
}
