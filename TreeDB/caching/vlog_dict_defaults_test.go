package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestValueLogDictDefaultTrainingRespectsAutotuneOff(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		AllowUnsafe:                 true,
		FlushThreshold:              1 << 20,
		ValueLogCompressionAutotune: valuelog.AutotuneOptions{Mode: valuelog.AutotuneOff},
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if db.valueLogDictTrainingEnabled() {
		t.Fatalf("expected dict training to remain disabled when autotune is off")
	}
	if db.valueLogDictTrainer != nil {
		t.Fatalf("expected no dict trainer when autotune is off")
	}
}
