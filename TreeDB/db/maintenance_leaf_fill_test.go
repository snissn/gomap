package db

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
)

func TestMaintenance_ModerateChurn_KeepsLeafFillReasonable(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                   dir,
		KeepRecent:            1,
		LeafFillTargetPPM:     850_000,
		InternalFillTargetPPM: 900_000,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 20_000
	valA := bytes.Repeat([]byte("a"), 32)
	valB := bytes.Repeat([]byte("b"), 32)

	// Seed data.
	{
		const batchSize = 1024
		for base := 0; base < keys; base += batchSize {
			b := d.NewBatch().(*Batch)
			limit := base + batchSize
			if limit > keys {
				limit = keys
			}
			for i := base; i < limit; i++ {
				k := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
				if err := b.Set(k, valA); err != nil {
					t.Fatalf("seed set: %v", err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("seed write: %v", err)
			}
			_ = b.Close()
		}
	}

	// Moderate churn: deterministic random overwrites + deletes + reinserts.
	rng := rand.New(rand.NewSource(1))
	{
		const rounds = 10
		const opsPerRound = 5000
		for r := 0; r < rounds; r++ {
			b := d.NewBatch().(*Batch)
			for i := 0; i < opsPerRound; i++ {
				n := rng.Intn(keys)
				k := []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
				switch i % 3 {
				case 0:
					if err := b.Set(k, valB); err != nil {
						t.Fatalf("churn set: %v", err)
					}
				case 1:
					if err := b.Delete(k); err != nil {
						t.Fatalf("churn del: %v", err)
					}
				default:
					if err := b.Set(k, valA); err != nil {
						t.Fatalf("churn set2: %v", err)
					}
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("churn write: %v", err)
			}
			_ = b.Close()
		}
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}

	p50Str := rep["treedb.user.leaf_fill_ppm_p50"]
	if p50Str == "" {
		t.Fatalf("missing leaf_fill_ppm_p50")
	}
	p50, err := strconv.ParseUint(p50Str, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf_fill_ppm_p50: %v", err)
	}

	avgStr := rep["treedb.user.leaf_fill_ppm_avg"]
	if avgStr == "" {
		t.Fatalf("missing leaf_fill_ppm_avg")
	}
	avg, err := strconv.ParseUint(avgStr, 10, 64)
	if err != nil {
		t.Fatalf("parse leaf_fill_ppm_avg: %v", err)
	}

	// These bounds are intentionally loose: the goal is to catch catastrophic
	// bloat/regressions, not micro-optimize packing.
	if p50 < 450_000 {
		t.Fatalf("expected leaf fill p50 >= 450k ppm, got %d", p50)
	}
	if avg < 450_000 {
		t.Fatalf("expected leaf fill avg >= 450k ppm, got %d", avg)
	}
}
