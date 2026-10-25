package collections

import (
	"math"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestNativeHNSWPublicRebuildDeletedRatioReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			c, closeAll, checkpoint, def, dbOpts := openHNSWPublicFixture(t, "native", 3, 2, profile)
			closed := false
			defer func() {
				if !closed {
					closeAll()
				}
			}()
			opts := vectorIndexOptionsFromDefinition(def)
			opts.RebuildDeletedRatio = .75
			idx, err := c.BuildVectorIndex(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.Delete([]byte("row-00000")); err != nil {
				t.Fatal(err)
			}
			check := func(idx *VectorIndex) {
				t.Helper()
				stats := idx.Stats()
				if stats.DeletedDocs != 1 || stats.RebuildNeeded {
					t.Fatalf("nondefault ratio lost: %+v", stats)
				}
			}
			check(idx)
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if err = checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeAll()
			closed = true
			d, err := treedb.Open(dbOpts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			h, err := d.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			m := NewCollectionManager(h)
			defer m.Close()
			c, err = m.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			// The persisted tuning, rather than a caller's default load options,
			// determines the restored graph's maintained rebuild behavior.
			idx, status, err := c.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
			if err != nil || !status.Loaded {
				t.Fatalf("fresh load: %+v %v", status, err)
			}
			check(idx)
			rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
			if err != nil || len(rows) != 2 {
				t.Fatalf("search: rows=%d err=%v", len(rows), err)
			}
		})
	}
}

func TestNativeHNSWPublicInvalidRebuildDeletedRatio(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 1, 2)
	defer closeAll()
	original, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	for _, ratio := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1.01} {
		opts := vectorIndexOptionsFromDefinition(def)
		opts.RebuildDeletedRatio = ratio
		if _, err = c.BuildVectorIndex(opts); err == nil {
			t.Fatalf("invalid ratio %v accepted", ratio)
		}
		if c.registeredVectorIndex(def.Name) != original {
			t.Fatalf("invalid ratio %v replaced shared graph", ratio)
		}
	}
}
