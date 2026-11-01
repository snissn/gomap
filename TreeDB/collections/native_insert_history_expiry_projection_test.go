package collections

import (
	"math"
	"testing"
)

// Characterize trusted projection custody across genuine history expiry using
// the same unchanged history ring and public advancing effects as the BSON case.
func TestNativeOrdinaryInsertHistoryExpiryValidatedFloat32Projection(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	meta := &CollectionMeta{
		Name: "target",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore: &ColumnStoreConfig{
				Enabled:                 true,
				RetainedPayload:         ColumnRetainedPayloadFull,
				RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
				Columns: []ColumnStoreColumn{{
					Name: "embedding", Path: "embedding", Owner: TypedStorageOwnerColumnPart,
					ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2,
				}},
			},
		},
		VectorIndexes: []VectorIndexDefinition{{
			Name: "vec", Field: "embedding", Dimensions: 2,
			Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph,
		}},
	}
	if err := ValidateOrdinaryColumnGraphCollectionMeta(*meta); err != nil {
		t.Fatal(err)
	}
	id := []byte("original-request")
	document := []byte(`{"embedding":[1,-0],"marker":"exact acknowledged contents"}`)
	vector := []float32{1, math.Float32frombits(1 << 31)}
	stats := testNativeOrdinaryInsertHistoryExpiry(t, meta, id, document,
		func(target *Collection) ([][]byte, CollectionInsertStats, error) {
			return target.InsertBatchWithStatsValidatedFloat32Projection(
				[][]byte{id}, [][]byte{document}, "embedding", VectorMetricCosine, [][]float32{vector},
			)
		}, func(col *Collection, physical bool) {
			t.Helper()
			read, err := col.nativeJSON.Capture("target")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := read.Close(); err != nil {
					t.Error(err)
				}
			}()
			if physical {
				cfg := read.meta.Options.ColumnStore
				if cfg == nil || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil || *cfg.ActiveManifest != *cfg.RecoveryAuthoritativeManifest {
					t.Fatal("checkpoint/reopen lacks the recovery-authoritative physical manifest")
				}
				if read.domain.domain != nil {
					if _, _, pending := read.domain.domain.typed.find(id); pending {
						t.Fatal("checkpoint/reopen vector check still uses a pending typed row")
					}
				}
			}
			// Compare authoritative typed bits, independently of retained JSON.
			// The signed-zero control proves the oracle observes vector storage.
			for _, check := range []struct {
				vector []float32
				equal  bool
			}{{vector, true}, {[]float32{1, 0}, false}} {
				found, equal, err := read.typedRowEqual(id, document, []columnDeclaredValue{{
					Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: check.vector,
				}})
				if err != nil || !found || equal != check.equal {
					t.Fatalf("projection bits: physical=%v found=%v equal=%v want=%v err=%v", physical, found, equal, check.equal, err)
				}
			}
		})
	if stats.HistoryExpiryRetries != 1 || stats.Documents != 1 {
		t.Fatalf("trusted projection stats=%+v want exactly one history retry and one accepted document", stats)
	}
}
