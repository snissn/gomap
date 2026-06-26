package zipper

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSpanNativeApplyPointDeleteAndOverwriteEdgeParity(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T, *batch.Batch)
	}{
		{
			name: "delete_only",
			build: func(t *testing.T, delta *batch.Batch) {
				t.Helper()
				for _, idx := range []int{10, 11, 12, 255, 511} {
					if err := delta.Delete([]byte(fmt.Sprintf("key-%06d", idx))); err != nil {
						t.Fatalf("Delete %d: %v", idx, err)
					}
				}
			},
		},
		{
			name: "update_delete_mix",
			build: func(t *testing.T, delta *batch.Batch) {
				t.Helper()
				for _, idx := range []int{9, 10, 300, 777} {
					if err := delta.Set([]byte(fmt.Sprintf("key-%06d", idx)), []byte(fmt.Sprintf("updated-%06d", idx))); err != nil {
						t.Fatalf("Set %d: %v", idx, err)
					}
				}
				for _, idx := range []int{11, 12, 512, 900} {
					if err := delta.Delete([]byte(fmt.Sprintf("key-%06d", idx))); err != nil {
						t.Fatalf("Delete %d: %v", idx, err)
					}
				}
			},
		},
		{
			name: "duplicate_same_key_overwrite",
			build: func(t *testing.T, delta *batch.Batch) {
				t.Helper()
				if err := delta.Set([]byte("key-000042"), []byte("first")); err != nil {
					t.Fatalf("Set first: %v", err)
				}
				if err := delta.Set([]byte("key-000042"), []byte("second")); err != nil {
					t.Fatalf("Set second: %v", err)
				}
				if err := delta.Set([]byte("key-000043"), bytes.Repeat([]byte("v"), 200)); err != nil {
					t.Fatalf("Set adjacent split pressure: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, serial := newReadOnlyPrepareZipper(t)
			_, native := newReadOnlyPrepareZipper(t)
			serialRoot := buildReadOnlyPrepareRootWithKeys(t, serial, 1024)
			nativeRoot := buildReadOnlyPrepareRootWithKeys(t, native, 1024)

			buildDelta := func() *batch.Batch {
				delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
				tc.build(t, delta)
				return delta
			}
			serialDelta := buildDelta()
			defer func() { _ = serialDelta.Close() }()
			nativeDelta := buildDelta()
			defer func() { _ = nativeDelta.Close() }()

			serialNewRoot, _, _, err := serial.Apply(serialRoot, serialDelta)
			if err != nil {
				t.Fatalf("serial Apply: %v", err)
			}
			result, err := native.ApplyWithOptions(nativeRoot, nativeDelta, ApplyOptions{
				SpanNativeApply:                    true,
				SpanNativeAllowMaintenancePointOps: true,
				ParallelApplyConcurrency:           2,
			})
			if err != nil {
				t.Fatalf("span-native ApplyWithOptions: %v", err)
			}
			if !result.SpanNativeEligible || !result.SpanNativeUsed {
				t.Fatalf("span-native flags eligible/used=%v/%v fallback=%q summary=%+v", result.SpanNativeEligible, result.SpanNativeUsed, result.SpanNativeFallbackReason, result.ReadOnlyPrepare.LeafSpanSummary())
			}
			if !bytes.Equal(collectRootLeafPairs(t, serial, serialNewRoot), collectRootLeafPairs(t, native, result.RootID)) {
				t.Fatalf("span-native %s output mismatch", tc.name)
			}
		})
	}
}
