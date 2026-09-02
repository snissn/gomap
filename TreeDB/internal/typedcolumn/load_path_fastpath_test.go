package typedcolumn

import (
	"slices"
	"testing"
)

func TestColumnPartBuilderIdentitySortedOrderFastPath(t *testing.T) {
	builder := mustLoadPathFastPathBuilder(t, []SortKeyColumn{{Column: "id"}})
	batch := loadPathFastPathBatch([]int64{10, 11, 12, 13})
	if !builder.canUseIdentitySortedOrder(batch, len(batch.Columns["id"]), "id") {
		t.Fatalf("identity sorted-order fast path did not admit contiguous ascending primary IDs")
	}
	order, err := builder.sortedOrder(batch, len(batch.Columns["id"]), "id")
	if err != nil {
		t.Fatalf("sortedOrder: %v", err)
	}
	if !slices.Equal(order, []int{0, 1, 2, 3}) {
		t.Fatalf("order=%v want identity", order)
	}
}

func TestColumnPartBuilderIdentitySortedOrderFastPathFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sortKey []SortKeyColumn
		ids     []int64
		want    []int
	}{
		{
			name:    "gap",
			sortKey: []SortKeyColumn{{Column: "id"}},
			ids:     []int64{10, 11, 13, 14},
			want:    []int{0, 1, 2, 3},
		},
		{
			name:    "non_ascending",
			sortKey: []SortKeyColumn{{Column: "id"}},
			ids:     []int64{10, 11, 14, 13},
			want:    []int{0, 1, 3, 2},
		},
		{
			name:    "duplicate",
			sortKey: []SortKeyColumn{{Column: "id"}},
			ids:     []int64{10, 11, 11, 12},
			want:    []int{0, 1, 2, 3},
		},
		{
			name:    "non_default_null_order",
			sortKey: []SortKeyColumn{{Column: "id", Nulls: SortKeyNullsFirst}},
			ids:     []int64{10, 11, 12, 13},
			want:    []int{0, 1, 2, 3},
		},
		{
			name:    "logical_sort_key",
			sortKey: []SortKeyColumn{{Column: "value"}},
			ids:     []int64{10, 11, 12, 13},
			want:    []int{3, 2, 1, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := mustLoadPathFastPathBuilder(t, tc.sortKey)
			batch := loadPathFastPathBatch(tc.ids)
			if builder.canUseIdentitySortedOrder(batch, len(tc.ids), "id") {
				t.Fatalf("identity sorted-order fast path admitted %s case", tc.name)
			}
			order, err := builder.sortedOrder(batch, len(tc.ids), "id")
			if err != nil {
				t.Fatalf("sortedOrder: %v", err)
			}
			if !slices.Equal(order, tc.want) {
				t.Fatalf("order=%v want %v", order, tc.want)
			}
		})
	}
}

func TestColumnPartImageContiguousRowLocatorsFastPathRoundTrip(t *testing.T) {
	part := mustTransplantPart(t, 246801, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), loadPathFastPathBatch([]int64{10, 11, 12, 13}))
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section := mustValidationSection(t, image, ColumnPartImageSectionRowLocators)
	if section.Encoding != EncodingRowLocatorContiguous {
		t.Fatalf("row locator section encoding=%s want %s", section.Encoding, EncodingRowLocatorContiguous)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	for primaryID, wantRow := range map[int64]int{10: 0, 11: 1, 12: 2, 13: 3} {
		locator, ok := reconstructed.LocatePrimaryID(primaryID)
		if !ok {
			t.Fatalf("missing locator for primary id %d", primaryID)
		}
		if locator.PartRow != wantRow || locator.PrimaryID != primaryID {
			t.Fatalf("locator for primary id %d=%+v want row=%d", primaryID, locator, wantRow)
		}
	}
}

func BenchmarkColumnPartBuilderSortedOrderIdentityFastPath(b *testing.B) {
	const rows = 65536
	builder := mustLoadPathFastPathBuilder(b, []SortKeyColumn{{Column: "id"}})
	batch := loadPathFastPathSequentialBatch(rows)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := builder.sortedOrder(batch, rows, "id"); err != nil {
			b.Fatalf("sortedOrder: %v", err)
		}
	}
}

func BenchmarkColumnPartBuilderSortedOrderFallback(b *testing.B) {
	const rows = 65536
	builder := mustLoadPathFastPathBuilder(b, []SortKeyColumn{{Column: "id"}})
	batch := loadPathFastPathSequentialBatch(rows)
	batch.Columns["id"][rows-1], batch.Columns["id"][rows-2] = batch.Columns["id"][rows-2], batch.Columns["id"][rows-1]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := builder.sortedOrder(batch, rows, "id"); err != nil {
			b.Fatalf("sortedOrder: %v", err)
		}
	}
}

func BenchmarkColumnPartImageRowLocatorsContiguousFastPath(b *testing.B) {
	const rows = 65536
	part, err := BuildColumnPart(246802, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), loadPathFastPathSequentialBatch(rows))
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	benchmarkAddRowLocatorsSection(b, part)
}

func BenchmarkColumnPartImageRowLocatorsRawFallback(b *testing.B) {
	const rows = 65536
	part, err := BuildColumnPart(246803, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), loadPathFastPathSequentialBatch(rows))
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	part = clonePartWithLocators(part)
	lastPrimaryID := int64(rows - 1)
	locator := part.Locators[lastPrimaryID]
	delete(part.Locators, lastPrimaryID)
	locator.PrimaryID = int64(rows * 2)
	part.Locators[locator.PrimaryID] = locator
	benchmarkAddRowLocatorsSection(b, part)
}

func benchmarkAddRowLocatorsSection(b *testing.B, part *ColumnPart) {
	b.Helper()
	builder := columnPartImageBuilder{part: part}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder.sections = builder.sections[:0]
		if err := builder.addRowLocatorsSection(); err != nil {
			b.Fatalf("addRowLocatorsSection: %v", err)
		}
	}
}

func mustLoadPathFastPathBuilder(tb testing.TB, sortKey []SortKeyColumn) *ColumnPartBuilder {
	tb.Helper()
	builder, err := NewColumnPartBuilder(transplantTestOptions(sortKey))
	if err != nil {
		tb.Fatalf("NewColumnPartBuilder: %v", err)
	}
	return builder
}

func loadPathFastPathBatch(ids []int64) Batch {
	values := make([]int64, len(ids))
	timeUS := make([]int64, len(ids))
	kindCode := make([]int64, len(ids))
	hasReply := make([]int64, len(ids))
	for i := range ids {
		values[i] = int64((len(ids) - i) * 100)
		timeUS[i] = int64((len(ids) - i) * 10)
		kindCode[i] = int64(i % 3)
		hasReply[i] = int64(i % 2)
	}
	return Batch{Columns: map[string][]int64{
		"id":        append([]int64(nil), ids...),
		"time_us":   timeUS,
		"value":     values,
		"kind_code": kindCode,
		"has_reply": hasReply,
	}}
}

func loadPathFastPathSequentialBatch(rows int) Batch {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	return loadPathFastPathBatch(ids)
}
