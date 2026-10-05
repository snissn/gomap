package collections

import (
	"fmt"
	"testing"
)

// This file deliberately uses only predecessor APIs so the identical guard can
// measure the shared iterator/JSON/typed-point paths before and after this change.
func BenchmarkPopulationSharedIteratorGuardV1(b *testing.B) {
	older, newer := newCollectionRunTable(1024), newCollectionRunTable(1024)
	defer resetCollectionRunTable(older)
	defer resetCollectionRunTable(newer)
	for i := 0; i < 1024; i++ {
		key := []byte(fmt.Sprintf("%04d", i))
		setCollectionRunValue(older, key, []byte("old"))
		if i%2 == 0 {
			newer.DeleteSteal(key)
		} else {
			setCollectionRunValue(newer, key, []byte("current"))
		}
	}
	older.Freeze()
	newer.Freeze()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect([]bufferedRootRunIteratorSource{{iter: newer.NewIterator(nil, nil)}, {iter: older.NewIterator(nil, nil)}}, nil, nil, false, true, false, 4096, nil)
		rows := 0
		for it.Valid() {
			rows++
			it.Next()
		}
		if err := it.Error(); err != nil || rows != 512 {
			b.Fatalf("rows=%d err=%v", rows, err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPopulationSharedJSONGuardV1(b *testing.B) {
	for name, raw := range map[string]string{"valid": `{"embedding":[1,0,-0]}`, "missing": `{"other":1}`, "null": `{"embedding":null}`, "invalid": `{"embedding":"bad"}`} {
		b.Run(name, func(b *testing.B) {
			input, path := []byte(raw), []string{"embedding"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = vectorFromJSONField(input, path)
			}
		})
	}
}

func BenchmarkPopulationSharedTypedPointGuardV1(b *testing.B) {
	field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
	field.VectorDims = 128
	input := make([]typedColumnAdapterRow, 1024)
	for i := range input {
		v := make([]float32, 128)
		v[i%128] = 1
		input[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: v}}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 42, RowsPerGranule: 64, Fields: []TypedStorageField{field}}, input)
	if err != nil {
		b.Fatal(err)
	}
	column := part.Part.Columns["embedding"]
	b.ReportAllocs()
	b.SetBytes(128 * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := typedColumnPointFloat32Vector(column, i%1024)
		if err != nil || len(v) != 128 {
			b.Fatalf("vector len=%d err=%v", len(v), err)
		}
	}
}
