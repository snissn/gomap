package collections

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

const (
	templateV1ShapeBenchDefaultBatchSize  = 8192
	templateV1ShapeBenchDefaultFieldCount = 32
	templateV1ShapeBenchDefaultShapeCount = 4096
)

func BenchmarkTemplateV1PrepareHomogeneousWide(b *testing.B) {
	batchSize := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_BATCH_SIZE", templateV1ShapeBenchDefaultBatchSize)
	fieldCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_FIELD_COUNT", templateV1ShapeBenchDefaultFieldCount)
	docs := templateV1ShapeBenchDocuments(b, batchSize, fieldCount, 1)
	benchmarkTemplateV1PrepareDocuments(b, docs, fieldCount, 1, nil)
}

func BenchmarkTemplateV1PrepareHeterogeneousShapes(b *testing.B) {
	batchSize := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_BATCH_SIZE", templateV1ShapeBenchDefaultBatchSize)
	fieldCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_FIELD_COUNT", 8)
	shapeCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_COUNT", templateV1ShapeBenchDefaultShapeCount)
	docs := templateV1ShapeBenchDocuments(b, batchSize, fieldCount, shapeCount)
	benchmarkTemplateV1PrepareDocuments(b, docs, fieldCount, shapeCount, nil)
}

func BenchmarkTemplateV1PrepareHomogeneousWideLearnedIDs(b *testing.B) {
	batchSize := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_BATCH_SIZE", templateV1ShapeBenchDefaultBatchSize)
	fieldCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_FIELD_COUNT", templateV1ShapeBenchDefaultFieldCount)
	docs, resolver := templateV1ShapeBenchLearnedDocuments(b, batchSize, fieldCount, 1)
	benchmarkTemplateV1PrepareDocuments(b, docs, fieldCount, 1, resolver)
}

func BenchmarkTemplateV1PrepareHeterogeneousShapesLearnedIDs(b *testing.B) {
	batchSize := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_BATCH_SIZE", templateV1ShapeBenchDefaultBatchSize)
	fieldCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_FIELD_COUNT", 8)
	shapeCount := templateV1ShapeBenchEnvInt(b, "TREEDB_TEMPLATE_V1_SHAPE_COUNT", templateV1ShapeBenchDefaultShapeCount)
	docs, resolver := templateV1ShapeBenchLearnedDocuments(b, batchSize, fieldCount, shapeCount)
	benchmarkTemplateV1PrepareDocuments(b, docs, fieldCount, shapeCount, resolver)
}

func benchmarkTemplateV1PrepareDocuments(b *testing.B, docs [][]byte, fieldCount, shapeCount int, resolver templateV1Resolver) {
	b.Helper()
	prepared, records, _, preparedResolver, err := prepareTemplateV1InsertDocuments(docs, resolver, false, true)
	if err != nil {
		b.Fatalf("validate template-v1 shape documents: %v", err)
	}
	if len(prepared) != len(docs) {
		b.Fatalf("prepared docs=%d want %d", len(prepared), len(docs))
	}
	if preparedResolver == nil {
		b.Fatal("expected template resolver")
	}
	templateV1BenchmarkSink = prepared[0]

	b.ReportAllocs()
	b.ReportMetric(float64(len(docs)), "docs/batch")
	b.ReportMetric(float64(fieldCount), "fields/doc")
	b.ReportMetric(float64(shapeCount), "configured_shapes")
	b.ReportMetric(float64(len(records)), "published_templates")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, _, _, _, err := prepareTemplateV1InsertDocuments(docs, resolver, false, true)
		if err != nil {
			b.Fatalf("prepare template-v1 shape documents: %v", err)
		}
		templateV1BenchmarkSink = prepared[i%len(prepared)]
	}
}

func templateV1ShapeBenchDocuments(tb testing.TB, count, fieldCount, shapeCount int) [][]byte {
	tb.Helper()
	if count <= 0 {
		tb.Fatalf("count must be > 0")
	}
	if fieldCount <= 0 {
		tb.Fatalf("field count must be > 0")
	}
	if shapeCount <= 0 {
		tb.Fatalf("shape count must be > 0")
	}
	docs := make([][]byte, count)
	var encoder TemplateV1Encoder
	for i := 0; i < count; i++ {
		fields, values := templateV1ShapeBenchFieldsAndValues(fieldCount, i, i%shapeCount)
		doc, err := encoder.EncodeDocument(fields, values)
		if err != nil {
			tb.Fatalf("encode shape doc %d: %v", i, err)
		}
		docs[i] = doc
	}
	return docs
}

func templateV1ShapeBenchLearnedDocuments(tb testing.TB, count, fieldCount, shapeCount int) ([][]byte, templateV1Resolver) {
	tb.Helper()
	seed := templateV1ShapeBenchDocuments(tb, count, fieldCount, shapeCount)
	_, _, learned, resolver, err := prepareTemplateV1InsertDocuments(seed, nil, true, true)
	if err != nil {
		tb.Fatalf("prepare seed shape documents: %v", err)
	}
	var encoder TemplateV1Encoder
	encoder.learnTemplateV1Templates(nil, learned)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		fields, values := templateV1ShapeBenchFieldsAndValues(fieldCount, i, i%shapeCount)
		doc, err := encoder.EncodeDocument(fields, values)
		if err != nil {
			tb.Fatalf("encode learned shape doc %d: %v", i, err)
		}
		if !hasTemplateV1Magic(doc, templateV1StoredMagic) {
			tb.Fatalf("learned shape doc %d prefix=%q, want stored magic", i, doc[:min(len(doc), len(templateV1StoredMagic))])
		}
		docs[i] = doc
	}
	return docs, resolver
}

func templateV1ShapeBenchFieldsAndValues(fieldCount, docOrdinal, shapeOrdinal int) ([]string, []any) {
	fields := make([]string, fieldCount)
	values := make([]any, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if shapeOrdinal == 0 {
			fields[i] = fmt.Sprintf("field_%03d", i)
		} else {
			fields[i] = fmt.Sprintf("field_%06d_%03d", shapeOrdinal, i)
		}
		values[i] = fmt.Sprintf("value_%03d_%09d", i, docOrdinal)
	}
	return fields, values
}

func templateV1ShapeBenchEnvInt(tb testing.TB, key string, fallback int) int {
	tb.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		tb.Fatalf("unsupported %s=%q", key, raw)
	}
	return n
}
