package documentservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkHTTPUpsertDecodePrepareNumericVsCompact(b *testing.B) {
	const (
		dimension = 768
		documents = 128
	)
	embedding := make([]float32, dimension)
	for i := range embedding {
		embedding[i] = float32(i%31+1) / 31
	}
	numericDocs := make([]Document, documents)
	compactDocs := make([]Document, documents)
	encoded := encodeFloat32LEBase64ForTest(embedding)
	for i := range numericDocs {
		id := fmt.Sprintf("doc-%03d", i)
		numericDocs[i] = Document{ID: id, Embedding: embedding}
		compactDocs[i] = Document{ID: id, EmbeddingF32LEBase64: encoded}
	}
	info := IndexInfo{Dimension: dimension, Metric: MetricCosine}
	for _, tc := range []struct {
		name string
		req  UpsertDocumentsRequest
	}{
		{name: "numeric_json", req: UpsertDocumentsRequest{Documents: numericDocs}},
		{name: "compact_f32le_base64", req: UpsertDocumentsRequest{Documents: compactDocs}},
	} {
		payload, err := json.Marshal(tc.req)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var req UpsertDocumentsRequest
				dec := json.NewDecoder(bytes.NewReader(payload))
				dec.UseNumber()
				dec.DisallowUnknownFields()
				if err := dec.Decode(&req); err != nil {
					b.Fatal(err)
				}
				if _, err := prepareDocumentsForWrite(req.Documents, info); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
