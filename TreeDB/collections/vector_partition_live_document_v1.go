package collections

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ProveVectorPartitionLiveDocumentV1 proves current content, not document
// incarnation: replacing a document with identical content is indistinguishable.
// The live pin and backend snapshot are captured under publication admission.
// Both the document generation and reconstructed JSON come from that snapshot;
// an ordinary Get would instead read buffered state or acquire a second snapshot.
// Only reconstructable JSON content is supported. No document incarnation or
// durability beyond the caller's existing committed-apply proof is inferred.
func (c *Collection) ProveVectorPartitionLiveDocumentV1(ctx context.Context, manifest VectorPartitionManifestV1, id, expected []byte) (VectorIndexPartitionLiveStatusV1, error) {
	var zero VectorIndexPartitionLiveStatusV1
	if c == nil || c.db == nil || c.writeDomain == nil || len(id) == 0 || manifest.Collection != c.collectionName() {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockAdmission := c.lockVectorIndexSynchronousPublicationAdmission()
	defer unlockAdmission()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	defer snap.Close()
	pin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
	if err != nil {
		return zero, err
	}
	defer pin.Release()
	return c.proveVectorPartitionLiveDocumentAtSnapshotV1(ctx, snap, pin, manifest, id, expected)
}

// The caller owns both immutable handles. Keeping this read separate lets a
// retained proof be checked after newer publication without consulting current
// collection state or confusing the two revisions.
func (c *Collection) proveVectorPartitionLiveDocumentAtSnapshotV1(ctx context.Context, snap *backenddb.Snapshot, pin *VectorIndexPartitionLiveSearchPinV1, manifest VectorPartitionManifestV1, id, expected []byte) (VectorIndexPartitionLiveStatusV1, error) {
	var zero VectorIndexPartitionLiveStatusV1
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return zero, err
	}
	if catalog == nil {
		return zero, ErrVectorIndexPartitionLiveUnavailableV1
	}
	generation, err := vectorIndexDocumentGenerationForCollection(snap, c.collectionName())
	if err != nil {
		return zero, err
	}
	status := pin.StatusV1()
	if status.Generation != manifest.Generation || status.Revision == 0 || generation == 0 || status.Coverage != generation || !pin.ContainsLiveIDV1(string(id)) {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	current, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, catalog.primaryRootName, id, nil)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	if columnStoreCanReconstructDocument(catalog.meta) {
		current, err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, id, current)
		if err != nil {
			return zero, err
		}
	}
	equal, err := vectorPartitionLiveDocumentEqualV1(catalog.meta, id, expected, current)
	if err != nil {
		return zero, err
	}
	if !equal {
		return zero, ErrVectorIndexPartitionLiveMismatchV1
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return status, nil
}

// VectorPartitionLiveDocumentProofSupportedV1 reports whether the collection
// can compare a submitted JSON document with its retained live contents. It is
// checked before a public vector listener advertises insert support and again
// by the proof, so unsupported storage modes fail before submission.
func VectorPartitionLiveDocumentProofSupportedV1(meta CollectionMeta) bool {
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON {
		return false
	}
	if !columnStoreNeedsRetainedPayloadTransform(meta) {
		return true
	}
	return meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadNonColumn && columnStoreCanReconstructDocument(meta)
}

func vectorPartitionLiveDocumentEqualV1(meta CollectionMeta, id, expected, current []byte) (bool, error) {
	if len(current) == 0 {
		return false, nil
	}
	if !VectorPartitionLiveDocumentProofSupportedV1(meta) {
		return false, ErrVectorIndexPartitionLiveUnavailableV1
	}
	if columnStoreNeedsRetainedPayloadTransform(meta) {
		cfg := *meta.Options.ColumnStore
		retained, err := columnRetainedPayloadFromJSONDocument(cfg, expected)
		if err != nil {
			return false, err
		}
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(cfg, []columnWriteDocument{{ID: id, Document: expected}})
		if err != nil {
			return false, err
		}
		expected, err = reconstructColumnJSONDocument(cfg, retained, rows[0].Values)
		if err != nil {
			return false, err
		}
	}
	want, err := decodeColumnJSONObject(expected) // UseNumber and EOF required.
	if err != nil {
		return false, err
	}
	got, err := decodeColumnJSONObject(current)
	if err != nil {
		return false, err
	}
	return vectorPartitionLiveJSONEqualV1(want, got), nil
}

func vectorPartitionLiveJSONEqualV1(a, b any) bool {
	switch a := a.(type) {
	case map[string]any:
		b, ok := b.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, exists := b[key]
			if !exists || !vectorPartitionLiveJSONEqualV1(value, other) {
				return false
			}
		}
		return true
	case []any:
		b, ok := b.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !vectorPartitionLiveJSONEqualV1(a[i], b[i]) {
				return false
			}
		}
		return true
	case json.Number:
		b, ok := b.(json.Number)
		if !ok {
			return false
		}
		if a == b {
			return true
		}
		// Compare exact decimal coefficients/scales, without float64 rounding
		// or allocating 10^exponent for a short hostile exponent token.
		x, xe := vectorPartitionLiveJSONNumberV1(string(a))
		y, ye := vectorPartitionLiveJSONNumberV1(string(b))
		return x == y && xe.Cmp(ye) == 0
	default: // JSON null, boolean, or string; never a float64.
		return a == b
	}
}

// Inputs were validated as JSON numbers by decodeColumnJSONObject. Work and
// storage depend on the token length, not on the numeric exponent's magnitude.
func vectorPartitionLiveJSONNumberV1(number string) (string, *big.Int) {
	exponent := new(big.Int)
	if i := strings.IndexAny(number, "eE"); i >= 0 {
		exponent.SetString(number[i+1:], 10)
		number = number[:i]
	}
	sign := ""
	if strings.HasPrefix(number, "-") {
		sign, number = "-", number[1:]
	}
	fraction := 0
	if i := strings.IndexByte(number, '.'); i >= 0 {
		fraction = len(number) - i - 1
		number = number[:i] + number[i+1:]
	}
	number = strings.TrimLeft(number, "0")
	if number == "" {
		return "0", new(big.Int)
	}
	coefficient := strings.TrimRight(number, "0")
	exponent.Add(exponent, big.NewInt(int64(len(number)-len(coefficient)-fraction)))
	return sign + coefficient, exponent
}
