package collectionwal

import (
	"errors"
	"testing"
)

func TestValidateV1RefClass(t *testing.T) {
	wantNames := map[RefClass]string{
		RefClassValueLogRecord:         "ValueLogRecord",
		RefClassLeafLogRecord:          "LeafLogRecord",
		RefClassRootDeltaPayload:       "RootDeltaPayload",
		RefClassColumnManifest:         "ColumnManifest",
		RefClassColumnSubstreamFile:    "ColumnSubstreamFile",
		RefClassColumnFilterFile:       "ColumnFilterFile",
		RefClassColumnDeleteBitmapFile: "ColumnDeleteBitmapFile",
		RefClassColumnDictionaryFile:   "ColumnDictionaryFile",
		RefClassColumnMetadataFile:     "ColumnMetadataFile",
	}
	for c := RefClassValueLogRecord; c <= RefClassColumnMetadataFile; c++ {
		if err := ValidateV1RefClass(c); err != nil {
			t.Fatalf("ValidateV1RefClass(%s): %v", c, err)
		}
		if c.String() != wantNames[c] {
			t.Fatalf("RefClass(%d).String()=%q want %q", c, c.String(), wantNames[c])
		}
	}
	for _, c := range []RefClass{RefClassInvalid, 10, 255} {
		err := ValidateV1RefClass(c)
		if !errors.Is(err, ErrCollectionWALUnsupportedVersion) {
			t.Fatalf("ValidateV1RefClass(%d)=%v want unsupported version", c, err)
		}
	}
}
