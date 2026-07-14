package valuelog

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestWriterStableIdentityPreservesUnresolvedResourceClassification(t *testing.T) {
	tests := []struct {
		name   string
		writer *Writer
	}{
		{name: "nil-receiver"},
		{name: "non-file-backed", writer: &Writer{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity, err := test.writer.StableIdentity()
			if identity != (rootpublication.StableIdentity{}) || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("StableIdentity identity=%+v error=%v want zero identity and unresolved resource", identity, err)
			}
		})
	}
}
