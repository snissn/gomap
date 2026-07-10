package treedb_test

import (
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestErrLeafGenerationGCStaleScanReexportsBackendSentinel(t *testing.T) {
	if !errors.Is(treedbdb.ErrLeafGenerationGCStaleScan, treedb.ErrLeafGenerationGCStaleScan) {
		t.Fatal("public ErrLeafGenerationGCStaleScan does not match backend sentinel")
	}
}
