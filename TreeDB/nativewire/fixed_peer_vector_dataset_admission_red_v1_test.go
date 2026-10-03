package nativewire

import (
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"strings"
	"testing"
)

// This file deliberately uses only APIs present at e606f7d, so the old512-row
// refusal is a behavior failure rather than missing-symbol compilation failure.
func TestFixedPeerVectorDatasetTargetAdmissionRedV1(t *testing.T) {
	config := initializationTestConfigsV1(t)[0]
	config.VectorInitialization.MaxSourceRows = 10003
	config.VectorInitialization.IndexDefinition.Dimensions = 128
	if _, err := InspectFixedPeerTCPConfigV1(config); err != nil {
		t.Fatalf("10003rows128D production intent refused: %v", err)
	}
}
func TestVectorPrepareDatasetTargetAdmissionRedV1(t *testing.T) {
	command := commitlog.VectorPrepareV1{Version: 1, Operation: "rebuild", Collection: "docs", Index: "embedding_graph", Group: "group-1", IndexDefinitionDigest: strings.Repeat("a", 64), Generation: 1, MaxSourceRows: 10003}
	if err := command.ValidateV1(); err != nil {
		t.Fatalf("10003row rebuild admission refused: %v", err)
	}
}
