package db_test

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// The original DATA V1 witness remains immutable historical evidence, with no
// current runner coverage. Deletion, relabeling, or replacing it with V5/V6
// bytes must fail independently of the successor witnesses' assertions.
func TestHistoricalStaleBuildV1ArchiveExactLineageAndNoCurrentCoverage(t *testing.T) {
	root := filepath.Join("..", "testdata", "power_loss_legacy_stale_build_v1")
	expected := map[string]string{
		"witness.go.txt": "0276e8409fdcdc098f19f5cd60ee44eed6699229",
		"ledger.json":    "38559ed2122b96de56c5d24e95cc8e9eda61dd12",
		"contracts.json": "35d61ed90f5315449da949c16a414a9c6f4750ff",
	}
	for name, blob := range expected {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		object := append([]byte(fmt.Sprintf("blob %d\x00", len(data))), data...)
		sum := sha1.Sum(object)
		if got := hex.EncodeToString(sum[:]); got != blob {
			t.Fatalf("historical %s blob=%s want=%s", name, got, blob)
		}
	}
	var binding struct {
		Schema          string `json:"schema_version"`
		SourceHead      string `json:"source_head"`
		CurrentCoverage *bool  `json:"current_coverage"`
		HistoricalID    string `json:"historical_id"`
		HistoricalCut   string `json:"historical_cut"`
		HistoricalSeed  uint64 `json:"historical_seed"`
		Generation      uint64 `json:"freelist_generation"`
		ReceiptSHA256   string `json:"replay_receipt_sha256"`
		BinarySHA256    string `json:"replay_binary_sha256"`
		Bindings        []struct {
			Archive    string `json:"archive"`
			SourcePath string `json:"source_path"`
			Blob       string `json:"git_blob"`
			SHA256     string `json:"sha256"`
			Bytes      int    `json:"bytes"`
		} `json:"bindings"`
	}
	data, err := os.ReadFile(filepath.Join(root, "binding.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &binding); err != nil {
		t.Fatal(err)
	}
	const historicalID = "stale-build-base-root-publication"
	const historicalCut = "cut/public-stale-build-base-retry-stable-image/after-meta-sync/000"
	if binding.Schema != "treedb-historical-stale-build-archive/v1" || binding.SourceHead != "19ba9fdbca4ca85afc544a8b342d66aad32f01b2" || binding.CurrentCoverage == nil || *binding.CurrentCoverage || binding.HistoricalID != historicalID || binding.HistoricalCut != historicalCut || binding.HistoricalSeed != 12505447533306515078 || binding.Generation != 8 || binding.BinarySHA256 != "3239853dcba11be07c4921fa0bef9670b741f0b1d32466bdce6596ee28fd6425" {
		t.Fatalf("historical lineage changed: %+v", binding)
	}
	paths := map[string]string{"witness.go.txt": "TreeDB/db/power_loss_certification_stale_build_test.go", "ledger.json": "TreeDB/testdata/power_loss_counterexamples.json", "contracts.json": "TreeDB/testdata/power_loss_witness_contracts.json"}
	seen := make(map[string]bool)
	for _, source := range binding.Bindings {
		if seen[source.Archive] || paths[source.Archive] != source.SourcePath || expected[source.Archive] != source.Blob {
			t.Fatalf("substituted historical source binding: %+v", source)
		}
		seen[source.Archive] = true
		data, err := os.ReadFile(filepath.Join(root, source.Archive))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if len(data) != source.Bytes || hex.EncodeToString(sum[:]) != source.SHA256 {
			t.Fatalf("source binding does not address exact bytes: %+v", source)
		}
	}
	if len(seen) != len(expected) {
		t.Fatal("historical source binding deleted")
	}
	receipt, err := os.ReadFile(filepath.Join(root, "original-replay.log"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(receipt)
	if hex.EncodeToString(sum[:]) != binding.ReceiptSHA256 || !bytes.Contains(receipt, []byte("--- PASS: TestPowerLossCertificationStaleBuildBasePublicReopen")) {
		t.Fatal("historical replay receipt changed")
	}
	for _, name := range []string{"power_loss_counterexamples.json", "power_loss_witness_contracts.json", "power_loss_risk_inventory.json"} {
		active, err := os.ReadFile(filepath.Join("..", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(active, []byte(historicalID)) || bytes.Contains(active, []byte(historicalCut)) {
			t.Fatalf("historical witness claims current coverage in %s", name)
		}
		for _, successor := range []string{"stale-build-base-saved-primary-v5", "stale-build-base-primary-capsule-v6"} {
			if !bytes.Contains(active, []byte(successor)) {
				t.Fatalf("%s lost independent successor %s", name, successor)
			}
		}
	}
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if witness.ID == historicalID {
			t.Fatal("historical witness reentered current runner registry")
		}
	}
}
