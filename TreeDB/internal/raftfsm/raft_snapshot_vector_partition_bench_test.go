//go:build treedb_benchmark

package raftfsm

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func BenchmarkRaftSnapshotV1VectorPartitionArchiveInstall(b *testing.B) {
	for _, rows := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("memberships=%d", rows), func(b *testing.B) {
			root := b.TempDir()
			sourceDir := filepath.Join(root, "source")
			sourceDB := openRaftSnapshotFSMTestDB(b, sourceDir, true)
			defer sourceDB.Close()
			sourceFSM := openRaftSnapshotFSMForTest(b, sourceDB, sourceDir, true)
			defer sourceFSM.Close()
			applySnapshotSourceEntries(b, sourceFSM, []byte(`{"_id":"benchmark","name":"snapshot"}`))
			manifest := stageRaftSnapshotReadyVectorPartitionForTest(b, sourceDB, 1)
			stageRaftSnapshotSyntheticReadyScaleForBenchmark(b, sourceDir, manifest, rows)
			b.Run("archive", func(b *testing.B) {
				var archiveBytes int
				for i := 0; i < b.N; i++ {
					snapshot, err := sourceFSM.ExportRaftSnapshotV1()
					if err != nil {
						b.Fatal(err)
					}
					archiveBytes = len(readRaftSnapshotArchiveForTest(b, snapshot))
				}
				b.ReportMetric(float64(archiveBytes), "archive-bytes")
			})
			snapshot, err := sourceFSM.ExportRaftSnapshotV1()
			if err != nil {
				b.Fatal(err)
			}
			b.Run("install", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					targetDir := filepath.Join(root, fmt.Sprintf("target-%d", i))
					targetDB := openRaftSnapshotFSMTestDB(b, targetDir, false)
					targetFSM := openRaftSnapshotFSMForTest(b, targetDB, targetDir, true)
					installRaftSnapshotForTest(b, targetFSM, snapshot)
					targetStore, err := collections.OpenExistingVectorPartitionStoreV1(targetDir)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := targetStore.OpenActive("docs", "embedding"); err != nil {
						b.Fatal(err)
					}
					targetFSM.Close()
					targetDB.Close()
				}
			})
		})
	}
}

// stageRaftSnapshotSyntheticReadyScaleForBenchmark expands only the durable
// M1 manifest record after a real collection-authorized ready generation has
// been staged. It intentionally excludes construction of a rows-sized TVIS or
// HNSW graph: archive/install evidence here measures bounded manifest metadata
// and side-store transport, while TestRaftSnapshotV1InstallPreserves...
// separately proves authority publication and active ready recovery.
func stageRaftSnapshotSyntheticReadyScaleForBenchmark(tb testing.TB, root string, manifest collections.VectorPartitionManifestV1, rows int) {
	tb.Helper()
	if rows < 1 {
		tb.Fatal("synthetic ready scale requires positive rows")
	}
	expected := manifest
	manifest.SourceRowCount = uint64(rows)
	manifest.Memberships = make([]collections.VectorPartitionMembershipV1, rows)
	for i := range manifest.Memberships {
		manifest.Memberships[i] = collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	manifest.Canonicalize()
	raw, err := collections.EncodeVectorPartitionManifestV1(manifest)
	if err != nil {
		tb.Fatal(err)
	}
	if rows == 1_000_000 && len(raw) > rows*64 {
		tb.Fatalf("synthetic ready manifest=%d bytes exceeds 64 B/vector", len(raw))
	}
	if err := collections.StageSyntheticReadyVectorPartitionForBenchmarkV1(root, expected, manifest); err != nil {
		tb.Fatal(err)
	}
	store, err := collections.OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		tb.Fatal(err)
	}
	got, err := store.OpenActive("docs", "embedding")
	if err != nil || got.SourceRowCount != uint64(rows) {
		tb.Fatalf("synthetic ready fixture active=%+v err=%v", got, err)
	}
}

func TestStageSyntheticReadyVectorPartitionForBenchmarkV1RequiresExactFixture(t *testing.T) {
	root := t.TempDir()
	sourceDB := openRaftSnapshotFSMTestDB(t, root, true)
	defer sourceDB.Close()
	original := stageRaftSnapshotReadyVectorPartitionForTest(t, sourceDB, 1)
	replacement := original
	replacement.SourceRowCount = 2
	replacement.Memberships = append(replacement.Memberships, collections.VectorPartitionMembershipV1{
		VectorOrdinal: 1,
		PartitionID:   0,
	})
	replacement.Canonicalize()

	if err := collections.StageSyntheticReadyVectorPartitionForBenchmarkV1(root, replacement, replacement); err == nil {
		t.Fatal("synthetic benchmark replacement accepted a mismatched fixture expectation")
	}
	store, err := collections.OpenExistingVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	active, err := store.OpenActive(original.Collection, original.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	if active.SourceRowCount != original.SourceRowCount || len(active.Memberships) != len(original.Memberships) {
		t.Fatalf("rejected synthetic replacement mutated active fixture: %+v", active)
	}
}
