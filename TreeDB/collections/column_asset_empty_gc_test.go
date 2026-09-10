package collections

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestTypedGraphWorkEpochEmptyDeniedOutput(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	// Fixture writes are visible before deferred root publication finishes.
	// Settle the recovery roots before testing destructive GC.
	if err := col.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 4 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphTestWorkEpochLimits()
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); err != nil {
		t.Fatal(err)
	}
	seq, root := dbCommitSeqAndSystemRoot(col.db)
	err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldAssetLimits{Bytes: 1, AppenderAttempts: 4}, nil)
	if !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("expected pre-write byte denial: %v", err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
		t.Fatal("byte denial changed authority")
	}
	coord := col.collectionSchemaCoordinator()
	if coord.typedGraphCandidateAttempts != 1 || coord.typedGraphCandidateBytes != 0 {
		t.Fatalf("empty output debt attempts=%d bytes=%d", coord.typedGraphCandidateAttempts, coord.typedGraphCandidateBytes)
	}
	epoch := coord.typedGraphWorkEpoch
	stats, err := col.renewTypedGraphWorkEpoch(context.Background(), limits)
	if !rootpublication.StableRelativeNamespaceSupported() {
		wantErr := ErrColumnAssetReachabilityIncomplete
		if rootpublication.StableNamespaceCreationSupported() {
			// Exact discovery can verify the empty file while deletion is unsupported.
			wantErr = rootpublication.ErrNamespacePersistenceUnsupported
		}
		if !errors.Is(err, wantErr) || stats.Columns.Plan.Complete != rootpublication.StableNamespaceCreationSupported() || stats.Columns.SegmentsDeleted != 0 || coord.typedGraphCandidateAttempts != 1 || coord.typedGraphWorkEpoch != epoch {
			t.Fatalf("unsupported empty cleanup must retain debt and authority: stats=%+v err=%v", stats, err)
		}
		return
	}
	if err != nil || stats.Columns.SegmentsDeleted != 1 || stats.Columns.BytesDeleted != 0 {
		t.Fatalf("empty denied output renewal=%+v err=%v", stats, err)
	}
	if coord.typedGraphCandidateAttempts != 0 {
		t.Fatal("successful empty cleanup did not renew attempt debt")
	}
}

func TestColumnAssetGCEmptyConstructionPin(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	// Fixture writes are visible before deferred root publication finishes.
	// Settle the recovery roots before testing destructive GC.
	if err := col.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(col.db.ColumnAssetRootDir(), *col.Meta().Options.ColumnStore, col.db.StableResourceIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer appender.abort()
	path := appender.assetPath
	stats, err := col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if err != nil || stats.SegmentsEligible != 1 || stats.SegmentsDeleted != 0 || stats.BytesEligible != 0 {
		t.Fatalf("live empty construction stats=%+v err=%v", stats, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("deleted live construction: %v", err)
	}
	if err := appender.abort(); err != nil {
		t.Fatal(err)
	}
	stats, err = col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{})
	if err != nil || stats.SegmentsDeleted != 1 || stats.BytesDeleted != 0 {
		t.Fatalf("released empty construction stats=%+v err=%v", stats, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released empty output still present: %v", err)
	}
}

func TestColumnAssetGCEmptyReferencedOrChanged(t *testing.T) {
	for _, mode := range []string{"referenced", "changed_after_plan"} {
		t.Run(mode, func(t *testing.T) {
			requireColumnAssetExactDestructiveGCTest(t)
			col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
			defer base.Close()
			// Fixture writes are visible before deferred root publication finishes.
			// Settle the recovery roots before testing destructive GC.
			if err := col.db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(col.db.ColumnAssetRootDir(), *col.Meta().Options.ColumnStore, col.db.StableResourceIdentityPinRegistry())
			if err != nil {
				t.Fatal(err)
			}
			path, fileID := appender.assetPath, appender.fileID
			if err := appender.abort(); err != nil {
				t.Fatal(err)
			}
			opts := ColumnAssetGCOptions{}
			want := ErrColumnAssetReachabilityIncomplete
			if mode == "referenced" {
				opts.PinnedRefs = []ColumnAssetRef{{Kind: ColumnAssetKindTCS1PartImage, Namespace: col.Meta().Options.ColumnStore.AssetManager.Namespace, Generation: 1, PartID: 99, FileID: fileID, Length: 1}}
			} else {
				want = ErrColumnAssetGCPlanStale
				restore := setColumnAssetStableDeleteAfterPlanTestHook(func() {
					if err := os.WriteFile(path, []byte("post-plan bytes"), 0o600); err != nil {
						t.Fatal(err)
					}
				})
				defer restore()
			}
			stats, err := col.ColumnAssetGC(context.Background(), opts)
			if !errors.Is(err, want) || stats.SegmentsDeleted != 0 {
				t.Fatalf("unsafe empty deletion stats=%+v err=%v want=%v", stats, err, want)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("unsafe empty output removed: %v", err)
			}
		})
	}
}

func TestColumnAssetGCEmptyNonregularAndQuarantine(t *testing.T) {
	for _, mode := range []string{"directory", "symlink", "unknown_name", "quarantine"} {
		t.Run(mode, func(t *testing.T) {
			requireColumnAssetExactDestructiveGCTest(t)
			col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
			defer base.Close()
			// Fixture writes are visible before deferred root publication finishes.
			// Settle the recovery roots before testing destructive GC.
			if err := col.db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(col.db.ColumnAssetRootDir(), *col.Meta().Options.ColumnStore, col.db.StableResourceIdentityPinRegistry())
			if err != nil {
				t.Fatal(err)
			}
			path, fileID := appender.assetPath, appender.fileID
			if err := appender.abort(); err != nil {
				t.Fatal(err)
			}
			opts := ColumnAssetGCOptions{}
			switch mode {
			case "directory", "symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if mode == "directory" {
					err = os.Mkdir(path, 0o700)
				} else {
					err = os.Symlink(t.TempDir(), path)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "unknown_name":
				if err := os.Rename(path, path+".unknown"); err != nil {
					t.Fatal(err)
				}
				path += ".unknown"
			case "quarantine":
				opts.QuarantineSegments = []ColumnAssetQuarantineSegment{{Namespace: col.Meta().Options.ColumnStore.AssetManager.Namespace, FileID: fileID}}
			}
			stats, err := col.ColumnAssetGC(context.Background(), opts)
			if mode == "quarantine" {
				if err != nil || !stats.Plan.Complete || stats.Plan.Segments.QuarantineSegments != 1 {
					t.Fatalf("empty quarantine plan=%+v err=%v", stats.Plan, err)
				}
			} else if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) {
				t.Fatalf("nonregular/unknown plan became complete: %+v err=%v", stats.Plan, err)
			}
			if stats.SegmentsDeleted != 0 {
				t.Fatalf("deleted protected entry: %+v", stats)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("protected entry removed: %v", err)
			}
		})
	}
}
