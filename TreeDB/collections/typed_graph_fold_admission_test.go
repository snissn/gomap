package collections

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func typedGraphFoldTestAssetLimits() typedGraphFoldAssetLimits {
	return typedGraphFoldAssetLimits{Bytes: 1 << 30, AppenderAttempts: 4096}
}

func TestTypedGraphFoldCandidateFailureDebt(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	limits := typedGraphFoldAssetLimits{Bytes: 1 << 20, AppenderAttempts: 4}
	injected := errors.New("after candidate row preparation")
	restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(ColumnPublishPreparedAssets) error { return injected })
	defer restore()
	seq, root := dbCommitSeqAndSystemRoot(col.db)
	beforeBytes, beforeFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
	beforeWork := workstats.Read().Fold
	var previousBytes int64
	for i := 0; i < 6; i++ {
		err := col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, limits, nil)
		if !errors.Is(err, injected) && !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
			t.Fatalf("attempt %d: %v", i, err)
		}
		coord := col.collectionSchemaCoordinator()
		if coord.typedGraphCandidateBytes <= 0 || coord.typedGraphCandidateBytes < previousBytes || coord.typedGraphCandidateBytes > limits.Bytes || coord.typedGraphCandidateAttempts > limits.AppenderAttempts {
			t.Fatal("failed candidate lost/exceeded debt")
		}
		previousBytes = coord.typedGraphCandidateBytes
		if coord.typedGraphFoldActive.Load() {
			t.Fatal("failed candidate retained builder")
		}
	}
	afterWork := workstats.Read().Fold
	if afterWork.Build.Attempts-beforeWork.Build.Attempts != 6 || afterWork.Build.Errors-beforeWork.Build.Errors != 6 || afterWork.Build.Completed != beforeWork.Build.Completed || afterWork.Publications != beforeWork.Publications || afterWork.CandidateBytesCharged-beforeWork.CandidateBytesCharged != uint64(previousBytes) || afterWork.AppenderAttemptsCharged == beforeWork.AppenderAttemptsCharged {
		t.Fatalf("failed candidate producer before=%+v after=%+v", beforeWork, afterWork)
	}
	afterBytes, afterFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
	if afterBytes-beforeBytes > previousBytes || afterFiles-beforeFiles > limits.AppenderAttempts {
		t.Fatal("attempt debt does not cover retained files")
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
		t.Fatal("failed candidate changed authority")
	}
}

func TestTypedGraphFoldCandidateOutputCharge(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	beforeBytes, beforeFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
	limits := typedGraphFoldTestAssetLimits()
	coord := col.collectionSchemaCoordinator()
	for i := 0; i < 2; i++ {
		if err := col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, limits, nil); err != nil {
			t.Fatal(err)
		}
		afterBytes, afterFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
		if coord.typedGraphCandidateBytes != afterBytes-beforeBytes || coord.typedGraphCandidateBytes == 0 || afterFiles-beforeFiles > coord.typedGraphCandidateAttempts {
			t.Fatalf("fold %d charged=%d files=%d actual bytes=%d files=%d", i, coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts, afterBytes-beforeBytes, afterFiles-beforeFiles)
		}
	}
	other, err := NewCollectionManager(col.db).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	limits.AppenderAttempts++
	if err := other.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, limits, nil); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("other manager replaced bound identity: %v", err)
	}
}

func TestTypedGraphCandidateAppenderPaddedAdmission(t *testing.T) {
	for _, route := range []string{"aligned", "reserved", "batch"} {
		for _, admit := range []bool{false, true} {
			t.Run(route+map[bool]string{false: "/reject", true: "/admit"}[admit], func(t *testing.T) {
				file, err := os.CreateTemp(t.TempDir(), "candidate")
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				if _, err := file.Write([]byte{0}); err != nil {
					t.Fatal(err)
				}
				alignment := int64(4)
				kind := ColumnAssetKindTCS1PartImage
				if route == "batch" {
					kind = ColumnAssetKindTCS1DictionaryCodes
					alignment = dictionaryCodesDirectViewAssetAlignment
				}
				want := alignment - 1 + 3
				limit := want
				if !admit {
					limit--
				}
				coord := &collectionSchemaCoordinator{}
				admission, err := coord.bindTypedGraphFoldAssetAdmission(typedGraphFoldAssetLimits{Bytes: limit, AppenderAttempts: 1})
				if err != nil {
					t.Fatal(err)
				}
				a := &columnPhysicalAssetSegmentAppender{file: file, assetPath: file.Name(), offset: 1, candidateAdmission: admission, cfg: ColumnStoreConfig{AssetManager: &ColumnAssetManagerConfig{Namespace: "candidate"}}}
				emitted := false
				switch route {
				case "aligned":
					_, err = a.appendKindWithAlignment([]byte{1, 2, 3}, kind, 1, 1, alignment)
				case "reserved":
					_, err = a.appendKindWithReservedPayload(3, kind, 1, 1, alignment, func(p *columnAssetReservedPayload) error { emitted = true; _, e := p.Write([]byte{1, 2, 3}); return e })
				case "batch":
					_, err = a.appendKinds([]columnPhysicalAssetAppendItem{{payload: []byte{1, 2, 3}, kind: kind, generation: 1, partID: 1}})
				}
				info, statErr := file.Stat()
				if statErr != nil {
					t.Fatal(statErr)
				}
				if admit {
					if err != nil || coord.typedGraphCandidateBytes != want || info.Size() != 1+want {
						t.Fatalf("admit err=%v charged=%d file=%d want=%d", err, coord.typedGraphCandidateBytes, info.Size(), want)
					}
				} else if !errors.Is(err, errTypedGraphOverlayFoldNeeded) || emitted || coord.typedGraphCandidateBytes != 0 || info.Size() != 1 {
					t.Fatalf("reject err=%v emitted=%v charged=%d file=%d", err, emitted, coord.typedGraphCandidateBytes, info.Size())
				}
			})
		}
	}
}

func TestTypedGraphCandidateReservedFailureDebt(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "candidate")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	coord := &collectionSchemaCoordinator{}
	admission, err := coord.bindTypedGraphFoldAssetAdmission(typedGraphFoldAssetLimits{Bytes: 3, AppenderAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := &columnPhysicalAssetSegmentAppender{file: file, assetPath: file.Name(), candidateAdmission: admission}
	injected := errors.New("partial emit")
	_, err = a.appendKindWithReservedPayload(3, ColumnAssetKindTCS1PartImage, 1, 1, 4, func(p *columnAssetReservedPayload) error {
		_, err := p.Write([]byte{1})
		return errors.Join(err, injected)
	})
	if !errors.Is(err, injected) || coord.typedGraphCandidateBytes != 3 {
		t.Fatalf("partial output err=%v debt=%d", err, coord.typedGraphCandidateBytes)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := admission.charge(1, 0); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("close refunded partial attempt: %v", err)
	}
}

func typedGraphFoldTestDisk(t *testing.T, dir string) (bytes, files int64) {
	t.Helper()
	if err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		files++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return
}

func TestTypedGraphFoldCandidateAdmission(t *testing.T) {
	col, _, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	seq, root := dbCommitSeqAndSystemRoot(col.db)
	beforeBytes, beforeFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
	limits := typedGraphFoldAssetLimits{Bytes: 1, AppenderAttempts: 2}
	for i := 0; i < 5; i++ {
		if err := col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, limits, nil); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
			t.Fatalf("one-byte candidate budget must reject before publication: %v", err)
		}
		if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
			t.Fatal("rejected candidate changed authority")
		}
	}
	afterBytes, afterFiles := typedGraphFoldTestDisk(t, col.db.ColumnAssetRootDir())
	if afterBytes != beforeBytes || afterFiles-beforeFiles > limits.AppenderAttempts {
		t.Fatalf("rejected retries grew bytes %d files %d", afterBytes-beforeBytes, afterFiles-beforeFiles)
	}
	coord := col.collectionSchemaCoordinator()
	if coord.typedGraphCandidateBytes != 0 || coord.typedGraphCandidateAttempts != limits.AppenderAttempts || coord.typedGraphFoldActive.Load() {
		t.Fatal("candidate debt or ownership mismatch")
	}
	limits.Bytes++
	if err := col.foldTypedGraph(context.Background(), typedGraphOverlapLimits().Cold, 128, limits, nil); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("changed bound bypassed retained debt: %v", err)
	}
}
