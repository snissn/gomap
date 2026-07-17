package treedb_test

import (
	"context"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCachedHandleRejectsMutationAfterPostMetaFailure(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("stable/old"), []byte("old-value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Set([]byte("dirty/new"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}

	cutErr := errors.New("public admission: stop after meta dirty")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterMetaWrite {
			return cutErr
		}
		return nil
	})
	err = database.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("post-meta checkpoint error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if err := database.Set([]byte("after/poison"), []byte("must-reopen")); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("public Set after post-meta failure error=%v, want ErrRecoveryRequired", err)
	}
	if batch := database.NewBatch(); batch != nil {
		_ = batch.Close()
		t.Fatal("public NewBatch after post-meta failure returned a writable batch")
	}
	if _, err := database.CompactStorage(context.Background(), treedb.CompactStorageOptions{}); !errors.Is(err, treedb.ErrRecoveryRequired) {
		t.Fatalf("public CompactStorage after post-meta failure error=%v, want ErrRecoveryRequired", err)
	}
}
