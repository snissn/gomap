package caching

import (
	"context"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestApplyValueLogDictProfileUpdatesKForSameDict(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir, Mode: db.ModeBackend, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open dictdb: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	store := dictdb.New(backend)

	ctx := context.Background()
	dictBytes := []byte("dictionary-bytes")
	dictID, err := store.PutDictBytes(ctx, dictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := store.SetCurrent(ctx, dictID); err != nil {
		t.Fatalf("SetCurrent: %v", err)
	}

	tr := &compression.Trainer{}
	dictHash := xxhash.Sum64(dictBytes)
	tr.AcceptProfile(&compression.ActiveProfile{
		DictHash:     dictHash,
		DictBytes:    len(dictBytes),
		Dict:         dictBytes,
		K:            16,
		PayloadRatio: 0.5,
		TotalRatio:   0.5,
		Timestamp:    time.Now(),
	})

	db := &DB{
		dictStore: store,
	}
	db.valueLogDictTrainer = tr
	db.valueLogDictLastAppliedDictHash.Store(dictHash)
	db.valueLogDictLastAppliedDictID.Store(dictID)
	db.dictCurrentCached.Store(dictID)
	db.valueLogDictCurrentK.Store(8)

	db.applyValueLogDictProfile()

	gotK, err := store.GetK(ctx, dictID)
	if err != nil {
		t.Fatalf("GetK: %v", err)
	}
	if gotK != 16 {
		t.Fatalf("expected k=16, got %d", gotK)
	}
	if curK := int(db.valueLogDictCurrentK.Load()); curK != 16 {
		t.Fatalf("expected currentK=16, got %d", curK)
	}
}
