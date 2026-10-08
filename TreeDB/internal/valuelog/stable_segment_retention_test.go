package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

func TestStableManagerRetentionPreventsForgetAndZombieDeletion(t *testing.T) {
	dir := t.TempDir()
	id := mustEncodeFileID(t, 0, 1)
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 1, []byte("retained")); err != nil {
		writer.Close()
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err = manager.RegisterSegment(path, id); err != nil {
		t.Fatal(err)
	}
	denied := errors.New("retention debit denied")
	deny := true
	metadata, err := NewFiniteStableMetadata(7, 3, func(uint64) error {
		if deny {
			return denied
		}
		return nil
	})
	// The facade constructor itself must first have actual credit.
	if !errors.Is(err, denied) {
		t.Fatal(err)
	}
	deny = false
	metadata, err = NewFiniteStableMetadata(7, 3, func(uint64) error {
		if deny {
			return denied
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	deny = true
	if hold, err := manager.AcquireStableSegmentRetention(id, 1, metadata); hold != nil || !errors.Is(err, denied) {
		t.Fatalf("hold=%v err=%v", hold, err)
	}
	if manager.files[id].RefCount.Load() != 0 || metadata.retained != 0 {
		t.Fatal("denied constructor pinned actual File")
	}
	deny = false
	hold, err := manager.AcquireStableSegmentRetention(id, 1, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.EvictSegment(id); err == nil {
		t.Fatal("manager forgot actually retained segment")
	}
	if _, err = hold.RetainedBackingCensus(); !errors.Is(err, rootpublication.ErrStableMetadataShapeUnsupported) {
		t.Fatal("wrapper size pretended to cover Manager backing")
	}
	if err = manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	if err = manager.deleteZombieFile(manager.files[id]); err != nil {
		t.Fatal(err)
	}
	if manager.RegisteredFileCountNoRefresh() != 1 {
		t.Fatal("zombie left registered capacity while retained")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("retained zombie deleted", err)
	}
	hold.Release()
	hold.Release()
	if metadata.retained != 0 || manager.RegisteredFileCountNoRefresh() != 0 {
		t.Fatal("actual last hold leaked metadata or registration")
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unreferenced zombie not deleted", err)
	}
	if err = metadata.Close(); err != nil {
		t.Fatal(err)
	}
}
