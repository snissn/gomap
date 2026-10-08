package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestFreelistPatriciaV2FreshFeatureBeforeIndexAndReopen5108(t *testing.T) {
	for _, emptyFile := range []bool{false, true} {
		for _, ignore := range []bool{false, true} {
			dir := t.TempDir()
			if emptyFile {
				if err := os.WriteFile(filepath.Join(dir, indexFileName), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			database, err := Open(Options{Dir: dir, IgnoreFormatConfig: ignore, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			cfg, ok, err := LoadFormatConfig(dir)
			if err != nil || !ok || !cfg.RequiresFreelistPatriciaV2() {
				t.Fatalf("fresh required feature: %+v %t %v", cfg, ok, err)
			}
			batch := database.NewBatch()
			if err = batch.Set([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err = batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err = batch.Close(); err != nil {
				t.Fatal(err)
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			for _, opener := range []func(Options) (*DB, error){Open, openReadOnlyNoLock} {
				reopened, err := opener(Options{Dir: dir, ReadOnly: true, IgnoreFormatConfig: ignore})
				if err != nil {
					t.Fatal(err)
				}
				got, err := reopened.Get([]byte("key"))
				if err != nil || !bytes.Equal(got, []byte("value")) {
					t.Fatalf("read %q %v", got, err)
				}
				if err = reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err = SaveFormatConfig(dir, FormatConfig{}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
				t.Fatalf("removed marker: %v", err)
			}
		}
	}
}

func TestFreelistPatriciaV2MissingMarkerBeforeStorageDecode5108(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		dir := t.TempDir()
		before := bytes.Repeat([]byte{0xa5}, page.PageSize)
		if err := os.WriteFile(filepath.Join(dir, indexFileName), before, 0600); err != nil {
			t.Fatal(err)
		}
		for _, opener := range []func(Options) (*DB, error){Open, openReadOnlyNoLock} {
			if database, err := opener(Options{Dir: dir, IgnoreFormatConfig: ignore}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
				if database != nil {
					_ = database.Close()
				}
				t.Fatalf("old index accepted or decoded: %v", err)
			}
		}
		if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureFreelistPatriciaV2}}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
			t.Fatalf("retroactive marker: %v", err)
		}
		after, err := os.ReadFile(filepath.Join(dir, indexFileName))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("refused index changed")
		}
	}
}

func TestFreelistPatriciaV2OldPagesCannotBecomeFallback5108(t *testing.T) {
	for _, mode := range []string{"both-old", "old-fallback-corrupt-newest", "valid-fallback-corrupt-newest"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				batch := database.NewBatch()
				if err = batch.Set([]byte("key"), []byte{byte(i)}); err != nil {
					t.Fatal(err)
				}
				if err = batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
				if err = batch.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err = database.Close(); err != nil {
				t.Fatal(err)
			}
			indexPath := filepath.Join(dir, indexFileName)
			image, err := os.ReadFile(indexPath)
			if err != nil {
				t.Fatal(err)
			}
			type slotInfo struct{ commit, header uint64 }
			var slots [2]slotInfo
			for slot := 0; slot < 2; slot++ {
				meta, err := page.DecodeDurableMetaV1(image[slot*page.PageSize+page.PageHeaderSize : (slot+1)*page.PageSize])
				if err != nil {
					t.Fatal(err)
				}
				offset := int(meta.RootRecordPageID) * page.PageSize
				record, err := rootpublication.DecodeDurableRootRecordV1(image[offset:offset+page.PageSize], meta.RootRecordPageID, meta.RootRecordDigest)
				if err != nil {
					t.Fatal(err)
				}
				slots[slot] = slotInfo{meta.CommitSeq, record.Freelist.HeaderPageID}
			}
			newer, older := 0, 1
			if slots[1].commit > slots[0].commit {
				newer, older = 1, 0
			}
			if slots[newer].commit == slots[older].commit || slots[newer].header == slots[older].header {
				t.Fatal("fixture lacks two separate recoverable generations")
			}
			for slot, info := range slots {
				offset := int(info.header) * page.PageSize
				header := image[offset : offset+page.PageSize]
				if mode == "both-old" || mode == "old-fallback-corrupt-newest" && slot == older {
					copy(header[16:24], []byte("FLGENV1\x00"))
					binary.LittleEndian.PutUint16(header[24:26], 1)
					page.UpdateChecksum(header)
				} else if slot == newer {
					header[8] ^= 1
				}
			}
			if err = os.WriteFile(indexPath, image, 0600); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if mode == "valid-fallback-corrupt-newest" {
				if err != nil {
					t.Fatalf("valid V2 fallback refused: %v", err)
				}
				if err = reopened.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if reopened != nil {
					_ = reopened.Close()
				}
				if !errors.Is(err, ErrNoRecoverableMeta) {
					t.Fatalf("old physical fallback accepted: %v", err)
				}
			}
		})
	}
}
