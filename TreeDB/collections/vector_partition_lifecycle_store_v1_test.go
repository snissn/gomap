package collections

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireLifecycleAppendSupportV1(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("anonymous lifecycle publication unsupported: %v", err)
	}
	t.Fatal(err)
}

func writeLifecycleSlotV1(t *testing.T, dir *os.File, name string, raw []byte) {
	t.Helper()
	f, err := rootpublication.OpenStableChildFile(dir, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func lifecycleStoreBuildV1(t *testing.T, store *VectorPartitionStoreV1) ([]byte, VectorPartitionManifestV1) {
	t.Helper()
	raw, manifest := lifecycleManifestPayloadV1(t, "building")
	_, err := store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, manifest.Generation, raw)
	requireLifecycleAppendSupportV1(t, err)
	return raw, manifest
}

func TestVectorPartitionLifecycleStoreV1AppendLoadAndIdempotentSlot(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	_, ready := lifecycleManifestPayloadV1(t, "ready")
	readyPromotion := lifecycleReadyPromotionPayloadV1(t, build, ready)
	first, err := store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	requireLifecycleAppendSupportV1(t, err)
	second, err := store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleReadyV1, build.Generation, readyPromotion)
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || second.PreviousDigest != first.Digest {
		t.Fatalf("chain records first=%+v second=%+v", first, second)
	}
	records, state, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || state.LastDigest != second.Digest || state.Generations[build.Generation].Manifest.State != "ready" {
		t.Fatalf("loaded lifecycle state=%+v records=%d", state, len(records))
	}

	// Replaying an exact already-published record is a retry, not a new slot.
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(second)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := vectorPartitionLifecycleNameV1("docs", "embedding", second.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	anonymous, err := rootpublication.OpenStableAnonymousFile(dir, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer anonymous.Close()
	if _, err := anonymous.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := rootpublication.SyncStableFile(anonymous); err != nil {
		t.Fatal(err)
	}
	installed, err := rootpublication.InstallStableFileHandleNoReplace(anonymous, dir, slot)
	if installed || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("duplicate install installed=%v err=%v", installed, err)
	}
	if got, err := readVectorPartitionLifecycleSlotV1(dir, slot, vectorPartitionStoreMaxBytesV1); err != nil || string(got) != string(raw) {
		t.Fatalf("idempotent slot got=%x err=%v", got, err)
	}
}

func TestVectorPartitionLifecycleStoreV1PreInstallFailureAndIllegalTransitionLeaveNoSlot(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	forced := errors.New("before install")
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "before_install" {
			return forced
		}
		return nil
	})
	_, err = store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	restore()
	if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("anonymous lifecycle publication unsupported: %v", err)
	}
	if !errors.Is(err, forced) {
		t.Fatalf("pre-install append err=%v", err)
	}
	if records, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); err != nil || len(records) != 0 {
		t.Fatalf("pre-install slots records=%d err=%v", len(records), err)
	}
	_, err = store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleDeactivateV1, build.Generation, nil)
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("illegal transition err=%v", err)
	}
	if records, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); err != nil || len(records) != 0 {
		t.Fatalf("illegal transition created slots records=%d err=%v", len(records), err)
	}
}

func TestVectorPartitionLifecycleStoreV1PostInstallRetryUsesTail(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	forced := errors.New("after install")
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_install" {
			return forced
		}
		return nil
	})
	_, err = store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	restore()
	if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("anonymous lifecycle publication unsupported: %v", err)
	}
	if !errors.Is(err, forced) {
		t.Fatalf("post-install append err=%v", err)
	}
	got, err := store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != 1 {
		t.Fatalf("retry sequence=%d want 1", got.Sequence)
	}
	if records, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); err != nil || len(records) != 1 {
		t.Fatalf("retry chain records=%d err=%v", len(records), err)
	}
}

func TestVectorPartitionLifecycleStoreV1EveryPersistedPrefixReduces(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range lifecycleLegalChainV1(t) {
		got, err := store.appendVectorPartitionLifecycleRecordV1(want.Collection, want.IndexName, want.Operation, want.Generation, want.Payload)
		if i == 0 {
			requireLifecycleAppendSupportV1(t, err)
		} else if err != nil {
			t.Fatal(err)
		}
		if got.Sequence != uint64(i+1) {
			t.Fatalf("prefix %d sequence=%d", i+1, got.Sequence)
		}
		records, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding")
		if err != nil {
			t.Fatalf("prefix %d load: %v", i+1, err)
		}
		if _, err := reduceVectorPartitionLifecycleChainV1(records); err != nil || len(records) != i+1 {
			t.Fatalf("prefix %d reduce err=%v records=%d", i+1, err, len(records))
		}
	}
}

func TestVectorPartitionLifecycleStoreV1DifferentOccupantRejectedThroughAppend(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	name, err := vectorPartitionLifecycleNameV1("docs", "embedding", 1)
	if err != nil {
		t.Fatal(err)
	}
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "before_install" {
			writeLifecycleSlotV1(t, dir, name, []byte("different"))
		}
		return nil
	})
	_, err = store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	restore()
	if errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Skipf("anonymous lifecycle publication unsupported: %v", err)
	}
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("different occupant err=%v", err)
	}
	if raw, readErr := readVectorPartitionLifecycleSlotV1(dir, name, vectorPartitionStoreMaxBytesV1); readErr != nil || string(raw) != "different" {
		t.Fatalf("occupant preserved raw=%q err=%v", raw, readErr)
	}
}

func TestVectorPartitionLifecycleStoreV1RejectsBadSlotsAndDirectoryRebind(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name string
		raw  func(t *testing.T) []byte
	}{
		{"malformed", func(t *testing.T) []byte { return []byte("bad") }},
		{"checksum", func(t *testing.T) []byte {
			r := lifecycleLegalChainV1(t)[0]
			raw, _ := encodeVectorPartitionLifecycleRecordCanonicalV1(r)
			raw[len(raw)-1] ^= 1
			return raw
		}},
		{"wrong_identity", func(t *testing.T) []byte {
			_, manifest := lifecycleManifestPayloadV1(t, "building")
			manifest.Collection = "other"
			manifest.Canonicalize()
			payload, err := EncodeVectorPartitionManifestV1(manifest)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(vectorPartitionLifecycleRecordV1{Collection: "other", IndexName: "embedding", Sequence: 1, Operation: vectorPartitionLifecycleBuildV1, Generation: manifest.Generation, Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			dir, err := store.openDir()
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			name, _ := vectorPartitionLifecycleNameV1("docs", "embedding", 1)
			writeLifecycleSlotV1(t, dir, name, tc.raw(t))
			if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("load err=%v", err)
			}
		})
	}

	t.Run("gap", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir, err := store.openDir()
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()
		record := lifecycleLegalChainV1(t)[0]
		record.Sequence = 2
		record.PreviousDigest = [32]byte{}
		raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
		if err != nil {
			t.Fatal(err)
		}
		name, _ := vectorPartitionLifecycleNameV1("docs", "embedding", 2)
		writeLifecycleSlotV1(t, dir, name, raw)
		if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
			t.Fatalf("gap load err=%v", err)
		}
	})

	root := t.TempDir()
	store, err := OpenVectorPartitionStoreV1(root)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleStoreBuildV1(t, store)
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_scan" {
			if err := os.Rename(store.dir, store.dir+".old"); err != nil {
				return err
			}
			return os.Mkdir(store.dir, 0o700)
		}
		return nil
	})
	_, _, err = store.loadVectorPartitionLifecycleChainV1("docs", "embedding")
	restore()
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("rebound load err=%v", err)
	}
}

func TestVectorPartitionLifecycleStoreV1RejectsSameBytesSlotSwap(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, build := lifecycleManifestPayloadV1(t, "building")
	record, err := store.appendVectorPartitionLifecycleRecordV1("docs", "embedding", vectorPartitionLifecycleBuildV1, build.Generation, buildRaw)
	requireLifecycleAppendSupportV1(t, err)
	raw, err := encodeVectorPartitionLifecycleRecordCanonicalV1(record)
	if err != nil {
		t.Fatal(err)
	}
	name, err := vectorPartitionLifecycleNameV1("docs", "embedding", 1)
	if err != nil {
		t.Fatal(err)
	}
	replaced := false
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary != "after_slot_read" || replaced {
			return nil
		}
		replaced = true
		path := filepath.Join(store.dir, name)
		if err := os.Rename(path, path+".old"); err != nil {
			return err
		}
		return os.WriteFile(path, raw, 0o600)
	})
	_, _, err = store.loadVectorPartitionLifecycleChainV1("docs", "embedding")
	restore()
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("same-bytes swap load err=%v", err)
	}
}

func TestVectorPartitionLifecycleStoreV1RejectsEmptyDirectoryRebind(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary != "after_scan" {
			return nil
		}
		if err := os.Rename(store.dir, store.dir+".old"); err != nil {
			return err
		}
		return os.Mkdir(store.dir, 0o700)
	})
	records, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding")
	restore()
	if records != nil || !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("empty rebound records=%v err=%v", records, err)
	}
}

func TestVectorPartitionLifecycleStoreV1RecordAndAggregateCaps(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	for i := 1; i <= vectorPartitionLifecycleMaxRecordsV1+1; i++ {
		name, _ := vectorPartitionLifecycleNameV1("docs", "embedding", uint64(i))
		writeLifecycleSlotV1(t, dir, name, nil)
	}
	if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("record cap err=%v", err)
	}

	store, err = OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, err = store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	for i := 1; i <= 2; i++ {
		name, _ := vectorPartitionLifecycleNameV1("docs", "embedding", uint64(i))
		f, err := rootpublication.OpenStableChildFile(dir, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(int64(vectorPartitionStoreMaxBytesV1/2 + 1)); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("aggregate cap err=%v", err)
	}
}

func TestVectorPartitionLifecycleStoreV1FailsClosedMalformedPrefixAndSlotConflict(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	store, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, err := store.openDir()
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	prefix := vectorPartitionLifecycleNamePrefixV1("docs", "embedding")
	malformed, err := rootpublication.OpenStableChildFile(dir, prefix+"oops.vlc", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := malformed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.loadVectorPartitionLifecycleChainV1("docs", "embedding"); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("malformed prefix load err=%v", err)
	}
}
