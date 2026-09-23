package collections

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodePreparedColumnPublishedMeta(t *testing.T) {
	base, err := normalizeCollectionMeta(CollectionMeta{
		Name: "events",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := ColumnManifestIdentity{Generation: 1, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 41}
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "first", true: "warm"}[warm], func(t *testing.T) {
			current := copyCollectionMeta(base)
			if warm {
				cfg := current.Options.ColumnStore.copy()
				cfg.ActiveManifest = &identity
				cfg.RecoveryAuthoritativeManifest = &identity
				cfg.RecoveryAuthoritativeAppliedCommandLSN = 7
				current.Options.ColumnStore = &cfg
			}
			raw, err := encodeNormalizedCollectionMeta(current)
			if err != nil {
				t.Fatal(err)
			}
			original := bytes.Clone(raw)
			next := copyCollectionMeta(current)
			cfg := next.Options.ColumnStore.copy()
			updatedIdentity := identity
			updatedIdentity.Generation++
			cfg.ActiveManifest = &updatedIdentity
			cfg.RecoveryAuthoritativeManifest = &updatedIdentity
			cfg.RecoveryAuthoritativeAppliedCommandLSN = 8
			next.Options.ColumnStore = &cfg
			got, err := encodePreparedColumnPublishedMeta(raw, current, next)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("captured system metadata changed")
			}
			decoded, err := decodeCollectionMeta(got)
			if err != nil || !sameCollectionMeta(decoded, next) {
				t.Fatalf("patched metadata mismatch: %v", err)
			}
		})
	}
	if _, err := encodePreparedColumnPublishedMeta(bytes.Repeat([]byte("x"), preparedInsertMaxSystemMetaJSONBytes+1), base, base); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("oversized metadata err=%v", err)
	}
}

func TestPreparedInsertSystemDeltaSourceLimit(t *testing.T) {
	roots := []string{collectionPrimaryRootName("events"), collectionColumnManifestRootName("events"), collectionColumnRowLocatorRootName("events")}
	entries, maxBytes, err := preparedInsertSystemDeltaSourceLimit("events", roots)
	if err != nil {
		t.Fatal(err)
	}
	if entries != 5 || maxBytes < preparedInsertMaxPublishedMetaJSONBytes {
		t.Fatalf("entries=%d maxBytes=%d", entries, maxBytes)
	}
	tooMany := make([]string, preparedInsertMaxSystemDeltaRoots+1)
	if _, _, err := preparedInsertSystemDeltaSourceLimit("events", tooMany); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("too many roots err=%v", err)
	}
	if _, _, err := preparedInsertSystemDeltaSourceLimit("events", []string{string(bytes.Repeat([]byte{'x'}, preparedInsertMaxSystemDeltaKeyBytes+1))}); !errors.Is(err, ErrPreparedInsertResourceLimit) {
		t.Fatalf("oversized root name err=%v", err)
	}
}
