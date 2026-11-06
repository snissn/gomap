//go:build linux

package caching_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

// This is a real collections engine, not testSemanticState or a sentinel
// refresher. Only the existing dictionary pack fixture/export bridge is private.
// This dictionary-only fixture selects its maintained dictionary appender and
// encoding options at owner creation so BOTH actual SYSTEM operands are eligible.
// Common Block fixtures, the original native lifecycle and public corpus are
// unchanged. Linux supplies the required exact-handle pack primitive.
func TestNativeEngineDictionaryPairedBasisHandoff(t *testing.T) {
	cache, backend, dictID := caching.NewNativeDictionaryHandoffTestFixture(t)
	manager := collections.NewCollectionManager(backend)
	managerOpen := true
	var held *collections.NativeJSONRead
	var capture backenddb.NativeSemanticCapture
	t.Cleanup(func() {
		if capture != nil {
			if err := capture.Close(); err != nil {
				t.Error(err)
			}
		}
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
		if managerOpen {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if !manager.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not activate the original native collection manager")
	}
	// Several actual metadata/incarnation/domain descriptors make a compressible
	// SYSTEM catalog. No encoded catalog bytes or engine state are fabricated.
	for i := 0; i < 8; i++ {
		if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: fmt.Sprintf("dict_native_%02d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := manager.OpenCollection("dict_native_00")
	if err != nil {
		t.Fatal(err)
	}
	id := func(i int) []byte { return []byte(fmt.Sprintf("native-row-%02d", i)) }
	document := func(i int) []byte {
		return []byte(fmt.Sprintf(`{"row":%d,"value":"actual-native-dictionary-handoff"}`, i))
	}
	const rows = 8
	for i := 0; i < rows; i++ {
		got, err := collection.Insert(id(i), document(i))
		if err != nil || !bytes.Equal(got, id(i)) {
			t.Fatalf("real native ACK/ID %d: idMatches=%t err=%v", i, bytes.Equal(got, id(i)), err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	first, err := caching.PackNativeDictionaryHandoffTestFixture(cache)
	if err != nil || !first.Ran || first.Pack.BytesCopied == 0 {
		t.Fatalf("prepare actual packed catalog: ran=%t copied=%d err=%v", first.Ran, first.Pack.BytesCopied, err)
	}
	native, err := collections.NewNativeJSONManager(backend)
	if err != nil {
		t.Fatal(err)
	}
	held, err = native.Capture("dict_native_00")
	if err != nil {
		t.Fatal(err)
	}
	capture, err = backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%T", capture.State()) != "*collections.nativeEngineVersion" {
		t.Fatalf("capture does not own the actual native engine: %T", capture.State())
	}
	beforeToken, ok := capture.Snapshot().StateToken()
	if !ok || beforeToken.SystemRootPageID == 0 {
		t.Fatal("actual native catalog was not persisted")
	}
	logical := capture.LogicalSequence()
	catalogValue := func(snapshot *backenddb.Snapshot, key string) []byte {
		t.Helper()
		token, ok := snapshot.StateToken()
		if !ok || token.SystemRootPageID == 0 {
			t.Fatal("catalog snapshot has no SYSTEM root")
		}
		value, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte(key))
		if err != nil || len(value) == 0 {
			t.Fatalf("actual native catalog descriptor %q: bytes=%d err=%v", key, len(value), err)
		}
		return bytes.Clone(value)
	}
	const incarnationKey = "collections/native/v1/incarnation/dict_native_00"
	const metadataKey = "collections/meta/dict_native_00"
	const primaryKey = "collections/root/dict_native_00/primary"
	incarnation := catalogValue(capture.Snapshot(), incarnationKey)
	metadata := catalogValue(capture.Snapshot(), metadataKey)
	primary := func(snapshot *backenddb.Snapshot) uint64 {
		t.Helper()
		raw := catalogValue(snapshot, primaryKey)
		if len(raw) != 8 || binary.BigEndian.Uint64(raw) == 0 {
			t.Fatal("actual native primary root is unavailable")
		}
		return binary.BigEndian.Uint64(raw)
	}
	beforeRoot := primary(capture.Snapshot())
	var revisions [rows]page.EntryRevision
	verify := func(snapshot *backenddb.Snapshot, read *collections.NativeJSONRead, record bool) {
		t.Helper()
		root := primary(snapshot)
		if !bytes.Equal(catalogValue(snapshot, incarnationKey), incarnation) || !bytes.Equal(catalogValue(snapshot, metadataKey), metadata) {
			t.Fatal("actual native catalog identity/metadata changed")
		}
		for i := 0; i < rows; i++ {
			got, err := read.Get(id(i))
			if err != nil || !bytes.Equal(got, document(i)) {
				t.Fatalf("held/native ACK value %d: err=%v", i, err)
			}
			entry, err := snapshot.GetEntryAtRoot(root, id(i))
			if err != nil {
				t.Fatal(err)
			}
			if record {
				revisions[i] = entry.Revision
			} else if entry.Revision != revisions[i] {
				t.Fatalf("native ACK revision changed at row %d", i)
			}
		}
	}
	verify(capture.Snapshot(), held, true)
	receipt, err := caching.CheckNativeDictionaryHandoffForTest(cache, capture, dictID)
	t.Logf("native-engine-dictionary-handoff: BeforeSystem=%d AfterSystem=%d BeforeReads=%d AfterReads=%d Definitions=%d Releases=%d DictionarySHA256=%x DefinitionSHA256=%x SourceSelected=%t DestinationCreated=%t UnscopedRefused=%t ReversedRefused=%t Refunded=%t ResultChecked=%t SameResult=%t FirstFailedClause=%d", receipt.Before.SystemRootPageID, receipt.After.SystemRootPageID, receipt.BeforeReads, receipt.AfterReads, receipt.Definitions, receipt.DefinitionReleases, receipt.DictionarySHA256, receipt.DefinitionSHA256, receipt.SourceSelected, receipt.DestinationCreated, receipt.UnscopedRefused, receipt.ReversedRefused, receipt.Refunded, receipt.ResultChecked, receipt.SameResult, receipt.FirstFailedClause)
	t.Logf("native-engine-dictionary-identities: DictionaryIndex=%+v DictionaryFile=%+v BeforeFile=%+v AfterFile=%+v BeforeLeaf=%+v AfterLeaf=%+v", receipt.DictionaryIndex, receipt.DictionaryFile, receipt.BeforeFile, receipt.AfterFile, receipt.BeforeLeaf, receipt.AfterLeaf)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Before != beforeToken || receipt.Before.SystemRootPageID == receipt.After.SystemRootPageID || receipt.Before.AppliedCommandLSN != receipt.After.AppliedCommandLSN || receipt.BeforeReads == 0 || receipt.AfterReads == 0 || receipt.Definitions != 1 || receipt.DictionarySHA256 != receipt.DefinitionSHA256 || receipt.DictionarySHA256 == ([32]byte{}) || !receipt.Refunded || !receipt.UnscopedRefused || !receipt.ReversedRefused || !receipt.SourceSelected || !receipt.DestinationCreated || !receipt.ResultChecked || receipt.FirstFailedClause != 0 {
		t.Fatal("real native engine paired dictionary handoff receipt is incomplete")
	}
	verify(capture.Snapshot(), held, false)
	current, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if err := current.Close(); err != nil {
				t.Error(err)
			}
		}()
		token, ok := current.Snapshot().StateToken()
		if !ok || token != receipt.After || current.LogicalSequence() != logical || current.State() == nil {
			t.Fatal("original cache did not adopt the actual equivalent native physical successor")
		}
		// Leaf-log maintenance must adopt the authenticated after basis. It need
		// not relocate a pager-backed collection root or rebuild its engine.
		currentRoot := primary(current.Snapshot())
		t.Logf("native-engine-dictionary-basis: PrimaryUnchanged=%t", currentRoot == beforeRoot)
		currentRead, err := native.Capture("dict_native_00")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := currentRead.Close(); err != nil {
				t.Error(err)
			}
		}()
		verify(current.Snapshot(), currentRead, false)
	}()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	capture = nil
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	managerOpen = false
	if err := caching.CloseNativeDictionaryHandoffTestFixture(cache); err != nil {
		t.Fatal(err)
	}
	reopened, reopenedBackend := caching.ReopenNativeDictionaryHandoffTestFixture(t, cache)
	reopenedManager := collections.NewCollectionManager(reopenedBackend)
	reopenedManagerOpen := true
	t.Cleanup(func() {
		if reopenedManagerOpen {
			if err := reopenedManager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if !reopenedManager.UsesNativeCommandWALApply() {
		t.Fatal("reopen lost the native owner")
	}
	reopenedNative, err := collections.NewNativeJSONManager(reopenedBackend)
	if err != nil {
		t.Fatal(err)
	}
	reopenedRead, err := reopenedNative.Capture("dict_native_00")
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if err := reopenedRead.Close(); err != nil {
				t.Error(err)
			}
		}()
		reopenedCapture, err := reopenedBackend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := reopenedCapture.Close(); err != nil {
				t.Error(err)
			}
		}()
		verify(reopenedCapture.Snapshot(), reopenedRead, false)
	}()
	if err := reopenedManager.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedManagerOpen = false
	if err := caching.CloseNativeDictionaryHandoffTestFixture(reopened); err != nil {
		t.Fatal(err)
	}
}
