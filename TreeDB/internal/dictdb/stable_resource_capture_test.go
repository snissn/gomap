//go:build !windows

package dictdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCaptureDictionaryResourcesReturnsExactTransitiveClosureForReusedPointerID(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	payload := bytes.Repeat([]byte("stable-dictionary-payload|"), 64)
	dictID, err := store.PutDictBytes(ctx, payload)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	reusedID, err := store.PutDictBytes(ctx, payload)
	if err != nil {
		t.Fatalf("reuse dictionary: %v", err)
	}
	if reusedID != dictID {
		t.Fatalf("reused id=%d want %d", reusedID, dictID)
	}
	registry := store.backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	baselineLinks := registry.ActiveStableNamespaceLinks()

	resources, err := store.CaptureDictionaryResources(ctx, reusedID)
	if err != nil {
		t.Fatalf("capture reused dictionary: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("resource closure len=%d want index+value-log", got)
	}
	if got := registry.ActivePins(); got != baselinePins+2 {
		t.Fatalf("active index/value-log identity pins=%d want baseline %d + 2 capture pins", got, baselinePins)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != baselineLinks+1 {
		t.Fatalf("stable namespace links=%d want baseline %d + 1 value-log link", got, baselineLinks)
	}
	reusedResources, err := store.CaptureDictionaryResources(ctx, reusedID)
	if err != nil {
		t.Fatalf("recapture reused dictionary: %v", err)
	}
	if got := registry.ActiveStableNamespaceLinks(); got != baselineLinks+1 {
		t.Fatalf("stable namespace links after recapture=%d want cached %d", got, baselineLinks+1)
	}
	reusedResources.Release()

	wantDigest := sha256.Sum256(payload)
	paths := make(map[string]bool, 2)
	for _, token := range resources.Tokens() {
		if token.Kind() != rootpublication.ResourceDictionary {
			t.Fatalf("resource kind=%q want dictionary", token.Kind())
		}
		if token.Reachability() != rootpublication.ReachabilityDictionaryGeneration {
			t.Fatalf("resource reachability=%q want dictionary generation", token.Reachability())
		}
		paths[token.DiagnosticPath()] = true
		if token.DiagnosticPath() == "index.db" && token.Namespace() == nil {
			t.Fatal("index token missing stable namespace evidence")
		}
		if token.DiagnosticPath() == "value_vlog/value-l0-000001.log" && token.Namespace() == nil {
			t.Fatal("value-log token missing stable namespace evidence")
		}
		obligations := token.LogicalObligations()
		if len(obligations) != 1 {
			t.Fatalf("resource %q obligations=%d want 1", token.DiagnosticPath(), len(obligations))
		}
		obligation := obligations[0]
		if obligation.Generation != dictID || obligation.FileID != dictID ||
			obligation.Length != int64(len(payload)) || obligation.Digest != wantDigest {
			t.Fatalf("resource %q obligation=%+v want dict=%d len=%d digest=%x", token.DiagnosticPath(), obligation, dictID, len(payload), wantDigest)
		}
	}
	if !paths["index.db"] || !paths["value_vlog/value-l0-000001.log"] {
		t.Fatalf("resource paths=%v want exact index and value-log segment", paths)
	}
	if err := resources.SyncThrough(); err != nil {
		t.Fatalf("sync closure: %v", err)
	}
	resources.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active identity pins after release=%d want baseline %d", got, baselinePins)
	}
}

func TestCaptureDictionaryResourcesReopenedPointerUsesExactRecordFrontier(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	payload := bytes.Repeat([]byte("reopened-dictionary|"), 128)
	dictID, err := store.PutDictBytes(context.Background(), payload)
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	snapshot := store.backend.AcquireSnapshot()
	entry, err := snapshot.GetEntryExact(bytesKey(dictID))
	if err != nil {
		t.Fatalf("get dictionary entry: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if entry.Flags&1 == 0 {
		t.Fatal("dictionary unexpectedly stored inline")
	}
	wantFrontier := entry.ValuePtr.Offset + uint64(page.ValuePtrRecordLength(entry.ValuePtr))
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	store, err = Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	resources, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatalf("capture reopened dictionary: %v", err)
	}
	defer resources.Release()
	for _, token := range resources.Tokens() {
		if token.DiagnosticPath() != "value_vlog/value-l0-000001.log" {
			continue
		}
		if token.Frontier().Bytes != wantFrontier {
			t.Fatalf("value-log frontier=%d want exact record end %d", token.Frontier().Bytes, wantFrontier)
		}
		if token.Namespace() == nil {
			t.Fatal("reopened value-log token missing stable namespace evidence")
		}
	}
}

func TestCaptureDictionaryResourcesFailureDoesNotLeakVacuumFence(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	if resources, err := store.CaptureDictionaryResources(context.Background(), 42); err == nil || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("missing dictionary capture resources=%v err=%v", resources, err)
	}
	if got := store.backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("pins after failed capture=%d want 0", got)
	}
}

func TestCaptureDictionaryResourcesRejectsEachMissingPointerChild(t *testing.T) {
	for _, omitted := range []dictionaryStablePhysicalRole{dictionaryStableIndexRole, dictionaryStableValueLogRole} {
		t.Run(string(omitted), func(t *testing.T) {
			store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			payload := bytes.Repeat([]byte("pointer-dictionary-child-omission|"), 256)
			dictID, err := store.PutDictBytes(context.Background(), payload)
			if err != nil {
				t.Fatal(err)
			}
			registry := store.backend.StableResourceIdentityPinRegistry()
			baselinePins := registry.ActivePins()
			var withheld []*rootpublication.StableResourceToken
			previous := addDictionaryStableResourceToken
			addDictionaryStableResourceToken = func(builder *rootpublication.StableResourceSetBuilder, token *rootpublication.StableResourceToken, role dictionaryStablePhysicalRole) error {
				if role == omitted {
					withheld = append(withheld, token)
					return nil
				}
				return builder.Add(token)
			}
			resources, captureErr := store.CaptureDictionaryResources(context.Background(), dictID)
			addDictionaryStableResourceToken = previous
			for _, token := range withheld {
				token.Release()
			}
			if resources != nil {
				resources.Release()
			}
			if !errors.Is(captureErr, rootpublication.ErrUnresolvedResource) || resources != nil {
				t.Fatalf("omitted %s resources=%v err=%v want ErrUnresolvedResource", omitted, resources, captureErr)
			}
			if got := registry.ActivePins(); got != baselinePins {
				t.Fatalf("omitted %s active pins=%d want baseline %d", omitted, got, baselinePins)
			}
		})
	}
}

func TestCaptureDictionaryResourcesBlocksOnlineIndexVacuumUntilRelease(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	dictID, err := store.PutDictBytes(context.Background(), []byte("stable-inline-dictionary"))
	if err != nil {
		t.Fatalf("put dictionary: %v", err)
	}
	resources, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatalf("capture dictionary: %v", err)
	}
	if err := store.backend.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		resources.Release()
		t.Fatalf("vacuum with live capture error=%v want ErrResourcePinned", err)
	}
	resources.Release()
	if err := store.backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum after release: %v", err)
	}
}

func TestCaptureDictionaryResourcesInlineDefinitionNeedsOnlyPinnedIndexGeneration(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	dictID, err := store.PutDictBytes(ctx, []byte("inline"))
	if err != nil {
		t.Fatalf("put inline dictionary: %v", err)
	}
	resources, err := store.CaptureDictionaryResources(ctx, dictID)
	if err != nil {
		t.Fatalf("capture inline dictionary: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 1 {
		t.Fatalf("inline closure len=%d want index only", got)
	}
	tokens := resources.Tokens()
	if len(tokens) != 1 || tokens[0].DiagnosticPath() != "index.db" {
		t.Fatalf("inline closure tokens=%v want index.db", tokens)
	}
}

func TestCaptureDictionaryResourcesUnionMultiplePointerIDsIsDeterministic(t *testing.T) {
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	firstPayload := bytes.Repeat([]byte("first-shared-dictionary-"), 256)
	secondPayload := bytes.Repeat([]byte("second-shared-dictionary-"), 256)
	put := func(payload []byte) uint64 {
		t.Helper()
		id, err := store.PutDictBytes(context.Background(), payload)
		if err != nil {
			t.Fatalf("put dictionary: %v", err)
		}
		return id
	}
	firstID, secondID := put(firstPayload), put(secondPayload)
	registry := store.backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	capture := func(id uint64) *rootpublication.StableResourceSet {
		t.Helper()
		resources, err := store.CaptureDictionaryResources(context.Background(), id)
		if err != nil {
			t.Fatalf("capture dictionary %d: %v", id, err)
		}
		return resources
	}
	firstForward, secondForward := capture(firstID), capture(secondID)
	firstReverse, secondReverse := capture(firstID), capture(secondID)
	defer firstForward.Release()
	defer secondForward.Release()
	defer firstReverse.Release()
	defer secondReverse.Release()

	union := func(first, second *rootpublication.StableResourceSet) *rootpublication.StableResourceSet {
		t.Helper()
		builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityDictionaryGeneration)
		defer builder.Abandon()
		if err := builder.Merge(first); err != nil {
			t.Fatalf("merge first dictionary: %v", err)
		}
		if err := builder.Merge(second); err != nil {
			t.Fatalf("merge second dictionary sharing physical storage: %v", err)
		}
		resources, err := builder.Freeze()
		if err != nil {
			t.Fatalf("freeze dictionary union: %v", err)
		}
		return resources
	}
	forward := union(firstForward, secondForward)
	reverse := union(secondReverse, firstReverse)
	defer forward.Release()
	defer reverse.Release()
	if got := forward.Len(); got != 2 {
		t.Fatalf("forward union physical resources=%d want one index and one shared value-log segment", got)
	}
	if got := reverse.Len(); got != 2 {
		t.Fatalf("reverse union physical resources=%d want one index and one shared value-log segment", got)
	}
	if !reflect.DeepEqual(forward.Descriptors(), reverse.Descriptors()) {
		t.Fatalf("dictionary union descriptors depend on merge order:\nforward=%+v\nreverse=%+v", forward.Descriptors(), reverse.Descriptors())
	}

	want := map[uint64][32]byte{
		firstID:  sha256.Sum256(firstPayload),
		secondID: sha256.Sum256(secondPayload),
	}
	for _, descriptor := range forward.Descriptors() {
		obligations := descriptor.LogicalObligations()
		if len(obligations) != len(want) {
			t.Fatalf("descriptor generation %d obligations=%d want %d", descriptor.Generation(), len(obligations), len(want))
		}
		seen := make(map[uint64]struct{}, len(obligations))
		for _, obligation := range obligations {
			digest, ok := want[obligation.Generation]
			if !ok || obligation.FileID != obligation.Generation || obligation.Digest != digest ||
				obligation.Reachability != rootpublication.ReachabilityDictionaryGeneration {
				t.Fatalf("union lost or changed dictionary obligation: %+v", obligation)
			}
			seen[obligation.Generation] = struct{}{}
		}
		if len(seen) != len(want) {
			t.Fatalf("descriptor generation %d retained %d dictionary IDs want %d", descriptor.Generation(), len(seen), len(want))
		}
	}

	forward.Release()
	reverse.Release()
	firstForward.Release()
	secondForward.Release()
	firstReverse.Release()
	secondReverse.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("dictionary union release left active pins=%d want baseline %d", got, baselinePins)
	}
	if err := store.backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("dictionary union release vacuum: %v", err)
	}
}

func TestCaptureDictionaryResourcesDedupeDoesNotGrowPinsOrDescriptors(t *testing.T) {
	const iterations = 320
	store, err := Open(t.TempDir(), db.Options{ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	payload := bytes.Repeat([]byte("dedupe-resource-plateau-"), 256)
	dictID, err := store.PutDictBytes(context.Background(), payload)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	registry := store.backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	warm, err := store.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		t.Fatalf("warm capture: %v", err)
	}
	if got := warm.Len(); got != 2 {
		warm.Release()
		t.Fatalf("warm resources=%d want index+value-log closure", got)
	}
	warm.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("warm release left active pins=%d want baseline %d", got, baselinePins)
	}

	beforeFDs, fdErr := os.ReadDir("/dev/fd")
	checkFDs := fdErr == nil
	for i := 0; i < iterations; i++ {
		if got := registry.ActivePins(); got != baselinePins {
			t.Fatalf("dedupe %d pre-capture active pins=%d want baseline %d", i, got, baselinePins)
		}
		reusedID, err := store.PutDictBytes(context.Background(), payload)
		if err != nil {
			t.Fatalf("dedupe put %d: %v", i, err)
		}
		if reusedID != dictID {
			t.Fatalf("dedupe put %d id=%d want %d", i, reusedID, dictID)
		}
		resources, err := store.CaptureDictionaryResources(context.Background(), reusedID)
		if err != nil {
			t.Fatalf("dedupe capture %d: %v", i, err)
		}
		if got := resources.Len(); got != 2 {
			resources.Release()
			t.Fatalf("dedupe %d resources=%d want index+value-log closure", i, got)
		}
		if got := registry.ActivePins(); got != baselinePins+2 {
			resources.Release()
			t.Fatalf("dedupe %d active index/value-log pins=%d want baseline %d + 2 capture pins", i, got, baselinePins)
		}
		resources.Release()
		if got := registry.ActivePins(); got != baselinePins {
			t.Fatalf("dedupe %d left active pins=%d want baseline %d", i, got, baselinePins)
		}
	}
	if checkFDs {
		afterFDs, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatalf("read descriptor directory after stress: %v", err)
		}
		if len(afterFDs) > len(beforeFDs)+2 {
			t.Fatalf("descriptor count grew from %d to %d after %d captures", len(beforeFDs), len(afterFDs), iterations)
		}
	}
	if err := store.backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("dedupe release vacuum: %v", err)
	}
}

func BenchmarkCaptureDictionaryResources(b *testing.B) {
	ctx := context.Background()
	cases := []struct {
		name    string
		payload []byte
		unique  bool
	}{
		{name: "inline-dedupe", payload: []byte("bench-inline-dictionary")},
		{name: "pointer-dedupe", payload: bytes.Repeat([]byte("bench-pointer-dictionary-"), 256)},
		{name: "pointer-create", payload: bytes.Repeat([]byte("bench-pointer-create-"), 256), unique: true},
	}
	for _, benchmark := range cases {
		b.Run(benchmark.name, func(b *testing.B) {
			store, err := Open(b.TempDir(), db.Options{ChunkSize: 64 * 1024})
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			warmID, err := store.PutDictBytes(ctx, benchmark.payload)
			if err != nil {
				b.Fatal(err)
			}
			warm, err := store.CaptureDictionaryResources(ctx, warmID)
			if err != nil {
				b.Fatal(err)
			}
			resourcesPerOp := warm.Len()
			var pinHighWater uint64
			for _, stats := range warm.Stats(time.Now()) {
				if stats.PinHighWater > pinHighWater {
					pinHighWater = stats.PinHighWater
				}
			}
			warm.Release()
			var beforeFileSyncs, beforeNamespaceSyncs uint64
			if store.vlog != nil {
				stats := store.vlog.DurabilityStats()
				beforeFileSyncs = stats.FileSyncCalls
				beforeNamespaceSyncs = stats.DirectorySyncCalls
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				payload := benchmark.payload
				if benchmark.unique {
					payload = make([]byte, len(benchmark.payload)+8)
					copy(payload, benchmark.payload)
					binary.LittleEndian.PutUint64(payload[len(benchmark.payload):], uint64(i+1))
				}
				dictID, err := store.PutDictBytes(ctx, payload)
				if err != nil {
					b.Fatal(err)
				}
				resources, err := store.CaptureDictionaryResources(ctx, dictID)
				if err != nil {
					b.Fatal(err)
				}
				if resources.Len() != resourcesPerOp {
					resources.Release()
					b.Fatalf("dictionary closure resources=%d want %d", resources.Len(), resourcesPerOp)
				}
				resources.Release()
			}
			b.StopTimer()
			b.ReportMetric(float64(resourcesPerOp), "descriptors/op")
			b.ReportMetric(float64(pinHighWater), "pin_high_water")
			if store.vlog != nil {
				stats := store.vlog.DurabilityStats()
				b.ReportMetric(float64(stats.FileSyncCalls-beforeFileSyncs)/float64(b.N), "vlog_file_syncs/op")
				b.ReportMetric(float64(stats.DirectorySyncCalls-beforeNamespaceSyncs)/float64(b.N), "vlog_namespace_syncs/op")
			} else {
				b.ReportMetric(0, "vlog_file_syncs/op")
				b.ReportMetric(0, "vlog_namespace_syncs/op")
			}
		})
	}
}
