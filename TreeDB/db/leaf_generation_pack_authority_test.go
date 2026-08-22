//go:build linux

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPackAuthorityFixture struct {
	db          *DB
	registry    *rootpublication.IdentityPinRegistry
	stagingRoot string
	stagingDir  string
	destination string
	path        string
	fileID      uint32
	writer      *valuelog.Writer
	created     rewriteCreatedSegment
	pointer     page.LeafLogPtr
}

func newLeafPackAuthorityFixture(tb testing.TB) leafPackAuthorityFixture {
	tb.Helper()
	root := tb.TempDir()
	destination := filepath.Join(root, leafVLogDirName)
	stagingDir := filepath.Join(destination, ".leaf-pack-copy-test", leafVLogDirName)
	for _, dir := range []string{destination, stagingDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			tb.Fatalf("MkdirAll %s: %v", dir, err)
		}
	}
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 1)
	if err != nil {
		tb.Fatal(err)
	}
	path := valuelog.SegmentPath(stagingDir, fileID)
	writer, err := valuelog.NewStagingWriter(path, fileID)
	if err != nil {
		tb.Fatalf("NewStagingWriter: %v", err)
	}
	valuePtr, err := writer.Append(0, nil, 1, []byte("packed-outer-leaf"))
	if err != nil {
		tb.Fatalf("Append: %v", err)
	}
	if err := writer.Sync(); err != nil {
		tb.Fatalf("Sync: %v", err)
	}
	identity, err := writer.StableIdentity()
	if err != nil {
		tb.Fatal(err)
	}
	pointer, err := page.LeafLogPtrFromValuePtr(valuePtr)
	if err != nil {
		tb.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	return leafPackAuthorityFixture{
		db: &DB{dir: root, valueLogIdentityPins: registry}, registry: registry, stagingRoot: filepath.Dir(stagingDir),
		stagingDir: stagingDir, destination: destination, path: path, fileID: fileID, writer: writer,
		created: rewriteCreatedSegment{path: path, fileID: fileID, identity: identity}, pointer: pointer,
	}
}

func (fixture *leafPackAuthorityFixture) close(tb testing.TB) {
	tb.Helper()
	if fixture.writer != nil {
		if err := fixture.writer.Close(); err != nil {
			tb.Fatalf("Close writer: %v", err)
		}
		fixture.writer = nil
	}
}

func newLeafPackAuthorityFixtureSegment(tb testing.TB, fixture *leafPackAuthorityFixture, seq uint32) (rewriteCreatedSegment, page.LeafLogPtr, *valuelog.Writer) {
	tb.Helper()
	fileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, seq)
	if err != nil {
		tb.Fatal(err)
	}
	path := valuelog.SegmentPath(fixture.stagingDir, fileID)
	writer, err := valuelog.NewStagingWriter(path, fileID)
	if err != nil {
		tb.Fatal(err)
	}
	valuePtr, err := writer.Append(0, nil, uint64(seq), []byte("additional-packed-outer-leaf"))
	if err != nil {
		_ = writer.Close()
		tb.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		_ = writer.Close()
		tb.Fatal(err)
	}
	identity, err := writer.StableIdentity()
	if err != nil {
		_ = writer.Close()
		tb.Fatal(err)
	}
	pointer, err := page.LeafLogPtrFromValuePtr(valuePtr)
	if err != nil {
		_ = writer.Close()
		tb.Fatal(err)
	}
	return rewriteCreatedSegment{path: path, fileID: fileID, identity: identity}, pointer, writer
}

func BenchmarkLeafGenerationPackPromotionAuthority(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len("packed-outer-leaf")))
	b.ResetTimer()
	var descriptors, obligations, contentSyncs, namespaceSyncs uint64
	var pinHighWater uint64
	var namespaceSyncNanos int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fixture := newLeafPackAuthorityFixture(b)
		authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		fixture.close(b)
		b.StartTimer()
		if _, mutated, err := authority.promote(); err != nil || !mutated {
			b.Fatalf("promote mutated=%t err=%v", mutated, err)
		}
		descriptors += uint64(len(authority.resources.Descriptors()))
		for _, descriptor := range authority.resources.Descriptors() {
			obligations += uint64(len(descriptor.LogicalObligations()))
		}
		for _, stats := range authority.resources.Stats(time.Now()) {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		namespaceSyncNanos += authority.namespaceSyncNanos
		b.StopTimer()
		if err := authority.release(); err != nil {
			b.Fatal(err)
		}
		if got := fixture.registry.ActivePins(); got != 0 {
			b.Fatalf("active packed outer-leaf pins after release=%d want 0", got)
		}
		if err := os.RemoveAll(fixture.db.dir); err != nil {
			b.Fatalf("remove fixture %s: %v", fixture.db.dir, err)
		}
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(descriptors)/float64(b.N), "descriptors/op")
	b.ReportMetric(float64(obligations)/float64(b.N), "logical_obligations/op")
	b.ReportMetric(float64(pinHighWater), "pin_high_water")
	b.ReportMetric(float64(contentSyncs)/float64(b.N), "capture_content_syncs/op")
	b.ReportMetric(1, "creation_file_syncs/op")
	b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "namespace_syncs/op")
	b.ReportMetric(float64(namespaceSyncNanos)/float64(b.N), "namespace_sync_ns/op")
}

func TestLeafGenerationPackPromotionAuthorityRetainsExactPackedResourceThroughRegistration(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	fixture.close(t)
	if got := fixture.registry.ActivePins(); got != 1 {
		t.Fatalf("capture pins after writer close=%d want 1", got)
	}
	if _, err := fixture.registry.BeginDelete(fixture.created.identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("BeginDelete during capture=%v want pinned", err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	promoted, mutated, err := authority.promote()
	restore()
	if err != nil || !mutated || len(promoted) != 1 {
		t.Fatalf("promote mutated=%v promoted=%v err=%v", mutated, promoted, err)
	}
	destinationPath := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if promoted[0].path != destinationPath {
		t.Fatalf("promoted path=%q want %q", promoted[0].path, destinationPath)
	}
	if err := rootpublication.ValidateStableChildLink(authority.stagingParent, authority.segments[0].handle, filepath.Base(fixture.path)); err != nil {
		t.Fatalf("recovery-owned staging alias changed: %v", err)
	}
	if got := fixture.registry.ActivePins(); got != 2 {
		t.Fatalf("capture+frozen-set pins=%d want 2", got)
	}
	descriptors := authority.resources.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Kind() != rootpublication.ResourceOuterLeafPack ||
		descriptors[0].Generation() != uint64(fixture.fileID) || descriptors[0].Frontier().Bytes < fixture.pointer.Offset+uint64(fixture.pointer.RecordLength()) {
		t.Fatalf("packed descriptors=%+v", descriptors)
	}
	resourceStats := authority.resources.Stats(time.Now())
	if len(resourceStats) != 1 || resourceStats[0].NamespaceSyncs != 2 {
		t.Fatalf("packed namespace sync stats=%+v want two exact parents", resourceStats)
	}
	var createEvents, beforeSync, afterSync int
	for _, event := range events {
		if event.Namespace == durabilitycut.NamespaceCreate {
			createEvents++
		}
		if event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeSync++
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterSync++
		}
	}
	if createEvents != 1 || beforeSync != 2 || afterSync != 2 {
		t.Fatalf("namespace evidence create=%d before-sync=%d after-sync=%d", createEvents, beforeSync, afterSync)
	}

	manager, err := valuelog.NewManagerWithStableResourcePinRegistry(fixture.destination, fixture.registry)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer manager.Close()
	fixture.db.valueLogManager = manager
	if err := authority.verifyManagerRegistration(); err != nil {
		t.Fatalf("verifyManagerRegistration: %v", err)
	}
	if err := authority.release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := fixture.registry.ActivePins(); got != 0 {
		t.Fatalf("pins after manager handoff=%d want 0", got)
	}
	if _, ok := manager.StableSegmentIdentity(fixture.fileID); !ok {
		t.Fatal("manager lost promoted identity after authority release")
	}
}

func TestLeafGenerationPackPromotionAuthorityMergesAndOwnsDictionaryClosure(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	const dictID = uint64(7401)
	dictionary := []byte("packed dictionary authority")
	provider := newTestStableDictionaryProvider(t, dictID, dictionary)
	fixture.db.SetStableDictionaryResourceProvider(provider)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.captureDictionary(context.Background(), dictID, dictionary); err != nil {
		t.Fatal(err)
	}
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	if _, mutated, err := authority.promote(); err != nil || !mutated {
		t.Fatalf("promote mutated=%v err=%v", mutated, err)
	}
	var hasDictionary, hasPacked bool
	for _, descriptor := range authority.resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			switch field {
			case rootpublication.ReachabilityDictionaryGeneration:
				hasDictionary = true
			case rootpublication.ReachabilityOuterLeafPackedPointer:
				hasPacked = true
			}
		}
	}
	if !hasDictionary || !hasPacked || provider.releaseCalls.Load() != 0 {
		t.Fatalf("promoted closure dictionary=%v packed=%v releaseCalls=%d", hasDictionary, hasPacked, provider.releaseCalls.Load())
	}
	if err := authority.release(); err != nil {
		t.Fatal(err)
	}
	if got := provider.releaseCalls.Load(); got != 1 {
		t.Fatalf("dictionary releases=%d want 1", got)
	}
}

func TestLeafGenerationPackPromotionAuthorityRollsBackPostLinkPoisonAndRetainsDictionaryUntilRelease(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	const dictID = uint64(7402)
	dictionary := []byte("poisoned packed dictionary authority")
	provider := newTestStableDictionaryProvider(t, dictID, dictionary)
	fixture.db.SetStableDictionaryResourceProvider(provider)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.captureDictionary(context.Background(), dictID, dictionary); err != nil {
		t.Fatal(err)
	}
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	poison := errors.New("post-link poison")
	authority.moveNoReplace = func(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
		installed, err := rootpublication.MoveStableChildFileNoReplace(sourceParent, expected, oldName, destinationParent, newName)
		if err != nil || !installed {
			return installed, err
		}
		return true, poison
	}
	if _, mutated, err := authority.promote(); mutated || !errors.Is(err, poison) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("poisoned promote mutated=%v err=%v", mutated, err)
	}
	if authority.retainedForRecovery || provider.releaseCalls.Load() != 0 {
		t.Fatalf("poisoned authority retained=%v dictionary releases=%d", authority.retainedForRecovery, provider.releaseCalls.Load())
	}
	destination := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("poisoned destination survived exact rollback: %v", statErr)
	}
	if err := authority.release(); err != nil {
		t.Fatal(err)
	}
	if got := provider.releaseCalls.Load(); got != 1 {
		t.Fatalf("dictionary releases=%d want 1", got)
	}
}

func TestLeafGenerationPackPromotionAuthorityMultipleSegmentsShareParentSyncs(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	secondFileID, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 2)
	if err != nil {
		t.Fatal(err)
	}
	secondPath := valuelog.SegmentPath(fixture.stagingDir, secondFileID)
	secondWriter, err := valuelog.NewStagingWriter(secondPath, secondFileID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if secondWriter != nil {
			_ = secondWriter.Close()
		}
	}()
	secondValuePtr, err := secondWriter.Append(0, nil, 2, []byte("second-packed-outer-leaf"))
	if err != nil {
		t.Fatal(err)
	}
	if err := secondWriter.Sync(); err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := secondWriter.StableIdentity()
	if err != nil {
		t.Fatal(err)
	}
	secondPointer, err := page.LeafLogPtrFromValuePtr(secondValuePtr)
	if err != nil {
		t.Fatal(err)
	}
	secondCreated := rewriteCreatedSegment{path: secondPath, fileID: secondFileID, identity: secondIdentity}

	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture(
		[]rewriteCreatedSegment{fixture.created, secondCreated},
		[]page.LeafLogPtr{fixture.pointer, secondPointer},
	); err != nil {
		t.Fatal(err)
	}
	if got := fixture.writer.DurabilityStats().FileSyncCalls; got != 1 {
		t.Fatalf("first content syncs=%d want 1", got)
	}
	if got := secondWriter.DurabilityStats().FileSyncCalls; got != 1 {
		t.Fatalf("second content syncs=%d want 1", got)
	}
	fixture.close(t)
	if err := secondWriter.Close(); err != nil {
		t.Fatal(err)
	}
	secondWriter = nil

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	promoted, mutated, err := authority.promote()
	restore()
	if err != nil || !mutated || len(promoted) != 2 {
		t.Fatalf("promote mutated=%v promoted=%v err=%v", mutated, promoted, err)
	}
	if got := fixture.registry.ActivePins(); got != 4 {
		t.Fatalf("two capture and two resource-set pins=%d want 4", got)
	}
	if descriptors := authority.resources.Descriptors(); len(descriptors) != 2 {
		t.Fatalf("packed descriptors=%d want 2", len(descriptors))
	}
	if stats := authority.resources.Stats(time.Now()); len(stats) != 1 || stats[0].NamespaceSyncs != 2 {
		t.Fatalf("multi-segment namespace stats=%+v want two distinct parents", stats)
	}
	var creates, beforeSyncs, afterSyncs int
	for _, event := range events {
		if event.Namespace == durabilitycut.NamespaceCreate {
			creates++
		}
		if event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeSyncs++
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterSyncs++
		}
	}
	if creates != 2 || beforeSyncs != 2 || afterSyncs != 2 {
		t.Fatalf("two files share exact parents: creates=%d before-sync=%d after-sync=%d", creates, beforeSyncs, afterSyncs)
	}
}

func TestLeafGenerationPackPromotionAuthorityRejectsExistingDestinationBeforeMutation(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	replacement := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if err := os.WriteFile(replacement, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, mutated, err := authority.promote()
	if mutated || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("promote mutated=%v err=%v want pre-mutation conflict", mutated, err)
	}
	if err := rootpublication.ValidateStableChildLink(authority.stagingParent, authority.segments[0].handle, filepath.Base(fixture.path)); err != nil {
		t.Fatalf("staging identity lost: %v", err)
	}
	data, err := os.ReadFile(replacement)
	if err != nil || string(data) != "replacement" {
		t.Fatalf("replacement changed data=%q err=%v", data, err)
	}
}

func TestLeafGenerationPackPromotionAuthorityRejectsReboundSourceBeforeMutation(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	originalPath := fixture.path + ".original"
	if err := os.Rename(fixture.path, originalPath); err != nil {
		t.Fatal(err)
	}
	originalBefore, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	const replacement = "rebound-staging-replacement"
	if err := os.WriteFile(fixture.path, []byte(replacement), 0o600); err != nil {
		t.Fatal(err)
	}

	promoted, mutated, err := authority.promote()
	if mutated || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("promote mutated=%v promoted=%v err=%v want pre-mutation conflict", mutated, promoted, err)
	}
	if len(promoted) != 1 || promoted[0] != fixture.created {
		t.Fatalf("cleanup state=%+v want original %+v", promoted, fixture.created)
	}
	if originalAfter, readErr := os.ReadFile(originalPath); readErr != nil || !bytes.Equal(originalAfter, originalBefore) {
		t.Fatalf("original changed after rebound rejection: bytes_equal=%v err=%v", bytes.Equal(originalAfter, originalBefore), readErr)
	}
	if replacementAfter, readErr := os.ReadFile(fixture.path); readErr != nil || string(replacementAfter) != replacement {
		t.Fatalf("replacement changed after rebound rejection: data=%q err=%v", replacementAfter, readErr)
	}
	destination := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination appeared after rebound rejection: %v", statErr)
	}
}

func TestLeafGenerationPackTargetProbeFailsBeforePublicationOrStaging(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)

	originalProbe := leafGenerationPackPromotionTargetProbe
	probeCalls := 0
	probeErr := fmt.Errorf("%w: injected target filesystem", rootpublication.ErrNamespacePersistenceUnsupported)
	leafGenerationPackPromotionTargetProbe = func(*os.File) error {
		probeCalls++
		return probeErr
	}
	defer func() { leafGenerationPackPromotionTargetProbe = originalProbe }()
	epochBefore := db.systemRootPublishEpoch.Load()
	stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID}, Force: true,
	})
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("LeafGenerationPack error=%v want typed target capability failure", err)
	}
	if probeCalls != 1 || stats.CopyAttempts != 0 {
		t.Fatalf("probe calls=%d copy attempts=%d want 1,0", probeCalls, stats.CopyAttempts)
	}
	if epochAfter := db.systemRootPublishEpoch.Load(); epochAfter != epochBefore {
		t.Fatalf("publication epoch changed before=%d after=%d", epochBefore, epochAfter)
	}
	staging, globErr := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*"))
	if globErr != nil || len(staging) != 0 {
		t.Fatalf("staging after target preflight failure=%v err=%v", staging, globErr)
	}
}

func TestLeafGenerationPackTargetProbePreservesTwoParentSyncContract(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	var beforeSyncs, afterSyncs int
	counting := false
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase == leafGenerationPackAfterManifestPreparation {
			counting = true
		}
		return nil
	})
	defer unregister()
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if counting && event.Point == durabilitycut.BeforeNewFileDirectorySync {
			beforeSyncs++
		}
		if counting && event.Point == durabilitycut.AfterNewFileDirectorySync {
			afterSyncs++
		}
		return nil
	})
	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID}, Force: true,
	})
	restore()
	if err != nil {
		t.Fatalf("LeafGenerationPack: %v", err)
	}
	if beforeSyncs != 2 || afterSyncs != 2 {
		t.Fatalf("target probe changed namespace sync contract: before=%d after=%d want 2,2", beforeSyncs, afterSyncs)
	}
}

func TestLeafGenerationPackBeforePromotionReboundPreservesBothSourceFiles(t *testing.T) {
	dir := t.TempDir()
	db, leafLog := openLeafGenerationPackPublicationTestDB(t, dir)
	defer closeLeafGenerationPackPublicationTestDB(t, db, leafLog)
	candidate := prepareLeafGenerationPackTestCandidate(t, db, leafLog, 512)
	const replacement = "end-to-end-staging-replacement"
	var stagingPath, originalPath, destinationPath string
	var originalBefore []byte
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase != leafGenerationPackBeforePromotion {
			return nil
		}
		matches, err := filepath.Glob(filepath.Join(LeafLogDirPath(dir), ".leaf-pack-copy-*", leafVLogDirName, "value-l*.log"))
		if err != nil || len(matches) != 1 {
			return fmt.Errorf("staging candidates=%v err=%v", matches, err)
		}
		stagingPath = matches[0]
		originalPath = stagingPath + ".original"
		destinationPath = filepath.Join(LeafLogDirPath(dir), filepath.Base(stagingPath))
		if err := os.Rename(stagingPath, originalPath); err != nil {
			return err
		}
		originalBefore, err = os.ReadFile(originalPath)
		if err != nil {
			return err
		}
		return os.WriteFile(stagingPath, []byte(replacement), 0o600)
	})
	_, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
		GenerationIDs: []uint64{candidate.generation.GenerationID}, Force: true,
	})
	unregister()
	if !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("LeafGenerationPack error=%v want conflict plus recovery-required cleanup ownership", err)
	}
	if stagingPath == "" {
		t.Fatal("before-promotion interposition did not run")
	}
	if got, readErr := os.ReadFile(originalPath); readErr != nil || !bytes.Equal(got, originalBefore) {
		t.Fatalf("original staging file changed: equal=%v err=%v", bytes.Equal(got, originalBefore), readErr)
	}
	if got, readErr := os.ReadFile(stagingPath); readErr != nil || string(got) != replacement {
		t.Fatalf("replacement staging file changed: data=%q err=%v", got, readErr)
	}
	if _, statErr := os.Stat(destinationPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("destination appeared after pre-promotion rebound: %v", statErr)
	}
}

func TestLeafGenerationPackPromotionAuthorityRetainsPostMutationAmbiguity(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	secondCreated, secondPointer, secondWriter := newLeafPackAuthorityFixtureSegment(t, &fixture, 2)
	defer secondWriter.Close()
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture(
		[]rewriteCreatedSegment{fixture.created, secondCreated},
		[]page.LeafLogPtr{fixture.pointer, secondPointer},
	); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	if err := secondWriter.Close(); err != nil {
		t.Fatal(err)
	}
	testErr := errors.New("post-install source identity ambiguity")
	authority.moveNoReplace = func(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
		installed, err := rootpublication.MoveStableChildFileNoReplace(sourceParent, expected, oldName, destinationParent, newName)
		if err != nil || !installed {
			return installed, err
		}
		return true, testErr
	}
	deletionSyncErr := errors.New("injected promoted cleanup deletion sync failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeDeletionDirectorySync {
			return deletionSyncErr
		}
		return nil
	})
	promoted, mutated, err := authority.promote()
	restore()
	if !mutated || !errors.Is(err, testErr) || !errors.Is(err, deletionSyncErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("promote mutated=%v err=%v want ambiguous recovery", mutated, err)
	}
	destination := filepath.Join(fixture.destination, filepath.Base(fixture.path))
	if len(promoted) != 2 || promoted[0].path != destination || promoted[1] != secondCreated {
		t.Fatalf("ambiguous cleanup state=%+v want exact destination %q", promoted, destination)
	}
	if !authority.retainedForRecovery || fixture.registry.ActivePins() == 0 {
		t.Fatalf("ambiguous authority retained=%v pins=%d", authority.retainedForRecovery, fixture.registry.ActivePins())
	}
	if len(authority.recoveryCleanup) != 1 || authority.recoveryCleanup[0].path != destination || authority.recoveryStagingRoot != fixture.stagingRoot {
		t.Fatalf("retained cleanup=%+v staging_root=%q want exact child and %q", authority.recoveryCleanup, authority.recoveryStagingRoot, fixture.stagingRoot)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("ambiguous cleanup left destination linked: %v", statErr)
	}
	secondDestination := filepath.Join(fixture.destination, filepath.Base(secondCreated.path))
	if _, statErr := os.Stat(secondDestination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uninstalled second segment became a cleanup target: %v", statErr)
	}
	if err := fixture.db.runCaptureTeardownHooksLocked(); err != nil {
		t.Fatalf("teardown retry: %v", err)
	}
	if _, statErr := os.Stat(fixture.stagingRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("teardown retained staging root: %v", statErr)
	}
	if stats := fixture.registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 || stats.ActiveStableNamespaceLinks != 0 {
		t.Fatalf("teardown retained registry state: %+v", stats)
	}
}

func TestLeafGenerationPackPromotionAuthorityRollsBackCumulativePartialInstall(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	defer fixture.close(t)
	secondCreated, secondPointer, secondWriter := newLeafPackAuthorityFixtureSegment(t, &fixture, 2)
	defer secondWriter.Close()
	authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.release()
	if err := authority.capture(
		[]rewriteCreatedSegment{fixture.created, secondCreated},
		[]page.LeafLogPtr{fixture.pointer, secondPointer},
	); err != nil {
		t.Fatal(err)
	}
	fixture.close(t)
	if err := secondWriter.Close(); err != nil {
		t.Fatal(err)
	}

	testErr := errors.New("second segment did not install")
	moveCalls := 0
	authority.moveNoReplace = func(sourceParent, expected *os.File, oldName string, destinationParent *os.File, newName string) (bool, error) {
		moveCalls++
		if moveCalls == 1 {
			return rootpublication.MoveStableChildFileNoReplace(sourceParent, expected, oldName, destinationParent, newName)
		}
		return false, testErr
	}
	promoted, mutated, err := authority.promote()
	if moveCalls != 2 || mutated || !errors.Is(err, testErr) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("promote calls=%d mutated=%v err=%v want exact cumulative rollback", moveCalls, mutated, err)
	}
	if authority.retainedForRecovery || fixture.registry.ActivePins() == 0 {
		t.Fatalf("partial-install authority retained=%v pins=%d", authority.retainedForRecovery, fixture.registry.ActivePins())
	}
	firstDestination := filepath.Join(fixture.destination, filepath.Base(fixture.created.path))
	if len(promoted) != 2 || promoted[0].path != firstDestination || promoted[1] != secondCreated {
		t.Fatalf("partial promoted state=%+v want first destination and second original", promoted)
	}
	if _, statErr := os.Stat(firstDestination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("first destination survived exact rollback: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(fixture.destination, filepath.Base(secondCreated.path))); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("second destination unexpectedly installed: %v", statErr)
	}
}

func TestLeafGenerationPackPromotionAuthorityRollsBackPostInstallCutFailures(t *testing.T) {
	tests := []struct {
		name  string
		match func(durabilitycut.Event) bool
	}{
		{name: "namespace-observation", match: func(event durabilitycut.Event) bool { return event.Namespace == durabilitycut.NamespaceCreate }},
		{name: "before-parent-sync", match: func(event durabilitycut.Event) bool { return event.Point == durabilitycut.BeforeNewFileDirectorySync }},
		{name: "after-parent-sync", match: func(event durabilitycut.Event) bool { return event.Point == durabilitycut.AfterNewFileDirectorySync }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLeafPackAuthorityFixture(t)
			defer fixture.close(t)
			authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
			if err != nil {
				t.Fatal(err)
			}
			defer authority.release()
			if err := authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{fixture.pointer}); err != nil {
				t.Fatal(err)
			}
			fixture.close(t)

			testErr := errors.New("post-install durability cut")
			cutCalls := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if test.match(event) {
					cutCalls++
					return testErr
				}
				return nil
			})
			_, mutated, promoteErr := authority.promote()
			restore()
			if cutCalls != 1 || mutated || !errors.Is(promoteErr, testErr) || errors.Is(promoteErr, ErrRecoveryRequired) {
				t.Fatalf("promote cuts=%d mutated=%v err=%v want exact post-install rollback", cutCalls, mutated, promoteErr)
			}
			if authority.retainedForRecovery || fixture.registry.ActivePins() == 0 {
				t.Fatalf("post-install authority retained=%v pins=%d", authority.retainedForRecovery, fixture.registry.ActivePins())
			}
			destination := filepath.Join(fixture.destination, filepath.Base(fixture.path))
			if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("post-install destination survived exact rollback: %v", statErr)
			}
		})
	}
}

func TestLeafGenerationPackPromotionAuthorityRejectsMalformedPointers(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(page.LeafLogPtr) page.LeafLogPtr
		wantErr error
	}{
		{name: "zero-length", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.RecordLengthHint = 0; return ptr }, wantErr: rootpublication.ErrUnresolvedResource},
		{name: "foreign-file", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.FileID++; return ptr }, wantErr: rootpublication.ErrResourceConflict},
		{name: "overflow", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.Offset = ^uint64(0) - 1; return ptr }, wantErr: rootpublication.ErrFrontierBeyondResource},
		{name: "past-frontier", mutate: func(ptr page.LeafLogPtr) page.LeafLogPtr { ptr.Offset += 1 << 20; return ptr }, wantErr: rootpublication.ErrFrontierBeyondResource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLeafPackAuthorityFixture(t)
			defer fixture.close(t)
			authority, err := newLeafGenerationPackPromotionAuthority(fixture.db, fixture.stagingRoot, fixture.stagingDir, fixture.destination)
			if err != nil {
				t.Fatal(err)
			}
			defer authority.release()
			err = authority.capture([]rewriteCreatedSegment{fixture.created}, []page.LeafLogPtr{test.mutate(fixture.pointer)})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("capture error=%v want %v", err, test.wantErr)
			}
			if fixture.registry.ActivePins() != 0 || fixture.registry.ActiveIdentities() != 0 {
				t.Fatalf("failed capture leaked pins=%d identities=%d", fixture.registry.ActivePins(), fixture.registry.ActiveIdentities())
			}
		})
	}
}

func TestCleanupRewriteCreatedSegmentRejectsReboundPath(t *testing.T) {
	fixture := newLeafPackAuthorityFixture(t)
	fixture.close(t)
	if err := os.Rename(fixture.path, fixture.path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.path, []byte("rebound"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := fixture.db.cleanupRewriteCreatedSegments([]rewriteCreatedSegment{fixture.created})
	if !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("cleanup error=%v want conflict and recovery-required", err)
	}
	data, readErr := os.ReadFile(fixture.path)
	if readErr != nil || string(data) != "rebound" {
		t.Fatalf("rebound path was deleted data=%q err=%v", data, readErr)
	}
}
