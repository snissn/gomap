package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// After genuine production vacuum priming, this fixture changes only the
// producer's physical append sequence in the supported reserved leaf lane.
// Ascending is the writer invariant; lower sequence is a controlled filter
// contract inquiry, not a claim about naturally supported writer history.
// It never injects manifest membership or substitutes the compatibility view.
func TestExternalManifestWriterACKCoverage(t *testing.T) {
	for _, path := range []string{"serialized", "queued", "build-group"} {
		for _, order := range []string{"ascending", "controlled-present-lower-sequence"} {
			t.Run(path+"/"+order, func(t *testing.T) {
				dir := t.TempDir()
				opts := Options{Dir: dir, CommandWAL: true, Durability: DurabilityDurable,
					IndexOuterLeavesInValueLog: true, IndexPackedValuePtr: true, DisableBackgroundPrune: true}
				// Persist the full index format. The command-only helper would
				// overwrite outer/packed true with false during Open.
				if err := SaveFormatConfig(dir, formatConfigFromOptions(opts)); err != nil {
					t.Fatal(err)
				}
				database, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if database != nil {
						_ = database.Close()
					}
				}()
				primeSegment, primeWriter := openLeafPageLogTestWriter(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 10)
				leafLog := &multiReportedLeafPageLog{appendSegment: primeSegment, writer: primeWriter, currentSegments: []LeafPageLogSegment{primeSegment}, createdSegments: []LeafPageLogSegment{primeSegment}}
				defer leafLog.Close()
				database.SetLeafPageLog(leafLog)
				key := func(i int) []byte { return []byte(fmt.Sprintf("manifest-key-%04d-%0120d", i, i)) }
				value := bytes.Repeat([]byte("v"), 32)
				publish := func(first, count int) {
					entries := make([]batchpkg.Entry, count)
					for i := range entries {
						entries[i] = batchpkg.Entry{Type: batchpkg.OpPut, Key: key(first + i), Value: value}
					}
					lsn, err := database.AppendRawKVCommandWALOrderedEntries(entries, true)
					if err != nil || lsn == 0 {
						t.Fatalf("real command frame lsn=%d err=%v", lsn, err)
					}
					var group *RootPublicationBuildGroup
					parts := 1
					if path == "build-group" {
						group, err = database.BeginRootPublicationBuildGroup()
						if err != nil {
							t.Fatal(err)
						}
						defer group.Close()
						parts = 2
					}
					for part := 0; part < parts; part++ {
						b := database.NewPhysicalBatch().(*Batch)
						start, end := part*count/parts, (part+1)*count/parts
						for _, e := range entries[start:end] {
							if err := b.Set(e.Key, e.Value); err != nil {
								_ = b.Close()
								t.Fatal(err)
							}
						}
						final := part == parts-1
						if final {
							if err := b.SetCommandWALPublish(lsn, []CommandWALLSNRange{{First: lsn, Last: lsn}}); err != nil {
								_ = b.Close()
								t.Fatal(err)
							}
						}
						if group != nil {
							if err := b.SetRootPublicationBuildGroup(group, final); err != nil {
								_ = b.Close()
								t.Fatal(err)
							}
						}
						if path == "serialized" {
							err = b.writeSerialized(true, b.commandWALPublishIntent, database.assignBatchEntryRevisions(b.batch), nil)
						} else if final {
							err = b.WriteSync()
						} else {
							err = b.Write()
						}
						closeErr := b.Close()
						if err != nil || closeErr != nil {
							t.Fatalf("writer err=%v close=%v", err, closeErr)
						}
					}
					if group != nil {
						if err := group.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if got := database.State().AppliedCommandLSN; got != lsn {
						t.Fatalf("ACK AppliedCommandLSN=%d want %d", got, lsn)
					}
					if database.rootPublication != nil {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						err = database.rootPublication.coordinator.WaitThrough(ctx, database.currentCommitSeq())
						cancel()
						if err != nil {
							t.Fatalf("join existing root publication after ACK: %v", err)
						}
					}
				}
				// The ordinary outer-leaf vacuum preserves system leaf refs when
				// no collection descriptor relocates. Seed real collection roots
				// and a multi-page system catalog, as in the production vacuum
				// system-leaf policy fixture; no manifest/resources are injected.
				publish(0, 128)
				if !database.indexOuterLeavesInValueLog || !database.indexPackedValuePtr {
					t.Fatal("SETUP: persisted format disabled external/packed leaves")
				}
				const paddingKey = "collections/root/aaa-padding/primary"
				_, collectionRoots, err := database.PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(
					[]OrderedRootDeltaPublishInput{
						{Iter: mustFrozenSystemMemtable(t, vacuumTestDocumentKey, vacuumTestDocumentValue).NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
						{Iter: mustFrozenSystemMemtable(t, systemRangeKVs(1024, nil)...).NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves},
					}, mustRawKVCommandWALIntent(t, database, "system/fixture-prime", "authority"),
					func(ids []uint64) (iterator.UnsafeIterator, error) {
						kvs := make([]any, 0, 4100)
						for _, s := range systemRangeKVs(2048, nil) {
							kvs = append(kvs, s)
						}
						kvs = append(kvs, vacuumTestCollectionRootKey, encodeCollectionRootDescriptorRootID(ids[0]), paddingKey, encodeCollectionRootDescriptorRootID(ids[1]))
						return mustFrozenRawMemtable(t, kvs...).NewIterator(nil, nil), nil
					},
				)
				if err != nil || len(collectionRoots) != 2 {
					t.Fatalf("SETUP: genuine collection/system publication roots=%v err=%v", collectionRoots, err)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("SETUP: seal collection catalog: %v", err)
				}
				// The padding catalog key sorts before the small target. Moving
				// its real pager tree gives both slots an application catalog and
				// makes the target's source/destination allocation order differ.
				_, movedPadding, err := database.PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(
					[]OrderedRootDeltaPublishInput{{BaseRoot: collectionRoots[1], Iter: mustFrozenSystemMemtable(t, "doc/padding-update", "updated").NewIterator(nil, nil), StoragePolicy: OrderedRootStoragePagerLeaves}},
					mustRawKVCommandWALIntent(t, database, "system/fixture-padding", "authority"),
					func(ids []uint64) (iterator.UnsafeIterator, error) {
						return mustFrozenRawMemtable(t, paddingKey, encodeCollectionRootDescriptorRootID(ids[0])).NewIterator(nil, nil), nil
					},
				)
				if err != nil || len(movedPadding) != 1 || movedPadding[0] == collectionRoots[1] {
					t.Fatalf("SETUP: real padding root did not move roots=%v err=%v", movedPadding, err)
				}
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("SETUP: seal successor catalog: %v", err)
				}
				if n := len(collectLeafRefIDsFromRoot(t, database, database.State().SystemRootPageID)); n < 2 {
					t.Fatalf("SETUP: genuine multi-page system outer-leaf pointers=%d want >=2", n)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = database.VacuumIndexOnline(ctx)
				cancel()
				if err != nil {
					t.Fatalf("SETUP: production vacuum priming: %v", err)
				}
				primeSnapshot := database.AcquireSnapshot()
				if primeSnapshot == nil {
					t.Fatal("SETUP: missing post-vacuum snapshot")
				}
				descriptor, descriptorErr := primeSnapshot.GetAtRoot(primeSnapshot.state.SystemRootPageID, []byte(vacuumTestCollectionRootKey))
				snapshotCloseErr := primeSnapshot.Close()
				if descriptorErr != nil || snapshotCloseErr != nil || len(descriptor) != 8 {
					t.Fatalf("SETUP: exact post-vacuum descriptor=%x read=%v close=%v", descriptor, descriptorErr, snapshotCloseErr)
				}
				if relocated := binary.BigEndian.Uint64(descriptor); relocated == collectionRoots[0] || relocated == 0 {
					t.Fatalf("SETUP: production vacuum retained target descriptor source=%d destination=%d", collectionRoots[0], relocated)
				} else {
					t.Logf("SETUP: production vacuum relocated target collection root %d -> %d", collectionRoots[0], relocated)
				}
				qsv13CheckExternalManifestWriterRoots(t, database, "prime-vacuum")
				if t.Failed() {
					t.Fatal("SETUP: priming did not establish genuine pointers and exact recoverable resources; causal phase not entered")
				}
				t.Log("CAUSAL: genuine relocated catalog, outer-leaf pointers and exact both-slot manifest coverage established")
				if err := leafLog.writer.Close(); err != nil {
					t.Fatal(err)
				}
				lane, seq := uint32(rewriteLeafLogLaneID), uint32(11)
				if order == "controlled-present-lower-sequence" {
					seq = 9
				}
				segment, writer := openLeafPageLogTestWriter(t, LeafLogDirPath(dir), lane, seq)
				leafLog.appendSegment, leafLog.writer = segment, writer
				leafLog.currentSegments = []LeafPageLogSegment{segment}
				leafLog.createdSegments = append(leafLog.createdSegments, segment)
				publish(128, 128)
				qsv13CheckExternalManifestWriterRoots(t, database, "new-producer-ACK", segment.FileID)
				// A later ACK diagnoses a one-publication lag without manufacturing mappings.
				publish(256, 2)
				qsv13CheckExternalManifestWriterRoots(t, database, "same-producer-next-ACK", segment.FileID)
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = nil
				if err := leafLog.Close(); err != nil {
					t.Fatal(err)
				}
				opts.ReadOnly = true
				reopened, err := Open(opts)
				if err != nil {
					t.Fatalf("ReadOnly reopen: %v", err)
				}
				defer reopened.Close()
				qsv13CheckExternalManifestWriterRoots(t, reopened, "ReadOnly-reopen", segment.FileID)
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				stats, censusErr := reopened.LeafGenerationGC(ctx, LeafGenerationGCOptions{DryRun: true})
				cancel()
				if censusErr != nil {
					t.Errorf("unchanged ReadOnly DryRun census: %v (raw stats=%+v; UNKNOWN on error)", censusErr, stats)
				}
				for i := 0; i < 258; i++ {
					got, err := reopened.Get(key(i))
					if err != nil || !bytes.Equal(got, value) {
						t.Fatalf("reopen key%d value=%q err=%v", i, got, err)
					}
				}
			})
		}
	}
}

// Freeze writer publication while inspecting each slot's own retained authority.
// The verifier callback supplies the actual containing pager page for each ptr.
func qsv13CheckExternalManifestWriterRoots(t *testing.T, database *DB, phase string, expectedCurrentFileIDs ...uint32) {
	t.Helper()
	database.writeMu.Lock()
	defer database.writeMu.Unlock()
	database.durablePublishMu.Lock()
	defer database.durablePublishMu.Unlock()
	idx := database.idx.Load()
	for slot, record := range database.durableRoot.slotRecord {
		if record.CommitSeq == 0 {
			continue
		}
		// Walk before requiring resources: a truly empty genesis slot is
		// legitimate, but its emptiness must be proved from its own roots.
		type locatedPtr struct {
			pageID uint64
			ptr    page.LeafLogPtr
		}
		var pointers []locatedPtr
		var walkedPage uint64
		if err := leafrefscan.WalkRoots(context.Background(), []uint64{record.UserRootPageID, record.SystemRootPageID}, idx.pager.Get,
			func(id uint64, _ node.Node) error { walkedPage = id; return nil },
			func(ptr page.LeafLogPtr) error { pointers = append(pointers, locatedPtr{walkedPage, ptr}); return nil }); err != nil {
			t.Errorf("%s slot%d root walk: %v", phase, slot, err)
			continue
		}
		if record.CommitSeq == 1 && len(pointers) == 0 {
			t.Logf("%s slot%d commit1: genuinely pointer-free genesis", phase, slot)
			continue
		}
		if record.CommitSeq == database.currentCommitSeq() && len(pointers) == 0 {
			t.Errorf("%s SETUP: selected root has no genuine outer-leaf pointers", phase)
			continue
		}
		if record.CommitSeq == database.currentCommitSeq() {
			for _, expected := range expectedCurrentFileIDs {
				found := false
				for _, located := range pointers {
					found = found || located.ptr.ValueLogFileID() == expected
				}
				if !found {
					t.Errorf("%s SETUP: current root does not reference actual new producer rawID%d", phase, expected)
				}
			}
		}
		resources := database.durableRoot.slotResources[slot]
		if resources == nil {
			t.Errorf("%s slot%d commit%d lacks resources", phase, slot, record.CommitSeq)
			continue
		}
		var token *rootpublication.StableResourceToken
		for _, candidate := range resources.Tokens() {
			if candidate.Kind() != rootpublication.ResourceOuterLeafManifest {
				continue
			}
			if token != nil {
				t.Errorf("%s slot%d multiple manifest tokens", phase, slot)
				return
			}
			token = candidate
		}
		if token == nil {
			t.Errorf("%s slot%d commit%d lacks manifest token", phase, slot, record.CommitSeq)
			continue
		}
		// Only exact pinned frontier bytes; never read manifest.json or newest filename.
		var data []byte
		err := token.WithPinnedFile(func(file *os.File) error {
			var e error
			data, e = io.ReadAll(io.NewSectionReader(file, 0, int64(token.Frontier().Bytes)))
			return e
		})
		if err != nil || uint64(len(data)) != token.Frontier().Bytes || sha256.Sum256(data) != token.Digest() {
			t.Errorf("%s slot%d exact manifest read/digest failed err=%v", phase, slot, err)
			continue
		}
		manifest, err := decodeLeafGenerationManifest(data, token.ResourceID())
		if err != nil || manifest.ManifestRevision != token.Generation() {
			t.Errorf("%s slot%d manifest decode/revision err=%v", phase, slot, err)
			continue
		}
		t.Logf("%s slot%d commit%d manifest=%s revision%d digest=%x frontier=%d", phase, slot, record.CommitSeq, token.ResourceID(), token.Generation(), token.Digest(), token.Frontier().Bytes)
		view := newLeafGenerationView(manifest)
		t.Logf("%s slot%d genuine outer-leaf pointers=%d", phase, slot, len(pointers))
		for _, located := range pointers {
			ownerPage, ptr := located.pageID, located.ptr
			t.Logf("%s slot%d commit%d userRoot%d systemRoot%d page%d actual ptr%+v currentMaxRecoveredSeq%d", phase, slot, record.CommitSeq, record.UserRootPageID, record.SystemRootPageID, ownerPage, ptr, database.leafGenerationManifest.maxNonDeletedRecoveredSeq())
			marked := ptr.ValueLogFileID()
			var backing *rootpublication.StableResourceToken
			for _, candidate := range resources.Tokens() {
				if candidate.Kind() == rootpublication.ResourceOuterLeafLog && candidate.Generation() == uint64(marked) {
					backing = candidate
					break
				}
			}
			if backing == nil {
				t.Errorf("%s slot%d commit%d page%d ptr%+v missing raw resource", phase, slot, record.CommitSeq, ownerPage, ptr)
			} else {
				if ptr.Offset > backing.Frontier().Bytes || uint64(ptr.RecordLength()) > backing.Frontier().Bytes-ptr.Offset {
					t.Errorf("%s slot%d ptr%+v exceeds exact resource %s frontier%d", phase, slot, ptr, backing.ResourceID(), backing.Frontier().Bytes)
				}
				if err := backing.WithPinnedFile(func(f *os.File) error {
					info, e := f.Stat()
					if e != nil {
						return e
					}
					if uint64(info.Size()) < backing.Frontier().Bytes {
						return fmt.Errorf("physical file shorter than retained frontier")
					}
					return nil
				}); err != nil {
					t.Errorf("%s slot%d exact raw backing: %v", phase, slot, err)
				}
			}
			if _, ok := view.FileToGeneration[ptr.FileID]; !ok {
				currentHas := database.leafGenerationManifest.hasNonDeletedFileID(ptr.FileID)
				_, seq := valuelog.DecodeSegmentID(ptr.FileID)
				var filtered []uint32
				var filterErr error
				if !currentHas {
					filtered, filterErr = database.filterLeafGenerationRegistrableRawFileIDs(database.leafGenerationManifest, []uint32{ptr.FileID})
				}
				t.Errorf("%s slot%d commit%d page%d ptr%+v manifest%d missing; currentHas=%t maxRecoveredSeq=%d ptrSeq=%d filterIfCurrentMissing=%v filterErr=%v backing=%t", phase, slot, record.CommitSeq, ownerPage, ptr, manifest.ManifestRevision, currentHas, database.leafGenerationManifest.maxNonDeletedRecoveredSeq(), seq, filtered, filterErr, backing != nil)
			}
		}
	}
}
