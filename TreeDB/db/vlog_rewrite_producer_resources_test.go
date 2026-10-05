//go:build !windows

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// Exercise the ordinary public COW rewrite, rather than only calling the
// stable append API directly. Byte lookup alone cannot authorize publication.
func TestRewriteExactClosureDictionarySwitchProducerAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			testRewriteExactClosureDictionarySwitchProducerAuthority(t, replay)
		})
	}
}

func testRewriteExactClosureDictionarySwitchProducerAuthority(t *testing.T, replay bool) {
	for _, fallback := range []bool{false, true} {
		for _, authority := range []bool{false, true} {
			t.Run(fmt.Sprintf("fallback=%v/authority=%v", fallback, authority), func(t *testing.T) {
				db, writer, old, fresh := setupExactRewritePair(t)
				if replay {
					writer = installExactRewriteReplayLeafProducer(t, db)
				}
				pages := [][]byte{buildRewriteLeafPageFixture(t, "dictionary-a"), buildRewriteLeafPageFixture(t, "dictionary-b"), buildRewriteLeafPageFixture(t, "dictionary-c")}
				compact := make([][]byte, len(pages))
				for i := range pages {
					var err error
					compact[i], _, err = valuelog.MaybeCompactLeafLogPayload(pages[i])
					if err != nil {
						t.Fatal(err)
					}
				}
				const dictID = uint64(7401)
				dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{ID: uint32(dictID), Contents: compact, History: append([]byte(nil), compact[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
				if err != nil {
					t.Fatal(err)
				}
				provider := newTestStableDictionaryProvider(t, dictID, dictionary)
				db.valueLogManager.SetDictLookup(func(id uint64) ([]byte, error) {
					if id != dictID {
						return nil, fmt.Errorf("unknown dictionary %d", id)
					}
					return dictionary, nil
				})
				if authority {
					db.SetStableDictionaryResourceProvider(provider)
				}
				writer.SetKeepPolicy(0, 0, 0)
				writer.blockCompression = true
				writer.SetLeafDictMode(dictID, dictionary, false)
				if err := writer.rotateLeaf(); err != nil {
					t.Fatal(err)
				}
				if fallback {
					db.valueLogRefTracker.invalidate()
				}
				beforeSeq := db.currentCommitSeq()
				if authority {
					db.testFailFinalizeCommit.Store(true)
					err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true)
					db.testFailFinalizeCommit.Store(false)
					if !errors.Is(err, errTestFinalizeCommitFailpoint) || db.currentCommitSeq() != beforeSeq {
						t.Fatalf("producer abort err=%v sequence=%d want %d", err, db.currentCommitSeq(), beforeSeq)
					}
					if provider.captureCalls.Load() == 0 || provider.releaseCalls.Load() != provider.captureCalls.Load() {
						t.Fatalf("aborted producer captures=%d releases=%d", provider.captureCalls.Load(), provider.releaseCalls.Load())
					}
				}
				scans := 0
				db.testScanCandidateExternalReferencesHook = func() { scans++ }
				err = db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true)
				db.testScanCandidateExternalReferencesHook = nil
				if !authority {
					if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
						t.Fatalf("byte-only dictionary rewrite error=%v", err)
					}
					if db.currentCommitSeq() != beforeSeq {
						t.Fatal("unauthorized dictionary rewrite published")
					}
					if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("old")) {
						t.Fatalf("rejected read=%q %v", got, err)
					}
					if provider.captureCalls.Load() != 0 {
						t.Fatal("uninstalled provider was used")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				wantScans := 0
				if fallback {
					wantScans = 1
				}
				if scans != wantScans {
					t.Fatalf("full scans=%d want %d", scans, wantScans)
				}
				found := false
				for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
					if descriptor.Kind == rootpublication.ResourceDictionary && descriptor.Generation == dictID {
						found = true
					}
				}
				if !found || provider.captureCalls.Load() == 0 {
					t.Fatalf("new dictionary authority missing: found=%v captures=%d", found, provider.captureCalls.Load())
				}
				if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("new")) {
					t.Fatalf("dictionary rewrite owned read=%q %v", got, err)
				}
				assertCandidateTrackerMatchesFullScan(t, db)
				closeNoErr(t, db)
				if provider.releaseCalls.Load() != provider.captureCalls.Load() {
					t.Fatalf("closed producer captures=%d releases=%d", provider.captureCalls.Load(), provider.releaseCalls.Load())
				}
			})
		}
	}
}

func TestRewriteExactClosureTemplateProducerAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			testRewriteExactClosureTemplateProducerAuthority(t, replay)
		})
	}
}

func testRewriteExactClosureTemplateProducerAuthority(t *testing.T, replay bool) {
	db, writer, old, fresh := setupExactRewritePairWithValueLogOptions(t, ValueLogOptions{})
	if replay {
		writer = installExactRewriteReplayLeafProducer(t, db)
	}
	// A COW page changes its page identity/checksum and pointer bytes. Use an
	// invariant inline payload anchor rather than the old page's mutable header.
	anchor := bytes.Repeat([]byte("stable-template-inline-payload|"), 4)
	seed := db.NewBatch()
	for i := 0; i < 8; i++ {
		if err := seed.Set([]byte(fmt.Sprintf("template/%02d", i)), anchor); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.WriteSync(); err != nil {
		t.Fatal(err)
	}
	closeNoErr(t, seed)
	snapshot := db.AcquireSnapshot()
	entry, err := snapshot.GetEntryAtRoot(snapshot.state.RootPageID, []byte("template/00"))
	if err != nil || entry.Flags&node.FlagPointer != 0 || !bytes.Equal(entry.Value, anchor) {
		_ = snapshot.Close()
		t.Fatalf("template fixture anchor is not inline: flags=%d value=%q err=%v", entry.Flags, entry.Value, err)
	}
	closeNoErr(t, snapshot)
	cfg := templ.Config{MinSavingsBytes: 1, FingerprintK: 8, FingerprintW: 8,
		MaxFingerprints: 32, MaxFPReads: 32, MaxCandidatesPerFP: 8, MaxTemplateFetch: 8}
	// The default definition contract allows anchors of 16..64 bytes. Keep the
	// inline payload intact and use a stable substring inside those bounds.
	templateAnchor := anchor[:48]
	definition, err := templ.EncodeTemplateDef(templ.TemplateDef{Kind: templ.TemplateAnchors, Anchors: [][]byte{templateAnchor}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	decodedDef, err := templ.DecodeTemplateDef(definition)
	if err != nil || decodedDef.Kind != templ.TemplateAnchors || len(decodedDef.Anchors) != 1 || !bytes.Equal(decodedDef.Anchors[0], templateAnchor) {
		t.Fatalf("template definition roundtrip failed: %+v %v", decodedDef, err)
	}
	provider := newTestStableTemplateProvider(t, definition)
	// Validate the real persisted outer-leaf payload before configuring its
	// producer. Neither this byte-level check nor Encode grants stable authority.
	engine := templ.NewEngine(cfg)
	defer engine.Close()
	snapshot = db.AcquireSnapshot()
	matched := false
	err = leafrefscan.WalkRoots(context.Background(), []uint64{snapshot.state.RootPageID}, snapshot.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
		leafPage, err := snapshot.state.ValueLogSet.Read(ptr.ValuePtr())
		if err != nil {
			return err
		}
		compact, _, err := valuelog.MaybeCompactLeafLogPayload(leafPage)
		if err != nil || !bytes.Contains(compact, templateAnchor) {
			return err
		}
		payload, kept := engine.Encode(context.Background(), compact, provider)
		if !kept {
			return fmt.Errorf("persisted compact leaf cannot use fixture template: %v", engine.StatsSnapshot())
		}
		id, err := templ.EncodedPayloadTemplateID(payload)
		if err != nil || id != provider.templateID {
			return fmt.Errorf("fixture payload template ID=%d want %d: %v", id, provider.templateID, err)
		}
		decoded, err := templ.DecodePayload(payload, func(id uint64) ([]byte, error) {
			return provider.GetTemplateDef(context.Background(), id)
		}, templ.DecodeOptions{})
		if err != nil || !bytes.Equal(decoded, compact) {
			return fmt.Errorf("fixture compact payload roundtrip failed: %v", err)
		}
		matched = true
		return nil
	})
	closeNoErr(t, snapshot)
	if err != nil || !matched {
		t.Fatalf("template physical fixture prerequisite: matched=%v err=%v", matched, err)
	}
	writer.SetTemplateCompression(templ.TemplateOnly, cfg, provider)
	db.valueLogManager.SetTemplateLookup(func(id uint64) ([]byte, error) {
		return provider.GetTemplateDef(context.Background(), id)
	}, templ.DecodeOptions{})
	if err := db.applyRewriteSwapBatchSerialized([]rewriteSwap{{key: []byte("a"), oldPtr: old, newPtr: fresh}}, true); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, descriptor := range db.durableRoot.slotResources[db.durableRoot.slot].PhysicalDescriptors() {
		if descriptor.Kind == rootpublication.ResourceTemplate && descriptor.Generation == provider.templateID {
			found = true
		}
	}
	if !found || provider.captureCalls.Load() == 0 {
		t.Fatalf("actual COW template authority missing: found=%v captures=%d reasons=%v", found, provider.captureCalls.Load(), writer.templateClassReasonCounts(rewriteTemplateClassOuterLeaf))
	}
	if got, err := db.Get([]byte("a")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("template owned read=%q %v", got, err)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	closeNoErr(t, db)
	if provider.releaseCalls.Load() != provider.captureCalls.Load() {
		t.Fatalf("closed template captures=%d releases=%d", provider.captureCalls.Load(), provider.releaseCalls.Load())
	}
}

// Install the real command-WAL producer seam, including its RID owner and
// ordinary lane/record-length wrappers. The unchanged fixture remains readable
// through the previous generation; subsequent COW pages use this producer.
func installExactRewriteReplayLeafProducer(t *testing.T, db *DB) *rewriteWriter {
	t.Helper()
	if err := db.leafPageLog.Flush(); err != nil {
		t.Fatal(err)
	}
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		t.Fatal(err)
	}
	nextRID, err := nextReplayAppenderRIDStart(segments)
	if err != nil {
		t.Fatal(err)
	}
	appender, err := newReplayInlineAppenderWithNextRID(db, segments, nextRID)
	if err != nil {
		t.Fatal(err)
	}
	db.SetValueLogAppender(appender)
	db.SetLeafPageLog(replayInlineLeafPageLog{appender: appender})
	t.Cleanup(func() { _ = appender.close() })
	return appender.writer
}

func TestRewriteExactClosureReplayLeafProducerSharesRIDNamespace(t *testing.T) {
	db, _, _, _ := setupExactRewritePair(t)
	installExactRewriteReplayLeafProducer(t, db)
	appender := db.currentValueLogAppender().(*replayInlineAppender)
	startRID := appender.nextRID
	value, err := appender.append([]byte("ordinary-value-before-stable-leaves"))
	if err != nil {
		t.Fatal(err)
	}
	pages := [][]byte{buildRewriteLeafPageFixture(t, "replay-rid-a"), buildRewriteLeafPageFixture(t, "replay-rid-b")}
	stable := db.leafPageLog.(LeafPageStableBatchLog)
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if err := validateLeafPageStableResources(ptrs, resources); err != nil {
		t.Fatal(err)
	}
	ordinary, err := db.leafPageLog.AppendLeafPage(buildRewriteLeafPageFixture(t, "replay-rid-c"))
	if err != nil {
		t.Fatal(err)
	}
	if appender.nextRID != startRID+4 {
		t.Fatalf("shared RID frontier=%d want %d", appender.nextRID, startRID+4)
	}
	if err := appender.Flush(); err != nil {
		t.Fatal(err)
	}
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		t.Fatal(err)
	}
	byRID, err := scanValueLogSegments(segments, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []page.ValuePtr{value, ptrs[0].ValuePtr(), ptrs[1].ValuePtr(), ordinary.ValuePtr()}
	for i, ptr := range want {
		got, ok := byRID[startRID+uint64(i)]
		if !ok || got.FileID != ptr.FileID || got.Offset != ptr.Offset || page.ValuePtrIsGrouped(got) != page.ValuePtrIsGrouped(ptr) || page.ValuePtrSubIndex(got) != page.ValuePtrSubIndex(ptr) {
			t.Fatalf("RID %d resolved=%+v found=%v want %+v", startRID+uint64(i), got, ok, ptr)
		}
	}
}
