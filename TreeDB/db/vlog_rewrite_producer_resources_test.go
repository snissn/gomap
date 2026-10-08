//go:build !windows

package db

import (
	"bytes"
	"context"
	"crypto/sha256"
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

// Exercise the ordinary public COW publication, rather than only calling the
// stable append API directly. Byte lookup alone cannot authorize publication.
func TestOrdinaryExactClosureDictionarySwitchProducerAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) { testRewriteExactClosureDictionarySwitchProducerAuthority(t, replay, true) })
	}
}

func TestRewriteExactClosureDictionarySwitchProducerAuthority(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			testRewriteExactClosureDictionarySwitchProducerAuthority(t, replay, false)
		})
	}
}

func testRewriteExactClosureDictionarySwitchProducerAuthority(t *testing.T, replay, ordinary bool) {
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
					err := applyExactClosureFixtureReplacement(db, old, fresh, ordinary)
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
				err = applyExactClosureFixtureReplacement(db, old, fresh, ordinary)
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
	testExactClosureTemplateProducerAuthority(t, false)
}
func TestOrdinaryExactClosureTemplateProducerAuthority(t *testing.T) {
	testExactClosureTemplateProducerAuthority(t, true)
}
func testExactClosureTemplateProducerAuthority(t *testing.T, ordinary bool) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			testRewriteExactClosureTemplateProducerAuthority(t, replay, ordinary)
		})
	}
}

func testRewriteExactClosureTemplateProducerAuthority(t *testing.T, replay, ordinary bool) {
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
	if err := applyExactClosureFixtureReplacement(db, old, fresh, ordinary); err != nil {
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

// The installed production adapters must share immutable dictionary authority
// across every stable append belonging to one private COW Apply attempt.
func TestApplyLeafDictionaryCaptureIsAttemptScoped(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%v", replay), func(t *testing.T) {
			database, writer, _, _ := setupExactRewritePair(t)
			if replay {
				writer = installExactRewriteReplayLeafProducer(t, database)
			}
			pages := [][]byte{buildRewriteLeafPageFixture(t, "scope-a"), buildRewriteLeafPageFixture(t, "scope-b"), buildRewriteLeafPageFixture(t, "scope-c")}
			compact := make([][]byte, len(pages))
			for i := range pages {
				var err error
				compact[i], _, err = valuelog.MaybeCompactLeafLogPayload(pages[i])
				if err != nil {
					t.Fatal(err)
				}
			}
			const dictID = uint64(7408)
			dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{ID: uint32(dictID), Contents: compact, History: append([]byte(nil), compact[0]...), Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest})
			if err != nil {
				t.Fatal(err)
			}
			provider := newTestStableDictionaryProvider(t, dictID, dictionary)
			database.SetStableDictionaryResourceProvider(provider)
			writer.blockCompression = true
			writer.SetLeafDictMode(dictID, dictionary, false)
			capture, err := newApplyLeafResourceLog(database.leafPageLog)
			if err != nil {
				t.Fatal(err)
			}
			defer capture.abandon()
			for i, data := range pages {
				var log LeafPageLog = capture
				if !replay && i == 1 {
					var ok bool
					log, ok = capture.LeafPageLogLane(1)
					if !ok {
						t.Fatal("installed rewrite producer did not expose cloned lane")
					}
				}
				if _, err := log.AppendLeafPage(data); err != nil {
					t.Fatal(err)
				}
			}
			if got := provider.captureCalls.Load(); got != 1 {
				t.Fatalf("dictionary captures=%d want one per Apply attempt", got)
			}
			resources, err := capture.freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer resources.Release()
			if provider.releaseCalls.Load() != 0 {
				t.Fatal("provider snapshot lease released before frozen output")
			}
			if err := ValidateStableDictionaryResourceClosure(resources, dictID, dictionary); err != nil {
				t.Fatalf("frozen dictionary authority: %v", err)
			}
			next, err := newApplyLeafResourceLog(database.leafPageLog)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := next.AppendLeafPage(pages[0]); err != nil {
				t.Fatal(err)
			}
			next.abandon()
			if provider.captureCalls.Load() != 2 || provider.releaseCalls.Load() != 1 {
				t.Fatalf("retry ownership captures=%d releases=%d", provider.captureCalls.Load(), provider.releaseCalls.Load())
			}
			if err := provider.file.Close(); err != nil {
				t.Fatal(err)
			}
			for _, token := range resources.Tokens() {
				if token.Kind() != rootpublication.ResourceDictionary {
					continue
				}
				got := make([]byte, len(dictionary))
				if _, err := token.ReadAt(got, 0); err != nil || !bytes.Equal(got, dictionary) {
					t.Fatalf("frozen exact dictionary pin lost after scope/provider release: %v", err)
				}
			}
			resources.Release()
			if provider.releaseCalls.Load() != 2 {
				t.Fatal("frozen output leaked provider snapshot lease")
			}
		})
	}
}

type nonComparableDictionaryProvider struct {
	provider *testStableDictionaryProvider
	unused   []byte
}

func (p nonComparableDictionaryProvider) CaptureDictionaryResources(ctx context.Context, id uint64) (*rootpublication.StableResourceSet, error) {
	return p.provider.CaptureDictionaryResources(ctx, id)
}

func TestApplyLeafDictionaryCaptureAuthorityBoundaries(t *testing.T) {
	const id = uint64(7410)
	dictionary := []byte("immutable writer dictionary definition")
	writer := &rewriteWriter{}
	writer.SetLeafDictMode(id, dictionary, false)
	definition := append([]byte(nil), writer.leafDict...)
	dictionary[0] ^= 1
	if !bytes.Equal(writer.leafDict, definition) {
		t.Fatal("writer retained mutable caller dictionary bytes")
	}
	provider := newTestStableDictionaryProvider(t, id, definition)
	var scope applyLeafDictionaryCapture
	defer scope.release()
	capture := func(provider StableDictionaryResourceProvider) {
		t.Helper()
		resources, err := scope.capture(context.Background(), writer, provider, id, writer.leafDict)
		if err != nil {
			t.Fatal(err)
		}
		resources.Release()
	}
	capture(provider)
	capture(provider)
	if provider.captureCalls.Load() != 1 {
		t.Fatal("unchanged immutable definition was recaptured")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if resources, err := scope.capture(ctx, writer, provider, id, writer.leafDict); resources != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reuse returned resources=%v err=%v", resources != nil, err)
	}
	other := newTestStableDictionaryProvider(t, id, definition)
	capture(other)
	if other.captureCalls.Load() != 1 {
		t.Fatal("provider replacement reused another provider's authority")
	}
	writer.SetLeafDictMode(id, definition, false)
	capture(provider)
	if provider.captureCalls.Load() != 2 {
		t.Fatal("same-ID reconfiguration reused an earlier definition")
	}
	writer.SetLeafDictMode(id, []byte("mismatched same-ID definition"), false)
	for i := 0; i < 2; i++ {
		resources, err := scope.capture(context.Background(), writer, provider, id, writer.leafDict)
		if resources != nil || !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("changed definition reused authority: resources=%v err=%v", resources != nil, err)
		}
	}
	if provider.captureCalls.Load() != 4 {
		t.Fatal("failed validation was retained as reusable authority")
	}
	writer.SetLeafDictMode(id, definition, false)
	generic := nonComparableDictionaryProvider{provider: provider}
	capture(generic)
	capture(generic)
	if provider.captureCalls.Load() != 6 {
		t.Fatal("unidentifiable provider skipped full validation")
	}
	scope.release()
	if provider.releaseCalls.Load() != provider.captureCalls.Load() || other.releaseCalls.Load() != other.captureCalls.Load() {
		t.Fatal("attempt abandon leaked captured authority")
	}
}

type snapshotDictionaryProvider struct {
	*testStableDictionaryProvider
	database *DB
}

func (p *snapshotDictionaryProvider) CaptureDictionaryResources(ctx context.Context, id uint64) (*rootpublication.StableResourceSet, error) {
	resources, err := p.testStableDictionaryProvider.CaptureDictionaryResources(ctx, id)
	if err != nil {
		return nil, err
	}
	snapshot := p.database.AcquireStableSnapshot()
	if snapshot == nil {
		resources.Release()
		return nil, ErrClosed
	}
	token, err := snapshot.NewStableIndexGenerationResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceDictionary, LogicalLane: "test/dictionary-index", ResourceID: "index",
		Digest: sha256.Sum256([]byte("test-dictionary-index")), ContentSynced: true,
		Reachability: rootpublication.ReachabilityDictionaryGeneration,
		LogicalObligations: []rootpublication.StableLogicalObligation{{
			Class: "dictionary-generation", Kind: "dictionary", Namespace: "test", Generation: id, FileID: id,
			Length: int64(len(p.dictionary)), Reachability: rootpublication.ReachabilityDictionaryGeneration,
			Digest: sha256.Sum256(p.dictionary),
		}},
	}, rootpublication.NewStableResourceToken)
	if err != nil {
		resources.Release()
		_ = snapshot.Close()
		return nil, err
	}
	builder := rootpublication.NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := builder.Merge(resources); err != nil {
		resources.Release()
		token.Release()
		return nil, err
	}
	if err := builder.Add(token); err != nil {
		token.Release()
		return nil, err
	}
	return builder.Freeze()
}

func TestApplyLeafDictionaryCaptureRetainsSnapshotFence(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	const id = uint64(7412)
	dictionary := []byte("dictionary with real stable index capture lease")
	provider := &snapshotDictionaryProvider{testStableDictionaryProvider: newTestStableDictionaryProvider(t, id, dictionary), database: database}
	writer := &rewriteWriter{}
	writer.SetLeafDictMode(id, dictionary, false)
	for _, abandon := range []bool{false, true} {
		capture, err := newApplyLeafResourceLog(&stableContractTestLeafLog{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			resources, err := capture.capture.dictionaries.capture(context.Background(), writer, provider, id, writer.leafDict)
			if err != nil || resources != nil {
				capture.abandon()
				t.Fatalf("private authority capture resources=%v err=%v", resources != nil, err)
			}
		}
		if database.stableIndexCaptures.Load() != 1 {
			capture.abandon()
			t.Fatal("cached authority lost original stable snapshot fence")
		}
		if abandon {
			capture.abandon()
		} else {
			resources, err := capture.freeze()
			if err != nil {
				t.Fatal(err)
			}
			if database.stableIndexCaptures.Load() != 1 {
				resources.Release()
				t.Fatal("freeze released provider fence before output release")
			}
			if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
				resources.Release()
				t.Fatalf("vacuum crossed live dictionary fence: %v", err)
			}
			resources.Release()
		}
		if database.stableIndexCaptures.Load() != 0 || provider.releaseCalls.Load() != provider.captureCalls.Load() {
			t.Fatal("completed attempt leaked original snapshot authority")
		}
	}
}
