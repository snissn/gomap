//go:build !windows

package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestStandaloneStableLeafRewriteIgnoresTemplateMagicInRawPage(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.leafDictUseRawPages = true
	database.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = database.Close()
		_ = writer.Close()
	})

	leafPage := buildRewriteLeafPageFixture(t, "raw-template-magic")
	// A valid little-endian page ID can begin with the template envelope's
	// magic/version/flag bytes. Raw pages must never be probed as encoded
	// template payloads merely because their first four bytes collide.
	binary.LittleEndian.PutUint64(leafPage[:8], 0x01014d54)
	if !templ.IsEncodedPayload(leafPage) {
		t.Fatal("raw-page regression fixture does not collide with template magic")
	}

	stable := database.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(leafPage)
	if err != nil {
		t.Fatalf("stable raw leaf append: %v", err)
	}
	if resources == nil {
		t.Fatal("stable raw leaf append returned nil resources")
	}
	defer resources.Release()
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			if field == rootpublication.ReachabilityTemplateGeneration {
				t.Fatal("raw leaf page acquired template authority")
			}
		}
	}
}

func stableLeafTemplateFixture(t *testing.T, pageBytes []byte) (templ.Config, *testStableTemplateProvider) {
	t.Helper()
	compact, _, err := valuelog.MaybeCompactLeafLogPayload(pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(compact) < 64 {
		t.Fatalf("compact leaf fixture too small: %d", len(compact))
	}
	definition, err := templ.EncodeTemplateDef(templ.TemplateDef{
		Kind: templ.TemplateAnchors,
		Anchors: [][]byte{
			append([]byte(nil), compact[:24]...),
			append([]byte(nil), compact[len(compact)-24:]...),
		},
	}, templ.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return templ.Config{
		MinSavingsBytes: 1, FingerprintK: 8, FingerprintW: 8,
		MaxFingerprints: 32, MaxFPReads: 32, MaxCandidatesPerFP: 8, MaxTemplateFetch: 8,
	}, newTestStableTemplateProvider(t, definition)
}

func TestStandaloneStableLeafRewriteMergesTemplateClosure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := buildRewriteLeafPageFixture(t, "stable-template")
	cfg, provider := stableLeafTemplateFixture(t, pageBytes)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.SetTemplateCompression(templ.TemplateOnly, cfg, provider)
	db.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = db.Close()
		_ = writer.Close()
	})

	stable := db.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(pageBytes)
	if err != nil {
		t.Fatalf("stable template rewrite: %v", err)
	}
	if resources == nil {
		t.Fatal("stable template rewrite returned nil resources")
	}
	defer resources.Release()
	var hasTemplate, hasOuterLeaf bool
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			switch field {
			case rootpublication.ReachabilityTemplateGeneration:
				hasTemplate = true
			case rootpublication.ReachabilityOuterLeafRawPointer:
				hasOuterLeaf = true
			}
		}
	}
	if !hasTemplate || !hasOuterLeaf || provider.captureCalls.Load() != 1 {
		t.Fatalf("stable rewrite closure template=%v outer-leaf=%v captureCalls=%d", hasTemplate, hasOuterLeaf, provider.captureCalls.Load())
	}
}

func TestStandaloneStableLeafRewriteBatchDeduplicatesTemplateClosure(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := buildRewriteLeafPageFixture(t, "stable-template-batch")
	cfg, provider := stableLeafTemplateFixture(t, pageBytes)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.SetTemplateCompression(templ.TemplateOnly, cfg, provider)
	database.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = database.Close()
		_ = writer.Close()
	})

	stable := database.leafPageLog.(LeafPageStableBatchLog)
	pages := [][]byte{pageBytes, append([]byte(nil), pageBytes...), append([]byte(nil), pageBytes...)}
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable template batch rewrite: %v", err)
	}
	if resources == nil {
		t.Fatal("stable template batch rewrite returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) {
		t.Fatalf("stable template batch ptrs=%d want %d", len(ptrs), len(pages))
	}
	if got := provider.captureCalls.Load(); got != 1 {
		t.Fatalf("stable template batch capture calls=%d want 1", got)
	}
}

func TestStandaloneStableLeafRewriteBatchUnionsTemplateGenerations(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	seeds := []string{"template-alpha|", "template-beta|"}
	pages := make([][]byte, len(seeds))
	definitions := make([][]byte, len(seeds))
	for i, seed := range seeds {
		pages[i] = buildRewriteLeafPageFixture(t, seed)
		compact, _, err := valuelog.MaybeCompactLeafLogPayload(pages[i])
		if err != nil {
			t.Fatal(err)
		}
		anchor := []byte(seed + seed + seed)
		if !bytes.Contains(compact, anchor) {
			t.Fatalf("compact page %d does not contain its unique template anchor", i)
		}
		definitions[i], err = templ.EncodeTemplateDef(templ.TemplateDef{
			Kind:    templ.TemplateAnchors,
			Anchors: [][]byte{anchor},
		}, templ.Config{})
		if err != nil {
			t.Fatal(err)
		}
	}
	provider := newTestMultiStableTemplateProvider(t, definitions...)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.SetTemplateCompression(templ.TemplateOnly, templ.Config{
		MinSavingsBytes: 1, FingerprintK: 8, FingerprintW: 8,
		MaxFingerprints: 32, MaxFPReads: 32, MaxCandidatesPerFP: 8, MaxTemplateFetch: 8,
	}, provider)
	database.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = database.Close()
		_ = writer.Close()
	})

	stableBatchLog, ok := database.leafPageLog.(LeafPageStableBatchLog)
	if !ok {
		t.Fatalf("leaf-page log %T does not implement LeafPageStableBatchLog", database.leafPageLog)
	}
	ptrs, resources, err := stableBatchLog.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable multi-template batch rewrite: %v", err)
	}
	if resources == nil {
		t.Fatal("stable multi-template batch rewrite returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) || writer.templateKept != len(pages) {
		t.Fatalf("multi-template batch ptrs=%d keeps=%d want %d each", len(ptrs), writer.templateKept, len(pages))
	}
	if got := provider.captureCalls.Load(); got != int32(len(pages)) {
		t.Fatalf("multi-template capture calls=%d want %d", got, len(pages))
	}
	templateDescriptors := 0
	templateIDs := make(map[uint64]bool, len(pages))
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			if field != rootpublication.ReachabilityTemplateGeneration {
				continue
			}
			templateDescriptors++
			for _, obligation := range descriptor.LogicalObligations() {
				if obligation.Reachability == rootpublication.ReachabilityTemplateGeneration {
					templateIDs[obligation.Generation] = true
				}
			}
		}
	}
	if templateDescriptors != 1 {
		t.Fatalf("coalesced template descriptors=%d want 1", templateDescriptors)
	}
	for _, templateID := range provider.templateIDs {
		if !templateIDs[templateID] {
			t.Fatalf("coalesced template closure missing generation %d: %v", templateID, templateIDs)
		}
	}
}

func TestStandaloneStableLeafRewriteIgnoresRawTemplateShapedPayload(t *testing.T) {
	for _, batch := range []bool{false, true} {
		batchName := "single"
		if batch {
			batchName = "batch"
		}
		for _, mode := range []templ.Mode{templ.TemplateOff, templ.TemplateOnly} {
			modeName := "off"
			if mode == templ.TemplateOnly {
				modeName = "only-no-match"
			}
			t.Run(batchName+"/"+modeName, func(t *testing.T) {
				dir := t.TempDir()
				database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
				if err != nil {
					t.Fatal(err)
				}
				definition, err := templ.EncodeTemplateDef(templ.TemplateDef{
					Kind: templ.TemplateAnchors,
					Anchors: [][]byte{
						[]byte("template-anchor-absent-a"),
						[]byte("template-anchor-absent-b"),
					},
				}, templ.Config{})
				if err != nil {
					t.Fatal(err)
				}
				provider := newTestStableTemplateProvider(t, definition)
				pageBytes := buildRewriteLeafPageFixture(t, "raw-template-shaped")
				binary.LittleEndian.PutUint64(pageBytes[:8], 0x01014d54)
				if !templ.IsEncodedPayload(pageBytes) {
					t.Fatal("raw page fixture does not have the intended template-shaped prefix")
				}
				if _, err := templ.EncodedPayloadTemplateID(pageBytes); !errors.Is(err, templ.ErrCorrupt) {
					t.Fatalf("raw page template-shaped decode error=%v want ErrCorrupt", err)
				}

				writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
				writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
				// Exercise the production raw-page payload branch without introducing
				// unrelated dictionary authority into this template-boundary test.
				writer.leafDictUseRawPages = true
				writer.SetTemplateCompression(mode, templ.Config{
					MinSavingsBytes: 1, FingerprintK: 8, FingerprintW: 8,
					MaxFingerprints: 32, MaxFPReads: 32, MaxCandidatesPerFP: 8, MaxTemplateFetch: 8,
				}, provider)
				database.SetLeafPageLog(writer)
				t.Cleanup(func() {
					_ = database.Close()
					_ = writer.Close()
				})

				var resources *rootpublication.StableResourceSet
				if batch {
					_, resources, err = database.leafPageLog.(LeafPageStableBatchLog).AppendLeafPagesWithStableResources([][]byte{
						pageBytes,
						append([]byte(nil), pageBytes...),
					})
				} else {
					_, resources, err = database.leafPageLog.(LeafPageStableLog).AppendLeafPageWithStableResources(pageBytes)
				}
				if err != nil {
					t.Fatalf("stable raw-page rewrite: %v", err)
				}
				if resources == nil {
					t.Fatal("stable raw-page rewrite returned nil resources")
				}
				defer resources.Release()
				if got := writer.templateKept; got != 0 {
					t.Fatalf("raw page template keeps=%d want 0", got)
				}
				if got := provider.captureCalls.Load(); got != 0 {
					t.Fatalf("raw page template capture calls=%d want 0", got)
				}
				for _, descriptor := range resources.Descriptors() {
					for _, field := range descriptor.ReachabilityFields() {
						if field == rootpublication.ReachabilityTemplateGeneration {
							t.Fatalf("raw unencoded page returned template authority: %+v", descriptor)
						}
					}
				}
			})
		}
	}
}

func TestStandaloneStableLeafRewriteRejectsTemplateAuthorityBeforeFileCreation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := buildRewriteLeafPageFixture(t, "stable-template-failure")
	cfg, provider := stableLeafTemplateFixture(t, pageBytes)
	injected := errors.New("injected template authority failure")
	provider.captureErr = injected
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.SetTemplateCompression(templ.TemplateOnly, cfg, provider)
	db.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = db.Close()
		_ = writer.Close()
	})
	before, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	stable := db.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(pageBytes)
	if resources != nil {
		resources.Release()
		t.Fatal("failed template authority returned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("stable template error=%v want injected failure", err)
	}
	after, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || writer.leafW != nil {
		t.Fatalf("template authority failure mutated leaf namespace: before=%d after=%d writer=%v", len(before), len(after), writer.leafW)
	}
}

func TestStandaloneStableLeafRewriteBatchPreflightsAllTemplateAuthorityBeforeFileCreation(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	alpha := buildRewriteLeafPageFixture(t, "preflight-template-alpha|")
	beta := buildRewriteLeafPageFixture(t, "preflight-template-beta|")
	pages := make([][]byte, rewriteLeafLogBatchMaxK+1)
	for i := 0; i < rewriteLeafLogBatchMaxK; i++ {
		pages[i] = append([]byte(nil), alpha...)
	}
	pages[len(pages)-1] = beta

	definitions := make([][]byte, 0, 2)
	for i, fixture := range [][]byte{alpha, beta} {
		compact, _, err := valuelog.MaybeCompactLeafLogPayload(fixture)
		if err != nil {
			t.Fatal(err)
		}
		seed := "preflight-template-alpha|"
		if i == 1 {
			seed = "preflight-template-beta|"
		}
		anchor := []byte(seed + seed)
		if !bytes.Contains(compact, anchor) {
			t.Fatalf("compact page %d does not contain its unique template anchor", i)
		}
		definition, err := templ.EncodeTemplateDef(templ.TemplateDef{
			Kind:    templ.TemplateAnchors,
			Anchors: [][]byte{anchor},
		}, templ.Config{})
		if err != nil {
			t.Fatal(err)
		}
		definitions = append(definitions, definition)
	}
	provider := newTestMultiStableTemplateProvider(t, definitions...)
	injected := errors.New("injected second-chunk template authority failure")
	betaID := templ.TemplateID(definitions[1], 0)
	provider.captureErrs = map[uint64]error{betaID: injected}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.SetTemplateCompression(templ.TemplateOnly, templ.Config{
		MinSavingsBytes: 1, FingerprintK: 8, FingerprintW: 8,
		MaxFingerprints: 32, MaxFPReads: 32, MaxCandidatesPerFP: 8, MaxTemplateFetch: 8,
	}, provider)
	database.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = database.Close()
		_ = writer.Close()
	})

	before, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	ptrs, resources, err := database.leafPageLog.(LeafPageStableBatchLog).AppendLeafPagesWithStableResources(pages)
	if resources != nil {
		resources.Release()
		t.Fatal("failed batch template preflight returned resources")
	}
	if ptrs != nil {
		t.Fatalf("failed batch template preflight returned %d pointers", len(ptrs))
	}
	if !errors.Is(err, injected) {
		t.Fatalf("stable batch preflight error=%v want injected failure", err)
	}
	after, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || writer.leafW != nil {
		t.Fatalf("late template authority failure mutated leaf namespace: before=%d after=%d writer=%v", len(before), len(after), writer.leafW)
	}
	if got := provider.captureCalls.Load(); got != 2 {
		t.Fatalf("template preflight capture calls=%d want alpha then failing beta", got)
	}
}

func TestStandaloneStableLeafRewriteMergesDictionaryAndTemplateClosure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := buildRewriteLeafPageFixture(t, "tda")
	pages := [][]byte{
		pageBytes,
		buildRewriteLeafPageFixture(t, "tdb"),
		buildRewriteLeafPageFixture(t, "tdc"),
	}
	compact := make([][]byte, len(pages))
	for i := range pages {
		compact[i], _, err = valuelog.MaybeCompactLeafLogPayload(pages[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	const dictID = uint64(7303)
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: uint32(dictID), Contents: compact, History: append([]byte(nil), compact[0]...),
		Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	dictionaryProvider := newTestStableDictionaryProvider(t, dictID, dictionary)
	db.SetStableDictionaryResourceProvider(dictionaryProvider)
	templateCfg, templateProvider := stableLeafTemplateFixture(t, pageBytes)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.blockCompression = true
	writer.SetLeafDictMode(dictID, dictionary, false)
	writer.SetTemplateCompression(templ.TemplatePrepass, templateCfg, templateProvider)
	db.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = db.Close()
		_ = writer.Close()
	})

	stable := db.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(pageBytes)
	if err != nil {
		t.Fatalf("stable dictionary+template rewrite: %v", err)
	}
	if resources == nil {
		t.Fatal("stable dictionary+template rewrite returned nil resources")
	}
	defer resources.Release()
	fields := make(map[rootpublication.ReachabilityField]bool)
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			fields[field] = true
		}
	}
	for _, field := range []rootpublication.ReachabilityField{
		rootpublication.ReachabilityDictionaryGeneration,
		rootpublication.ReachabilityTemplateGeneration,
		rootpublication.ReachabilityOuterLeafRawPointer,
	} {
		if !fields[field] {
			t.Fatalf("stable dictionary+template closure missing %s: %v", field, fields)
		}
	}
	if dictionaryProvider.captureCalls.Load() != 1 || templateProvider.captureCalls.Load() != 1 {
		t.Fatalf("capture calls dictionary=%d template=%d want 1 each", dictionaryProvider.captureCalls.Load(), templateProvider.captureCalls.Load())
	}
}

func TestStandaloneStableLeafRewriteCapturesOnlyEmittedDictionaryAuthority(t *testing.T) {
	for _, test := range []struct {
		name         string
		configure    func(*testing.T, *rewriteWriter)
		wantTemplate bool
	}{
		{
			name: "compression-off",
			configure: func(_ *testing.T, writer *rewriteWriter) {
				writer.blockCompression = false
			},
		},
		{
			name: "template-only",
			configure: func(t *testing.T, writer *rewriteWriter) {
				pageBytes := buildRewriteLeafPageFixture(t, "eda")
				cfg, provider := stableLeafTemplateFixture(t, pageBytes)
				writer.SetTemplateCompression(templ.TemplateOnly, cfg, provider)
			},
			wantTemplate: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
			if err != nil {
				t.Fatal(err)
			}
			const dictID = uint64(7304)
			dictionaryProvider := newTestStableDictionaryProvider(t, dictID, []byte("provider-dictionary-that-must-not-be-captured"))
			database.SetStableDictionaryResourceProvider(dictionaryProvider)
			writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
			writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
			writer.SetLeafDictMode(dictID, []byte("configured-dictionary-not-emitted"), false)
			test.configure(t, writer)
			database.SetLeafPageLog(writer)
			t.Cleanup(func() {
				_ = database.Close()
				_ = writer.Close()
			})

			pageBytes := buildRewriteLeafPageFixture(t, "eda")
			_, resources, err := database.leafPageLog.(LeafPageStableLog).AppendLeafPageWithStableResources(pageBytes)
			if err != nil {
				t.Fatalf("stable append: %v", err)
			}
			if resources == nil {
				t.Fatal("stable append returned nil resources")
			}
			defer resources.Release()
			if got := dictionaryProvider.captureCalls.Load(); got != 0 {
				t.Fatalf("dictionary capture calls=%d want 0 for emitted dictID 0", got)
			}
			var hasDictionary, hasTemplate bool
			for _, descriptor := range resources.Descriptors() {
				for _, field := range descriptor.ReachabilityFields() {
					hasDictionary = hasDictionary || field == rootpublication.ReachabilityDictionaryGeneration
					hasTemplate = hasTemplate || field == rootpublication.ReachabilityTemplateGeneration
				}
			}
			if hasDictionary || hasTemplate != test.wantTemplate {
				t.Fatalf("closure dictionary=%v template=%v want dictionary=false template=%v", hasDictionary, hasTemplate, test.wantTemplate)
			}
		})
	}
}

func TestStandaloneStableLeafRewriteBatchCapturesOnlyEmittedDictionaryAuthority(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	const dictID = uint64(7305)
	dictionaryProvider := newTestStableDictionaryProvider(t, dictID, []byte("provider-dictionary-that-must-not-be-captured"))
	database.SetStableDictionaryResourceProvider(dictionaryProvider)
	pageBytes := buildRewriteLeafPageFixture(t, "edb")
	cfg, templateProvider := stableLeafTemplateFixture(t, pageBytes)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.blockCompression = true
	writer.SetLeafDictMode(dictID, []byte("configured-dictionary-not-emitted"), false)
	writer.SetTemplateCompression(templ.TemplateOnly, cfg, templateProvider)
	database.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = database.Close()
		_ = writer.Close()
	})

	pages := make([][]byte, rewriteLeafLogBatchMaxK+1)
	for i := range pages {
		pages[i] = append([]byte(nil), pageBytes...)
	}
	ptrs, resources, err := database.leafPageLog.(LeafPageStableBatchLog).AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable wide batch append: %v", err)
	}
	if resources == nil {
		t.Fatal("stable wide batch append returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) {
		t.Fatalf("stable wide batch pointers=%d want %d", len(ptrs), len(pages))
	}
	if got := dictionaryProvider.captureCalls.Load(); got != 0 {
		t.Fatalf("dictionary capture calls=%d want 0 for emitted dictID 0", got)
	}
	var hasDictionary, hasTemplate bool
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			hasDictionary = hasDictionary || field == rootpublication.ReachabilityDictionaryGeneration
			hasTemplate = hasTemplate || field == rootpublication.ReachabilityTemplateGeneration
		}
	}
	if hasDictionary || !hasTemplate {
		t.Fatalf("closure dictionary=%v template=%v want dictionary=false template=true", hasDictionary, hasTemplate)
	}
}

func TestStandaloneStableLeafRewriteLateBindsAndMergesDictionaryClosure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := buildRewriteLeafPageFixture(t, "stable-a")
	pages := [][]byte{
		pageBytes,
		buildRewriteLeafPageFixture(t, "stable-b"),
		buildRewriteLeafPageFixture(t, "stable-c"),
	}
	compact := make([][]byte, len(pages))
	for i := range pages {
		compact[i], _, err = valuelog.MaybeCompactLeafLogPayload(pages[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	const dictID = uint64(7301)
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID: uint32(dictID), Contents: compact, History: append([]byte(nil), compact[0]...),
		Offsets: [3]int{1, 4, 8}, Level: zstd.SpeedFastest,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := newTestStableDictionaryProvider(t, dictID, dictionary)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.blockCompression = true
	writer.SetLeafDictMode(dictID, dictionary, false)
	db.SetLeafPageLog(writer)
	// Production installs the rewrite leaf log before caching.SetDictStore can
	// publish its dictionary authority. The stable append must consult the DB's
	// current provider rather than retaining the nil provider seen at install.
	db.SetStableDictionaryResourceProvider(provider)
	t.Cleanup(func() {
		_ = db.Close()
		_ = writer.Close()
	})

	stable := db.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(pageBytes)
	if err != nil {
		t.Fatalf("stable dictionary rewrite: %v", err)
	}
	if resources == nil {
		t.Fatal("stable dictionary rewrite returned nil resources")
	}
	defer resources.Release()
	var hasDictionary, hasOuterLeaf bool
	for _, descriptor := range resources.Descriptors() {
		for _, field := range descriptor.ReachabilityFields() {
			switch field {
			case rootpublication.ReachabilityDictionaryGeneration:
				hasDictionary = true
			case rootpublication.ReachabilityOuterLeafRawPointer:
				hasOuterLeaf = true
			}
		}
	}
	if !hasDictionary || !hasOuterLeaf || provider.captureCalls.Load() != 1 {
		t.Fatalf("stable rewrite closure dictionary=%v outer-leaf=%v captureCalls=%d", hasDictionary, hasOuterLeaf, provider.captureCalls.Load())
	}
}

func TestStandaloneStableLeafRewriteRejectsDictionaryAuthorityMismatchBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	const dictID = uint64(7302)
	provider := newTestStableDictionaryProvider(t, dictID, []byte("different-provider-dictionary"))
	db.SetStableDictionaryResourceProvider(provider)
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	writer.blockCompression = true
	writer.SetLeafDictMode(dictID, []byte("writer-selected-dictionary"), false)
	db.SetLeafPageLog(writer)
	t.Cleanup(func() {
		_ = db.Close()
		_ = writer.Close()
	})
	before, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	stable := db.leafPageLog.(LeafPageStableLog)
	_, resources, err := stable.AppendLeafPageWithStableResources(buildRewriteLeafPageFixture(t, "mismatch"))
	if resources != nil {
		resources.Release()
		t.Fatal("dictionary mismatch returned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("stable mismatch error=%v want resource conflict", err)
	}
	after, err := os.ReadDir(LeafLogDirPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || writer.leafW != nil {
		t.Fatalf("dictionary mismatch mutated leaf writer: before=%d after=%d writer=%v", len(before), len(after), writer.leafW)
	}
}

func standaloneStableLeafPages(count, size int) [][]byte {
	pages := make([][]byte, count)
	for i := range pages {
		page := make([]byte, size)
		for j := range page {
			page[j] = byte((i*37 + j*131 + j/7) % 251)
		}
		pages[i] = page
	}
	return pages
}

func BenchmarkStandaloneLeafPageStableBatchAuthority(b *testing.B) {
	dir := b.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		b.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		MaxSegmentBytes: 512,
		Compression:     ValueLogCompressionOff,
	})
	if err != nil {
		_ = database.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = database.Close()
		_ = log.Close()
	})
	database.SetLeafPageLog(log)
	writer, ok := log.(*rewriteWriter)
	if !ok {
		b.Fatalf("standalone leaf log type=%T want *rewriteWriter", log)
	}
	stable, ok := database.leafPageLog.(LeafPageStableBatchLog)
	if !ok {
		b.Fatalf("installed standalone leaf log %T erased stable batch capture", database.leafPageLog)
	}
	// One more than the grouped-frame cap forces two physical records; the tiny
	// segment target then makes the authority path observe a real rotation.
	pages := standaloneStableLeafPages(valuelog.MaxFrameK+1, page.PageSize)

	b.ReportAllocs()
	b.SetBytes(int64(len(pages) * page.PageSize))
	b.ResetTimer()
	var descriptors, obligations, contentSyncs, namespaceSyncs uint64
	var pinHighWater uint64
	for i := 0; i < b.N; i++ {
		_, resources, err := stable.AppendLeafPagesWithStableResources(pages)
		if err != nil {
			b.Fatal(err)
		}
		descriptors += uint64(len(resources.Descriptors()))
		for _, descriptor := range resources.Descriptors() {
			obligations += uint64(len(descriptor.LogicalObligations()))
		}
		for _, stats := range resources.Stats(time.Now()) {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		resources.Release()
	}
	b.StopTimer()
	if got := database.valueLogIdentityPins.ActivePins(); got != 0 {
		b.Fatalf("active raw outer-leaf pins after release=%d want 0", got)
	}
	physical := writer.leafW.DurabilityStats()
	b.ReportMetric(float64(descriptors)/float64(b.N), "descriptors/op")
	b.ReportMetric(float64(obligations)/float64(b.N), "logical_obligations/op")
	b.ReportMetric(float64(pinHighWater), "pin_high_water")
	b.ReportMetric(float64(contentSyncs)/float64(b.N), "token_sync_attempts/op")
	b.ReportMetric(float64(physical.FileSyncCalls)/float64(b.N), "file_syncs/op")
	b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "namespace_token_syncs/op")
	b.ReportMetric(float64(physical.DirectorySyncCalls)/float64(b.N), "directory_syncs/op")
}

func TestStandaloneLeafPageLogStableBatchCapturesExactRotatedSegments(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		MaxSegmentBytes: 512,
		Compression:     ValueLogCompressionOff,
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = log.Close()
	})
	db.SetLeafPageLog(log)

	stable, ok := db.leafPageLog.(LeafPageStableBatchLog)
	if !ok {
		t.Fatalf("installed standalone leaf log %T erased stable batch capture", db.leafPageLog)
	}
	pages := standaloneStableLeafPages(valuelog.MaxFrameK*2+8, page.PageSize)
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable batch: %v", err)
	}
	if resources == nil {
		t.Fatal("stable batch returned nil resource authority")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) {
		t.Fatalf("ptrs=%d want %d", len(ptrs), len(pages))
	}

	referenced := make(map[uint64]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		referenced[uint64(ptr.ValueLogFileID())] = struct{}{}
	}
	if len(referenced) < 2 {
		t.Fatalf("referenced segments=%d want rotation", len(referenced))
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != len(referenced) {
		t.Fatalf("descriptors=%d referenced=%d", len(descriptors), len(referenced))
	}
	for _, descriptor := range descriptors {
		if descriptor.Kind() != rootpublication.ResourceOuterLeafLog {
			t.Fatalf("kind=%q", descriptor.Kind())
		}
		if _, ok := referenced[descriptor.Generation()]; !ok {
			t.Fatalf("captured unrelated generation=%d", descriptor.Generation())
		}
		delete(referenced, descriptor.Generation())
		if _, err := db.valueLogIdentityPins.BeginDelete(descriptor.Identity()); !errors.Is(err, rootpublication.ErrResourcePinned) {
			t.Fatalf("delete captured generation %d error=%v want ErrResourcePinned", descriptor.Generation(), err)
		}
	}
	if len(referenced) != 0 {
		t.Fatalf("missing referenced generations: %v", referenced)
	}
	namespaceTokens := 0
	for _, token := range resources.Tokens() {
		if token.Namespace() != nil {
			namespaceTokens++
		}
	}
	if namespaceTokens != len(descriptors) {
		t.Fatalf("namespace tokens=%d descriptors=%d", namespaceTokens, len(descriptors))
	}
	stats := resources.Stats(time.Now())
	if len(stats) != 1 || stats[0].NamespaceSyncs != uint64(namespaceTokens) {
		t.Fatalf("resource stats=%+v", stats)
	}
}

func TestStandaloneLeafPageLogStableCaptureSurvivesLaneClone(t *testing.T) {
	db, log := openLeafPageLogLaneTestDB(t)
	t.Cleanup(func() {
		_ = db.Close()
		_ = log.Close()
	})
	lane, ok := db.leafPageLogLaneForWorkerIndex(1)
	if !ok {
		t.Fatal("nonzero standalone leaf lane unavailable")
	}
	stable, ok := lane.(LeafPageStableLog)
	if !ok {
		t.Fatalf("cloned lane %T erased stable capture", lane)
	}
	_, resources, err := stable.AppendLeafPageWithStableResources([]byte("stable cloned lane"))
	if err != nil {
		t.Fatalf("stable cloned lane append: %v", err)
	}
	if resources == nil || resources.Len() != 1 {
		t.Fatalf("cloned lane resources=%v", resources)
	}
	resources.Release()
}

func TestStandaloneLeafPageLogLateRegistryBindFailsStableOnly(t *testing.T) {
	dir := t.TempDir()
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.AppendLeafPage([]byte("opened before DB installation")); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = log.Close()
	})
	db.SetLeafPageLog(log)
	stable := db.leafPageLog.(LeafPageStableLog)
	if _, resources, err := stable.AppendLeafPageWithStableResources([]byte("must fail closed")); !errors.Is(err, rootpublication.ErrUnresolvedResource) || resources != nil {
		t.Fatalf("late-bind stable append resources=%v err=%v", resources, err)
	}
	if _, err := db.leafPageLog.AppendLeafPage([]byte("ordinary compatibility remains")); err != nil {
		t.Fatalf("ordinary append after late bind: %v", err)
	}
}
