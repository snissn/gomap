//go:build !windows

package db

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
	diagnostics, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := diagnostics.CloseWithError(); err != nil {
			t.Error(err)
		}
	}()
	for _, descriptor := range diagnostics.Physical() {
		for _, field := range descriptor.ReachabilityFields() {
			if field == rootpublication.ReachabilityTemplateGeneration {
				t.Fatal("raw leaf page acquired template authority")
			}
		}
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
		diagnostics, err := resources.AcquirePhysicalDiagnostics()
		if err != nil {
			b.Fatal(err)
		}
		descriptors += uint64(len(diagnostics.Physical()))
		if err := diagnostics.CloseWithError(); err != nil {
			b.Fatal(err)
		}
		if err := resources.WalkLogicalObligations(func(_ rootpublication.StableResourcePhysicalDescriptor, _ rootpublication.StableLogicalObligation) error {
			obligations++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		statsDiagnostics, err := resources.AcquireStats(time.Now())
		if err != nil {
			b.Fatal(err)
		}
		for _, stats := range statsDiagnostics.Stats() {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		if err := statsDiagnostics.CloseWithError(); err != nil {
			b.Fatal(err)
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
	diagnostics, err := resources.AcquirePhysicalDiagnostics()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := diagnostics.CloseWithError(); err != nil {
			t.Error(err)
		}
	}()
	descriptors := diagnostics.Physical()
	identities := make([]rootpublication.StableIdentity, 0, len(descriptors))
	namespaceTokens := 0
	if len(descriptors) != len(referenced) {
		t.Fatalf("descriptors=%d referenced=%d", len(descriptors), len(referenced))
	}
	for _, descriptor := range descriptors {
		if descriptor.Kind != rootpublication.ResourceOuterLeafLog {
			t.Fatalf("kind=%q", descriptor.Kind)
		}
		if _, ok := referenced[descriptor.Generation]; !ok {
			t.Fatalf("captured unrelated generation=%d", descriptor.Generation)
		}
		delete(referenced, descriptor.Generation)
		identities = append(identities, descriptor.Identity())
		if _, ok := descriptor.Namespace(); ok {
			namespaceTokens++
		}
	}
	if len(referenced) != 0 {
		t.Fatalf("missing referenced generations: %v", referenced)
	}
	descriptorCount := len(descriptors)
	if namespaceTokens != descriptorCount {
		t.Fatalf("namespace tokens=%d descriptors=%d", namespaceTokens, descriptorCount)
	}
	if err := diagnostics.CloseWithError(); err != nil {
		t.Fatal(err)
	}
	descriptors = nil
	// The producer resources alone must retain the physical deletion pins.
	for _, identity := range identities {
		if _, err := db.valueLogIdentityPins.BeginDelete(identity); !errors.Is(err, rootpublication.ErrResourcePinned) {
			t.Fatalf("delete captured identity %v error=%v want ErrResourcePinned", identity, err)
		}
	}
	statsDiagnostics, err := resources.AcquireStats(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := statsDiagnostics.CloseWithError(); err != nil {
			t.Error(err)
		}
	}()
	stats := statsDiagnostics.Stats()
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

func TestSelectedStandaloneLeafCaptureAdmissionAndLegacyImport(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexPrimaryDirectory: true, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	pageBytes := buildRewriteLeafPageFixture(t, "selected-legacy-leaf")
	database.SetLeafPageLog(writer)
	t.Cleanup(func() { _ = database.Close(); _ = writer.Close() })
	owner := database.StableResourceMetadataOwner()
	baseline := owner.Bytes()
	capture, err := newRewriteStableOuterLeafCapture(writer)
	if err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() <= baseline {
		capture.abandon()
		t.Fatal("selected capture wrapper/builder escaped producer admission")
	}
	capture.abandon()
	if owner.Bytes() != baseline {
		t.Fatalf("capture unwind bytes=%d baseline=%d", owner.Bytes(), baseline)
	}
	_, resources, err := writer.AppendLeafPageWithStableResources(pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if resources.MetadataOwner() != owner {
		resources.Release()
		t.Fatal("selected rewrite returned unadmitted generic resources")
	}
	var outer bool
	err = resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
		for _, token := range tokens {
			if token.Kind() == rootpublication.ResourceOuterLeafLog {
				outer = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !outer {
		t.Fatalf("outer=%v want true", outer)
	}
	resources.Release()
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != baseline {
		t.Fatalf("selected cleanup bytes=%d baseline=%d", owner.Bytes(), baseline)
	}
	var refused retainedalloc.Owner
	if err = refused.Close(); err != nil {
		t.Fatal(err)
	}
	writer.stableResourceMetadata = &refused
	if capture, err = newRewriteStableOuterLeafCapture(writer); err == nil {
		capture.abandon()
		t.Fatal("closed metadata admitted new capture")
	}
}
