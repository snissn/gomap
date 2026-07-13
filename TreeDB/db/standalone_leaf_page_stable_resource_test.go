//go:build !windows

package db

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

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
