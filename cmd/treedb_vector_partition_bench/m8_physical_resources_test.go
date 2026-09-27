package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func testM8PhysicalReceiptV1(t *testing.T) (m8PhysicalResourceReceiptV1, m8ProductionReportV1, collections.VectorPartitionManifestV1) {
	t.Helper()
	manifest := collections.VectorPartitionManifestV1{Generation: 7, IntegrityDigest: strings.Repeat("a", 64), PartitionCount: 2, DomainCount: 1, DomainPacks: []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}}}
	header := m8PhysicalResourceHeaderV1{Kind: "physical_resources", Contract: m8PhysicalResourceContractV1, Scope: m8PhysicalResourceScopeV1, ParentExecutionID: "parent", ParentHeadSHA: strings.Repeat("b", 40), ParentExecutableSHA256: strings.Repeat("c", 64), ManifestIntegrityDigest: manifest.IntegrityDigest, Generation: 7, ServingPartitions: []uint32{0, 1}, TopK: 10, EfSearch: []int{32, 96}}
	parent := m8ProductionReportV1{ExecutionID: header.ParentExecutionID, HeadSHA: header.ParentHeadSHA, ExecutableSHA256: header.ParentExecutableSHA256, Config: m8ProductionConfigEvidenceV1{Partitions: 2, DomainCount: 1, TopK: 10, EfSearch: []int{32, 96}}}
	r := m8PhysicalResourceReceiptV1{Header: header, Complete: true}
	for _, p := range header.ServingPartitions {
		r.Packs = append(r.Packs, m8PhysicalResourcePackV1{Live: collections.VectorPartitionPhysicalResourcesV1{Generation: 7, PartitionID: p, PackBytes: 100, MappedExtentBytes: 128, MetadataBytesBound: 80, StableIDBytesBound: 16, LogicalHandles: 3, RequiredChunks: 2, OpenedChunks: 2, ValidatedChunks: 2}, Scratch: []m8PhysicalScratchV1{{10, 32, 200}, {10, 96, 300}}, AfterClose: collections.VectorPartitionPhysicalReleaseV1{Acquires: 3, Releases: 3}})
	}
	var err error
	r.Totals, err = m8PhysicalTotalsV1(r.Packs, header)
	if err != nil {
		t.Fatal(err)
	}
	return r, parent, manifest
}

func TestM8PhysicalResourceReaderV1(t *testing.T) {
	valid, parent, manifest := testM8PhysicalReceiptV1(t)
	raw, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(raw), valid.Header, parent, manifest); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*m8PhysicalResourceReceiptV1){
		"missing pack":       func(r *m8PhysicalResourceReceiptV1) { r.Packs = r.Packs[:1] },
		"duplicate pack":     func(r *m8PhysicalResourceReceiptV1) { r.Packs[1] = r.Packs[0] },
		"wrong generation":   func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Live.Generation++ },
		"layout":             func(r *m8PhysicalResourceReceiptV1) { r.Header.DomainGraphs = true },
		"parent":             func(r *m8PhysicalResourceReceiptV1) { r.Header.ParentExecutionID = "other" },
		"collector":          func(r *m8PhysicalResourceReceiptV1) { r.Header.HeadSHA = "other" },
		"unopened chunk":     func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Live.OpenedChunks-- },
		"unvalidated chunk":  func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Live.ValidatedChunks-- },
		"root handle":        func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Live.LogicalHandles-- },
		"release error":      func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].AfterClose.Errors = 1 },
		"live handle":        func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].AfterClose.LogicalHandles = 1 },
		"live mapping":       func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].AfterClose.MappedExtentBytes = 1 },
		"live fallback":      func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].AfterClose.HeapCopyBytes = 1 },
		"release imbalance":  func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].AfterClose.Releases-- },
		"scratch omission":   func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Scratch = r.Packs[0].Scratch[:1] },
		"scratch coordinate": func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Scratch[1].EfSearch = 32 },
		"zero scratch":       func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Scratch[0].BytesBound = 0 },
		"mapped totals":      func(r *m8PhysicalResourceReceiptV1) { r.Totals.MappedExtentBytes++ },
		"fallback overflow": func(r *m8PhysicalResourceReceiptV1) {
			r.Packs[0].Live.HeapCopyBytes = math.MaxUint64
			r.Packs[1].Live.HeapCopyBytes = 1
		},
		"totals":     func(r *m8PhysicalResourceReceiptV1) { r.Totals.HeapCopyBytes++ },
		"overflow":   func(r *m8PhysicalResourceReceiptV1) { r.Packs[0].Live.PackBytes = math.MaxUint64 },
		"incomplete": func(r *m8PhysicalResourceReceiptV1) { r.Complete = false },
	} {
		t.Run(name, func(t *testing.T) {
			var r m8PhysicalResourceReceiptV1
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			mutate(&r)
			b, _ := json.Marshal(r)
			if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(b), valid.Header, parent, manifest); err == nil {
				t.Fatal("accepted hostile receipt")
			}
		})
	}
	for name, data := range map[string][]byte{"truncated": raw[:len(raw)-1], "trailing": append(append([]byte(nil), raw...), []byte(" {}")...), "unknown": bytes.Replace(raw, []byte(`"complete":true`), []byte(`"complete":true,"foreign":1`), 1)} {
		t.Run(name, func(t *testing.T) {
			if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(data), valid.Header, parent, manifest); err == nil {
				t.Fatal("accepted malformed receipt")
			}
		})
	}
}

func TestM8PhysicalResourceRetainedHarnessV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	f := m8QualificationFixturesV1[0]
	f.Vectors, f.Dimensions, f.Queries = 256, 8, 8
	vectors, _ := fixtureData(f)
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer assets.Close()
	for _, canceled := range []bool{false, true} {
		h, err := newM8AttributionHarnessV1(assets)
		if err != nil {
			t.Fatal(err)
		}
		header := m8PhysicalResourceHeaderV1{Generation: assets.manifest.Generation, DomainGraphs: h.domainGraphs, ServingPartitions: append([]uint32(nil), h.servingPartitions...), TopK: 10, EfSearch: []int{32, 96}}
		var observers []func() collections.VectorPartitionPhysicalReleaseV1
		for _, p := range h.servingPartitions {
			_, observe, err := h.searchers[p].InspectPhysicalResourcesV1(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			observers = append(observers, observe)
		}
		ctx, cancel := context.WithCancel(t.Context())
		if canceled {
			cancel()
		}
		receipt, err := m8ObservePhysicalResourcesV1(ctx, h, header)
		cancel()
		if canceled {
			if !errors.Is(err, context.Canceled) || receipt.Complete {
				t.Fatalf("cancellation=%v complete=%v", err, receipt.Complete)
			}
		} else {
			if err != nil || !receipt.Complete || len(receipt.Packs) != len(header.ServingPartitions) {
				t.Fatalf("observation=%+v err=%v", receipt, err)
			}
			total, sumErr := m8PhysicalTotalsV1(receipt.Packs, header)
			if sumErr != nil || !reflect.DeepEqual(total, receipt.Totals) {
				t.Fatal("totals mismatch")
			}
		}
		for _, observe := range observers {
			released := observe()
			if released.LogicalHandles != 0 || released.MappedExtentBytes != 0 || released.HeapCopyBytes != 0 || released.Acquires != released.Releases {
				t.Fatalf("leaked owned resources %+v", released)
			}
		}
	}
}

func TestM8PhysicalResourceDomainAnchorReaderV1(t *testing.T) {
	r, parent, manifest := testM8PhysicalReceiptV1(t)
	manifest.Assets = []collections.VectorPartitionAssetV1{{PartitionID: 0, GraphVariant: string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1)}}
	r.Header.DomainGraphs = true
	r.Header.ServingPartitions = []uint32{0}
	r.Packs = r.Packs[:1]
	var err error
	r.Totals, err = m8PhysicalTotalsV1(r.Packs, r.Header)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(r)
	if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(raw), r.Header, parent, manifest); err != nil {
		t.Fatal(err)
	}
	// Internally coherent but wrong non-anchor coverage must still be rejected
	// against the independently authenticated manifest, even if a caller's
	// frozen expected header accidentally includes the wrong physical pack.
	r.Header.ServingPartitions = []uint32{1}
	r.Packs[0].Live.PartitionID = 1
	raw, _ = json.Marshal(r)
	if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(raw), r.Header, parent, manifest); err == nil {
		t.Fatal("accepted non-anchor domain inventory")
	}
}
