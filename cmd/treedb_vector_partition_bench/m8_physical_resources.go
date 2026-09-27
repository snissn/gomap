package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/snissn/gomap/TreeDB/collections"
)

const m8PhysicalResourceContractV1 = "untimed_retained_pack_physical_resources_not_qualification_v1"
const m8PhysicalResourceScopeV1 = "new collector opens pinned assets through retained attribution searchers; mapped extents are not RSS; logical handles are not OS FDs; chunks are fully opened/validated, not query touches; metadata and per-search scratch are conservative bounds, not heap/peak measurements; post-close counters prove owned handle release, not Go reclamation; excludes router, native topology and historical measured-process resources"

type m8PhysicalResourceHeaderV1 struct {
	Kind                    string                     `json:"kind"`
	Contract                string                     `json:"contract"`
	Scope                   string                     `json:"scope"`
	ReplayReceipt           string                     `json:"replay_receipt"`
	HeadSHA                 string                     `json:"head_sha"`
	ExecutableSHA256        string                     `json:"executable_sha256"`
	SourceCheckout          string                     `json:"source_checkout"`
	Command                 []string                   `json:"command"`
	ParentExecutionID       string                     `json:"parent_execution_id"`
	ParentHeadSHA           string                     `json:"parent_head_sha"`
	ParentExecutableSHA256  string                     `json:"parent_executable_sha256"`
	ManifestIntegrityDigest string                     `json:"manifest_integrity_digest"`
	Generation              uint64                     `json:"generation"`
	DomainGraphs            bool                       `json:"domain_graphs"`
	ServingPartitions       []uint32                   `json:"serving_partitions"`
	TopK                    int                        `json:"top_k"`
	EfSearch                []int                      `json:"ef_search"`
	GoVersion               string                     `json:"go_version"`
	GOOS                    string                     `json:"goos"`
	GOARCH                  string                     `json:"goarch"`
	PageSize                int                        `json:"page_size"`
	Host                    m8ProductionHostEvidenceV1 `json:"host"`
}

type m8PhysicalScratchV1 struct {
	TopK       int    `json:"top_k"`
	EfSearch   int    `json:"ef_search"`
	BytesBound uint64 `json:"bytes_bound"`
}
type m8PhysicalResourcePackV1 struct {
	Live       collections.VectorPartitionPhysicalResourcesV1 `json:"live"`
	Scratch    []m8PhysicalScratchV1                          `json:"scratch"`
	AfterClose collections.VectorPartitionPhysicalReleaseV1   `json:"after_close"`
}
type m8PhysicalResourceTotalsV1 struct {
	PackBytes          uint64 `json:"pack_bytes"`
	MappedExtentBytes  uint64 `json:"mapped_extent_bytes"`
	HeapCopyBytes      uint64 `json:"heap_copy_bytes"`
	MetadataBytesBound uint64 `json:"metadata_bytes_bound"`
	StableIDBytesBound uint64 `json:"stable_id_bytes_bound"`
	LogicalHandles     uint64 `json:"logical_handles"`
	RequiredChunks     uint64 `json:"required_chunks"`
	OpenedChunks       uint64 `json:"opened_chunks"`
	ValidatedChunks    uint64 `json:"validated_chunks"`
	// Sum over the entire retained inventory, one hypothetical search per pack.
	// This is neither selected fanout nor a concurrency-multiplied peak.
	Scratch []m8PhysicalScratchV1 `json:"inventory_scratch_bounds"`
}
type m8PhysicalResourceReceiptV1 struct {
	Header   m8PhysicalResourceHeaderV1 `json:"header"`
	Packs    []m8PhysicalResourcePackV1 `json:"packs"`
	Totals   m8PhysicalResourceTotalsV1 `json:"totals"`
	Complete bool                       `json:"complete"`
}

func m8PhysicalTotalsV1(packs []m8PhysicalResourcePackV1, header m8PhysicalResourceHeaderV1) (m8PhysicalResourceTotalsV1, error) {
	var total m8PhysicalResourceTotalsV1
	if len(header.ServingPartitions) == 0 || len(header.ServingPartitions) > maxPartitions || len(packs) != len(header.ServingPartitions) || header.TopK < 1 || len(header.EfSearch) == 0 || len(header.EfSearch) > 32 {
		return total, errors.New("incomplete physical inventory or scratch coordinates")
	}
	add := func(dst *uint64, n uint64) error {
		if n > math.MaxUint64-*dst {
			return errors.New("physical resource total overflow")
		}
		*dst += n
		return nil
	}
	seenPartitions, seenEF := map[uint32]bool{}, map[int]bool{}
	for _, ef := range header.EfSearch {
		if ef < 0 || seenEF[ef] {
			return total, errors.New("invalid duplicate scratch coordinate")
		}
		seenEF[ef] = true
		total.Scratch = append(total.Scratch, m8PhysicalScratchV1{TopK: header.TopK, EfSearch: ef})
	}
	for i, pack := range packs {
		p, released := pack.Live, pack.AfterClose
		if p.PartitionID != header.ServingPartitions[i] || seenPartitions[p.PartitionID] || p.Generation != header.Generation || p.PackBytes == 0 || p.LogicalHandles == 0 || p.MetadataBytesBound == 0 || (p.MappedExtentBytes == 0 && p.HeapCopyBytes == 0) || p.RequiredChunks != p.OpenedChunks || p.RequiredChunks != p.ValidatedChunks || p.RequiredChunks == math.MaxUint64 || p.LogicalHandles != p.RequiredChunks+1 || len(pack.Scratch) != len(header.EfSearch) {
			return total, errors.New("physical pack identity, coverage or resource accounting mismatch")
		}
		seenPartitions[p.PartitionID] = true
		if released.Errors != 0 || released.LogicalHandles != 0 || released.MappedExtentBytes != 0 || released.HeapCopyBytes != 0 || released.Acquires != released.Releases || released.Acquires != p.LogicalHandles {
			return total, errors.New("physical pack owned handles were not released")
		}
		sums := []struct {
			dst   *uint64
			value uint64
		}{{&total.PackBytes, p.PackBytes}, {&total.MappedExtentBytes, p.MappedExtentBytes}, {&total.HeapCopyBytes, p.HeapCopyBytes}, {&total.MetadataBytesBound, p.MetadataBytesBound}, {&total.StableIDBytesBound, p.StableIDBytesBound}, {&total.LogicalHandles, p.LogicalHandles}, {&total.RequiredChunks, p.RequiredChunks}, {&total.OpenedChunks, p.OpenedChunks}, {&total.ValidatedChunks, p.ValidatedChunks}}
		for _, s := range sums {
			if err := add(s.dst, s.value); err != nil {
				return total, err
			}
		}
		for j, scratch := range pack.Scratch {
			if scratch.TopK != header.TopK || scratch.EfSearch != header.EfSearch[j] || scratch.BytesBound == 0 {
				return total, errors.New("physical scratch coordinate mismatch")
			}
			if err := add(&total.Scratch[j].BytesBound, scratch.BytesBound); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// Owns and closes the harness even on cancellation or partial observation.
func m8ObservePhysicalResourcesV1(ctx context.Context, h *m8AttributionHarnessV1, header m8PhysicalResourceHeaderV1) (_ m8PhysicalResourceReceiptV1, runErr error) {
	defer func() { runErr = errors.Join(runErr, h.Close()) }()
	result := m8PhysicalResourceReceiptV1{Header: header}
	if h == nil || !reflect.DeepEqual(h.servingPartitions, header.ServingPartitions) || h.domainGraphs != header.DomainGraphs {
		return result, errors.New("physical harness differs from declared inventory")
	}
	observers := make([]func() collections.VectorPartitionPhysicalReleaseV1, 0, len(h.servingPartitions))
	for _, partition := range h.servingPartitions {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		searcher := h.searchers[partition]
		live, observe, err := searcher.InspectPhysicalResourcesV1(ctx)
		if err != nil {
			return result, err
		}
		pack := m8PhysicalResourcePackV1{Live: live}
		for _, ef := range header.EfSearch {
			_, bound, err := searcher.SearchPreflightV1(collections.VectorPartitionSearchOptionsV1{TopK: header.TopK, EfSearch: ef})
			if err != nil {
				return result, err
			}
			pack.Scratch = append(pack.Scratch, m8PhysicalScratchV1{header.TopK, ef, bound})
		}
		result.Packs = append(result.Packs, pack)
		observers = append(observers, observe)
	}
	if err := h.Close(); err != nil {
		return result, err
	}
	for i, observe := range observers {
		result.Packs[i].AfterClose = observe()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	var err error
	result.Totals, err = m8PhysicalTotalsV1(result.Packs, header)
	result.Complete = err == nil
	return result, err
}

// expected and manifest come from independently frozen collector provenance and
// strictly replayed parent assets. This reader checks the receipt, not its origin.
func m8ReadPhysicalResourcesV1(input io.Reader, expected m8PhysicalResourceHeaderV1, parent m8ProductionReportV1, manifest collections.VectorPartitionManifestV1) (m8PhysicalResourceReceiptV1, error) {
	var r m8PhysicalResourceReceiptV1
	bounded := &io.LimitedReader{R: input, N: 8<<20 + 1}
	decoder := json.NewDecoder(bounded)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, err
	}
	h := r.Header
	domainGraphs := m8ManifestUsesDomainGraphsV1(manifest)
	partitions, err := m8ServingPartitionsV1(manifest, domainGraphs)
	if err != nil {
		return r, err
	}
	if !reflect.DeepEqual(h, expected) || h.Kind != "physical_resources" || h.Contract != m8PhysicalResourceContractV1 || h.Scope != m8PhysicalResourceScopeV1 || h.ParentExecutionID != parent.ExecutionID || h.ParentHeadSHA != parent.HeadSHA || h.ParentExecutableSHA256 != parent.ExecutableSHA256 || h.ManifestIntegrityDigest != manifest.IntegrityDigest || h.Generation != manifest.Generation || h.DomainGraphs != domainGraphs || !reflect.DeepEqual(h.ServingPartitions, partitions) || h.TopK != parent.Config.TopK || !reflect.DeepEqual(h.EfSearch, parent.Config.EfSearch) || int(manifest.PartitionCount) != parent.Config.Partitions || int(manifest.DomainCount) != parent.Config.DomainCount || !r.Complete {
		return r, errors.New("physical resource receipt differs from frozen collector/parent/layout or is incomplete")
	}
	totals, err := m8PhysicalTotalsV1(r.Packs, h)
	if err != nil {
		return r, err
	}
	if !reflect.DeepEqual(totals, r.Totals) {
		return r, errors.New("physical resource totals do not reconcile")
	}
	if err := decoder.Decode(new(any)); err != io.EOF || bounded.N == 0 {
		return r, errors.New("physical resource receipt trailing or oversized data")
	}
	return r, nil
}

// This is a new untimed observation of old pinned assets, not old telemetry.
// CPU serving-resources v1 remains a separate, unchanged contract.
func runM8PhysicalResourcesV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("physical-resources", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var out, source, head, executableSHA string
	fs.StringVar(&out, "out", "", "fresh absolute physical receipt")
	fs.StringVar(&source, "source-checkout", "", "clean collector Git toplevel")
	fs.StringVar(&head, "head-sha", "", "frozen collector revision")
	fs.StringVar(&executableSHA, "executable-sha256", "", "frozen collector executable digest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !filepath.IsAbs(out) || !m8QualificationGitSHAV1(head) || !m8QualificationSHA256V1(executableSHA) || len(fs.Args()) == 0 {
		return errors.New("physical-resources requires fresh absolute out, collector provenance and -- replay arguments")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("physical resource output must not exist")
	}
	checkout, err := m8SourceCheckoutV1(source, head)
	if err != nil || m8GitDirtyInV1(checkout) {
		return errors.New("physical collector requires matching clean source")
	}
	executable, err := os.Executable()
	if err != nil || !m8QualificationBenchmarkExecutableV1(filepath.Dir(executable), executable, head, executableSHA) {
		return errors.New("physical collector executable does not match frozen source/digest")
	}
	var parent m8ProductionReportV1
	var replay bytes.Buffer
	if err := replayM8ReportV1(fs.Args(), &replay, &parent); err != nil {
		return err
	}
	if parent.Variant == nil || parent.Config.MeasurementAccounting != m8CompleteAttemptsV1 {
		return errors.New("physical resources require retained complete-attempt M8 assets")
	}
	cfg, err := parseConfig(parent.Command[1:])
	if err != nil {
		return err
	}
	assets, err := m8OpenServingResourceAssetsV1(parent, cfg)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, assets.Close()) }()
	h, err := newM8AttributionHarnessV1(assets)
	if err != nil {
		return err
	}
	header := m8PhysicalResourceHeaderV1{
		Kind: "physical_resources", Contract: m8PhysicalResourceContractV1, Scope: m8PhysicalResourceScopeV1, ReplayReceipt: replay.String(),
		HeadSHA: head, ExecutableSHA256: executableSHA, SourceCheckout: checkout, Command: append([]string{executable, "physical-resources"}, args...),
		ParentExecutionID: parent.ExecutionID, ParentHeadSHA: parent.HeadSHA, ParentExecutableSHA256: parent.ExecutableSHA256,
		ManifestIntegrityDigest: assets.manifest.IntegrityDigest, Generation: assets.manifest.Generation, DomainGraphs: h.domainGraphs, ServingPartitions: append([]uint32(nil), h.servingPartitions...), TopK: parent.Config.TopK, EfSearch: parent.Config.EfSearch,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, PageSize: os.Getpagesize(), Host: m8ProductionHostV1(config{out: filepath.Dir(out), dataset: parent.DatasetDirectory}, assets.dir),
	}
	receipt, err := m8ObservePhysicalResourcesV1(context.Background(), h, header)
	if err != nil {
		return err
	}
	// Encode and validate before publishing an exclusive complete receipt.
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := m8ReadPhysicalResourcesV1(bytes.NewReader(raw), header, parent, assets.manifest); err != nil {
		return err
	}
	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(raw, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "PHYSICAL_RESOURCE_OBSERVATIONS_NOT_QUALIFICATION", out)
	return err
}
