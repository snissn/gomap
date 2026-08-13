package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

// m0-locality-capture is read-only, offline evidence. The reported page scope
// is graph+vector pack sections; document-ID result materialization is out of
// scope because M0 selects traversal/layout locality, not result encoding.
type m0LocalityCaptureRowV1 struct {
	Query                 int    `json:"query_ordinal"`
	Pages                 uint64 `json:"unique_graph_vector_pages"`
	Expanded              uint64 `json:"expanded_neighbor_lists"`
	Edges                 uint64 `json:"edges"`
	AdjacencyPageAccesses uint64 `json:"adjacency_page_accesses"`
}
type m0LocalityTracePartitionV1 struct {
	Partition      uint32                                        `json:"partition"`
	LevelOrdinals  []uint32                                      `json:"level_ordinals"`
	ScoreOrdinals  []uint32                                      `json:"score_ordinals"`
	AdjacencyReads []collections.VectorPartitionSearchPageReadV1 `json:"adjacency_reads"`
}
type m0LocalityTraceRowV1 struct {
	Query      int                          `json:"query_ordinal"`
	Route      []uint32                     `json:"route"`
	Partitions []m0LocalityTracePartitionV1 `json:"partitions"`
}
type m0LocalityCaptureV1 struct {
	Schema           string                                                     `json:"schema"`
	DB               string                                                     `json:"retained_db"`
	Split            string                                                     `json:"split_sha256"`
	Artifact         string                                                     `json:"graph_artifact_sha256,omitempty"`
	Descriptor       string                                                     `json:"descriptor_sha256,omitempty"`
	Source           vectorpartition.Source                                     `json:"source,omitempty"`
	Manifest         string                                                     `json:"manifest_integrity_digest,omitempty"`
	ReadySet         string                                                     `json:"ready_set_digest,omitempty"`
	RouterModel      string                                                     `json:"router_model_digest,omitempty"`
	Probes           int                                                        `json:"probes"`
	RouterCandidates int                                                        `json:"router_candidates"`
	EF               int                                                        `json:"ef_search"`
	PageScope        string                                                     `json:"page_scope"`
	MedianPages      uint64                                                     `json:"median_unique_graph_vector_pages"`
	P95Pages         uint64                                                     `json:"p95_unique_graph_vector_pages"`
	Rows             []m0LocalityCaptureRowV1                                   `json:"rows"`
	Traces           []m0LocalityTraceRowV1                                     `json:"traces,omitempty"`
	Snapshots        map[uint32]collections.VectorPartitionPackLayoutSnapshotV1 `json:"snapshots,omitempty"`
}

func runM0LocalityCaptureV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-locality-capture", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, db, splitPath, out, artifactPath string
	var probes, candidates, ef int
	var rawTraces bool
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&db, "retained-db", "", "read-only retained M3 DB")
	fs.StringVar(&splitPath, "split", "", "frozen query split manifest")
	fs.StringVar(&out, "out", "", "fresh JSON output")
	fs.StringVar(&artifactPath, "artifact", "", "frozen graph artifact for raw layout identity")
	fs.IntVar(&probes, "probes", 2, "router partition probes")
	fs.IntVar(&candidates, "router-candidates", 64, "router candidate budget")
	fs.IntVar(&ef, "ef-search", 128, "native ef search")
	fs.BoolVar(&rawTraces, "raw-traces", false, "persist offline trace events and pack layout snapshots")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || db == "" || splitPath == "" || out == "" || probes < 1 || candidates < probes || ef < 1 {
		return errors.New("m0-locality-capture requires frozen inputs and positive bounded probes/router-candidates/ef")
	}
	ordinals := map[string]uint32{}
	var artifact vectorpartition.Artifact
	artifactSHA := ""
	if rawTraces {
		if artifactPath == "" {
			return errors.New("raw traces require frozen graph artifact")
		}
		raw, e := os.ReadFile(artifactPath)
		if e != nil {
			return e
		}
		artifact, e = vectorpartition.DecodeArtifact(raw, len(raw))
		if e != nil {
			return e
		}
		artifactSHA = fmt.Sprintf("%x", sha256.Sum256(raw))
		for ordinal, id := range artifact.IDs {
			if _, duplicate := ordinals[id]; duplicate {
				return errors.New("duplicate graph artifact ID")
			}
			ordinals[id] = uint32(ordinal)
		}
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	_, queries := fixtureData(fixture)
	split, splitSHA, err := loadLocalHNSWQuerySplitV1(splitPath)
	if err != nil {
		return err
	}
	if split.DatasetChecksum != fixture.Checksum {
		return errors.New("split fixture identity")
	}
	assets, err := openM8ProductionExistingAssetSetV1(db)
	if err != nil {
		return err
	}
	defer assets.Close()
	descriptor, err := m3ReadVariantDescriptorV1(db)
	if err != nil {
		return err
	}
	descriptorSHA, err := localHNSWAttributionRegularFileSHA256V1(filepath.Join(db, m3VariantDescriptorFileV1), m3VariantDescriptorMaxBytesV1)
	if err != nil {
		return err
	}
	if rawTraces && (descriptor.Source != artifact.Source || descriptor.GraphArtifactSHA256 != artifactSHA) {
		return errors.New("retained capture source does not match graph artifact")
	}
	if err := m3DescriptorMatchesManifestV1(descriptor, fixture, assets.manifest, assets.status.ModelDigest, assets.status.Config); err != nil {
		return fmt.Errorf("retained capture descriptor: %w", err)
	}
	if int(assets.manifest.PartitionCount) < probes {
		return errors.New("probes exceed retained partitions")
	}
	// Open/verify each immutable pack once. Reopening inside the query loop
	// would measure checksum/open work rather than traversal page locality.
	searchers := make([]*collections.VectorPartitionLocalSearcherV1, assets.manifest.PartitionCount)
	defer func() {
		for _, s := range searchers {
			if s != nil {
				_ = s.Close()
			}
		}
	}()
	for p := range searchers {
		searchers[p], err = assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(p))
		if err != nil {
			return err
		}
	}
	snapshots := map[uint32]collections.VectorPartitionPackLayoutSnapshotV1{}
	if rawTraces {
		for p, s := range searchers {
			snapshot, e := s.PackLayoutSnapshotV1(ordinals)
			if e != nil {
				return e
			}
			snapshots[uint32(p)] = snapshot
		}
	}
	report := m0LocalityCaptureV1{Schema: "treedb_vector_partition_m0_exact_pack_trace_v2", DB: db, Split: splitSHA, Artifact: artifactSHA, Descriptor: descriptorSHA, Source: descriptor.Source, Manifest: assets.manifest.IntegrityDigest, ReadySet: assets.manifest.ReadySetDigest, RouterModel: assets.status.ModelDigest, Probes: probes, RouterCandidates: candidates, EF: ef, PageScope: "unique 4KiB graph+vector pack pages/query; excludes document-ID result materialization", Snapshots: snapshots}
	for _, ordinal := range split.Ordinals {
		if ordinal < 0 || ordinal >= len(queries) {
			return errors.New("split ordinal")
		}
		q := m8Query32V1(queries[ordinal])
		route, err := localHNSWAttributionQueryRouteV1(context.Background(), assets, q, candidates, probes)
		if err != nil {
			return err
		}
		row := m0LocalityCaptureRowV1{Query: ordinal}
		traceRow := m0LocalityTraceRowV1{Query: ordinal, Route: append([]uint32(nil), route...)}
		tokens := map[collections.VectorPartitionSearchPageTokenV1]struct{}{}
		for _, p := range route {
			if int(p) >= len(searchers) || searchers[p] == nil {
				return errors.New("routed partition searcher")
			}
			s := searchers[p]
			_, m, t, e := s.SearchWithAttributionV1(context.Background(), q, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: ef})
			if e == nil {
				pages, x := s.PageAttributionForTraceV1(t, m0PageBytesV1)
				if x != nil {
					e = x
				} else {
					for _, token := range pages.Tokens {
						tokens[token] = struct{}{}
					}
					row.Expanded += uint64(len(t.AdjacencyReads))
					row.Edges += m.Edges
					row.AdjacencyPageAccesses += pages.AdjacencyPageAccesses
					if rawTraces {
						traceRow.Partitions = append(traceRow.Partitions, m0LocalityTracePartitionV1{Partition: p, LevelOrdinals: append([]uint32(nil), t.LevelOrdinals...), ScoreOrdinals: append([]uint32(nil), t.ScoreOrdinals...), AdjacencyReads: append([]collections.VectorPartitionSearchPageReadV1(nil), t.AdjacencyReads...)})
					}
				}
			}
			if e != nil {
				return e
			}
		}
		row.Pages = uint64(len(tokens))
		report.Rows = append(report.Rows, row)
		if rawTraces {
			report.Traces = append(report.Traces, traceRow)
		}
	}
	values := make([]uint64, len(report.Rows))
	for i, row := range report.Rows {
		values[i] = row.Pages
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) > 0 {
		report.MedianPages = values[len(values)/2]
		report.P95Pages = values[(len(values)*95+99)/100-1]
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err = os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err = os.WriteFile(out, raw, 0644); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "m0_locality_capture=%s queries=%d median_pages=%d p95_pages=%d\n", out, len(report.Rows), report.MedianPages, report.P95Pages)
	return err
}
