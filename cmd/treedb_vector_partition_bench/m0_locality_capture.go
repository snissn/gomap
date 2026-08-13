package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

// m0-locality-capture is read-only, offline evidence. The reported page scope
// is graph+vector pack sections; document-ID result materialization is out of
// scope because M0 selects traversal/layout locality, not result encoding.
type m0LocalityCaptureRowV1 struct {
	Query    int    `json:"query_ordinal"`
	Pages    uint64 `json:"unique_graph_vector_pages"`
	Expanded uint64 `json:"expanded_neighbor_lists"`
	Edges    uint64 `json:"edges"`
}
type m0LocalityCaptureV1 struct {
	Schema           string                   `json:"schema"`
	DB               string                   `json:"retained_db"`
	Split            string                   `json:"split_sha256"`
	Probes           int                      `json:"probes"`
	RouterCandidates int                      `json:"router_candidates"`
	EF               int                      `json:"ef_search"`
	PageScope        string                   `json:"page_scope"`
	MedianPages      uint64                   `json:"median_unique_graph_vector_pages"`
	P95Pages         uint64                   `json:"p95_unique_graph_vector_pages"`
	Rows             []m0LocalityCaptureRowV1 `json:"rows"`
}

func runM0LocalityCaptureV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-locality-capture", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, db, splitPath, out string
	var probes, candidates, ef int
	fs.StringVar(&dataset, "dataset", "", "frozen fixture directory")
	fs.StringVar(&db, "retained-db", "", "read-only retained M3 DB")
	fs.StringVar(&splitPath, "split", "", "frozen query split manifest")
	fs.StringVar(&out, "out", "", "fresh JSON output")
	fs.IntVar(&probes, "probes", 2, "router partition probes")
	fs.IntVar(&candidates, "router-candidates", 64, "router candidate budget")
	fs.IntVar(&ef, "ef-search", 128, "native ef search")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || db == "" || splitPath == "" || out == "" || probes < 1 || candidates < probes || ef < 1 {
		return errors.New("m0-locality-capture requires frozen inputs and positive bounded probes/router-candidates/ef")
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
	if int(assets.manifest.PartitionCount) < probes {
		return errors.New("probes exceed retained partitions")
	}
	report := m0LocalityCaptureV1{Schema: "treedb_vector_partition_m0_exact_pack_trace_v1", DB: db, Split: splitSHA, Probes: probes, RouterCandidates: candidates, EF: ef, PageScope: "unique 4KiB graph+vector pack pages/query; excludes document-ID result materialization"}
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
		tokens := map[collections.VectorPartitionSearchPageTokenV1]struct{}{}
		for _, p := range route {
			s, e := assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, p)
			if e != nil {
				return e
			}
			_, m, t, e := s.SearchWithAttributionV1(context.Background(), q, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: ef})
			if e == nil {
				pages, x := s.PageAttributionForTraceV1(t, 4096)
				if x != nil {
					e = x
				} else {
					for _, token := range pages.Tokens {
						tokens[token] = struct{}{}
					}
					row.Expanded += uint64(len(t.AdjacencyReads))
					row.Edges += m.Edges
				}
			}
			closeErr := s.Close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
		}
		row.Pages = uint64(len(tokens))
		report.Rows = append(report.Rows, row)
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
