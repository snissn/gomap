package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

func main() {
	os.Exit(run())
}

func run() (code int) {
	var dbDir string
	var collectionName string
	var pathsCSV string
	var maxDocuments int
	var shapeStats bool
	var shapeMaxDepth int
	var shapeMaxPaths int
	defer func() {
		if recovered := recover(); recovered != nil {
			code = writeFailure(collectionName, fmt.Errorf("retained payload audit panic: %v", recovered))
		}
	}()
	flag.StringVar(&dbDir, "db-dir", "", "TreeDB DB directory")
	flag.StringVar(&collectionName, "collection", "", "Collection name; defaults to the only collection")
	flag.StringVar(&pathsCSV, "paths", "", "Comma-separated JSON paths to require absent; defaults to collection column paths")
	flag.IntVar(&maxDocuments, "max-documents", 0, "Maximum retained rows to audit; zero audits all rows")
	flag.BoolVar(&shapeStats, "shape-stats", false, "Include decoded retained-payload path/value shape stats")
	flag.IntVar(&shapeMaxDepth, "shape-max-depth", 8, "Maximum retained-payload shape traversal depth; zero means unlimited")
	flag.IntVar(&shapeMaxPaths, "shape-max-paths", 128, "Maximum retained-payload shape path/kind rows to emit; zero means unlimited")
	flag.Parse()

	if strings.TrimSpace(dbDir) == "" {
		return writeFailure("", errors.New("-db-dir is required"))
	}
	db, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dbDir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		return writeFailure(collectionName, fmt.Errorf("open read-only DB: %w", err))
	}
	defer func() { _ = cleanup() }()

	manager := collections.NewCollectionManager(db)
	if strings.TrimSpace(collectionName) == "" {
		metas, err := manager.ListCollections()
		if err != nil {
			return writeFailure("", fmt.Errorf("list collections: %w", err))
		}
		switch len(metas) {
		case 0:
			return writeFailure("", errors.New("no collections found"))
		case 1:
			collectionName = metas[0].Name
		default:
			names := make([]string, 0, len(metas))
			for _, meta := range metas {
				names = append(names, meta.Name)
			}
			return writeFailure("", fmt.Errorf("multiple collections found; pass -collection explicitly: %s", strings.Join(names, ", ")))
		}
	}
	col, err := manager.OpenCollection(collectionName)
	if err != nil {
		return writeFailure(collectionName, fmt.Errorf("open collection: %w", err))
	}
	result, err := col.AuditRetainedPayloadDeclaredPathsAbsent(collections.ColumnRetainedPayloadCollectionAuditOptions{
		Paths:             splitCSV(pathsCSV),
		MaxDocuments:      maxDocuments,
		IncludeShapeStats: shapeStats,
		ShapeMaxDepth:     shapeMaxDepth,
		ShapeMaxPaths:     shapeMaxPaths,
	})
	if err != nil {
		writeResult(result)
		return 1
	}
	writeResult(result)
	return 0
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func writeFailure(collectionName string, err error) int {
	result := collections.ColumnRetainedPayloadCollectionAuditResult{
		Collection: collectionName,
		Status:     "failed",
	}
	if err != nil {
		result.Errors = []string{err.Error()}
	}
	writeResult(result)
	return 1
}

func writeResult(result collections.ColumnRetainedPayloadCollectionAuditResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
}
