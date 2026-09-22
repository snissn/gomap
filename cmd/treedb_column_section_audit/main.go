package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

type columnSectionAuditResult struct {
	Status             string                                    `json:"status"`
	Collection         string                                    `json:"collection,omitempty"`
	DetailedSections   bool                                      `json:"detailed_sections"`
	ReadIntegrity      collections.ColumnAssetReadIntegrity      `json:"read_integrity"`
	PhysicalAccounting collections.ColumnStorePhysicalAccounting `json:"physical_accounting,omitempty"`
	Errors             []string                                  `json:"errors,omitempty"`
}

func main() {
	os.Exit(run())
}

func run() (code int) {
	var dbDir string
	var collectionName string
	var detailedSections bool
	var readIntegrity string
	defer func() {
		if recovered := recover(); recovered != nil {
			code = writeFailure(collectionName, detailedSections, parseReadIntegrity(readIntegrity), fmt.Errorf("column section audit panic: %v", recovered))
		}
	}()
	flag.StringVar(&dbDir, "db-dir", "", "TreeDB DB directory")
	flag.StringVar(&collectionName, "collection", "", "Collection name; defaults to the only collection")
	flag.BoolVar(&detailedSections, "detailed-sections", true, "Decode typed-column serialized section accounting")
	flag.StringVar(&readIntegrity, "read-integrity", string(collections.ColumnAssetReadIntegrityVerify), "Column asset read integrity: verify, cached_verify, or skip_checksums")
	flag.Parse()

	if strings.TrimSpace(dbDir) == "" {
		return writeFailure("", detailedSections, parseReadIntegrity(readIntegrity), errors.New("-db-dir is required"))
	}
	integrity := parseReadIntegrity(readIntegrity)
	if integrity == "" {
		return writeFailure(collectionName, detailedSections, "", fmt.Errorf("unsupported -read-integrity %q", readIntegrity))
	}

	db, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dbDir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("open read-only DB: %w", err))
	}
	defer func() { _ = cleanup() }()

	manager := collections.NewCollectionManager(db)
	if strings.TrimSpace(collectionName) == "" {
		metas, err := manager.ListCollections()
		if err != nil {
			return writeFailure("", detailedSections, integrity, fmt.Errorf("list collections: %w", err))
		}
		switch len(metas) {
		case 0:
			return writeFailure("", detailedSections, integrity, errors.New("no collections found"))
		case 1:
			collectionName = metas[0].Name
		default:
			names := make([]string, 0, len(metas))
			for _, meta := range metas {
				names = append(names, meta.Name)
			}
			return writeFailure("", detailedSections, integrity, fmt.Errorf("multiple collections found; pass -collection explicitly: %s", strings.Join(names, ", ")))
		}
	}
	col, err := manager.OpenCollection(collectionName)
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("open collection: %w", err))
	}
	accounting, err := col.ColumnStorePhysicalAccounting(context.Background(), collections.ColumnStorePhysicalAccountingOptions{
		DetailedSections: detailedSections,
		ReadIntegrity:    integrity,
	})
	result := columnSectionAuditResult{
		Status:             "passed",
		Collection:         collectionName,
		DetailedSections:   detailedSections,
		ReadIntegrity:      integrity,
		PhysicalAccounting: accounting,
	}
	if err != nil {
		result.Status = "failed"
		result.Errors = []string{err.Error()}
		writeResult(result)
		return 1
	}
	writeResult(result)
	return 0
}

func parseReadIntegrity(value string) collections.ColumnAssetReadIntegrity {
	switch collections.ColumnAssetReadIntegrity(strings.TrimSpace(value)) {
	case "", collections.ColumnAssetReadIntegrityVerify:
		return collections.ColumnAssetReadIntegrityVerify
	case collections.ColumnAssetReadIntegrityCachedVerify:
		return collections.ColumnAssetReadIntegrityCachedVerify
	case collections.ColumnAssetReadIntegritySkipChecksums:
		return collections.ColumnAssetReadIntegritySkipChecksums
	default:
		return ""
	}
}

func writeFailure(collectionName string, detailedSections bool, integrity collections.ColumnAssetReadIntegrity, err error) int {
	if integrity == "" {
		integrity = collections.ColumnAssetReadIntegrityVerify
	}
	result := columnSectionAuditResult{
		Status:           "failed",
		Collection:       collectionName,
		DetailedSections: detailedSections,
		ReadIntegrity:    integrity,
	}
	if err != nil {
		result.Errors = []string{err.Error()}
	}
	writeResult(result)
	return 1
}

func writeResult(result columnSectionAuditResult) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(result)
}
