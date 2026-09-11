package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

const columnSectionAuditSchema = "treedb-column-section-audit/v2"

type storageOwnedFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	Domain string `json:"domain"`
}

func valueLogLaneAndSequence(fileID uint32) (uint32, uint32) {
	const sequenceBits = 23
	segmentID := page.ValueLogSegmentID(fileID)
	return segmentID >> sequenceBits, segmentID & ((1 << sequenceBits) - 1)
}

func isNonLeafValueLogFile(id uint32, leafIDs map[uint32]struct{}) bool {
	if !page.IsValueLogFileID(id) {
		return false
	}
	_, leaf := leafIDs[page.ValueLogSegmentID(id)]
	return !leaf
}

func leafGenerationOwnsStorage(generation backenddb.LeafGenerationPlanGeneration) bool {
	return !generation.WholeGenerationGCEligible
}

type columnSectionAuditResult struct {
	SchemaVersion      string                                    `json:"schema_version"`
	Status             string                                    `json:"status"`
	Collection         string                                    `json:"collection,omitempty"`
	DetailedSections   bool                                      `json:"detailed_sections"`
	ReadIntegrity      collections.ColumnAssetReadIntegrity      `json:"read_integrity"`
	PhysicalAccounting collections.ColumnStorePhysicalAccounting `json:"physical_accounting,omitempty"`
	StoragePlan        backenddb.CompactStorageStats             `json:"storage_plan"`
	AssetLifecycle     collections.ColumnAssetLifecycleReport    `json:"asset_lifecycle"`
	OwnedFiles         []storageOwnedFile                        `json:"owned_files"`
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
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("physical accounting: %w", err))
	}
	storagePlan, err := db.CompactStoragePlan(context.Background(), backenddb.CompactStorageOptions{})
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("storage reachability: %w", err))
	}
	assetLifecycle, err := col.PlanColumnAssetLifecycle(context.Background(), collections.ColumnAssetLifecycleOptions{
		SegmentDetails: true,
	})
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("column asset reachability: %w", err))
	}
	ownedFiles, err := collectOwnedFiles(context.Background(), db, storagePlan, assetLifecycle)
	if err != nil {
		return writeFailure(collectionName, detailedSections, integrity, fmt.Errorf("owned files: %w", err))
	}
	result := columnSectionAuditResult{
		SchemaVersion:      columnSectionAuditSchema,
		Status:             "passed",
		Collection:         collectionName,
		DetailedSections:   detailedSections,
		ReadIntegrity:      integrity,
		PhysicalAccounting: accounting,
		StoragePlan:        storagePlan,
		AssetLifecycle:     assetLifecycle,
		OwnedFiles:         ownedFiles,
	}
	writeResult(result)
	return 0
}

func collectOwnedFiles(
	ctx context.Context,
	db *backenddb.DB,
	storagePlan backenddb.CompactStorageStats,
	assetLifecycle collections.ColumnAssetLifecycleReport,
) ([]storageOwnedFile, error) {
	owned := make([]storageOwnedFile, 0)
	seen := make(map[string]struct{})
	add := func(path, domain string, wantBytes int64) error {
		if path == "" {
			return errors.New("empty owned file path")
		}
		if _, ok := seen[path]; ok {
			return fmt.Errorf("duplicate owned file path %q", path)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("owned path is not a regular file: %q", path)
		}
		if wantBytes >= 0 && info.Size() != wantBytes {
			return fmt.Errorf("owned file size changed for %q: got %d want %d", path, info.Size(), wantBytes)
		}
		seen[path] = struct{}{}
		owned = append(owned, storageOwnedFile{Path: path, Bytes: info.Size(), Domain: domain})
		return nil
	}

	domainPaths := make(map[string]string)
	for _, usage := range storagePlan.Before {
		domainPaths[usage.Name] = usage.Path
	}
	for _, usage := range storagePlan.Before {
		if usage.Name == "index" {
			if usage.Files != 1 {
				return nil, fmt.Errorf("index file count=%d want 1", usage.Files)
			}
			if err := add(usage.Path, "index", usage.Bytes); err != nil {
				return nil, err
			}
		}
	}

	leafIDs := make(map[uint32]struct{})
	for _, generation := range storagePlan.LeafGenerationPlan.Generations {
		for _, id := range generation.FileIDs {
			if _, duplicate := leafIDs[id]; duplicate {
				return nil, fmt.Errorf("duplicate leaf-log file id %d", id)
			}
			leafIDs[id] = struct{}{}
		}
	}

	state := db.State()
	if state == nil || state.ValueLogSet == nil {
		return nil, errors.New("value-log state unavailable")
	}
	for id, file := range state.ValueLogSet.Files {
		if !isNonLeafValueLogFile(id, leafIDs) {
			continue
		}
		stats, err := db.ValueLogGC(ctx, backenddb.ValueLogGCOptions{
			DryRun:                true,
			ObservedSourceFileIDs: []uint32{id},
		})
		if err != nil {
			return nil, fmt.Errorf("classify value-log file %d: %w", id, err)
		}
		if stats.ObservedSourceSegmentsReferenced == 0 {
			continue
		}
		if stats.ObservedSourceSegmentsReferenced != 1 ||
			stats.ObservedSourceBytesReferenced < 0 {
			return nil, fmt.Errorf("ambiguous referenced value-log file %d", id)
		}
		if err := add(file.Path, "value_vlog", stats.ObservedSourceBytesReferenced); err != nil {
			return nil, err
		}
	}

	for _, generation := range storagePlan.LeafGenerationPlan.Generations {
		var generationBytes int64
		for _, id := range generation.FileIDs {
			lane, sequence := valueLogLaneAndSequence(id)
			path := filepath.Join(
				domainPaths["leaf_vlog"],
				fmt.Sprintf("value-l%d-%06d.log", lane, sequence))
			info, err := os.Lstat(path)
			if err != nil {
				return nil, err
			}
			generationBytes += info.Size()
			if leafGenerationOwnsStorage(generation) {
				if err := add(path, "leaf_vlog", info.Size()); err != nil {
					return nil, err
				}
			}
		}
		if generationBytes != generation.BytesTotal {
			return nil, fmt.Errorf(
				"leaf generation %d bytes=%d want %d",
				generation.GenerationID, generationBytes, generation.BytesTotal)
		}
	}

	for _, entry := range assetLifecycle.Reachability.SegmentEntries {
		if entry.RefCount <= 0 {
			continue
		}
		if entry.Bytes <= 0 || entry.ProtectedBytes != entry.Bytes ||
			entry.ReclaimableBytes != 0 || entry.UnknownBytes != 0 {
			return nil, fmt.Errorf("column segment %d has ambiguous ownership", entry.FileID)
		}
		if err := add(entry.Path, "column_assets", entry.Bytes); err != nil {
			return nil, err
		}
	}

	sort.Slice(owned, func(i, j int) bool { return owned[i].Path < owned[j].Path })
	if len(owned) == 0 {
		return nil, errors.New("no owned storage files")
	}
	return owned, nil
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
		SchemaVersion:    columnSectionAuditSchema,
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
