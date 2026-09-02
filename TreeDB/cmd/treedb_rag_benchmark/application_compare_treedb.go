package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const treeDBComparisonArtifactSchema = "treedb-rag-treedb-comparison/v1"

type comparisonProcessUsage struct {
	Available         bool    `json:"available"`
	CPUSeconds        float64 `json:"cumulative_cpu_seconds"`
	PeakRSSBytes      int64   `json:"high_water_rss_bytes"`
	CapturedUnixNanos int64   `json:"captured_unix_nanos"`
}

type comparisonProcessResources struct {
	Available    bool                   `json:"available"`
	CPUSeconds   float64                `json:"cpu_seconds"`
	PeakRSSBytes int64                  `json:"peak_rss_bytes"`
	Before       comparisonProcessUsage `json:"before"`
	After        comparisonProcessUsage `json:"after"`
	CPUSemantics string                 `json:"cpu_semantics"`
	RSSSemantics string                 `json:"rss_semantics"`
	Scope        string                 `json:"scope"`
}

type treeDBComparisonArtifact struct {
	Schema               string                      `json:"schema"`
	Authority            string                      `json:"authority"`
	GeneratedAtUTC       string                      `json:"generated_at_utc"`
	ManifestSHA256       string                      `json:"manifest_sha256"`
	ProductBaseSHA       string                      `json:"product_base_sha"`
	HarnessRevision      string                      `json:"harness_revision"`
	BinarySHA256         string                      `json:"binary_sha256"`
	FixtureSHA256        string                      `json:"fixture_sha256"`
	SemanticVectorSHA256 string                      `json:"semantic_vector_sha256"`
	ConfigSHA256         string                      `json:"config_sha256"`
	Config               applicationComparisonConfig `json:"config"`
	SourceCount          int                         `json:"source_count"`
	ChunkCount           int                         `json:"chunk_count"`
	QueryCount           int                         `json:"query_count"`
	BuildReopenSeconds   float64                     `json:"build_reopen_seconds"`
	QuerySeconds         float64                     `json:"query_seconds"`
	StorageBytes         int64                       `json:"storage_bytes"`
	ProcessResources     comparisonProcessResources  `json:"process_resources"`
	Lifecycle            lifecycleEvidence           `json:"lifecycle"`
	Rows                 []applicationRow            `json:"rows"`
	Failures             []string                    `json:"failures"`
}

func createTreeDBComparisonRoot(root string) error {
	if root == "" {
		return fmt.Errorf("TreeDB comparison requires an explicit durable database directory")
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		return fmt.Errorf("TreeDB comparison database directory must be absent: %w", err)
	}
	return nil
}

func runTreeDBComparisonEvidence(cfg applicationConfig, outputPath string) error {
	manifest, err := buildApplicationComparisonManifest()
	if err != nil {
		return err
	}
	manifestRaw, err := applicationComparisonManifestBytes()
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestRaw)
	manifestSHA := hex.EncodeToString(manifestSum[:])
	fixture := buildApplicationFixture()
	bundle, err := loadSemanticVectors()
	if err != nil {
		return err
	}
	if err := validateApplicationFixture(&fixture); err != nil {
		return err
	}
	if err := validateSemanticVectors(&fixture, bundle); err != nil {
		return err
	}
	if err := registerSemanticProvider(bundle); err != nil {
		return err
	}
	cfg.Workload = "application"
	cfg.TopK = manifest.Config.TopK
	cfg.CandidateLimit = manifest.Config.CandidateLimit
	cfg.EfSearch = manifest.Config.TreeDBEfSearch
	cfg.EfConstruction = manifest.Config.TreeDBEfConstruction
	cfg.M = manifest.Config.TreeDBM
	cfg.WarmupQueries = manifest.Config.WarmupsPerCell
	cfg.Repetitions = manifest.Config.Repetitions
	cfg.SamplesPerRep = manifest.Config.SamplesPerCell
	cfg.ProductBaseSHA = manifest.ProductBaseSHA
	cfg.FinalEvidence = true
	settings, ok := runtimeBuildInfo()
	if _, err := resolveApplicationHarnessRevision(cfg, settings, ok); err != nil {
		return err
	}
	root := cfg.Dir
	if err := createTreeDBComparisonRoot(root); err != nil {
		return err
	}
	usageBefore, err := comparisonProcessUsageSnapshot()
	if err != nil {
		return err
	}
	dims, provider := embeddingCellConfig("semantic_minilm", bundle)
	setupStarted := time.Now()
	env, lifecycle, err := openApplicationEnvironment(cfg, &fixture, bundle, "semantic_minilm", provider, dims, filepath.Join(root, "semantic_minilm"))
	if err != nil {
		return fmt.Errorf("TreeDB comparison setup/reopen: %w", err)
	}
	defer env.close()
	buildReopenSeconds := time.Since(setupStarted).Seconds()
	if err := validateLifecycleEvidence("semantic_minilm", lifecycle); err != nil {
		return err
	}
	queryVectors, err := applicationQueryVectors(&fixture, bundle, "semantic_minilm", provider, dims)
	if err != nil {
		return err
	}
	cellCfg := cfg
	cellCfg.FinalEvidence = false // The comparison contract is exactly 3x100, not the older >=1000-sample baseline contract.
	queryStarted := time.Now()
	rows := make([]applicationRow, 0, 12)
	for _, route := range applicationRoutes {
		for _, filter := range applicationFilterOrder {
			cell := applicationCellIdentity{Route: route, Projection: "fetch_topk", Filter: filter, Collapse: "disabled", Surface: "direct_collection", Embedding: "semantic_minilm", Clients: 1}
			cell.VectorRoute = applicationVectorRoute(cell)
			row, rowErr := runApplicationCell(cellCfg, &fixture, env, queryVectors, cell, false)
			if rowErr != nil {
				return fmt.Errorf("TreeDB comparison cell %s/%s: %w", route, filter, rowErr)
			}
			if _, measuredQuality, qualityErr := recomputeTreeDBCellEvidence(row, manifest); qualityErr != nil {
				return fmt.Errorf("TreeDB comparison measured quality %s/%s: %w", route, filter, qualityErr)
			} else {
				row.Quality = measuredQuality
			}
			rows = append(rows, row)
		}
	}
	querySeconds := time.Since(queryStarted).Seconds()
	storageBytes, err := dirSize(root)
	if err != nil {
		return err
	}
	usageAfter, err := comparisonProcessUsageSnapshot()
	if err != nil {
		return err
	}
	resources := comparisonProcessResources{
		Available:    usageBefore.Available && usageAfter.Available,
		Before:       usageBefore,
		After:        usageAfter,
		CPUSemantics: "getrusage(RUSAGE_SELF) user+system CPU; cumulative before/after snapshots, aggregate is after-before",
		RSSSemantics: "getrusage(RUSAGE_SELF) process high-water RSS; before/after snapshots, aggregate is after high-water; Darwin bytes, Linux KiB normalized to bytes",
		Scope:        "fresh comparison process; build, lifecycle reopen, and all 12 query cells",
	}
	if resources.Available {
		resources.CPUSeconds = usageAfter.CPUSeconds - usageBefore.CPUSeconds
		resources.PeakRSSBytes = usageAfter.PeakRSSBytes
	} else {
		resources.CPUSemantics = "unavailable on this operating system"
		resources.RSSSemantics = "unavailable on this operating system"
	}
	provenance, err := buildApplicationProvenance(cfg, &fixture, bundle)
	if err != nil {
		return err
	}
	artifact := treeDBComparisonArtifact{
		Schema: treeDBComparisonArtifactSchema, Authority: "BOUNDED_COMPARISON_EVIDENCE",
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), ManifestSHA256: manifestSHA,
		ProductBaseSHA: provenance.ProductBaseSHA, HarnessRevision: provenance.HarnessRevision,
		BinarySHA256: provenance.BinarySHA256, FixtureSHA256: provenance.FixtureSHA256,
		SemanticVectorSHA256: provenance.SemanticVectorSHA256, ConfigSHA256: manifest.ConfigSHA256,
		Config: manifest.Config, SourceCount: len(manifest.Sources), ChunkCount: len(manifest.Chunks), QueryCount: len(manifest.Queries),
		BuildReopenSeconds: buildReopenSeconds, QuerySeconds: querySeconds, StorageBytes: storageBytes,
		Lifecycle: lifecycle, ProcessResources: resources, Rows: rows,
	}
	if _, err := validateTreeDBComparisonArtifact(&artifact, manifest, manifestSHA); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, append(raw, '\n'), 0o644)
}
