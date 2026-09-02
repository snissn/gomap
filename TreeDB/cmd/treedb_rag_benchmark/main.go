// Command treedb_rag_benchmark publishes the retained application-shaped RAG
// baseline for #4289. The report is fail-closed, exact-SHA/digest bound, and
// keeps unsupported product cells as typed capability evidence.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	cfg := defaultApplicationConfig()
	var (
		workload             = flag.String("workload", "application", "workload mode: application or minima")
		outDir               = flag.String("out-dir", ".", "directory receiving JSON, markdown, and artifact manifest")
		dir                  = flag.String("dir", "", "benchmark database root (default: temporary)")
		keepDir              = flag.Bool("keep-dir", false, "keep a temporary benchmark database root")
		productSHA           = flag.String("product-base-sha", "99929cdeb2ae2ec1e411236c853eb36942075d72", "exact accepted main product SHA")
		harnessSHA           = flag.String("harness-revision", "", "expected exact debug.ReadBuildInfo vcs.revision (must match the clean final binary)")
		hostNote             = flag.String("host-note", "", "free-form host note recorded in provenance")
		smoke                = flag.Bool("smoke", false, "run a bounded diagnostic that cannot claim final p99/QPS evidence")
		dumpInputs           = flag.String("dump-semantic-inputs", "", "write the exact semantic generation input manifest and exit")
		dumpComparison       = flag.String("dump-application-comparison-manifest", "", "write the frozen #4331 application comparison manifest and exit")
		compareManifest      = flag.String("application-comparison-manifest", "", "frozen #4331 comparison manifest")
		compareTree          = flag.String("application-comparison-treedb", "", "TreeDB application artifact for #4331 validation")
		compareQdrant        = flag.String("application-comparison-qdrant", "", "Qdrant artifact for #4331 validation")
		compareOutput        = flag.String("application-comparison-output", "", "validated #4331 comparison JSON")
		compareReport        = flag.String("application-comparison-report", "", "validated #4331 comparison markdown")
		dumpMinima           = flag.String("dump-minima-manifest", "", "write the frozen compact Minima fixture/operation manifest and exit")
		minimaTree           = flag.String("minima-treedb-evidence", "", "TreeDB partial backend evidence to compare and validate")
		validateMinima       = flag.String("validate-minima-artifact", "", "validate one Minima JSON artifact fail closed and exit")
		minimaQdrant         = flag.String("minima-qdrant-evidence", "", "Qdrant partial backend evidence to compare and validate")
		minimaOutput         = flag.String("minima-output", "", "single validated Minima comparison JSON artifact")
		minimaReport         = flag.String("minima-report", "", "concise Minima comparison markdown report")
		minimaRecommendation = flag.String("minima-recommendation", "ready_with_alpha_limitations", "readiness recommendation for a clean validated comparison")
		minimaExpectedCommit = flag.String("minima-expected-commit", "", "full expected merged commit for completed TreeDB/final Minima evidence")
		cellWorker           = flag.Bool("cell-worker", false, "serve long-lived JSON-line cell requests for per-cell interleaving")
	)
	flag.Parse()
	cfg.Workload = strings.TrimSpace(*workload)
	switch cfg.Workload {
	case "application":
		if hasMinimaFlag(*dumpMinima, *validateMinima, *minimaTree, *minimaQdrant, *minimaOutput, *minimaReport, *minimaExpectedCommit) {
			fmt.Fprintln(os.Stderr, "treedb_rag_benchmark: Minima flags require -workload=minima")
			os.Exit(2)
		}
	case "minima":
		if *dumpMinima != "" {
			if err := writeMinimaManifest(*dumpMinima); err != nil {
				fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: dump Minima manifest: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("wrote %s\n", *dumpMinima)
			return
		}
		if *validateMinima != "" {
			artifact, err := readMinimaArtifact(*validateMinima)
			if err == nil {
				err = validateMinimaArtifact(&artifact)
			}
			if err == nil {
				required := artifact.State != "partial"
				for _, backend := range artifact.Backends {
					required = required || backend.Name == "treedb" && backend.Operations.ManifestOrdered
				}
				err = validateMinimaExpectedCommit(&artifact, strings.TrimSpace(*minimaExpectedCommit), required)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: validate Minima artifact: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("validated %s\n", *validateMinima)
			return
		}
		if *minimaTree == "" || *minimaQdrant == "" || *minimaOutput == "" || *minimaReport == "" {
			fmt.Fprintln(os.Stderr, "treedb_rag_benchmark: Minima execution requires both backend evidence paths, -minima-output, and -minima-report")
			os.Exit(2)
		}
		if err := compareMinimaEvidence(*minimaTree, *minimaQdrant, *minimaOutput, *minimaReport, *minimaRecommendation, strings.TrimSpace(*minimaExpectedCommit)); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: Minima comparison failed closed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\nwrote %s\n", *minimaOutput, *minimaReport)
		return
	default:
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: unknown workload %q\n", cfg.Workload)
		os.Exit(2)
	}
	if *dumpInputs != "" {
		if err := writeSemanticInputManifest(*dumpInputs); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: dump semantic inputs: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", *dumpInputs)
		return
	}
	if *dumpComparison != "" {
		if err := writeApplicationComparisonManifest(*dumpComparison); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: dump application comparison manifest: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", *dumpComparison)
		return
	}
	if *compareManifest != "" || *compareTree != "" || *compareQdrant != "" || *compareOutput != "" || *compareReport != "" {
		if *compareManifest == "" || *compareTree == "" || *compareQdrant == "" || *compareOutput == "" || *compareReport == "" {
			fmt.Fprintln(os.Stderr, "treedb_rag_benchmark: application comparison requires manifest, TreeDB, Qdrant, JSON output, and markdown report paths")
			os.Exit(2)
		}
		if err := compareApplicationEvidence(*compareManifest, *compareTree, *compareQdrant, *compareOutput, *compareReport); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: application comparison failed closed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\nwrote %s\n", *compareOutput, *compareReport)
		return
	}
	cfg.Dir = strings.TrimSpace(*dir)
	cfg.KeepDir = *keepDir
	cfg.ProductBaseSHA = strings.TrimSpace(*productSHA)
	cfg.HarnessRevision = strings.TrimSpace(*harnessSHA)
	cfg.HostNote = strings.TrimSpace(*hostNote)
	cfg.Command = append([]string(nil), os.Args...)
	cfg.FinalEvidence = !*smoke
	if *smoke {
		cfg.WarmupQueries = 3
		cfg.Repetitions = 1
		cfg.SamplesPerRep = 9
		cfg.IngestionReps = 1
	}
	if *cellWorker {
		if err := runApplicationCellWorker(cfg, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: cell worker: %v\n", err)
			os.Exit(1)
		}
		return
	}
	report, err := runApplicationBaseline(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: %v\n", err)
		os.Exit(1)
	}
	jsonPath, markdownPath, manifestPath, err := writeApplicationArtifacts(report, *outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: write artifacts: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("fixture SHA-256: %s\n", report.Provenance.FixtureSHA256)
	fmt.Printf("semantic vectors SHA-256: %s\n", report.Provenance.SemanticVectorSHA256)
	fmt.Printf("wrote %s\nwrote %s\nwrote %s\n", jsonPath, markdownPath, manifestPath)
}

func hasMinimaFlag(values ...string) bool {
	for _, value := range values {
		if value != "" {
			return true
		}
	}
	return false
}
