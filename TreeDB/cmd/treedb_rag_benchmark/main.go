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
		outDir     = flag.String("out-dir", ".", "directory receiving JSON, markdown, and artifact manifest")
		dir        = flag.String("dir", "", "benchmark database root (default: temporary)")
		keepDir    = flag.Bool("keep-dir", false, "keep a temporary benchmark database root")
		productSHA = flag.String("product-base-sha", "99929cdeb2ae2ec1e411236c853eb36942075d72", "exact accepted main product SHA")
		hostNote   = flag.String("host-note", "", "free-form host note recorded in provenance")
		smoke      = flag.Bool("smoke", false, "run a bounded diagnostic that cannot claim final p99/QPS evidence")
		dumpInputs = flag.String("dump-semantic-inputs", "", "write the exact semantic generation input manifest and exit")
	)
	flag.Parse()
	if *dumpInputs != "" {
		if err := writeSemanticInputManifest(*dumpInputs); err != nil {
			fmt.Fprintf(os.Stderr, "treedb_rag_benchmark: dump semantic inputs: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", *dumpInputs)
		return
	}
	cfg.Dir = strings.TrimSpace(*dir)
	cfg.KeepDir = *keepDir
	cfg.ProductBaseSHA = strings.TrimSpace(*productSHA)
	cfg.HostNote = strings.TrimSpace(*hostNote)
	cfg.Command = append([]string(nil), os.Args...)
	cfg.FinalEvidence = !*smoke
	if *smoke {
		cfg.WarmupQueries = 3
		cfg.Repetitions = 1
		cfg.SamplesPerRep = 9
		cfg.IngestionReps = 1
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
