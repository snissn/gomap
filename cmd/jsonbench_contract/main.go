// Command jsonbench_contract validates canonical TreeDB JSONBench sidecar
// manifests after collection, outside measured benchmark intervals.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/internal/jsonbenchcontract"
)

type validationSummary struct {
	SchemaVersion       string   `json:"schema_version"`
	Valid               bool     `json:"valid"`
	Manifest            string   `json:"manifest"`
	IndependentAttempts int      `json:"independent_attempts"`
	TreeDBResults       []string `json:"treedb_results"`
	ClickHouseResults   []string `json:"clickhouse_results"`
	RequestedProfile    string   `json:"requested_profile"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("jsonbench_contract", flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifestPath := flags.String("manifest", "", "path to the canonical JSONBench sidecar manifest")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *manifestPath == "" {
		return fmt.Errorf("-manifest is required")
	}

	manifest, err := jsonbenchcontract.LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	manifestDir := filepath.Dir(*manifestPath)
	if err := jsonbenchcontract.Validate(manifest, manifestDir); err != nil {
		return err
	}

	treeDBResults := resolvePaths(manifestDir, manifest.TreeDB.ResultPaths)
	clickHouseResults := resolvePaths(manifestDir, manifest.ClickHouse.ResultPaths)
	return json.NewEncoder(stdout).Encode(validationSummary{
		SchemaVersion:       manifest.SchemaVersion,
		Valid:               true,
		Manifest:            *manifestPath,
		IndependentAttempts: manifest.Comparison.Attempts,
		TreeDBResults:       treeDBResults,
		ClickHouseResults:   clickHouseResults,
		RequestedProfile:    manifest.TreeDB.RequestedProfile,
	})
}

func resolvePaths(baseDir string, paths []string) []string {
	resolved := make([]string, len(paths))
	for index, path := range paths {
		if !filepath.IsAbs(path) {
			path = filepath.Join(baseDir, path)
		}
		resolved[index] = path
	}
	return resolved
}
