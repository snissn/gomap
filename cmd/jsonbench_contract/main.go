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
	SchemaVersion    string `json:"schema_version"`
	Valid            bool   `json:"valid"`
	Manifest         string `json:"manifest"`
	TreeDBResult     string `json:"treedb_result"`
	RequestedProfile string `json:"requested_profile"`
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

	resultPath := manifest.TreeDB.ResultPath
	if !filepath.IsAbs(resultPath) {
		resultPath = filepath.Join(manifestDir, resultPath)
	}
	return json.NewEncoder(stdout).Encode(validationSummary{
		SchemaVersion:    manifest.SchemaVersion,
		Valid:            true,
		Manifest:         *manifestPath,
		TreeDBResult:     resultPath,
		RequestedProfile: manifest.TreeDB.RequestedProfile,
	})
}
