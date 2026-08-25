// treedb_text_ingest_qual validates retained pure-text qualification artifacts.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	var manifestPath, reportPath string
	flag.StringVar(&manifestPath, "manifest", "", "path to manifest.json")
	flag.StringVar(&reportPath, "report", "", "path to report.json")
	flag.Parse()
	if manifestPath == "" || reportPath == "" {
		fmt.Fprintln(os.Stderr, "usage: treedb_text_ingest_qual -manifest manifest.json -report report.json")
		os.Exit(2)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read manifest: %v\n", err)
		os.Exit(1)
	}
	var manifest manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		fmt.Fprintf(os.Stderr, "decode manifest: %v\n", err)
		os.Exit(1)
	}
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read report: %v\n", err)
		os.Exit(1)
	}
	var report report
	if err := json.Unmarshal(reportBytes, &report); err != nil {
		fmt.Fprintf(os.Stderr, "decode report: %v\n", err)
		os.Exit(1)
	}
	sum := sha256.Sum256(manifestBytes)
	if err := validate(manifest, report, hex.EncodeToString(sum[:])); err != nil {
		fmt.Fprintf(os.Stderr, "invalid qualification artifact: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("valid pure-text qualification artifact")
}
