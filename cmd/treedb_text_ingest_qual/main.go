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
	var manifestPath, reportPath, produceDir, produceModeDir, produceModeName string
	var produceScale, repetition int
	flag.StringVar(&manifestPath, "manifest", "", "path to manifest.json")
	flag.StringVar(&reportPath, "report", "", "path to report.json")
	flag.StringVar(&produceDir, "produce-smoke", "", "directory for real raw rows for all modes")
	flag.StringVar(&produceModeName, "produce-mode", "", "internal: produce one mode in a fresh child process")
	flag.StringVar(&produceModeDir, "produce-dir", "", "internal: raw-row directory for -produce-mode")
	flag.IntVar(&produceScale, "scale", 10_000, "source documents for producer modes")
	flag.IntVar(&repetition, "repetition", 1, "retained repetition number for producer modes")
	flag.Parse()
	if produceModeName != "" {
		if produceModeDir == "" {
			fmt.Fprintln(os.Stderr, "-produce-mode requires -produce-dir")
			os.Exit(2)
		}
		if err := produceOneMode(produceModeDir, produceModeName, produceScale, repetition); err != nil {
			fmt.Fprintf(os.Stderr, "produce mode: %v\\n", err)
			os.Exit(1)
		}
		return
	}
	if produceDir != "" {
		if err := produceSmoke(produceDir, produceScale, repetition); err != nil {
			fmt.Fprintf(os.Stderr, "produce smoke: %v\\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote raw smoke rows to %s (not a retained report)\\n", produceDir)
		return
	}
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
