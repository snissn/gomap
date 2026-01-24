package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	suite := flag.String("suite", "vlog_autotune", "Benchmark suite to run")
	caseName := flag.String("case", "", "Case to run (default: all)")
	jsonOut := flag.Bool("json", false, "Emit JSON output")
	validate := flag.Bool("validate", false, "Exit non-zero on mark failure")
	flag.Parse()

	switch *suite {
	case "vlog_autotune":
		report, err := runVlogAutotuneSuite(*caseName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unified_bench: %v\n", err)
			os.Exit(1)
		}
		if *jsonOut {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(report)
		} else {
			printVlogAutotuneReport(report)
		}
		if *validate && report.Failures > 0 {
			os.Exit(2)
		}
	default:
		fmt.Fprintf(os.Stderr, "unified_bench: unknown suite %q\n", *suite)
		os.Exit(1)
	}
}
