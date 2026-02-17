package main

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/internal/benchprof"
)

func main() {
	if err := benchprof.RunFromArgs(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: %v\n", err)
		os.Exit(1)
	}
}
