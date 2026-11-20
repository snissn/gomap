//go:build ignore

// Invoke the live standalone GoMap owner's private benchmark checkpoint control.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/snissn/gomap/internal/ycsbevidence"
)

func run() error {
	evidence := flag.String("owner-evidence", "", "live gateway evidence directory")
	source := flag.String("source", "", "GoMap source commit")
	trial := flag.String("trial", "", "LOAD trial identity")
	log := flag.String("load-log", "", "validated original YCSB LOAD log")
	flag.Parse()
	if *evidence == "" || *source == "" || *trial == "" || *log == "" {
		return errors.New("owner-evidence, source, trial and load-log are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	receipt, err := ycsbevidence.RequestCheckpoint(ctx, *evidence, *source, *trial, *log)
	return errors.Join(err, json.NewEncoder(os.Stdout).Encode(receipt))
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
