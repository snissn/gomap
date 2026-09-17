// treedb-fixed-peer runs one trusted-network, fixed-membership TreeDB node.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

func run() error {
	path := flag.String("config", "", "required fixed-peer JSON configuration file")
	flag.Parse()
	if *path == "" || flag.NArg() != 0 {
		return fmt.Errorf("usage: treedb-fixed-peer -config node.json")
	}
	file, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer file.Close()
	config, err := readConfig(file)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	node, err := nativewire.OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		return err
	}
	// Open is not readiness: inspect quorum-backed routes and observational status.
	status, err := node.Status(ctx)
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(status)
	}
	if err != nil {
		_ = node.Close()
		return err
	}
	<-ctx.Done()
	return node.Close()
}

func readConfig(reader io.Reader) (nativewire.FixedPeerTCPConfigV1, error) {
	var config nativewire.FixedPeerTCPConfigV1
	raw, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil {
		return config, err
	}
	if len(raw) > 1<<20 {
		return config, fmt.Errorf("configuration exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return config, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return config, fmt.Errorf("invalid trailing config payload")
	}
	return config, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
