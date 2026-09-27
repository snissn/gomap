// treedb-fixed-peer runs an authenticated, fixed-membership TreeDB node.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
)

type binaryIdentity struct {
	SHA256, GoVersion, OS, Architecture, Revision string
	Modified                                      bool
	VCSAvailable                                  bool
}

func inspectBinary() (binaryIdentity, error) {
	result := binaryIdentity{GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH}
	path, err := os.Executable()
	if err != nil {
		return result, err
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err = io.Copy(digest, file); err != nil {
		return result, err
	}
	result.SHA256 = hex.EncodeToString(digest.Sum(nil))
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				result.Revision = setting.Value
				result.VCSAvailable = true
			case "vcs.modified":
				result.Modified = setting.Value == "true"
			}
		}
	}
	return result, nil
}
func runArgs(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("treedb-fixed-peer", flag.ContinueOnError)
	path := flags.String("config", "", "fixed-peer JSON configuration file")
	mode := flags.String("mode", "serve", "serve, inspect, status, ready, diagnostics, or version")
	trusted := flags.Bool("trusted-network-test", false, "explicitly allow plaintext legacy test fixtures")
	expected := flags.String("expected-binary-sha256", "", "require this executable SHA-256 before any stores or network activity")
	interval := flags.Duration("diagnostics-interval", 0, "emit diagnostics while serving; zero disables, minimum 1s")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	switch *mode {
	case "serve", "inspect", "status", "ready", "diagnostics", "version":
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
	if *interval < 0 || (*interval > 0 && *interval < time.Second) || *interval > time.Hour {
		return fmt.Errorf("diagnostics interval must be zero or 1s..1h")
	}
	binary, err := inspectBinary()
	if err != nil {
		return err
	}
	if *expected != "" {
		raw, e := hex.DecodeString(*expected)
		if e != nil || len(raw) != sha256.Size || strings.ToLower(*expected) != binary.SHA256 {
			return fmt.Errorf("executable SHA-256 mismatch")
		}
	}
	encoder := json.NewEncoder(output)
	if *mode == "version" {
		return encoder.Encode(binary)
	}
	if *path == "" {
		return fmt.Errorf("-config is required")
	}
	file, err := os.Open(*path)
	if err != nil {
		return err
	}
	config, err := readConfig(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if config.Credentials == nil && !*trusted {
		return fmt.Errorf("peer credentials required; plaintext is restricted to explicit -trusted-network-test fixtures")
	}
	identity, err := nativewire.InspectFixedPeerTCPConfigV1(config)
	if err != nil {
		return err
	}
	if *mode == "inspect" {
		return encoder.Encode(struct {
			Binary binaryIdentity
			Config nativewire.FixedPeerConfigIdentityV1
		}{binary, identity})
	}
	if *mode != "serve" {
		client, e := nativewire.NewFixedPeerTCPClientV1(config)
		if e != nil {
			return e
		}
		defer client.Close()
		switch *mode {
		case "status":
			report, e := client.Status(ctx, config.NodeID)
			if e != nil {
				return e
			}
			return encoder.Encode(report)
		case "ready":
			report, e := client.ReadinessV1(ctx, config.NodeID)
			if out := encoder.Encode(report); out != nil {
				return out
			}
			return e
		case "diagnostics":
			report, e := client.DiagnosticsV1(ctx, config.NodeID)
			if out := encoder.Encode(report); out != nil {
				return out
			}
			return e
		}
	}
	node, err := nativewire.OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		return err
	}
	defer node.Close()
	// Opening listeners is liveness only. Clients use -mode ready for fresh
	// catalog and hosted data-group quorum/apply evidence.
	if err := encoder.Encode(struct {
		Event  string
		Binary binaryIdentity
		Config nativewire.FixedPeerConfigIdentityV1
	}{"started", binary, identity}); err != nil {
		return err
	}
	var ticks <-chan time.Time
	if *interval > 0 {
		timer := time.NewTicker(*interval)
		defer timer.Stop()
		ticks = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			node.BeginDrainV1()
			return node.Close()
		case <-ticks:
			report, e := node.DiagnosticsV1(ctx)
			event := struct {
				Event  string
				Report nativewire.FixedPeerDiagnosticsV1
				Error  string `json:",omitempty"`
			}{Event: "diagnostics", Report: report}
			if e != nil {
				event.Error = e.Error()
			}
			if err := encoder.Encode(event); err != nil {
				return err
			}
		}
	}
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runArgs(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
