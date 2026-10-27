package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/cmd/internal/documentserviceowner"
)

func TestNativeServerCommandProcess(t *testing.T) {
	if os.Getenv("GOMAP_NATIVE_COMMAND_CHILD") != "1" {
		return
	}
	os.Args = append([]string{os.Args[0]}, strings.Split(os.Getenv("GOMAP_NATIVE_COMMAND_ARGS"), "\n")...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

func startNativeCommand(t *testing.T, dir string, extra ...string) (*exec.Cmd, *nativewire.Client) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	ctxProcess, cancelProcess := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancelProcess)
	cmd := exec.CommandContext(ctxProcess, os.Args[0], "-test.run=^TestNativeServerCommandProcess$")
	cmd.Env = append(os.Environ(), "GOMAP_NATIVE_COMMAND_CHILD=1", "GOMAP_NATIVE_COMMAND_ARGS="+strings.Join(append([]string{"-addr", addr, "-dir", dir}, extra...), "\n"))
	log, err := os.CreateTemp(t.TempDir(), "native-command-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	for {
		client, err := nativewire.DialContext(ctx, "tcp", addr)
		if err == nil {
			t.Cleanup(func() { _ = client.Close() })
			return cmd, client
		}
		if ctx.Err() != nil {
			raw, _ := os.ReadFile(log.Name())
			t.Fatalf("native command unavailable: %v\n%s", err, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNativeServerCommandSyncedACKSurvivesHardExit(t *testing.T) {
	dir := t.TempDir()
	cmd, client := startNativeCommand(t, dir)
	ctx := t.Context()
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: "wire", Indexes: []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}}}); err != nil {
		t.Fatal(err)
	}
	id, doc := []byte("acknowledged"), []byte(`{"tag":"blue"}`)
	ids, err := client.InsertBatch(ctx, "wire", collections.DocumentFormatJSON, [][]byte{id}, [][]byte{doc}, nativewire.AckSynced)
	if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], id) {
		t.Fatalf("synced ACK: ids=%q err=%v", ids, err)
	}
	// No explicit Flush, Checkpoint, signal shutdown or owner Close intervenes.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("hard-exit child returned success")
	}
	_ = client.Close()
	reopened, client := startNativeCommand(t, dir)
	docs, present, err := client.GetMany(ctx, "wire", [][]byte{id})
	if err != nil || len(present) != 1 || !present[0] || !bytes.Equal(docs[0], doc) {
		t.Fatalf("ACK lost after hard exit: docs=%q present=%v err=%v", docs, present, err)
	}
	got, truncated, err := client.IndexLookup(ctx, "wire", "tag", "blue", nativewire.CursorLimits{})
	if err != nil || truncated || len(got) != 1 || !bytes.Equal(got[0], id) {
		t.Fatalf("recovered index: %q truncated=%v err=%v", got, truncated, err)
	}
	if err := client.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err := reopened.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Wait(); err != nil {
		t.Fatalf("clean shutdown: %v", err)
	}
}

func TestNativeServerCommandIndexedCRUDAndDense(t *testing.T) {
	for _, profile := range []string{"command_wal_durable", "command_wal_relaxed", "no_wal_fast"} {
		t.Run(profile, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			// Compose this fixture with a service-seeded Dense index and a
			// separate wire-created CRUD collection. Metadata v7 supports
			// TextIndexes; this split does not express a transport limitation.
			owner, err := documentserviceowner.Open(treedb.OptionsFor(treedb.Profile(profile), dir), false)
			if err != nil {
				t.Fatal(err)
			}
			info, createErr := owner.Service.CreateIndex(ctx, documentservice.CreateIndexRequest{Name: "dense", Dimension: 2})
			if profile == "command_wal_durable" {
				if createErr != nil || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput {
					t.Fatalf("seed ordinary service index: %+v create=%v", info, createErr)
				}
			} else {
				if createErr == nil || documentservice.ErrorCodeOf(createErr) != documentservice.CodeInternal {
					t.Fatalf("unsupported profile accepted ordinary ColumnGraph: %+v %v", info, createErr)
				}
				if metas, err := owner.Manager.ListCollections(); err != nil || len(metas) != 0 {
					t.Fatalf("refused create retained schema: %+v %v", metas, err)
				}
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
			cmd, client := startNativeCommand(t, dir, "-profile", profile)
			meta := collections.CollectionMeta{Name: "docs", Indexes: []collections.IndexDefinition{{Name: "rank", Field: "meta.rank", ValueType: collections.IndexValueInt64}}}
			if _, err := client.CreateCollection(ctx, meta); err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("a"), []byte("b")}
			docs := [][]byte{[]byte(`{"id":"a","content":"refund","embedding":[1,0],"meta":{"rank":1}}`), []byte(`{"id":"b","content":"return","embedding":[0,1],"meta":{"rank":2}}`)}
			if _, err := client.InsertBatch(ctx, "docs", collections.DocumentFormatJSON, ids, docs, nativewire.AckVisible); err != nil {
				t.Fatal(err)
			}
			rangeIDs, truncated, err := client.IndexRange(ctx, "docs", "rank", nativewire.IndexRange{Lower: nativewire.Scalar{Type: collections.IndexValueInt64, Value: int64(1)}, LowerInclusive: true, Upper: nativewire.Scalar{Type: collections.IndexValueInt64, Value: int64(2)}, UpperInclusive: true})
			if err != nil || truncated || len(rangeIDs) != 2 {
				t.Fatalf("range: %q %v %v", rangeIDs, truncated, err)
			}
			if profile == "command_wal_durable" {
				if _, err := client.InsertBatch(ctx, "dense", collections.DocumentFormatJSON, ids, docs, nativewire.AckVisible); err != nil {
					t.Fatal(err)
				}
			}
			checkDense := func(client *nativewire.Client) {
				t.Helper()
				out, err := client.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: "dense", Query: []float32{1, 0}, TopK: 1, EfSearch: 8})
				if err != nil || len(out.Results) != 1 || string(out.Results[0].ID) != "a" || out.Route != documentservice.RouteAnn || out.TypedColumnGraph || out.FullDocumentScanFallbacks != 0 || out.ExactFallbacks != 0 {
					t.Fatalf("Dense: %+v %v", out, err)
				}
			}
			checkPending := func() {
				t.Helper()
				out, err := client.DenseVectorSearch(ctx, nativewire.DenseVectorSearchRequest{Index: "dense", Query: []float32{1, 0}, TopK: 1, EfSearch: 8})
				var wireErr *nativewire.WireError
				if !errors.As(err, &wireErr) || wireErr.Message != "consistency unavailable" || len(out.Results) != 0 || out.Route != "" || out.Exact || out.TypedColumnGraph || out.SearchPackSearch != 0 || out.ColumnGraphPreparedSearch != 0 {
					t.Fatalf("pending graph falsely served ANN: %+v %v", out, err)
				}
			}
			if profile == "command_wal_durable" {
				checkPending()
			}
			if matched, modified, err := client.ReplaceBatch(ctx, "docs", collections.DocumentFormatJSON, ids[:1], [][]byte{[]byte(`{"id":"a","content":"changed","embedding":[1,0],"meta":{"rank":3}}`)}, nativewire.AckVisible); err != nil || matched != 1 || modified != 1 {
				t.Fatalf("replace: %d/%d %v", matched, modified, err)
			}
			if deleted, err := client.DeleteBatch(ctx, "docs", ids[1:], nativewire.AckVisible); err != nil || deleted != 1 {
				t.Fatalf("delete: %d %v", deleted, err)
			}
			current, present, err := client.GetMany(ctx, "docs", ids)
			if err != nil || len(present) != 2 || !present[0] || present[1] || !bytes.Contains(current[0], []byte(`"rank":3`)) {
				t.Fatalf("CRUD state: docs=%q present=%v err=%v", current, present, err)
			}
			if err := client.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := client.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			if profile == "command_wal_durable" {
				// Persistence boundaries materialize rows, not a fresh ANN graph.
				checkPending()
			}
			_ = client.Close()
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			if profile == "command_wal_durable" {
				// The standalone wire server exposes no graph Optimize command.
				// Use the maintained public API after the original process stops.
				owner, err := documentserviceowner.Open(treedb.OptionsFor(treedb.Profile(profile), dir), false)
				if err != nil {
					t.Fatal(err)
				}
				_, optimizeErr := owner.Service.OptimizeIndex(ctx, "dense", documentservice.OptimizeIndexRequest{})
				closeErr := owner.Close()
				if optimizeErr != nil || closeErr != nil {
					t.Fatalf("offline graph maintenance: optimize=%v close=%v", optimizeErr, closeErr)
				}
			}
			cmd, client = startNativeCommand(t, dir, "-profile", profile)
			current, present, err = client.GetMany(ctx, "docs", ids)
			if err != nil || len(present) != 2 || !present[0] || present[1] || !bytes.Contains(current[0], []byte(`"rank":3`)) {
				t.Fatalf("reopened CRUD state: docs=%q present=%v err=%v", current, present, err)
			}
			if profile == "command_wal_durable" {
				checkDense(client)
			}
			_ = client.Close()
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeServerCommandListenFailureReleasesOwner(t *testing.T) {
	for _, flagName := range []string{"-addr", "-pprof"} {
		t.Run(flagName, func(t *testing.T) {
			busy, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer busy.Close()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeServerCommandProcess$")
			cmd.Env = append(os.Environ(), "GOMAP_NATIVE_COMMAND_CHILD=1", "GOMAP_NATIVE_COMMAND_ARGS="+strings.Join([]string{"-dir", dir, flagName, busy.Addr().String()}, "\n"))
			raw, err := cmd.CombinedOutput()
			if err == nil || !bytes.Contains(raw, []byte("listen")) {
				t.Fatalf("expected listen failure: %v %s", err, raw)
			}
			o, err := documentserviceowner.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir), false)
			if err != nil {
				t.Fatalf("startup leaked owner: %v", err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
