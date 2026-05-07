package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestServeTCPAndDialContext(t *testing.T) {
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx, ln) }()
	client, err := DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	_ = client.Close()
	cancel()
	_ = ln.Close()
	_ = server.Close()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestNewInProcessClientLocalEndpoint(t *testing.T) {
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	stats, err := client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got := stats["treedb.native_wire.connections.opened_total"]; got != "1" {
		t.Fatalf("opened_total=%q want 1", got)
	}
	if err := client.Goaway(ctx); err != nil {
		t.Fatalf("Goaway: %v", err)
	}
	if err := client.Ping(ctx); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Ping after Goaway err=%v want closed pipe", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if got := server.Stats()["treedb.native_wire.connections.closed_total"]; got != "1" {
		t.Fatalf("closed_total=%q want 1", got)
	}
}

func TestServeUnixSocketAndDialContext(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets are not available on windows")
	}
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := unixSocketPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx, ln) }()
	client, err := DialContext(ctx, "unix", path)
	if err != nil {
		t.Fatalf("DialContext unix: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	_ = client.Close()
	cancel()
	_ = ln.Close()
	_ = server.Close()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve unix: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve unix did not stop")
	}
}

func unixSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "nw")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}
