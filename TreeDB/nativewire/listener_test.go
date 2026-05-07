package nativewire

import (
	"context"
	"errors"
	"net"
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

func TestServeUnixSocketAndDialContext(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix sockets are not available on windows")
	}
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "nativewire.sock")
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
