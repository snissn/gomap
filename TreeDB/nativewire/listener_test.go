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

func TestServeContextCancelClosesListener(t *testing.T) {
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx, ln) }()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve err=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after context cancel")
	}
	_ = server.Close()
}

func TestServerCloseClosesListener(t *testing.T) {
	server := NewServer(ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(context.Background(), ln) }()
	waitForRegisteredListener(t, server)
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve err=%v want nil after server close", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after server close")
	}
}

func TestServeMaxConnectionsAppliesAcrossListeners(t *testing.T) {
	server := NewServer(ServerOptions{MaxConnections: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp 1: %v", err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp 2: %v", err)
	}
	errCh := make(chan error, 2)
	go func() { errCh <- server.Serve(ctx, ln1) }()
	go func() { errCh <- server.Serve(ctx, ln2) }()
	waitForRegisteredListenerCount(t, server, 2)

	client, err := DialContext(ctx, "tcp", ln1.Addr().String())
	if err != nil {
		t.Fatalf("DialContext first: %v", err)
	}
	defer client.Close()

	secondCtx, secondCancel := context.WithTimeout(ctx, time.Second)
	defer secondCancel()
	if second, err := DialContext(secondCtx, "tcp", ln2.Addr().String()); err == nil {
		_ = second.Close()
		t.Fatal("DialContext second succeeded despite MaxConnections=1")
	}
	waitForCounter(t, server, "connections.rejected_total", 1)

	cancel()
	_ = server.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Serve[%d] err=%v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Serve[%d] did not stop", i)
		}
	}
}

func waitForRegisteredListener(t *testing.T, server *Server) {
	t.Helper()
	waitForRegisteredListenerCount(t, server, 1)
}

func waitForRegisteredListenerCount(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		server.connMu.Lock()
		registered := len(server.listeners)
		server.connMu.Unlock()
		if registered >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("registered listeners=%d want %d", registered, want)
		case <-tick.C:
		}
	}
}

func TestNewInProcessClientRejectsNilServer(t *testing.T) {
	client, cleanup, err := NewInProcessClient(context.Background(), nil)
	if !errors.Is(err, ErrServerClosed) {
		t.Fatalf("NewInProcessClient err=%v want server closed", err)
	}
	if client != nil || cleanup != nil {
		t.Fatal("expected nil client and cleanup")
	}
}

func TestNewInProcessClientCleanupIsIdempotent(t *testing.T) {
	server := NewServer(ServerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup first: %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup second: %v", err)
	}
	_ = server.Close()
}

func unixSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "s.sock")
}
