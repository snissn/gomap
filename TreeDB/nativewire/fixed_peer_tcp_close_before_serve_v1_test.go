package nativewire

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Hold the actual Serve call before registration, rather than relying on which
// goroutine the scheduler runs first after Open returns.
func TestFixedPeerRuntimeCloseBeforeServeRegistrationV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	runtime := &FixedPeerTCPRuntimeV1{listener: listener, server: &http.Server{}, config: FixedPeerTCPConfigV1{RequestTimeout: time.Second}}
	allowServe, serveDone := make(chan struct{}), make(chan struct{})
	releaseServe := sync.OnceFunc(func() { close(allowServe) })
	go func() {
		defer close(serveDone)
		<-allowServe
		_ = runtime.server.Serve(listener)
	}()
	defer func() {
		releaseServe()
		select {
		case <-serveDone:
		case <-time.After(3 * time.Second):
			t.Error("gated Serve did not join")
		}
	}()
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Close returned before unregistered public listener closed: %v", err)
	}
	defer replacement.Close()
	releaseServe()
	select {
	case <-serveDone:
	case <-time.After(3 * time.Second):
		t.Fatal("late Serve did not join")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}
