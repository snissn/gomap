package nativewire

import (
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fixedPeerCloseObservedListenerV1 struct {
	net.Listener
	once   sync.Once
	closed chan struct{}
}

func (l *fixedPeerCloseObservedListenerV1) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func fixedPeerAwaitCloseEventV1(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not join", what)
	}
}

func TestFixedPeerRuntimeCloseJoinsDelayedServeV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &fixedPeerCloseObservedListenerV1{Listener: listener, closed: make(chan struct{})}
	runtime := &FixedPeerTCPRuntimeV1{
		listener: observed, server: &http.Server{}, serveDone: make(chan struct{}),
		config: FixedPeerTCPConfigV1{RequestTimeout: 3 * time.Second},
	}
	allowServe := make(chan struct{})
	releaseServe := sync.OnceFunc(func() { close(allowServe) })
	go func() {
		defer close(runtime.serveDone)
		<-allowServe
		_ = runtime.server.Serve(observed)
	}()
	closeDone, closeErr := make(chan struct{}), make(chan error, 1)
	go func() {
		defer close(closeDone)
		closeErr <- runtime.Close()
	}()
	defer func() {
		releaseServe()
		fixedPeerAwaitCloseEventV1(t, runtime.serveDone, "delayed Serve")
		fixedPeerAwaitCloseEventV1(t, closeDone, "Close")
		_ = listener.Close()
	}()
	fixedPeerAwaitCloseEventV1(t, observed.closed, "consumed listener Close")
	replacement, err := net.Listen("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("consumed listener still owns address: %v", err)
	}
	defer replacement.Close()
	select {
	case <-closeDone:
		t.Fatal("Close returned before its gated Serve goroutine completed")
	default:
	}
	releaseServe()
	fixedPeerAwaitCloseEventV1(t, closeDone, "Close")
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}
}

func TestFixedPeerRuntimeCloseDrainsActiveHTTPV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &fixedPeerCloseObservedListenerV1{Listener: listener, closed: make(chan struct{})}
	entered, release := make(chan struct{}), make(chan struct{})
	releaseRequest := sync.OnceFunc(func() { close(release) })
	runtime := &FixedPeerTCPRuntimeV1{
		listener: observed, serveDone: make(chan struct{}),
		config: FixedPeerTCPConfigV1{RequestTimeout: 3 * time.Second},
		server: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			_, _ = io.WriteString(w, "drained")
		})},
	}
	go func() {
		defer close(runtime.serveDone)
		_ = runtime.server.Serve(observed)
	}()
	requestDone := make(chan struct{})
	requestErr := make(chan error, 1)
	requestBody := make(chan string, 1)
	var closeDone chan struct{}
	client := &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	go func() {
		defer close(requestDone)
		response, err := client.Get("http://" + listener.Addr().String())
		if err != nil {
			requestErr <- err
			return
		}
		body, err := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if err == nil {
			err = closeErr
		}
		requestBody <- string(body)
		requestErr <- err
	}()
	defer func() {
		releaseRequest()
		_ = runtime.Close()
		fixedPeerAwaitCloseEventV1(t, requestDone, "active HTTP request")
		fixedPeerAwaitCloseEventV1(t, runtime.serveDone, "Serve")
		if closeDone != nil {
			fixedPeerAwaitCloseEventV1(t, closeDone, "Close")
		}
		client.CloseIdleConnections()
	}()
	fixedPeerAwaitCloseEventV1(t, entered, "HTTP handler entry")
	closeDone = make(chan struct{})
	closeErr := make(chan error, 1)
	go func() {
		defer close(closeDone)
		closeErr <- runtime.Close()
	}()
	fixedPeerAwaitCloseEventV1(t, observed.closed, "listener shutdown")
	select {
	case <-closeDone:
		t.Fatal("Close returned before the admitted HTTP handler drained")
	default:
	}
	releaseRequest()
	fixedPeerAwaitCloseEventV1(t, requestDone, "active HTTP request")
	if err := <-requestErr; err != nil {
		t.Fatal(err)
	}
	if body := <-requestBody; body != "drained" {
		t.Fatalf("admitted HTTP response = %q, want drained", body)
	}
	fixedPeerAwaitCloseEventV1(t, closeDone, "Close")
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}
}
