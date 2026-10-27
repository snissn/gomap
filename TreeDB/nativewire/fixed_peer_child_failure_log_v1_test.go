package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const fixedPeerRecoveredPanicMarkerV1 = "fixed-peer-recovered-handler-panic"
const fixedPeerCleanExitMarkerV1 = "fixed-peer-recovered-handler-clean-exit"

// A handler panic closes its request, but net/http recovers it and the process
// can still exit successfully. The failing parent must retain that child's log.
func TestFixedPeerChildFailureLogRetainsRecoveredHTTPPanicV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFixedPeerChildFailureLogParentV1$", "-test.v")
	command.Env = append(os.Environ(), "GOMAP_FIXED_PEER_FAILURE_LOG_PARENT=1")
	raw, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("intentional parent failure: %v\n%s", err, raw)
	}
	for _, marker := range []string{
		"control witnessed recovered handler and successful child exit",
		"http: panic serving", fixedPeerRecoveredPanicMarkerV1,
		"net/http.(*conn).serve.func1", fixedPeerCleanExitMarkerV1,
	} {
		if !strings.Contains(string(raw), marker) {
			t.Errorf("failed parent omitted child diagnostic %q\n%s", marker, raw)
		}
	}
}

func TestFixedPeerChildFailureLogParentV1(t *testing.T) {
	if os.Getenv("GOMAP_FIXED_PEER_FAILURE_LOG_PARENT") != "1" {
		return
	}
	log, err := os.CreateTemp(t.TempDir(), "recovered-handler-child-")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	command := exec.Command(os.Args[0], "-test.run=^TestFixedPeerRecoveredHTTPPanicProcessV1$", "-test.v")
	command.Env = append(os.Environ(), "GOMAP_FIXED_PEER_FAILURE_LOG_CHILD=1")
	command.Stdout, command.Stderr = log, log
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &fixedPeerTestProcessV1{command: command, input: input, log: log}
	t.Cleanup(func() { p.stop(t) })
	t.Error("intentional parent failure before child cleanup")
	p.stop(t)
	if !command.ProcessState.Success() {
		t.Fatal("control child did not exit successfully")
	}
	raw, err := os.ReadFile(log.Name())
	if err != nil || !strings.Contains(string(raw), "http: panic serving") || !strings.Contains(string(raw), fixedPeerCleanExitMarkerV1) {
		t.Fatal("control child did not recover its handler and complete", err)
	}
	t.Log("control witnessed recovered handler and successful child exit")
}

func TestFixedPeerRecoveredHTTPPanicProcessV1(t *testing.T) {
	if os.Getenv("GOMAP_FIXED_PEER_FAILURE_LOG_CHILD") != "1" {
		return
	}
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(fixedPeerRecoveredPanicMarkerV1)
	}))
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Post(server.URL, "application/json", strings.NewReader("{}"))
	if response != nil {
		response.Body.Close()
	}
	server.Close()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("recovered handler request error: %v", err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	fmt.Println(fixedPeerCleanExitMarkerV1)
}
