package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixedPeerCLISecureDefaultsBeforeStores(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(path, []byte(`{"NodeID":"test"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"serve", "inspect", "status", "ready", "diagnostics"} {
		var output bytes.Buffer
		err := runArgs(context.Background(), []string{"-config", path, "-mode", mode}, &output)
		if err == nil || !strings.Contains(err.Error(), "peer credentials required") || output.Len() != 0 {
			t.Fatalf("%s: %v %s", mode, err, output.String())
		}
	}
	var output bytes.Buffer
	if err := runArgs(context.Background(), []string{"-mode", "version", "-expected-binary-sha256", strings.Repeat("0", 64)}, &output); err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatal(err)
	}
	if err := runArgs(context.Background(), []string{"-mode", "version"}, &output); err != nil || !strings.Contains(output.String(), `"SHA256"`) {
		t.Fatalf("%v %s", err, output.String())
	}
}
