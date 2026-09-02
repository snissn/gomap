package templatedb

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNoInternalDBImport(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "github.com/snissn/gomap/TreeDB/internal/templatedb")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	if strings.Contains(string(out), "github.com/snissn/gomap/TreeDB/db") {
		t.Fatalf("templatedb must not depend on TreeDB/db")
	}
}
