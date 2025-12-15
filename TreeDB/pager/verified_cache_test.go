package pager

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestVerifiedCache_InvalidationOnGetForWriteAndWrite(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	id, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	p.MarkVerified(id)
	if !p.IsVerified(id) {
		t.Fatalf("expected page %d to be verified", id)
	}

	if _, err := p.GetForWrite(id); err != nil {
		t.Fatalf("GetForWrite: %v", err)
	}
	if p.IsVerified(id) {
		t.Fatalf("expected page %d to be unverified after GetForWrite", id)
	}

	p.MarkVerified(id)
	if !p.IsVerified(id) {
		t.Fatalf("expected page %d to be verified", id)
	}

	zeros := make([]byte, page.PageSize)
	if err := p.Write(id, zeros); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if p.IsVerified(id) {
		t.Fatalf("expected page %d to be unverified after Write", id)
	}
}
