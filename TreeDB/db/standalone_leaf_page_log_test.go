package db

import (
	"fmt"
	"testing"
)

func TestStandaloneLeafPageLogEnablesDirectOuterLeafWrites(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionOff,
	})
	if err != nil {
		t.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	d.SetLeafPageLog(leafLog)

	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		value := []byte(fmt.Sprintf("v%04d", i))
		if err := d.Set(key, value); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close db: %v", err)
	}
	if err := leafLog.Close(); err != nil {
		t.Fatalf("Close leaf log: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("k0255"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "v0255" {
		t.Fatalf("value after reopen=%q, want v0255", got)
	}
}
