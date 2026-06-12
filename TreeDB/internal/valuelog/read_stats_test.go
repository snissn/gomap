package valuelog

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestManagerReadStatsCountsVerifyCRCChecks(t *testing.T) {
	dir := t.TempDir()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptr1, err := writer.Append(0, nil, 1, []byte("alpha"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append alpha: %v", err)
	}
	ptr2, err := writer.Append(0, nil, 2, []byte("beta"))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append beta: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	if got := m.ReadStats().RecordCRCChecks; got != 0 {
		t.Fatalf("initial CRC checks=%d want 0", got)
	}
	got1, err := m.Read(ptr1)
	if err != nil {
		t.Fatalf("read alpha: %v", err)
	}
	if !bytes.Equal(got1, []byte("alpha")) {
		t.Fatalf("read alpha=%q", got1)
	}
	if got := m.ReadStats().RecordCRCChecks; got != 1 {
		t.Fatalf("CRC checks after first verified read=%d want 1", got)
	}

	got2, _, err := m.ReadUnsafeTo(ptr2, make([]byte, 0, len("beta")))
	if err != nil {
		t.Fatalf("read beta: %v", err)
	}
	if !bytes.Equal(got2, []byte("beta")) {
		t.Fatalf("read beta=%q", got2)
	}
	if got := m.ReadStats().RecordCRCChecks; got != 2 {
		t.Fatalf("CRC checks after second verified read=%d want 2", got)
	}

	m.SetDisableReadChecksum(true)
	before := m.ReadStats().RecordCRCChecks
	got1, err = m.Read(ptr1)
	if err != nil {
		t.Fatalf("read alpha checksum-disabled: %v", err)
	}
	if !bytes.Equal(got1, []byte("alpha")) {
		t.Fatalf("checksum-disabled read alpha=%q", got1)
	}
	if got := m.ReadStats().RecordCRCChecks; got != before {
		t.Fatalf("checksum-disabled read changed CRC checks from %d to %d", before, got)
	}
}
