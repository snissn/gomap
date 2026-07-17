package treedb_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const leafPrefixCompressedFlag uint16 = 0x8000

func leafFlagsInIndex(t *testing.T, indexPath string) uint16 {
	t.Helper()

	p, err := pager.OpenReadOnly(indexPath, 256*1024)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer func() { _ = p.Close() }()

	for id := uint64(2); id < p.PageCount(); id++ {
		data, err := p.Get(id)
		if err != nil {
			t.Fatalf("pager get %d: %v", id, err)
		}
		n := node.NewNodeView(data)
		if !n.VerifyChecksum() {
			continue
		}
		if n.Type() != page.PageTypeLeaf {
			continue
		}
		return binary.LittleEndian.Uint16(data[12:14])
	}

	t.Fatalf("no leaf pages found in %s", indexPath)
	return 0
}

func TestOfflineMaintenanceCommandWALGateBeforeSideStoreOpen(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := treedbdb.SaveFormatConfig(mainDir, treedbdb.FormatConfig{RequiredFeatures: []string{treedbdb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	// If public offline maintenance reaches side-store wiring, this malformed
	// dictdb path returns a different error. Index vacuum supports command-WAL
	// DBs and should reach that validation; value-log rewrite must reject
	// command-WAL before side-store wiring, even when IgnoreFormatConfig is set.
	if err := os.MkdirAll(filepath.Join(root, "dictdb", "index.db"), 0o755); err != nil {
		t.Fatalf("mkdir malformed dictdb index: %v", err)
	}

	for _, ignoreFormatConfig := range []bool{false, true} {
		err := treedb.VacuumIndexOffline(treedb.Options{Dir: root, IgnoreFormatConfig: ignoreFormatConfig})
		if err == nil || errors.Is(err, treedb.ErrCommandWALUnsupported) || !strings.Contains(err.Error(), "dictdb index path is a directory") {
			t.Fatalf("VacuumIndexOffline IgnoreFormatConfig=%v error=%v, want malformed dictdb error", ignoreFormatConfig, err)
		}
		if _, err := treedb.ValueLogRewriteOffline(treedb.Options{Dir: root, IgnoreFormatConfig: ignoreFormatConfig}); !errors.Is(err, treedb.ErrCommandWALUnsupported) {
			t.Fatalf("ValueLogRewriteOffline IgnoreFormatConfig=%v error=%v, want ErrCommandWALUnsupported", ignoreFormatConfig, err)
		}
	}
}

func TestVacuumIndexOffline_LoadsPersistedFormatConfig(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                   dir,
		DisableSideStores:     true,
		Durability:            treedb.DurabilityWALOffRelaxed,
		LeafPrefixCompression: true,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := bytes.Repeat([]byte{byte(i)}, 16)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := treedb.VacuumIndexOffline(treedb.Options{Dir: dir}); err != nil {
		t.Fatalf("VacuumIndexOffline: %v", err)
	}

	flags := leafFlagsInIndex(t, filepath.Join(dir, "index.db"))
	if flags&leafPrefixCompressedFlag == 0 {
		t.Fatalf("expected vacuum to preserve leaf prefix compression (flags=%#x)", flags)
	}
}

func TestValueLogRewriteOffline_LoadsPersistedFormatConfig(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.LeafPrefixCompression = true
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("p%04d", i))
		val := bytes.Repeat([]byte{byte(i)}, 64)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", string(key), err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	stats, err := treedb.ValueLogRewriteOffline(treedb.OptionsFor(treedb.ProfileNoWALFast, dir))
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.RecordsCopied == 0 {
		t.Fatalf("expected offline rewrite to copy records, stats=%+v", stats)
	}

	flags := leafFlagsInIndex(t, filepath.Join(dir, "index.db"))
	if flags&leafPrefixCompressedFlag == 0 {
		t.Fatalf("expected rewrite to preserve leaf prefix compression (flags=%#x)", flags)
	}
}
