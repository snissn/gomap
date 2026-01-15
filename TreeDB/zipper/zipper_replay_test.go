package zipper

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
)

const (
	replayPathEnv       = "TREEDB_ZIPPER_REPLAY_PATH"
	replayStreamPathEnv = "TREEDB_ZIPPER_REPLAY_STREAM_PATH"
)

type replayOp struct {
	Type  string
	Key   []byte
	Value []byte
}

type replayDump struct {
	Version int64
	Store   string
	Ops     []replayOp
}

type replayStreamHeader struct {
	Version int64
	Store   string
	Created int64
}

func TestZipperReplayDepthLimit(t *testing.T) {
	path := strings.TrimSpace(os.Getenv(replayPathEnv))
	if path == "" {
		t.Skipf("set %s to a bench trace to replay", replayPathEnv)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	var dump replayDump
	gob.RegisterName("main.traceDump", replayDump{})
	gob.RegisterName("main.traceOp", replayOp{})
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&dump); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if len(dump.Ops) == 0 {
		t.Fatalf("trace has no ops")
	}

	replayOps(t, dump.Ops)
}

func TestZipperReplayDepthLimitStream(t *testing.T) {
	path := strings.TrimSpace(os.Getenv(replayStreamPathEnv))
	if path == "" {
		t.Skipf("set %s to a bench trace stream to replay", replayStreamPathEnv)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	dec := gob.NewDecoder(bytes.NewReader(raw))
	gob.RegisterName("main.traceStreamHeader", replayStreamHeader{})
	gob.RegisterName("main.traceOp", replayOp{})
	var header replayStreamHeader
	if err := dec.Decode(&header); err != nil {
		t.Fatalf("decode stream header: %v", err)
	}
	var ops []replayOp
	for {
		var op replayOp
		if err := dec.Decode(&op); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode stream op: %v", err)
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		t.Fatalf("stream trace has no ops")
	}
	replayOps(t, ops)
}

func replayOps(t *testing.T, ops []replayOp) {
	t.Helper()

	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	// Match bench defaults explicitly.
	z.SetLeafPrefixCompression(false)
	z.SetFillTargets(1_000_000, 1_000_000)

	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	b := batch.New(sm, page.DefaultInlineThreshold)
	defer func() { _ = b.Close() }()
	applyBatch := func() bool {
		newRoot, _, _, err := z.Apply(rootID, b)
		if err != nil {
			if strings.Contains(err.Error(), "tree too deep") {
				return true
			}
			t.Fatalf("apply failed: %v", err)
		}
		rootID = newRoot
		_ = b.Close()
		b = batch.New(sm, page.DefaultInlineThreshold)
		return false
	}

	for _, op := range ops {
		switch op.Type {
		case "set":
			if err := b.Set(op.Key, op.Value); err != nil {
				t.Fatalf("set failed: %v", err)
			}
		case "del":
			if err := b.Delete(op.Key); err != nil {
				t.Fatalf("delete failed: %v", err)
			}
		case "commit":
			if hit := applyBatch(); hit {
				return
			}
		default:
			t.Fatalf("unknown op type %q", op.Type)
		}
	}

	if hit := applyBatch(); hit {
		return
	}

	t.Fatalf("replay completed without hitting depth limit")
}
