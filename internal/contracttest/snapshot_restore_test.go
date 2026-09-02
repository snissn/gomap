package contracttest

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/HashDB"
)

func TestContract_SnapshotRestoreRoundTrip(t *testing.T) {
	engines := []engine{
		engineFunc{name: "hashdb-single", open: func(dir string) (kv, error) { return openEngine("hashdb-single", dir) }},
		engineFunc{name: "hashdb-sharded", open: func(dir string) (kv, error) { return openEngine("hashdb-sharded", dir) }},
		engineFunc{name: "treedb-cached", open: func(dir string) (kv, error) { return openEngine("treedb-cached", dir) }},
	}

	for _, eng := range engines {
		t.Run(eng.Name(), func(t *testing.T) {
			srcDir := t.TempDir()
			dstDir := t.TempDir()

			src, err := eng.Open(srcDir)
			if err != nil {
				t.Fatalf("open src: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })

			bsrc, ok := src.(batchKV)
			if !ok {
				t.Fatalf("engine %q missing batch api", eng.Name())
			}
			isrc, ok := src.(iterableKV)
			if !ok {
				t.Fatalf("engine %q missing iteration api", eng.Name())
			}

			ops := []hashdb.BatchOp{
				hashdb.PutOp([]byte("a"), []byte("va1")),
				hashdb.PutOp([]byte("b"), []byte("vb1")),
				hashdb.PutOp([]byte("c"), []byte("vc1")),
				hashdb.PutOp([]byte("d"), []byte("vd1")),
				hashdb.DeleteOp([]byte("c")),
				hashdb.PutOp([]byte("b"), []byte("vb2")),
				hashdb.PutOp([]byte("e"), bytes.Repeat([]byte("x"), 128)),
			}

			expected := map[string][]byte{
				"a": []byte("va1"),
				"b": []byte("vb2"),
				"d": []byte("vd1"),
				"e": bytes.Repeat([]byte("x"), 128),
			}

			if err := bsrc.ApplyBatchSync(ops); err != nil {
				t.Fatalf("apply batch sync: %v", err)
			}

			snap := make(map[string][]byte)
			if err := isrc.ForEach(func(k, v []byte) error {
				ks := string(k)
				if _, exists := snap[ks]; exists {
					t.Fatalf("snapshot produced duplicate key %q", ks)
				}
				snap[ks] = append([]byte(nil), v...)
				return nil
			}); err != nil {
				t.Fatalf("snapshot iterate: %v", err)
			}

			if len(snap) != len(expected) {
				t.Fatalf("snapshot size mismatch: got %d, want %d", len(snap), len(expected))
			}
			for k, want := range expected {
				got, ok := snap[k]
				if !ok {
					t.Fatalf("missing key in snapshot: %q", k)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("snapshot value mismatch for %q: got %q, want %q", k, string(got), string(want))
				}
			}

			dst, err := eng.Open(dstDir)
			if err != nil {
				t.Fatalf("open dst: %v", err)
			}
			t.Cleanup(func() { _ = dst.Close() })

			bdst, ok := dst.(batchKV)
			if !ok {
				t.Fatalf("engine %q missing batch api", eng.Name())
			}

			var restoreOps []hashdb.BatchOp
			for k, v := range snap {
				restoreOps = append(restoreOps, hashdb.PutOp([]byte(k), v))
			}
			if err := bdst.ApplyBatchSync(restoreOps); err != nil {
				t.Fatalf("restore apply batch sync: %v", err)
			}

			for k, want := range expected {
				got, err := dst.Get([]byte(k))
				if err != nil {
					t.Fatalf("get dst %q: %v", k, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("restore value mismatch for %q: got %q, want %q", k, string(got), string(want))
				}
			}
			got, err := dst.Get([]byte("c"))
			if err != nil {
				t.Fatalf("get dst deleted key: %v", err)
			}
			if got != nil {
				t.Fatalf("deleted key resurrected: got %q, want nil", string(got))
			}
		})
	}
}
