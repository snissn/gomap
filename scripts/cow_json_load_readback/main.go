package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func run() (retErr error) {
	dir := flag.String("dir", "", "database directory")
	profile := flag.String("profile", "", "public profile")
	flag.Parse()
	opts := treedb.OptionsFor(treedb.Profile(*profile), *dir)
	opts.MemtableMode = "cow_btree"
	owner, err := treedb.Open(opts)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, owner.Close()) }()
	if mode := owner.Stats()["treedb.cache.memtable_mode"]; mode != "cow_btree" {
		return fmt.Errorf("wrong mode %q", mode)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		return err
	}
	mgr := collections.NewCollectionManager(backend)
	defer func() { retErr = errors.Join(retErr, mgr.Close()) }()
	if !mgr.UsesNativeCommandWALApply() {
		return errors.New("native owner unavailable")
	}
	srv := nativewire.NewServer(nativewire.ServerOptions{Collections: mgr, Backend: backend, MaxScanDocuments: 100001, MaxCursorRetainedBytes: 256 << 20})
	defer func() { retErr = errors.Join(retErr, srv.Close()) }()
	l, r := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- srv.ServeConn(context.Background(), r) }()
	client := nativewire.NewClient(l)
	defer func() { retErr = errors.Join(retErr, client.Close(), <-done) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err = client.Hello(ctx); err != nil {
		return err
	}
	scan, err := client.OpenScan(ctx, "usertable", nativewire.CursorLimits{MaxItems: 128})
	if err != nil {
		return err
	}
	// Pinned c195a9 workload defaults: insertstart=0, hashed insert order,
	// key prefix user, zero padding 1; its counter returns 0..99999.
	expected := make(map[string]bool, 100000)
	for i := int64(0); i < 100000; i++ {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(i))
		h := fnv.New64a()
		h.Write(b[:])
		n := int64(h.Sum64())
		if n < 0 {
			n = -n
		}
		expected[fmt.Sprintf("user%d", n)] = true
	}
	if len(expected) != 100000 {
		return errors.New("expected key collision")
	}
	seen := make(map[string]bool, 100000)
	hash := sha256.New()
	n := 0
	for {
		if scan.Truncated {
			return errors.New("server scan reported truncation")
		}
		if len(scan.IDs) != len(scan.Docs) {
			return errors.New("scan ID/document count mismatch")
		}
		for i, id := range scan.IDs {
			key := string(id)
			if !expected[key] {
				return fmt.Errorf("unexpected key %q", key)
			}
			if seen[key] {
				return fmt.Errorf("duplicate %q", key)
			}
			seen[key] = true
			var row map[string]string
			if err := json.Unmarshal(scan.Docs[i], &row); err != nil {
				return fmt.Errorf("decode %q: %w", key, err)
			}
			if len(row) != 11 || row["_ycsb_key"] != key {
				return fmt.Errorf("bad key or field count for %q: %d", key, len(row))
			}
			for j := 0; j < 10; j++ {
				f := fmt.Sprintf("field%d", j)
				v, ok := row[f]
				if !ok {
					return fmt.Errorf("missing %s/%s", key, f)
				}
				raw, e := base64.StdEncoding.DecodeString(v)
				if e != nil || len(raw) != 100 {
					return fmt.Errorf("invalid %s/%s bytes=%d err=%v", key, f, len(raw), e)
				}
			}
			fmt.Fprintf(hash, "%d:", len(id))
			hash.Write(id)
			fmt.Fprintf(hash, "%d:", len(scan.Docs[i]))
			hash.Write(scan.Docs[i])
			n++
		}
		if !scan.Cursor.HasMore {
			break
		}
		scan, err = client.CursorNext(ctx, scan.Cursor.CursorID, nativewire.CursorLimits{MaxItems: 128})
		if err != nil {
			return err
		}
	}
	if n != 100000 {
		return fmt.Errorf("count %d want 100000", n)
	}
	if err = client.Checkpoint(ctx); err != nil {
		return fmt.Errorf("checked checkpoint: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"profile": *profile, "dir": *dir, "actual_mode": "cow_btree", "native": true, "rows": n, "expected_hashed_keys_verified": true, "verification_max_scan_documents": 100001, "verification_max_cursor_retained_bytes": 256 << 20, "all_fields_base64_decoded_bytes": 100, "sha256_ordered_ids_documents": hex.EncodeToString(hash.Sum(nil)), "scope": "fresh reopened random fixture; no captured expected random values"})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
