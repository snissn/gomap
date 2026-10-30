//go:build ignore

// SYNTHETIC orchestration stub: no TreeDB, MongoDB, public writes, or WAL.
// It tests real CLI packet production, socket identity, SIGKILL, reap and reopen.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type obj = map[string]any

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func save(path string, v any) {
	b, e := json.Marshal(v)
	must(e)
	must(os.WriteFile(path, append(b, '\n'), 0600))
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func hash(path string) string {
	b, e := os.ReadFile(path)
	must(e)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func call(t string) obj {
	return obj{"attempted": true, "success": true, "error": "", "start_utc": t, "end_utc": t, "elapsed_ns": 0}
}
func process() obj {
	b, e := os.ReadFile("/proc/self/stat")
	must(e)
	f := strings.Fields(string(b)[strings.LastIndex(string(b), ")")+1:])
	ticks, e := strconv.ParseUint(f[19], 10, 64)
	must(e)
	_, present := os.LookupEnv("GOMAXPROCS")
	return obj{"pid": os.Getpid(), "start_ticks": ticks, "exe_sha256": hash("/proc/self/exe"), "gomaxprocs_unset": !present}
}
func main() {
	args := map[string]*string{}
	for _, n := range []string{"mode", "dir", "addr", "profile", "document-format", "target", "format", "source-id", "fixture-id", "expected", "observed", "receipt"} {
		args[n] = flag.String(n, "", "synthetic control")
	}
	flag.Parse()
	get := func(n string) string { return *args[n] }
	fmt.Println("SYNTHETIC offline orchestration stub; no product evidence")
	if get("mode") == "" {
		data := get("dir")
		info, e := os.Stat(data)
		must(e)
		stat := info.Sys().(*syscall.Stat_t)
		proc := process()
		owner := fmt.Sprintf("%032x", os.Getpid())
		observation := func() obj {
			return obj{"schema_version": 1, "kind": "live_owner_observation", "process": proc, "owner_id": owner, "data_dir": data, "data_device": stat.Dev, "data_inode": stat.Ino, "profile": get("profile"), "backend": "cow", "capture_error": "", "stats": map[string]string{"treedb.profile.resolved": get("profile"), "treedb.cache.memtable_mode": "cow_btree", "treedb.cache.cow.txn.logical_seq": "0"}, "observed_gomaxprocs": 1, "observed_utc": now()}
		}
		evidence := os.Getenv("YCSB_SERVER_RECEIPT_DIR")
		save(filepath.Join(evidence, "owner.open.json"), observation())
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		listener, e := net.Listen("tcp", get("addr"))
		must(e)
		go func() {
			for {
				conn, e := listener.Accept()
				if e != nil {
					return
				}
				conn.Close()
			}
		}()
		<-signals
		save(filepath.Join(evidence, "owner.before-close.json"), observation())
		must(listener.Close())
		save(filepath.Join(evidence, "owner.close.json"), obj{"schema_version": 1, "kind": "original_server_owner_close", "process": proc, "owner_id": owner, "close": call(now())})
		return
	}
	started := now()
	id := obj{"schema_version": 1, "source_id": get("source-id"), "fixture_id": get("fixture-id"), "target": get("target"), "format": get("format"), "profile": get("profile")}
	rows := []any{}
	for n := 0; n < 64; n++ {
		fields := obj{}
		for f := 0; f < 10; f++ {
			prefix := fmt.Sprintf("load-%04d-%02d", n, f)
			v := base64.StdEncoding.EncodeToString([]byte(prefix + strings.Repeat("x", 100-len(prefix))))
			if get("format") == "json" {
				fields[fmt.Sprintf("field%d", f)] = obj{"type": "string", "value": v}
			} else {
				fields[fmt.Sprintf("field%d", f)] = obj{"type": "binary", "subtype": 0, "base64": v}
			}
		}
		rows = append(rows, obj{"id": fmt.Sprintf("user%04d", n), "fields": fields})
	}
	names := []string{get("expected"), get("observed")}
	if get("mode") == "crash-write" {
		expected := obj{}
		for k, v := range id {
			expected[k] = v
		}
		expected["load"] = rows
		expected["updates"] = []any{}
		save(get("expected"), expected)
		journal := get("observed") + ".acknowledged.jsonl"
		file, e := os.Create(journal)
		must(e)
		ack := "AckVisible"
		if get("target") == "mongo" {
			ack = "w:1,j:absent"
		}
		for n := 0; n < 64; n++ {
			b, e := json.Marshal(obj{"schema_version": 1, "ordinal": n, "id": fmt.Sprintf("user%04d", n), "acknowledged": true, "requested_ack": ack, "write": call(now())})
			must(e)
			_, e = file.Write(append(b, '\n'))
			must(e)
		}
		must(file.Close())
		names = append(names, journal)
		save(filepath.Join(filepath.Dir(get("expected")), "db", "synthetic-state.json"), rows)
	} else {
		b, e := os.ReadFile(filepath.Join(filepath.Dir(get("expected")), "db", "synthetic-state.json"))
		must(e)
		must(json.Unmarshal(b, &rows))
	}
	observed := obj{}
	for k, v := range id {
		observed[k] = v
	}
	observed["stage"] = get("mode")
	observed["rows"] = rows
	observed["operations"] = []any{}
	save(get("observed"), observed)
	artifacts := map[string]string{}
	for _, name := range names {
		artifacts[filepath.Base(name)] = hash(name)
	}
	save(get("receipt"), obj{"schema_version": 1, "kind": "public_process_crash_client_close", "mode": get("mode"), "pid": os.Getpid(), "source_id": get("source-id"), "fixture_id": get("fixture-id"), "result_error": "", "close_error": "", "started_utc": started, "ended_utc": now(), "checkpoint_attempted": false, "checkpoint_succeeded": false, "checkpoint": nil, "owner_checkpoint": nil, "artifacts_sha256": artifacts})
	time.Sleep(20 * time.Millisecond)
}
