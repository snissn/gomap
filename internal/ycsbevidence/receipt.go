// Package ycsbevidence observes the original, live command owner and Close.
// It is enabled only by YCSB_SERVER_RECEIPT_DIR and is read-only unless the separate YCSB_OWNER_CHECKPOINT=1 control is enabled.
package ycsbevidence

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type Result struct {
	Attempted bool      `json:"attempted"`
	Success   bool      `json:"success"`
	Error     string    `json:"error"`
	StartUTC  time.Time `json:"start_utc"`
	EndUTC    time.Time `json:"end_utc"`
	ElapsedNS int64     `json:"elapsed_ns"`
}

func Call(fn func() error) Result {
	start := time.Now()
	err := fn()
	end := time.Now()
	r := Result{Attempted: true, Success: err == nil, StartUTC: start.UTC(), EndUTC: end.UTC(), ElapsedNS: end.Sub(start).Nanoseconds()}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

type Process struct {
	PID             int    `json:"pid"`
	StartTicks      uint64 `json:"start_ticks"`
	EXESHA256       string `json:"exe_sha256"`
	GOMAXPROCSUnset bool   `json:"gomaxprocs_unset"`
}
type Observation struct {
	SchemaVersion      int               `json:"schema_version"`
	Kind               string            `json:"kind"`
	OwnerID            string            `json:"owner_id"`
	Process            Process           `json:"process"`
	DataDir            string            `json:"data_dir"`
	DataDevice         uint64            `json:"data_device"`
	DataInode          uint64            `json:"data_inode"`
	Profile            string            `json:"profile"`
	ObservedGOMAXPROCS int               `json:"observed_gomaxprocs"`
	Backend            string            `json:"backend"`
	CaptureError       string            `json:"capture_error"`
	Stats              map[string]string `json:"stats"`
	ObservedUTC        time.Time         `json:"observed_utc"`
}
type Observer struct {
	dir     string
	ownerID string
	profile string
	dataDir string
	backend *backenddb.DB
	stats   func() map[string]string
}

func writeNew(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return errors.Join(err, file.Close())
}
func hashExe() (string, error) {
	f, e := os.Open("/proc/self/exe")
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// OpenFromEnv observes ownership using a real NativeSemanticCapture, rather
// than a caller-supplied label or the selected command flag.
func OpenFromEnv(profile, dataDir string, backend *backenddb.DB, stats func() map[string]string) (*Observer, error) {
	dir := os.Getenv("YCSB_SERVER_RECEIPT_DIR")
	if dir == "" {
		return nil, nil
	}
	if backend == nil || stats == nil {
		return nil, errors.New("evidence requires the actual live owner")
	}
	if !filepath.IsAbs(dir) {
		return nil, errors.New("evidence directory must be absolute")
	}
	canonical, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return nil, err
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return nil, err
	}
	token := make([]byte, 16)
	if _, err = rand.Read(token); err != nil {
		return nil, err
	}
	o := &Observer{dir: dir, ownerID: hex.EncodeToString(token), profile: profile, dataDir: canonical, backend: backend, stats: stats}
	observation, err := o.observe()
	if err != nil {
		return nil, err
	}
	if err = writeNew(filepath.Join(dir, "owner.open.json"), observation); err != nil {
		return nil, err
	}
	return o, nil
}
func (o *Observer) observe() (Observation, error) {
	identity, device, inode, err := processIdentity(o.dataDir)
	if err != nil {
		return Observation{}, err
	}
	stats := o.stats()
	if stats["treedb.profile.resolved"] != o.profile {
		return Observation{}, errors.New("actual resolved owner profile mismatch")
	}
	backend := ""
	captureError := ""
	capture, err := o.backend.CaptureNativeSemanticState()
	if err == nil {
		backend = "cow"
		if err = capture.Close(); err != nil {
			return Observation{}, err
		}
		if stats["treedb.cache.memtable_mode"] != "cow_btree" {
			return Observation{}, errors.New("native bridge lacks actual COW owner stats")
		}
		if _, err := strconv.ParseUint(stats["treedb.cache.cow.txn.logical_seq"], 10, 64); err != nil {
			return Observation{}, errors.New("missing actual COW logical sequence")
		}
	} else if errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		backend = "legacy"
		captureError = err.Error()
	} else {
		return Observation{}, fmt.Errorf("observe native owner: %w", err)
	}
	return Observation{1, "live_owner_observation", o.ownerID, identity, o.dataDir, device, inode, o.profile, runtime.GOMAXPROCS(0), backend, captureError, stats, time.Now().UTC()}, nil
}

// Close calls fn exactly once and retains its original return. Observation or
// receipt I/O failure is joined after the original Close has still completed.
func (o *Observer) Close(fn func() error) error {
	if o == nil {
		return fn()
	}
	before, observeErr := o.observe()
	beforeErr := observeErr
	if observeErr == nil {
		beforeErr = writeNew(filepath.Join(o.dir, "owner.before-close.json"), before)
	}
	var closeErr error
	r := Call(func() error { closeErr = fn(); return closeErr })
	receiptErr := writeNew(filepath.Join(o.dir, "owner.close.json"), map[string]any{"schema_version": 1, "kind": "original_server_owner_close", "owner_id": o.ownerID, "process": before.Process, "close": r})
	return errors.Join(closeErr, beforeErr, receiptErr)
}
