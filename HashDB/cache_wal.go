package hashdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CacheWALFsyncPolicy controls fsync behavior for the cache WAL.
type CacheWALFsyncPolicy uint8

const (
	// CacheWALDisabled disables the cache WAL entirely.
	CacheWALDisabled CacheWALFsyncPolicy = iota
	// CacheWALFsyncOnSync fsyncs the WAL only when SyncWAL is called.
	CacheWALFsyncOnSync
	// CacheWALFsyncAlways fsyncs the WAL after each append.
	CacheWALFsyncAlways
)

// CacheWALOptions configures the optional cache WAL.
type CacheWALOptions struct {
	FsyncPolicy CacheWALFsyncPolicy
}

type cacheWAL struct {
	path       string
	fsync      CacheWALFsyncPolicy
	f          *os.File
	workingDir string
}

const (
	cacheWALOpPut    = 1
	cacheWALOpDelete = 2
)

// cacheWAL record format:
// [op:1][klen:4][vlen:4][key:klen][value:vlen]
const cacheWALHeaderSize = 1 + 4 + 4

const (
	cacheWALMaxKeyLen   = 1 << 20   // 1MiB
	cacheWALMaxValueLen = 1 << 28   // 256MiB
	cacheWALMaxRecord   = 1 << 29   // 512MiB
	cacheWALBufSize     = 128 << 10 // 128KiB
)

func openCacheWAL(path string, policy CacheWALFsyncPolicy) (*cacheWAL, map[string]cacheEntry, error) {
	if policy == CacheWALDisabled {
		return nil, nil, nil
	}
	if path == "" {
		return nil, nil, fmt.Errorf("cache wal: path required")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}

	entries := make(map[string]cacheEntry)
	reader := bufio.NewReaderSize(f, cacheWALBufSize)

	var (
		pos         int64
		lastGoodPos int64
		header      [cacheWALHeaderSize]byte
	)

scan:
	for {
		_, err := io.ReadFull(reader, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			_ = f.Close()
			return nil, nil, err
		}
		pos += cacheWALHeaderSize

		op := header[0]
		klen := binary.LittleEndian.Uint32(header[1:5])
		vlen := binary.LittleEndian.Uint32(header[5:9])

		if klen == 0 || klen > cacheWALMaxKeyLen || vlen > cacheWALMaxValueLen || int64(klen)+int64(vlen) > cacheWALMaxRecord {
			// Treat as corruption; truncate to the last good position.
			break scan
		}

		key := make([]byte, int(klen))
		if _, err := io.ReadFull(reader, key); err != nil {
			break scan
		}
		pos += int64(klen)
		k := bytesToString(key)

		switch op {
		case cacheWALOpPut:
			if vlen == 0 {
				entries[k] = cacheEntry{key: key, value: nil}
				lastGoodPos = pos
				continue
			}
			val := make([]byte, int(vlen))
			if _, err := io.ReadFull(reader, val); err != nil {
				break scan
			}
			pos += int64(vlen)
			entries[k] = cacheEntry{key: key, value: val}
		case cacheWALOpDelete:
			// vlen is expected to be 0; ignore if not.
			if vlen > 0 {
				if _, err := io.CopyN(io.Discard, reader, int64(vlen)); err != nil {
					break scan
				}
				pos += int64(vlen)
			}
			entries[k] = cacheEntry{key: key, del: true}
		default:
			// Unknown op: treat as corruption.
			break scan
		}

		lastGoodPos = pos
	}

	// Truncate any torn/corrupt tail.
	if fi, statErr := f.Stat(); statErr == nil && lastGoodPos < fi.Size() {
		if err := f.Truncate(lastGoodPos); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	return &cacheWAL{path: path, fsync: policy, f: f, workingDir: dir}, entries, nil
}

// Close closes the underlying WAL file.
func (w *cacheWAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Sync fsyncs the underlying WAL file.
func (w *cacheWAL) Sync() error {
	if w == nil || w.f == nil {
		return nil
	}
	return w.f.Sync()
}

func (w *cacheWAL) appendPut(key, value []byte) error {
	if w == nil || w.f == nil {
		return nil
	}
	if len(key) == 0 {
		return fmt.Errorf("cache wal: empty key")
	}
	if len(key) > cacheWALMaxKeyLen {
		return fmt.Errorf("cache wal: key too large: %d", len(key))
	}
	if len(value) > cacheWALMaxValueLen {
		return fmt.Errorf("cache wal: value too large: %d", len(value))
	}

	var header [cacheWALHeaderSize]byte
	header[0] = cacheWALOpPut
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(value)))

	if _, err := writeAll(w.f, header[:]); err != nil {
		return err
	}
	if _, err := writeAll(w.f, key); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := writeAll(w.f, value); err != nil {
			return err
		}
	}

	if w.fsync == CacheWALFsyncAlways {
		return w.f.Sync()
	}
	return nil
}

func (w *cacheWAL) appendDelete(key []byte) error {
	if w == nil || w.f == nil {
		return nil
	}
	if len(key) == 0 {
		return fmt.Errorf("cache wal: empty key")
	}
	if len(key) > cacheWALMaxKeyLen {
		return fmt.Errorf("cache wal: key too large: %d", len(key))
	}

	var header [cacheWALHeaderSize]byte
	header[0] = cacheWALOpDelete
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], 0)

	if _, err := writeAll(w.f, header[:]); err != nil {
		return err
	}
	if _, err := writeAll(w.f, key); err != nil {
		return err
	}

	if w.fsync == CacheWALFsyncAlways {
		return w.f.Sync()
	}
	return nil
}

func (w *cacheWAL) rewrite(pending map[string]cacheEntry) error {
	if w == nil {
		return nil
	}
	if w.path == "" {
		return fmt.Errorf("cache wal: missing path")
	}

	tmpPath := w.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	tmpW := &cacheWAL{f: tmp}
	for k, e := range pending {
		key := e.key
		if len(key) == 0 && len(k) > 0 {
			key = []byte(k)
		}
		if e.del {
			if err := tmpW.appendDelete(key); err != nil {
				_ = tmp.Close()
				_ = os.Remove(tmpPath)
				return err
			}
			continue
		}
		if err := tmpW.appendPut(key, e.value); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// On Windows, we cannot replace an open file. Close before swapping.
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}

	if err := replaceFileAtomic(w.path, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	// Reopen for append.
	w.f, err = os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		_ = w.f.Close()
		w.f = nil
		return err
	}
	return nil
}
