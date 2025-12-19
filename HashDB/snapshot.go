package hashdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const snapshotMagicV1 = "HASHDBSNAP1\n"

// ErrSnapshotCorrupt indicates a snapshot stream failed validation.
var ErrSnapshotCorrupt = errors.New("hashdb: snapshot corrupt")

// Export writes a snapshot of all live key/value pairs to w.
//
// The iteration order is arbitrary and not stable across runs.
// Use TreeDB if you need ordered iteration.
func (h *DB) Export(w io.Writer) error {
	if h == nil {
		return nil
	}
	if _, err := writeAll(w, []byte(snapshotMagicV1)); err != nil {
		return err
	}

	if err := h.ForEach(func(key, value []byte) error {
		if len(key) == 0 {
			return fmt.Errorf("export: empty key is not supported")
		}
		if err := writeUvarint(w, uint64(len(key))); err != nil {
			return err
		}
		if err := writeUvarint(w, uint64(len(value))); err != nil {
			return err
		}
		if _, err := writeAll(w, key); err != nil {
			return err
		}
		if _, err := writeAll(w, value); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Terminator.
	if err := writeUvarint(w, 0); err != nil {
		return err
	}
	if err := writeUvarint(w, 0); err != nil {
		return err
	}
	return nil
}

// Restore reads a snapshot produced by Export and writes it into this DB.
//
// Restore uses ApplyBatchSync and is intended for durable, repeatable restores.
// Existing keys are overwritten.
func (h *DB) Restore(r io.Reader) error {
	if h == nil {
		return nil
	}
	return restoreIntoBatchKV(r, h)
}

// Export writes a snapshot of all live key/value pairs in the sharded store to w.
//
// The iteration order is arbitrary and not stable across runs.
func (h *HashDB) Export(w io.Writer) error {
	if h == nil {
		return nil
	}
	if _, err := writeAll(w, []byte(snapshotMagicV1)); err != nil {
		return err
	}

	if err := h.ForEach(func(key, value []byte) error {
		if len(key) == 0 {
			return fmt.Errorf("export: empty key is not supported")
		}
		if err := writeUvarint(w, uint64(len(key))); err != nil {
			return err
		}
		if err := writeUvarint(w, uint64(len(value))); err != nil {
			return err
		}
		if _, err := writeAll(w, key); err != nil {
			return err
		}
		if _, err := writeAll(w, value); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Terminator.
	if err := writeUvarint(w, 0); err != nil {
		return err
	}
	if err := writeUvarint(w, 0); err != nil {
		return err
	}
	return nil
}

// Restore reads a snapshot produced by Export and writes it into this sharded store.
func (h *HashDB) Restore(r io.Reader) error {
	if h == nil {
		return nil
	}
	return restoreIntoBatchKV(r, h)
}

type batchKVForRestore interface {
	ApplyBatchSync(ops []BatchOp) error
}

func restoreIntoBatchKV(r io.Reader, dst batchKVForRestore) error {
	br := bufio.NewReader(r)

	header := make([]byte, len(snapshotMagicV1))
	if _, err := io.ReadFull(br, header); err != nil {
		return err
	}
	if string(header) != snapshotMagicV1 {
		return ErrSnapshotCorrupt
	}

	const (
		restoreBatchOps   = 4096
		restoreBatchBytes = 4 << 20 // 4MB
		maxItemSize       = 256 << 20
	)

	var (
		ops       = make([]BatchOp, 0, restoreBatchOps)
		batchSize = 0
	)

	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := dst.ApplyBatchSync(ops); err != nil {
			return err
		}
		ops = ops[:0]
		batchSize = 0
		return nil
	}

	for {
		klen, err := readUvarint(br)
		if err != nil {
			return err
		}
		vlen, err := readUvarint(br)
		if err != nil {
			return err
		}
		if klen == 0 && vlen == 0 {
			break
		}
		if klen == 0 {
			return ErrSnapshotCorrupt
		}
		if klen > maxItemSize || vlen > maxItemSize {
			return fmt.Errorf("%w: oversized record (k=%d v=%d)", ErrSnapshotCorrupt, klen, vlen)
		}
		if klen+vlen > maxItemSize {
			return fmt.Errorf("%w: oversized record (k+v=%d)", ErrSnapshotCorrupt, klen+vlen)
		}

		key := make([]byte, klen)
		if _, err := io.ReadFull(br, key); err != nil {
			return err
		}
		val := make([]byte, vlen)
		if _, err := io.ReadFull(br, val); err != nil {
			return err
		}

		ops = append(ops, PutOp(key, val))
		batchSize += 16 + int(klen) + int(vlen)
		if len(ops) >= restoreBatchOps || batchSize >= restoreBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func writeUvarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := writeAll(w, buf[:n])
	return err
}

func readUvarint(r *bufio.Reader) (uint64, error) {
	v, err := binary.ReadUvarint(r)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, ErrSnapshotCorrupt
		}
		return 0, err
	}
	return v, nil
}
