package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/vlog"
	"github.com/snissn/gomap/TreeDB/internal/wal"
	"github.com/snissn/gomap/TreeDB/page"
)

type logSegment struct {
	seq      uint64
	path     string
	valueLog bool
	fileID   uint32
}

func listWALSegments(dir string) ([]logSegment, error) {
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []logSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		seq, valueLog, ok := parseLogSeq(name)
		if !ok {
			continue
		}

		seg := logSegment{
			seq:      seq,
			path:     filepath.Join(walDir, name),
			valueLog: valueLog,
		}
		if valueLog {
			seg.fileID = page.ValueLogFileID(uint32(seq))
		}
		segments = append(segments, seg)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].seq < segments[j].seq
	})
	return segments, nil
}

func parseLogSeq(name string) (uint64, bool, bool) {
	const (
		walPrefix  = "wal-"
		vlogPrefix = "vlog-"
		suffix     = ".log"
	)
	if !strings.HasSuffix(name, suffix) {
		return 0, false, false
	}
	if strings.HasPrefix(name, walPrefix) {
		num := strings.TrimSuffix(strings.TrimPrefix(name, walPrefix), suffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, false, false
		}
		return seq, false, true
	}
	if strings.HasPrefix(name, vlogPrefix) {
		num := strings.TrimSuffix(strings.TrimPrefix(name, vlogPrefix), suffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, false, false
		}
		return seq, true, true
	}
	return 0, false, false
}

func isTruncatedLogError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, wal.ErrCorrupt) ||
		errors.Is(err, vlog.ErrCorrupt)
}

func replayWALIntoBackend(db *DB, segments []logSegment) error {
	const maxOpsPerBatch = 10_000

	markedZombie := false
	for _, segment := range segments {
		if segment.valueLog {
			zombie, err := replayValueLogSegment(db, segment, maxOpsPerBatch)
			if err != nil {
				return err
			}
			if zombie {
				markedZombie = true
			}
			continue
		}
		if err := replayWALSegment(db, segment, maxOpsPerBatch); err != nil {
			return err
		}
	}

	if markedZombie {
		if err := db.RefreshSlabSet(); err != nil {
			return err
		}
	}
	return nil
}

func replayWALSegment(db *DB, segment logSegment, maxOpsPerBatch int) error {
	reader, err := wal.NewReader(segment.path)
	if err != nil {
		return err
	}

	var (
		opsInBatch int
		batch      = db.NewBatch()
	)

	for {
		op, key, val, err := reader.ReadNext()
		if err != nil {
			if isTruncatedLogError(err) {
				break
			}
			_ = batch.Close()
			_ = reader.Close()
			return err
		}

		switch op {
		case wal.OpSet:
			if err := batch.Set(key, val); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return err
			}
		case wal.OpDelete:
			if err := batch.Delete(key); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return err
			}
		default:
			_ = batch.Close()
			_ = reader.Close()
			return fmt.Errorf("wal: unknown op %d", op)
		}

		opsInBatch++
		if opsInBatch >= maxOpsPerBatch {
			if err := batch.WriteSync(); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return err
			}
			_ = batch.Close()
			batch = db.NewBatch()
			opsInBatch = 0
		}
	}

	_ = reader.Close()

	if opsInBatch > 0 {
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			return err
		}
	}
	_ = batch.Close()

	if err := os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func replayValueLogSegment(db *DB, segment logSegment, maxOpsPerBatch int) (bool, error) {
	reader, err := vlog.NewReader(segment.path, segment.fileID)
	if err != nil {
		return false, err
	}

	var (
		opsInBatch int
		batch      = db.NewBatch()
		threshold  = db.InlineThreshold()
		keepSeg    bool
	)
	ptrBatch, _ := batch.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})

	for {
		op, key, val, ptr, err := reader.ReadNext()
		if err != nil {
			if isTruncatedLogError(err) {
				break
			}
			_ = batch.Close()
			_ = reader.Close()
			return false, err
		}

		switch op {
		case vlog.OpSet:
			if len(val) > threshold && ptrBatch != nil {
				keepSeg = true
				if err := ptrBatch.SetPointer(key, ptr); err != nil {
					_ = batch.Close()
					_ = reader.Close()
					return false, err
				}
			} else if err := batch.Set(key, val); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return false, err
			}
		case vlog.OpDelete:
			if err := batch.Delete(key); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return false, err
			}
		default:
			_ = batch.Close()
			_ = reader.Close()
			return false, fmt.Errorf("vlog: unknown op %d", op)
		}

		opsInBatch++
		if opsInBatch >= maxOpsPerBatch {
			if err := batch.WriteSync(); err != nil {
				_ = batch.Close()
				_ = reader.Close()
				return false, err
			}
			_ = batch.Close()
			batch = db.NewBatch()
			ptrBatch, _ = batch.(interface {
				SetPointer(key []byte, ptr page.ValuePtr) error
			})
			opsInBatch = 0
		}
	}

	_ = reader.Close()

	if opsInBatch > 0 {
		if err := batch.WriteSync(); err != nil {
			_ = batch.Close()
			return false, err
		}
	}
	_ = batch.Close()

	if keepSeg {
		return false, nil
	}
	if segment.fileID != 0 {
		if err := db.valueLogManager.MarkZombie(segment.fileID); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	return false, nil
}
