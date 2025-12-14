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

	"github.com/snissn/gomap/TreeDB/internal/wal"
)

type walSegment struct {
	seq  uint64
	path string
}

func listWALSegments(dir string) ([]walSegment, error) {
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []walSegment
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		const prefix = "wal-"
		const suffix = ".log"
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		num := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			continue
		}

		segments = append(segments, walSegment{
			seq:  seq,
			path: filepath.Join(walDir, name),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].seq < segments[j].seq
	})
	return segments, nil
}

func isTruncatedWALError(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, wal.ErrCorrupt)
}

func replayWALIntoBackend(db *DB, segments []walSegment) error {
	const maxOpsPerBatch = 10_000

	for _, segment := range segments {
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
				if isTruncatedWALError(err) {
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
	}

	return nil
}
