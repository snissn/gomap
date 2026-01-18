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

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type logSegment struct {
	seq      uint64
	path     string
	valueLog bool
	fileID   uint32
}

func listWALSegments(dir string, includeValueLog bool) ([]logSegment, error) {
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
		if valueLog && !includeValueLog {
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
		commitPrefix = "commit-"
		valuePrefix  = "value-"
		walPrefix    = "wal-"
		vlogPrefix   = "vlog-"
		suffix       = ".log"
	)
	if !strings.HasSuffix(name, suffix) {
		return 0, false, false
	}
	if strings.HasPrefix(name, commitPrefix) {
		num := strings.TrimSuffix(strings.TrimPrefix(name, commitPrefix), suffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, false, false
		}
		return seq, false, true
	}
	if strings.HasPrefix(name, valuePrefix) {
		num := strings.TrimSuffix(strings.TrimPrefix(name, valuePrefix), suffix)
		seq, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return 0, false, false
		}
		return seq, true, true
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
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func replayWALIntoBackend(db *DB, segments []logSegment, maxSegmentBytes int64) error {
	ridMap, err := scanValueLogSegments(segments)
	if err != nil {
		return err
	}
	if err := replayCommitLogSegments(db, segments, ridMap, maxSegmentBytes); err != nil {
		return err
	}
	return removeValueLogSegments(db, segments)
}

func scanValueLogSegments(segments []logSegment) (map[uint64]page.ValuePtr, error) {
	ridMap := make(map[uint64]page.ValuePtr)
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		reader, err := valuelog.NewReader(segment.path, segment.fileID)
		if err != nil {
			return nil, err
		}
		for {
			rid, _, ptr, err := reader.ReadNext()
			if err == nil {
				if _, exists := ridMap[rid]; exists {
					_ = reader.Close()
					return nil, fmt.Errorf("valuelog: duplicate rid %d", rid)
				}
				ridMap[rid] = ptr
				continue
			}
			if isTruncatedLogError(err) {
				break
			}
			_ = reader.Close()
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	return ridMap, nil
}

func replayCommitLogSegments(db *DB, segments []logSegment, ridMap map[uint64]page.ValuePtr, maxSegmentBytes int64) error {
	for _, segment := range segments {
		if segment.valueLog {
			continue
		}
		reader, err := commitlog.NewReaderWithOptions(segment.path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
		if err != nil {
			return err
		}
		truncated := false
		for {
			records, err := reader.ReadBatch()
			if err == nil {
				if err := applyCommitBatch(db, records, ridMap); err != nil {
					_ = reader.Close()
					return err
				}
				continue
			}
			if isTruncatedLogError(err) {
				truncated = true
				break
			}
			_ = reader.Close()
			return err
		}
		if err := reader.Close(); err != nil {
			return err
		}
		if err := os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if truncated {
			break
		}
	}
	return nil
}

func applyCommitBatch(db *DB, records []commitlog.Record, ridMap map[uint64]page.ValuePtr) error {
	if len(records) == 0 {
		return nil
	}
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, rec := range records {
		switch rec.Op {
		case commitlog.OpDelete:
			if err := batch.Delete(rec.Key); err != nil {
				return err
			}
		case commitlog.OpSetInline:
			if err := batch.Set(rec.Key, rec.Value); err != nil {
				return err
			}
		case commitlog.OpSetRID:
			ptr, ok := ridMap[rec.RID]
			if !ok {
				return fmt.Errorf("commitlog: missing rid %d", rec.RID)
			}
			if db.valueLogManager == nil {
				return fmt.Errorf("commitlog: value log manager unavailable")
			}
			val, err := db.valueLogManager.Read(ptr)
			if err != nil {
				return fmt.Errorf("commitlog: read rid %d: %w", rec.RID, err)
			}
			if err := batch.Set(rec.Key, val); err != nil {
				return err
			}
		default:
			return fmt.Errorf("commitlog: unknown op %d", rec.Op)
		}
	}

	if err := batch.WriteSync(); err != nil {
		return err
	}
	return nil
}

func removeValueLogSegments(db *DB, segments []logSegment) error {
	for _, segment := range segments {
		if !segment.valueLog {
			continue
		}
		if db.valueLogManager != nil {
			if err := db.valueLogManager.RemoveSegmentForce(segment.fileID); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if err := os.Remove(segment.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
