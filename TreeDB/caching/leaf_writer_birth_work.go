package caching

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"os"
	"path/filepath"
	"unsafe"
)

type leafLogWriterBirth struct {
	creation valuelog.WriterCreationWork
	reader   valuelog.CurrentSegmentRegistrationWork
	backend  valuelog.CurrentSegmentRegistrationWork
	phase    uint8
	seq      int
	path     string
	id       uint32
	err      error
}

func (db *DB) ensureLeafWriterWithWorkMuHeld(l *lane, w *iterator.OrdinalScanWork) (bool, error) {
	if l.vlog != nil {
		return true, nil
	}
	s := &l.vlogBirth
	if s.err != nil {
		return false, s.err
	}
	reserve := func(r, b uint64) (bool, error) {
		if w == nil {
			return true, nil
		}
		if r > w.RecordLimit || b > w.ByteLimit {
			return false, &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
		}
		return w.Reserve(r, b), nil
	}
	fail := func(e error) (bool, error) { s.err = e; return false, e }
	switch s.phase {
	case 0:
		if ok, e := reserve(2, uint64(unsafe.Sizeof(leafLogWriterBirth{}))+uint64(4*len(db.valueLogDirForLane(l))+512)); !ok || e != nil {
			return false, e
		}
		seq, e := db.nextLeafLogAppendSeq()
		if e != nil {
			return fail(e)
		}
		id, e := valuelog.EncodeFileID(uint32(l.id), uint32(seq))
		if e != nil {
			return fail(e)
		}
		s.seq, s.id, s.path = seq, id, filepath.Join(db.valueLogDirForLane(l), valueLogName(l.id, seq))
		s.creation = valuelog.WriterCreationWork{Path: s.path, FileID: id, Registry: db.valueLogIdentityPins, SyncDirectory: true, Sync: func(f *os.File) error { return f.Sync() }}
	case 1:
		ready, e := s.creation.Step(w)
		if e != nil {
			return fail(e)
		}
		if !ready {
			return false, nil
		}
	case 2:
		if db.valueLogReader != nil {
			ready, e := db.valueLogReader.RegisterCurrentSegmentWithWorkV6(s.path, s.id, 0, &s.reader, w)
			if e != nil {
				return fail(e)
			}
			if !ready {
				return false, nil
			}
		}
	case 3:
		if producer, ok := db.backend.(interface {
			RegisterLeafSegmentWithWorkV6(string, uint32, *valuelog.CurrentSegmentRegistrationWork, *iterator.OrdinalScanWork) (bool, error)
		}); ok {
			ready, e := producer.RegisterLeafSegmentWithWorkV6(s.path, s.id, &s.backend, w)
			if e != nil {
				return fail(e)
			}
			if !ready {
				return false, nil
			}
		} else if w != nil {
			return false, fmt.Errorf("outer-leaf backend registration lacks work: %w", valuelog.ErrLeafAppendWorkUnsupported)
		} else if e := db.registerValueLogSegment(s.path, s.id); e != nil {
			return fail(e)
		}
	case 4:
		if ok, e := reserve(3, uint64(unsafe.Sizeof(leafLogWriterBirth{}))+512); !ok || e != nil {
			return false, e
		}
		_, registered := db.backend.(interface {
			RegisterValueLogSegmentReplacing(string, uint32, uint32) error
		})
		if db.cow == nil || !registered {
			if ok, e := reserve(1, uint64(len(l.vlogCreatedSegments)+1)*uint64(unsafe.Sizeof(laneValueLogSegment{}))); !ok || e != nil {
				return false, e
			}
		}
		writer := s.creation.Writer
		writer.SetDictFrameEncoderOptions(db.valueLogDictFrameEncodeLevel, db.valueLogDictFrameEnableEntropy)
		l.vlogModeSet = false
		l.vlogModeWriter = nil
		db.setVlogWriterMode(l, writer, db.defaultVlogWriteMode(), db.valueLogBlockCodec)
		writer.SetRawWritevStrategy(db.valueLogRawWritevMinAvgBytes, db.valueLogRawWritevMinRecords)
		l.vlog, l.vlogSeq, l.vlogPath = writer, s.seq, s.path
		l.vlogLiveBytes.Store(0)
		l.vlogRotateTotal.Add(1)
		if db.cow == nil || !registered {
			l.vlogCreatedSegments = append(l.vlogCreatedSegments, laneValueLogSegment{path: s.path, fileID: s.id})
		}
		// Containing state is detached only after both original registrations and
		// namespace proof succeeded. No caller can observe a partial current writer.
		*s = leafLogWriterBirth{}
		return true, nil
	}
	s.phase++
	return false, nil
}
func (s *leafLogWriterBirth) close() error {
	if s.phase == 0 {
		return nil
	}
	// Registration holds the original File when still private. Installed handles
	// belong to their original Managers and are closed by those Managers.
	e := errors.Join(s.reader.Segment.Close(), s.backend.Segment.Close())
	e = errors.Join(e, s.creation.Close())
	if e != nil {
		return e
	}
	*s = leafLogWriterBirth{}
	return nil
}
