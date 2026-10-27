package mvcc

import (
	"bytes"
	"fmt"
	"math"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// Only qualified, uncapped exact scans can consume a physical successor's owned
// value without first screening out future versions. Finite timestamps retain
// the raw key-before-value path.
func exactSuccessorEligible(options VersionIteratorOptions) bool {
	return options.ExactKey != nil && !options.Reverse &&
		(options.ReadTimestamp == 0 || options.ReadTimestamp == math.MaxUint64) &&
		logicalMatchesOptions(options.ExactKey, options)
}

func (it *VersionIterator) selectExactSuccessor(physical, record []byte) {
	it.stats.Visited++
	var timestamp uint64
	var err error
	it.keyBuf, timestamp, err = mvcckey.DecodeAppend(it.keyBuf[:0], physical)
	if err != nil {
		it.err = fmt.Errorf("%w: %w", ErrMalformedRecord, err)
		return
	}
	if !logicalMatchesOptions(it.keyBuf, it.options) {
		it.err = fmt.Errorf("%w: exact successor outside logical domain", ErrMalformedRecord)
		return
	}
	state, value, err := decodeRecordView(record)
	if err != nil {
		it.err = err
		return
	}
	it.current = Version{Key: it.keyBuf, Value: value, Timestamp: timestamp, State: state}
	it.stats.Retained++
	it.exactValid = true
	it.exactPhysical = physical
}

// openExactRaw changes representation once, preserving the original cut and
// domain. The already returned first record is not counted or read again.
func (it *VersionIterator) openExactRaw() bool {
	if !it.exactSuccessor {
		return it.raw != nil
	}
	it.exactSuccessor = false
	lower, upper, err := versionPhysicalBounds(it.options)
	if err != nil {
		it.err = err
		return false
	}
	it.raw, err = it.snapshot.Iterator(lower, upper)
	if err != nil {
		it.err = storageError("open version iterator", err)
		return false
	}
	if it.exactValid && !it.exactRawAt(it.exactPhysical) {
		return false
	}
	it.exactValid = false
	it.exactPhysical = nil
	return true
}

func (it *VersionIterator) exactRawAt(physical []byte) bool {
	valid := it.raw.Valid()
	if err := it.raw.Error(); err != nil {
		it.err = storageError("iterate versions", err)
		return false
	}
	var key []byte
	if valid {
		key = it.raw.Key()
	}
	if err := it.raw.Error(); err != nil {
		it.err = storageError("iterate versions", err)
		return false
	}
	if !valid || !bytes.Equal(key, physical) {
		it.err = fmt.Errorf("%w: exact successor disagrees with captured iterator", ErrMalformedRecord)
		return false
	}
	return true
}

func (it *VersionIterator) nextExactSuccessor() {
	// Append NUL is the strict byte successor. Encoding timestamp-1 would skip
	// malformed physical extensions that the ordinary iterator must inspect.
	// The first probe owns this physical key; extending its spare capacity
	// cannot mutate the decoded EntryView key or the captured source.
	start := append(it.exactPhysical, 0)
	// Until conversion, seekBuf retains the constructor's original upper bound.
	upper := it.seekBuf
	cut := it.snapshot.(treedb.MVCCReadSnapshot)
	physical, record, found, err := cut.SeekGEVersionRange(start, upper)
	if err != nil {
		it.err = storageError("read version iterator value", err)
		return
	}
	if !found {
		it.exactValid = false
		it.exactPhysical = nil
		it.current = Version{}
		return
	}
	if !it.openExactRaw() {
		return
	}
	it.raw.Next()
	if !it.exactRawAt(physical) {
		return
	}
	// The second probe already owns this payload. Do not read it again while
	// switching to the ordinary cursor for all subsequent movement.
	it.selectExactSuccessor(physical, record)
	it.exactValid = false
	it.exactPhysical = nil
}
