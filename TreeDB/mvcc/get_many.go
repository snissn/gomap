package mvcc

import (
	"bytes"
	"errors"
	"fmt"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// MaxGetManyAtKeys bounds one scalar read batch, including duplicate keys.
const MaxGetManyAtKeys = mvcckey.MaxReadBatch

var ErrBatchTooLarge = errors.New("mvcc: read batch exceeds 32 keys")

// GetManyResult is one ordered, independently owned logical read outcome.
// Err has exactly the same key/seek/record meaning as GetAt's error.
type GetManyResult struct {
	Result Result
	Err    error
}

// GetManyAt returns one owned result per input at timestamp. Qualified engines
// pin one physical cut; legacy engines retain scalar GetAt's visibility contract.
// Timestamp/store validation precedes empty/oversize handling. Invalid keys and
// read/decode errors are per item; floor/capture/Close errors affect the call.
// A Close error preserves produced results but must not be ignored by callers.
// Callers must not mutate keys until this synchronous call returns.
func (s *Store) GetManyAt(keys [][]byte, timestamp uint64) (results []GetManyResult, err error) {
	if timestamp == 0 {
		return nil, ErrZeroTimestamp
	}
	if s == nil || s.db == nil {
		return nil, storageError("acquire read cut", treedb.ErrClosed)
	}
	if len(keys) == 0 {
		return []GetManyResult{}, nil
	}
	if len(keys) > MaxGetManyAtKeys {
		return nil, ErrBatchTooLarge
	}
	results = make([]GetManyResult, len(keys))
	// A singleton retains the scalar operation and avoids the fixed batch plan.
	// Storage errors (including capture/cleanup) and floor failures affect this
	// sole call; invalid keys/records remain ordered per-key outcomes.
	if len(keys) == 1 {
		result, e := s.GetAt(keys[0], timestamp)
		if errors.Is(e, ErrStorage) || errors.Is(e, ErrReadBeforeDiscardFloor) {
			return nil, e
		}
		results[0] = GetManyResult{Result: result, Err: e}
		return results, nil
	}
	if s.readCuts == nil {
		for i, key := range keys {
			results[i].Result, results[i].Err = s.GetAt(key, timestamp)
		}
		return results, nil
	}
	var starts, ends [MaxGetManyAtKeys][]byte
	var order [MaxGetManyAtKeys]int
	n := 0
	for i, key := range keys {
		lower, e := mvcckey.Encode(key, timestamp)
		if e != nil {
			results[i].Err = fmt.Errorf("%w: %w", ErrInvalidKey, e)
			continue
		}
		upper, e := mvcckey.AppendKeyVersionsUpper(nil, key)
		if e != nil {
			results[i].Err = fmt.Errorf("%w: %w", ErrInvalidKey, e)
			continue
		}
		// Insertion sorting at most32 probes avoids a heap-owned planner/closure.
		j := n
		for j > 0 && bytes.Compare(starts[j-1], lower) > 0 {
			starts[j], ends[j], order[j] = starts[j-1], ends[j-1], order[j-1]
			j--
		}
		starts[j], ends[j], order[j] = lower, upper, i
		n++
	}
	if err = s.lockDiscardFloorRead(); err != nil {
		return nil, err
	}
	if floor := s.discardFloor; floor != 0 && timestamp <= floor {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: read timestamp %d is at or below floor %d", ErrReadBeforeDiscardFloor, timestamp, floor)
	}
	cut, e := s.readCuts.AcquireMVCCReadCut()
	s.mu.RUnlock()
	if e != nil {
		return nil, storageError("acquire read cut", e)
	}
	if cut == nil {
		return nil, storageError("acquire read cut", treedb.ErrClosed)
	}
	defer func() { err = errors.Join(err, storageError("close read cut", cut.Close())) }()
	var physical [MaxGetManyAtKeys]treedb.MVCCVersionRangeResult
	if batch, ok := cut.(treedb.MVCCReadBatchSnapshot); ok && n > 1 {
		if e = batch.SeekGEVersionRanges(starts[:n], ends[:n], physical[:n]); e != nil {
			return results, storageError("seek versions", e)
		}
	} else {
		for j := 0; j < n; j++ {
			r := &physical[j]
			r.Key, r.Value, r.Found, r.Err = cut.SeekGEVersionRange(starts[j], ends[j])
		}
	}
	for j := 0; j < n; j++ {
		i, p := order[j], physical[j]
		if p.Err != nil {
			results[i].Err = storageError("seek version", p.Err)
		} else if !p.Found {
			results[i].Result = Result{State: Absent}
		} else {
			results[i].Result, results[i].Err = decodePointResult(keys[i], timestamp, p.Key, p.Value, true)
		}
	}
	return results, nil
}
