package db

import (
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The optional cache is captured during unpublished Open. Its SAME tracker
// owns the original read handle and exact buffer until decode has finished.
// No buffer may be allocated from a file length before its producer admission.
func (t *valueLogRefTracker) readCache(path string) ([]byte, error) {
	if t == nil || t.metadata == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	t.mu.Lock()
	if t.cacheReadErr != nil {
		err := t.cacheReadErr
		t.mu.Unlock()
		return nil, err
	}
	if t.cacheReadFile != nil || t.cacheReadData != nil {
		t.mu.Unlock()
		return nil, rootpublication.ErrResourceOwnership
	}
	t.cacheReadAttempted = false
	owner := t.metadata
	t.mu.Unlock()
	charge, err := rootpublication.StableFileMetadataCharge(uint64(len(path)))
	if err != nil {
		return nil, err
	}
	if err = owner.AddPending(charge); err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.cacheReadFileCharge = charge
	t.mu.Unlock()
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.cacheReadFile = file
	t.mu.Unlock()
	before, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := before.Size()
	if !before.Mode().IsRegular() || size < 0 || uint64(size) > uint64(int(^uint(0)>>1)) {
		return nil, errValueLogRefCorrupt
	}
	charge = retainedalloc.AllocationCharge(uint64(size))
	if err = owner.AddPending(charge); err != nil {
		return nil, err
	}
	data := make([]byte, int(size))
	t.mu.Lock()
	t.cacheReadData, t.cacheReadDataCharge = data, charge
	t.mu.Unlock()
	if _, err = io.ReadFull(file, data); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = errors.Join(errValueLogRefCorrupt, err)
		}
		return nil, err
	}
	var tail [1]byte
	n, readErr := file.Read(tail[:])
	if n != 0 || readErr != io.EOF {
		return nil, errors.Join(errValueLogRefCorrupt, readErr)
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) || after.Size() != size {
		return nil, errValueLogRefCorrupt
	}
	return data, nil
}
func (t *valueLogRefTracker) finishCacheRead() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.cacheReadAttempted {
		err := t.cacheReadErr
		t.mu.Unlock()
		return err
	}
	t.cacheReadAttempted = true
	file, owner := t.cacheReadFile, t.metadata
	t.mu.Unlock()
	var err error
	if file != nil {
		err = file.Close()
	}
	if err != nil {
		receipt := t.cacheReadFailure.RetainOriginalCleanupFailure(t, err)
		if owner != nil {
			owner.CleanupFailed()
		}
		t.mu.Lock()
		t.cacheReadErr = receipt
		t.mu.Unlock()
		return receipt
	}
	t.mu.Lock()
	charge := t.cacheReadFileCharge + t.cacheReadDataCharge
	clear(t.cacheReadData)
	t.cacheReadData, t.cacheReadFile = nil, nil
	t.cacheReadFileCharge, t.cacheReadDataCharge = 0, 0
	t.mu.Unlock()
	if owner != nil {
		owner.RemovePending(charge)
	}
	return nil
}
