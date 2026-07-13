package valuelog

import (
	"errors"
	"fmt"
)

var ErrFileNotFound = errors.New("valuelog: file not found")

// ErrFilePinned reports that a value-log segment is still pinned by a live
// snapshot and cannot be removed yet.
var ErrFilePinned = errors.New("valuelog: file pinned")

// ErrStableDeleteRecoveryRequired reports an interrupted identity-gated
// segment deletion that must be reconciled by a read-write manager.
var ErrStableDeleteRecoveryRequired = errors.New("valuelog: stable delete recovery required")

type fileNotFoundError struct {
	id         uint32
	inSnapshot bool
}

func (e *fileNotFoundError) Error() string {
	if e == nil {
		return ErrFileNotFound.Error()
	}
	if e.inSnapshot {
		return fmt.Sprintf("valuelog file %d not found in snapshot", e.id)
	}
	return fmt.Sprintf("valuelog file %d not found", e.id)
}

func (e *fileNotFoundError) Is(target error) bool {
	return target == ErrFileNotFound
}

type filePinnedError struct {
	id uint32
	op string
}

func (e *filePinnedError) Error() string {
	if e == nil {
		return ErrFilePinned.Error()
	}
	if e.op == "" {
		return fmt.Sprintf("valuelog file %d still pinned", e.id)
	}
	return fmt.Sprintf("cannot %s valuelog file %d: still pinned", e.op, e.id)
}

func (e *filePinnedError) Is(target error) bool {
	return target == ErrFilePinned
}
