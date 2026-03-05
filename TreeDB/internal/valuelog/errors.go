package valuelog

import (
	"errors"
	"fmt"
)

var ErrFileNotFound = errors.New("valuelog: file not found")

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
