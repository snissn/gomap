package db

import (
	"errors"
	"testing"
)

func TestAppendValueLogValuesUnavailableReturnsSentinel(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.AppendValueLogValues([][]byte{[]byte("large value")})
	if !errors.Is(err, ErrValueLogAppenderUnavailable) {
		t.Fatalf("AppendValueLogValues err=%v want ErrValueLogAppenderUnavailable", err)
	}
}
