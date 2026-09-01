package db

import (
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type multiSegmentValueLogAppender struct {
	segments []ValueLogAppenderSegment
	err      error
}

func (*multiSegmentValueLogAppender) AppendValues([][]byte) ([]page.ValuePtr, error) { return nil, nil }
func (*multiSegmentValueLogAppender) Flush() error                                   { return nil }
func (*multiSegmentValueLogAppender) Sync() error                                    { return nil }
func (*multiSegmentValueLogAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "fallback", 99, true
}
func (a *multiSegmentValueLogAppender) CurrentValueLogSegmentsSnapshot() ([]ValueLogAppenderSegment, error) {
	return a.segments, a.err
}

func TestFinalizeDependencySyncEventIncludesEveryValueLogAppenderSegment(t *testing.T) {
	appender := &multiSegmentValueLogAppender{segments: []ValueLogAppenderSegment{
		{Path: "/vlog/lane-2", FileID: 2},
		{Path: "/vlog/lane-1", FileID: 1},
	}}
	event, ok, err := (&DB{dir: "/db"}).finalizeDependencySyncEvent(appender)
	if err != nil {
		t.Fatalf("finalizeDependencySyncEvent: %v", err)
	}
	if !ok {
		t.Fatal("finalizeDependencySyncEvent did not report dependencies")
	}
	if want := []string{"/vlog/lane-1", "/vlog/lane-2"}; !reflect.DeepEqual(event.Paths, want) {
		t.Fatalf("dependency paths=%v, want %v", event.Paths, want)
	}
}

func TestFinalizeDependencySyncEventFailsClosedOnValueLogSegmentEnumerationError(t *testing.T) {
	injected := errors.New("injected segment enumeration failure")
	_, _, err := (&DB{dir: "/db"}).finalizeDependencySyncEvent(&multiSegmentValueLogAppender{err: injected})
	if !errors.Is(err, injected) {
		t.Fatalf("finalizeDependencySyncEvent error=%v, want %v", err, injected)
	}
}
