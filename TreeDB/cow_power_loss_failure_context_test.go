package treedb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

const cowModeledFailureTextBytes = 192

// Failure metadata owns only bounded values, never the event's handles, slice,
// strings or error. Callers hold the original observerMu for record and report.
type cowModeledObserveFailure struct {
	recorded                                       bool
	point, resource, namespace                     cowModeledFailureText
	lsn                                            uint64
	stablePath, stableParent                       bool
	root, path, oldPath, newPath, createdDirectory cowModeledFailureText
	paths                                          [4]cowModeledFailureText
	pathsCount                                     int
	pathsTruncated                                 bool
}

type cowModeledFailureText struct {
	value     [cowModeledFailureTextBytes]byte
	n         uint16
	truncated bool
	outside   bool
}

func cowModeledFailureTextValue(value string) (text cowModeledFailureText) {
	text.n = uint16(copy(text.value[:], value))
	text.truncated = len(value) > len(text.value)
	return text
}

func cowModeledFailureRelativeName(root, path string) cowModeledFailureText {
	if path == "" {
		return cowModeledFailureText{}
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return cowModeledFailureText{outside: true}
	}
	return cowModeledFailureTextValue(filepath.ToSlash(rel))
}

func (text cowModeledFailureText) String() string {
	return fmt.Sprintf("%q{truncated=%t,outside=%t}", text.value[:text.n], text.truncated, text.outside)
}

func (failure *cowModeledObserveFailure) record(root string, event durabilitycut.Event, err error) error {
	if err == nil || failure.recorded {
		return err
	}
	failure.recorded = true
	failure.point = cowModeledFailureTextValue(string(event.Point))
	failure.resource = cowModeledFailureTextValue(string(event.Resource))
	failure.namespace = cowModeledFailureTextValue(string(event.Namespace))
	failure.lsn = event.LSN
	failure.stablePath, failure.stableParent = event.StablePath != nil, event.StableParent != nil
	failure.root = cowModeledFailureRelativeName(root, event.Root)
	failure.path = cowModeledFailureRelativeName(root, event.Path)
	failure.oldPath = cowModeledFailureRelativeName(root, event.OldPath)
	failure.newPath = cowModeledFailureRelativeName(root, event.NewPath)
	failure.createdDirectory = cowModeledFailureRelativeName(root, event.CreatedDirectory)
	failure.pathsCount = len(event.Paths)
	failure.pathsTruncated = len(event.Paths) > len(failure.paths)
	for i := 0; i < len(event.Paths) && i < len(failure.paths); i++ {
		failure.paths[i] = cowModeledFailureRelativeName(root, event.Paths[i])
	}
	return err
}

func TestCOWModeledObserveFailureFirstError(t *testing.T) {
	root := t.TempDir()
	event := durabilitycut.Event{
		Point: durabilitycut.AfterDependencyFileSync, Resource: durabilitycut.ResourceOuterLeaf,
		Namespace: durabilitycut.NamespaceRename, LSN: 7, Root: root,
		Path: filepath.Join(root, "leaf_vlog", "manifest"), Paths: []string{filepath.Join(root, "group")},
		StablePath: os.Stdout, StableParent: os.Stderr,
	}
	var failure cowModeledObserveFailure
	if err := failure.record(root, event, nil); err != nil || failure.recorded {
		t.Fatal("successful observation recorded a failure")
	}
	pathErr := &os.PathError{Op: "open", Path: event.Path, Err: os.ErrNotExist}
	err := errors.Join(pathErr, os.ErrPermission)
	if got := failure.record(root, event, err); got != err || !errors.Is(got, os.ErrNotExist) {
		t.Fatalf("original error replaced: %v", got)
	}
	var gotPath *os.PathError
	if !errors.As(err, &gotPath) || gotPath != pathErr {
		t.Fatal("original path error identity lost")
	}
	want := failure
	event.Paths[0] = filepath.Join(root, "changed")
	event.LSN++
	later := errors.New("later observation")
	if got := failure.record(root, event, later); got != later || failure != want {
		t.Fatal("later error replaced first metadata or its own return value")
	}
	if got := failure.record(root, event, nil); got != nil || failure != want {
		t.Fatal("later success reset first metadata")
	}
	if !failure.stablePath || !failure.stableParent || failure.lsn != 7 ||
		string(failure.paths[0].value[:failure.paths[0].n]) != "group" ||
		string(failure.path.value[:failure.path.n]) != "leaf_vlog/manifest" {
		t.Fatalf("first borrowed event was not copied by value: %+v", failure)
	}
}

func TestCOWModeledObserveFailureBounds(t *testing.T) {
	root := t.TempDir()
	long := strings.Repeat("x", cowModeledFailureTextBytes+1)
	event := durabilitycut.Event{
		Point: durabilitycut.Point(long), Resource: durabilitycut.Resource(long), Namespace: durabilitycut.NamespaceOperation(long),
		Root: root, Path: filepath.Join(root, long), OldPath: filepath.Join(root, "..", "outside"),
		Paths: []string{filepath.Join(root, "0"), filepath.Join(root, "1"), filepath.Join(root, "2"), filepath.Join(root, long), filepath.Join(root, "omitted")},
	}
	var failure cowModeledObserveFailure
	failure.record(root, event, os.ErrNotExist)
	for _, text := range []cowModeledFailureText{failure.point, failure.resource, failure.namespace, failure.path, failure.paths[3]} {
		if text.n != cowModeledFailureTextBytes || !text.truncated || text.outside || len(text.String()) > 4*cowModeledFailureTextBytes+64 {
			t.Fatalf("text bound or truncation marker lost: %s", text)
		}
	}
	if !failure.oldPath.outside || failure.oldPath.n != 0 || failure.createdDirectory.n != 0 ||
		failure.pathsCount != 5 || !failure.pathsTruncated || string(failure.root.value[:failure.root.n]) != "." {
		t.Fatalf("relative path/count bound lost: %+v", failure)
	}
}

func TestCOWModeledObserveFailureSharedLock(t *testing.T) {
	root := t.TempDir()
	var observerMu sync.Mutex
	var failure cowModeledObserveFailure
	first := errors.New("first")
	observerMu.Lock()
	failure.record(root, durabilitycut.Event{LSN: 1}, first)
	want := failure
	observerMu.Unlock()
	var joined sync.WaitGroup
	results := make(chan bool, 8)
	for i := 0; i < cap(results); i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			observerMu.Lock()
			defer observerMu.Unlock()
			later := errors.New("later")
			err := failure.record(root, durabilitycut.Event{LSN: 2}, later)
			results <- err == later && failure == want
		}()
	}
	joined.Wait()
	close(results)
	for ok := range results {
		if !ok {
			t.Fatal("record/read under shared observer lock changed first failure")
		}
	}
}
