//go:build darwin || linux || freebsd || netbsd || openbsd

package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func appendStablePendingTestCommand(t *testing.T, journal *CommandJournal) {
	t.Helper()
	if _, err := journal.AppendCommand(CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatal(err)
	}
}

func openDescriptorCount(t *testing.T) (int, bool) {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func TestCommandJournalStableRotationObserverFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 12})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendStablePendingTestCommand(t, journal)

	oldFile, oldPath, oldSeq := journal.writer.f, journal.path, journal.segmentSeq
	injected := errors.New("injected command-WAL create observer failure")
	createCalls := 0
	var createdInfo os.FileInfo
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL || event.Namespace != durabilitycut.NamespaceCreate {
			return nil
		}
		createCalls++
		if createCalls == 1 {
			var statErr error
			createdInfo, statErr = os.Stat(event.NewPath)
			if statErr != nil {
				t.Fatalf("stat created successor inside observer: %v", statErr)
			}
			return injected
		}
		return nil
	})
	defer restore()

	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	if journal.writer.f != oldFile || journal.path != oldPath || journal.segmentSeq != oldSeq {
		t.Fatalf("observer failure changed old authority: file=%p/%p path=%q/%q seq=%d/%d",
			journal.writer.f, oldFile, journal.path, oldPath, journal.segmentSeq, oldSeq)
	}

	rotation, err = journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	defer rotation.Release()
	if createCalls != 1 {
		t.Fatalf("create observer calls=%d want 1", createCalls)
	}
	installedInfo, err := journal.writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createdInfo == nil || !os.SameFile(createdInfo, installedInfo) {
		t.Fatal("retry did not install the exact successor created before observer failure")
	}
}

func TestCommandJournalPendingStableSuccessorRejectsOtherRotationPaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*CommandJournal) error
	}{
		{name: "ordinary", run: func(journal *CommandJournal) error {
			return journal.RotateActiveSegment(false)
		}},
		{name: "automatic", run: func(journal *CommandJournal) error {
			journal.segmentTargetBytes = 1
			journal.captureStableResources = true
			_, err := journal.AppendCommand(CommandEnvelope{
				Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
			})
			return err
		}},
		{name: "mismatched-stable", run: func(journal *CommandJournal) error {
			journal.segmentSeq++
			defer func() { journal.segmentSeq-- }()
			rotation, err := journal.RotateActiveSegmentWithStableResources(false)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 13})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			appendStablePendingTestCommand(t, journal)
			oldFile, oldPath, oldSeq := journal.writer.f, journal.path, journal.segmentSeq

			injected := errors.New("injected command-WAL namespace-token failure")
			originalFactory := newCommandWALStableNamespaceToken
			newCommandWALStableNamespaceToken = func(rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
				return nil, injected
			}
			rotation, err := journal.RotateActiveSegmentWithStableResources(false)
			newCommandWALStableNamespaceToken = originalFactory
			if rotation != nil {
				rotation.Release()
				t.Fatal("failed rotation returned owned resources")
			}
			if !errors.Is(err, injected) {
				t.Fatalf("first rotation error=%v want token failure", err)
			}

			if err := tc.run(journal); !errors.Is(err, rootpublication.ErrResourceOwnership) {
				t.Fatalf("competing rotation error=%v want ErrResourceOwnership", err)
			}
			if journal.writer.f != oldFile || journal.path != oldPath || journal.segmentSeq != oldSeq {
				t.Fatalf("competing rotation changed authority: file=%p/%p path=%q/%q seq=%d/%d",
					journal.writer.f, oldFile, journal.path, oldPath, journal.segmentSeq, oldSeq)
			}
		})
	}
}

func TestCommandJournalStableRotationTokenFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 14})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendStablePendingTestCommand(t, journal)

	createCalls := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
		}
		return nil
	})
	defer restore()
	injected := errors.New("injected command-WAL namespace-token failure")
	originalFactory := newCommandWALStableNamespaceToken
	factoryCalls := 0
	newCommandWALStableNamespaceToken = func(spec rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return nil, injected
		}
		return originalFactory(spec)
	}
	defer func() { newCommandWALStableNamespaceToken = originalFactory }()

	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want token failure", err)
	}
	createdInfo, err := os.Stat(filepath.Join(dir, CommandSegmentName(14, 2)))
	if err != nil {
		t.Fatal(err)
	}
	rotation, err = journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	defer rotation.Release()
	installedInfo, err := journal.writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createCalls != 1 || factoryCalls != 2 || !os.SameFile(createdInfo, installedInfo) {
		t.Fatalf("retry createCalls=%d factoryCalls=%d sameFile=%t, want 1/2/true",
			createCalls, factoryCalls, os.SameFile(createdInfo, installedInfo))
	}
}

func TestCommandJournalPendingStableSuccessorReplacementFailsClosedWithoutUnlink(t *testing.T) {
	beforeFDs, checkFDs := openDescriptorCount(t)
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 15})
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingTestCommand(t, journal)

	injected := errors.New("injected command-WAL observer failure before replacement")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Namespace == durabilitycut.NamespaceCreate {
			return injected
		}
		return nil
	})
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	restore()
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	nextPath := filepath.Join(dir, CommandSegmentName(15, 2))
	displacedPath := filepath.Join(dir, "displaced-command-wal.log")
	if err := os.Rename(nextPath, displacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nextPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotation, err = journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Error("conflicting retry returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Errorf("conflicting retry error=%v want ErrResourceConflict", err)
	}
	if err := journal.Close(); err != nil {
		t.Errorf("close journal: %v", err)
	}
	if got, err := os.ReadFile(nextPath); err != nil || string(got) != "replacement" {
		t.Errorf("replacement changed: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(displacedPath); err != nil {
		t.Errorf("displaced exact successor removed: %v", err)
	}
	if afterFDs, ok := openDescriptorCount(t); checkFDs && ok && afterFDs > beforeFDs+1 {
		t.Errorf("close retained descriptors: before=%d after=%d", beforeFDs, afterFDs)
	}
}

func TestCommandJournalStableRotationOldCloseFailureIsFailStop(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 16})
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingTestCommand(t, journal)
	injected := errors.New("injected command-WAL old close failure")
	journal.writer.closeRotateFn = func(file *os.File) error {
		journal.writer.closeRotateFn = nil
		return errors.Join(file.Close(), injected)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("old-close failure returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want old-close failure", err)
	}
	if err := journal.RotateActiveSegment(false); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("ordinary rotation after fail-stop error=%v want ErrResourceOwnership", err)
	}
	rotation, err = journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("stable retry after fail-stop returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("stable retry after fail-stop error=%v want ErrResourceOwnership", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, CommandSegmentName(16, 2))); err != nil {
		t.Fatalf("fail-stop close removed created successor: %v", err)
	}
}

func TestCommandJournalStableRotationFailStopIsStickyAcrossDurabilityAPIs(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 18})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendStablePendingTestCommand(t, journal)

	injected := errors.New("injected command-WAL old close failure for sticky APIs")
	journal.writer.closeRotateFn = func(file *os.File) error {
		journal.writer.closeRotateFn = nil
		return errors.Join(file.Close(), injected)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("old-close failure returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want old-close failure", err)
	}

	rotation, stickyErr := journal.RotateActiveSegmentWithStableResources(false)
	if rotation != nil {
		rotation.Release()
		t.Fatal("fail-stop retry returned owned resources")
	}
	if !errors.Is(stickyErr, rootpublication.ErrResourceOwnership) {
		t.Fatalf("sticky retry error=%v want ErrResourceOwnership", stickyErr)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL {
			events = append(events, event)
		}
		return nil
	})
	defer restore()

	command := CommandEnvelope{
		Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1,
	}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "flush", run: journal.Flush},
		{name: "sync", run: journal.Sync},
		{name: "flush-observed", run: func() error { return journal.FlushObserved(false) }},
		{name: "sync-observed", run: func() error { return journal.FlushObserved(true) }},
		{name: "append", run: func() error {
			_, err := journal.AppendCommand(command)
			return err
		}},
		{name: "append-observed", run: func() error {
			_, err := journal.AppendCommandObserved(command)
			return err
		}},
		{name: "ordinary-rotation", run: func() error { return journal.RotateActiveSegment(false) }},
		{name: "stable-rotation", run: func() error {
			rotation, err := journal.RotateActiveSegmentWithStableResources(false)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
	}
	for _, check := range checks {
		beforeEvents := len(events)
		err := check.run()
		if err != stickyErr {
			t.Errorf("%s error=%v (%p) want exact sticky error=%v (%p)", check.name, err, err, stickyErr, stickyErr)
		}
		if !errors.Is(err, rootpublication.ErrResourceOwnership) {
			t.Errorf("%s error=%v want ErrResourceOwnership", check.name, err)
		}
		if len(events) != beforeEvents {
			t.Errorf("%s emitted durability callbacks after fail-stop: before=%d after=%d events=%+v", check.name, beforeEvents, len(events), events[beforeEvents:])
		}
	}
}

func TestCommandJournalStableRotationRetryDescriptorPlateau(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{Lane: 17})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendStablePendingTestCommand(t, journal)
	baseline, checkFDs := openDescriptorCount(t)
	injected := errors.New("injected command-WAL retry cycle")
	createCalls := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
			return injected
		}
		return nil
	})
	defer restore()

	for cycle := 0; cycle < 64; cycle++ {
		rotation, err := journal.RotateActiveSegmentWithStableResources(false)
		if rotation != nil {
			rotation.Release()
			t.Fatalf("cycle %d first attempt returned resources", cycle)
		}
		if !errors.Is(err, injected) {
			t.Fatalf("cycle %d first attempt error=%v", cycle, err)
		}
		rotation, err = journal.RotateActiveSegmentWithStableResources(false)
		if err != nil {
			t.Fatalf("cycle %d retry: %v", cycle, err)
		}
		rotation.Release()
		appendStablePendingTestCommand(t, journal)
		if got, ok := openDescriptorCount(t); checkFDs && ok && got > baseline+1 {
			t.Fatalf("cycle %d descriptors=%d baseline=%d", cycle, got, baseline)
		}
	}
	if createCalls != 64 {
		t.Fatalf("create observer calls=%d want 64", createCalls)
	}
	if journal.segmentSeq != 65 {
		t.Fatalf("final segment seq=%d want 65", journal.segmentSeq)
	}
	if got := filepath.Base(journal.path); got != CommandSegmentName(17, 65) {
		t.Fatalf("final path=%q", got)
	}
}
