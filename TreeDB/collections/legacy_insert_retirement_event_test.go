package collections

import (
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type bufferedConflictEvent struct {
	durabilitycut.Event
	StablePathName, StableParentName string
	Retirement                       bool
}

// Snapshot.Close can release the last borrow of an already retired segment.
// Value-log segment removal reports that physical retirement as NamespaceUnlink,
// without accepting a new mutation.
// Every other descriptor remains a failure, including WAL deletion and sync.
func bufferedConflictRetirementEvent(event durabilitycut.Event) bool {
	dir := "value_vlog"
	if event.Resource == durabilitycut.ResourceOuterLeaf {
		dir = "leaf_vlog"
	} else if event.Resource != durabilitycut.ResourceValueLog {
		return false
	}
	// Mirror valuelog.listSegments' unexported filename parser and reuse its
	// exported range check, including legacy single-sequence segment names.
	core, ok := strings.CutPrefix(filepath.Base(event.OldPath), "value-")
	if !ok {
		return false
	}
	core, ok = strings.CutSuffix(core, ".log")
	if !ok {
		return false
	}
	laneText, seqText := "0", core
	if strings.HasPrefix(core, "l") {
		laneText, seqText, ok = strings.Cut(core[1:], "-")
		if !ok {
			return false
		}
	}
	lane, laneErr := strconv.ParseUint(laneText, 10, 32)
	seq, seqErr := strconv.ParseUint(seqText, 10, 32)
	_, idErr := valuelog.EncodeFileID(uint32(lane), uint32(seq))
	if laneErr != nil || seqErr != nil || idErr != nil {
		return false
	}
	return event.Namespace == durabilitycut.NamespaceUnlink && event.Point == "" &&
		event.OldPath != "" && event.NewPath == "" && event.Root == filepath.Dir(event.OldPath) &&
		filepath.Base(event.Root) == dir && event.Path == "" && len(event.Paths) == 0 &&
		event.LSN == 0 && event.Offset == 0 && event.Length == 0 && event.CreatedDirectory == "" &&
		event.StablePath == nil && event.StableParent == nil && event.StableOldName == "" && event.StableNewName == ""
}

func TestBufferedConflictRetirementEventBoundary(t *testing.T) {
	base := durabilitycut.Event{Namespace: durabilitycut.NamespaceUnlink, Resource: durabilitycut.ResourceValueLog, Root: filepath.Join("db", "value_vlog"), OldPath: filepath.Join("db", "value_vlog", "value-l0-000001.log")}
	for _, resource := range []durabilitycut.Resource{durabilitycut.ResourceValueLog, durabilitycut.ResourceOuterLeaf} {
		event := base
		event.Resource = resource
		if resource == durabilitycut.ResourceOuterLeaf {
			event.Root, event.OldPath = filepath.Join("db", "leaf_vlog"), filepath.Join("db", "leaf_vlog", "value-l0-000001.log")
		}
		if !bufferedConflictRetirementEvent(event) {
			t.Fatalf("retired segment unlink rejected: %+v", event)
		}
	}
	legacy := base
	legacy.OldPath = filepath.Join(legacy.Root, "value-000001.log")
	if !bufferedConflictRetirementEvent(legacy) {
		t.Fatal("legacy retired segment unlink rejected")
	}
	for _, mutate := range []func(*durabilitycut.Event){
		func(e *durabilitycut.Event) { e.Resource = durabilitycut.ResourceCommandWAL },
		func(e *durabilitycut.Event) { e.Resource = durabilitycut.ResourceMeta },
		func(e *durabilitycut.Event) { e.Resource = durabilitycut.ResourceIndex },
		func(e *durabilitycut.Event) { e.Resource = durabilitycut.ResourceSeal },
		func(e *durabilitycut.Event) { e.Namespace = durabilitycut.NamespaceCreate },
		func(e *durabilitycut.Event) { e.Namespace = durabilitycut.NamespaceRename },
		func(e *durabilitycut.Event) { e.Point = durabilitycut.BeforeDependencyAppend },
		func(e *durabilitycut.Event) { e.Point = durabilitycut.BeforeDependencyFileSync },
		func(e *durabilitycut.Event) { e.Point = durabilitycut.BeforeDeletionDirectorySync },
		func(e *durabilitycut.Event) { e.Point = durabilitycut.BeforeMetaWrite },
		func(e *durabilitycut.Event) { e.LSN = 1 },
		func(e *durabilitycut.Event) { e.Root = "unrecognized" },
		func(e *durabilitycut.Event) { e.NewPath = "new" },
		func(e *durabilitycut.Event) { e.Path = "unknown" },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "unknown") },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "value-l0-bad.log") },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "value-l256-000001.log") },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "value-l0-8388608.log") },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "value-8388608.log") },
		func(e *durabilitycut.Event) { e.OldPath = filepath.Join(e.Root, "value-l0-1.log.backup") },
		func(e *durabilitycut.Event) {
			e.Root = filepath.Join("db", "unrelated")
			e.OldPath = filepath.Join(e.Root, "value-l0-000001.log")
		},
		func(e *durabilitycut.Event) { e.Paths = []string{"unknown"} },
		func(e *durabilitycut.Event) { e.CreatedDirectory = "unknown" },
		func(e *durabilitycut.Event) { e.Offset = 1 },
		func(e *durabilitycut.Event) { e.Length = 1 },
		func(e *durabilitycut.Event) { e.StableOldName = "unknown" },
		func(e *durabilitycut.Event) { e.StableNewName = "unknown" },
	} {
		event := base
		mutate(&event)
		if bufferedConflictRetirementEvent(event) {
			t.Errorf("publication or unknown event admitted: %+v", event)
		}
	}
}
