package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestVectorPartitionLifecycleV1FullTransitionAndGuards(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	record := VectorPartitionLifecycleRecordV1{}
	begin := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b", "group-a"},
		PreviousActiveGeneration: 0, MutationEpoch: 10,
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, begin)
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleBuildingV1, 1)
	if got := record.RequiredGroups; !reflect.DeepEqual(got, []raftcluster.GroupID{"group-a", "group-b"}) {
		t.Fatalf("required groups=%v", got)
	}

	readyA := vectorPartitionLifecycleTestReadyV1("group-a", "b")
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = readyA
	}))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleStagedV1, 2)

	retry := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.ExpectedRevision--
		c.ExpectedState = VectorPartitionLifecycleBuildingV1
		c.GroupReady = readyA
	})
	retryBytes, err := EncodeVectorPartitionLifecycleCommandV1(retry)
	if err != nil {
		t.Fatal(err)
	}
	// Exact replay uses the originally committed bytes, including its revision.
	replayed, err := ApplyVectorPartitionLifecycleCommandV1(record, mustDecodeVectorPartitionLifecycleCommandV1(t, retryBytes))
	if err != nil {
		t.Fatalf("exact ready retry: %v", err)
	}
	if !reflect.DeepEqual(replayed, record) {
		t.Fatalf("exact retry changed record")
	}

	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-b", "c")
	}))
	digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	if err := record.CanPrepare(identity, digest); err != nil {
		t.Fatalf("CanPrepare: %v", err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.ReadySetDigest = digest
	}))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecyclePreparedV1, 4)

	if err := record.CanActivate(identity, 0, 10); err != nil {
		t.Fatalf("CanActivate: %v", err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.MutationEpoch = 0, 10
	}))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleActiveV1, 5)

	search := VectorPartitionLifecycleSearchProofV1{Identity: identity, ReadySetDigest: digest}
	if err := record.CanSearch(search); err != nil {
		t.Fatalf("CanSearch: %v", err)
	}
	mutation := VectorPartitionLifecycleMutationProofV1{
		IndexIdentity: identity.Index, ActiveGeneration: identity.Generation, InvalidationEpoch: 11,
	}
	if err := record.CanCommitRelevantMutation(mutation); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("active mutation admission err=%v", err)
	}

	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "vector field update", 11
	}))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleInvalidatedV1, 6)
	if err := record.CanSearch(search); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("invalidated search err=%v", err)
	}
	if err := record.CanCommitRelevantMutation(mutation); err != nil {
		t.Fatalf("invalidated mutation admission: %v", err)
	}

	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRetireV1, nil))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleRetiredV1, 7)
	if err := record.CanClean(VectorPartitionLifecycleReferencesV1{ReaderPins: 1}); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("pinned CanClean err=%v", err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleMarkCleanableV1, nil))
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleCleanableV1, 8)

	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupCleanupV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupID = "group-b"
	}))
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleCompleteCleanupV1, nil)); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("incomplete cleanup err=%v", err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupCleanupV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupID = "group-a"
	}))
	complete := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleCompleteCleanupV1, nil)
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, complete)
	assertVectorPartitionLifecycleStateV1(t, record, VectorPartitionLifecycleAbsentV1, 11)
	if !record.CleanupComplete || len(record.RequiredGroups) != 0 || len(record.ReadyGroups) != 0 {
		t.Fatalf("terminal record=%+v", record)
	}
	replayed, err = ApplyVectorPartitionLifecycleCommandV1(record, complete)
	if err != nil {
		t.Fatalf("complete cleanup retry: %v", err)
	}
	if !reflect.DeepEqual(replayed, record) {
		t.Fatalf("complete cleanup retry changed record")
	}
}

func TestVectorPartitionLifecycleV1AbortDoesNotRequireCompleteReadySet(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a", "group-b"},
		PreviousActiveGeneration: 6, MutationEpoch: 10,
	})
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "d")
	}))
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleAbortBuildV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason = "operator cancelled build"
	}))
	if record.State != VectorPartitionLifecycleRetiredV1 || !record.Aborted || record.InvalidationEpoch != 0 {
		t.Fatalf("aborted record=%+v", record)
	}
	if err := record.CanCommitRelevantMutation(VectorPartitionLifecycleMutationProofV1{
		IndexIdentity: identity.Index, ActiveGeneration: identity.Generation, InvalidationEpoch: 0,
	}); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("aborted build falsely proved active invalidation: %v", err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleMarkCleanableV1, nil))
	if record.State != VectorPartitionLifecycleCleanableV1 {
		t.Fatalf("aborted cleanable state=%q", record.State)
	}
}

func TestVectorPartitionLifecycleV1AtomicCutoverRetiresPreviousActive(t *testing.T) {
	oldIdentity := vectorPartitionLifecycleTestIdentityV1()
	oldIdentity.Generation = 6
	oldIdentity.Source.Generation = 10
	previous := vectorPartitionLifecycleBuildPreparedV1(t, oldIdentity, 0, 9)
	previous = applyVectorPartitionLifecycleTestCommandV1(t, previous, vectorPartitionLifecycleTestCommandV1(previous, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.MutationEpoch = 9
	}))

	newIdentity := vectorPartitionLifecycleTestIdentityV1()
	candidate := vectorPartitionLifecycleBuildPreparedV1(t, newIdentity, oldIdentity.Generation, 10)
	cutover := vectorPartitionLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration = oldIdentity.Generation
		c.PreviousActiveRevision = previous.Revision
		c.MutationEpoch = 10
	})
	if _, err := ApplyVectorPartitionLifecycleCommandV1(candidate, cutover); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("non-atomic activation with previous generation err=%v", err)
	}

	stale := cutover
	stale.PreviousActiveRevision--
	if _, _, err := ApplyVectorPartitionLifecycleCutoverV1(previous, candidate, stale); !errors.Is(err, ErrVectorPartitionLifecycleStale) {
		t.Fatalf("stale previous active revision err=%v", err)
	}

	retired, active, err := ApplyVectorPartitionLifecycleCutoverV1(previous, candidate, cutover)
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if retired.State != VectorPartitionLifecycleRetiredV1 ||
		retired.SupersededByGeneration != newIdentity.Generation ||
		retired.InvalidationEpoch != 0 || active.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("cutover old/new=%+v / %+v", retired, active)
	}
	if err := retired.CanSearch(VectorPartitionLifecycleSearchProofV1{
		Identity: oldIdentity, ReadySetDigest: retired.ReadySetDigest,
	}); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("superseded generation remained searchable: %v", err)
	}
	if err := active.CanSearch(VectorPartitionLifecycleSearchProofV1{
		Identity: newIdentity, ReadySetDigest: active.ReadySetDigest,
	}); err != nil {
		t.Fatalf("new active generation is not searchable: %v", err)
	}
	if err := retired.CanClean(VectorPartitionLifecycleReferencesV1{}); err != nil {
		t.Fatalf("superseded generation is not cleanup-eligible: %v", err)
	}
	if err := retired.CanCommitRelevantMutation(VectorPartitionLifecycleMutationProofV1{
		IndexIdentity: oldIdentity.Index, ActiveGeneration: oldIdentity.Generation,
	}); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("supersession falsely proved mutation invalidation: %v", err)
	}

	retryOld, retryNew, err := ApplyVectorPartitionLifecycleCutoverV1(retired, active, cutover)
	if err != nil {
		t.Fatalf("cutover retry: %v", err)
	}
	if !reflect.DeepEqual(retryOld, retired) || !reflect.DeepEqual(retryNew, active) {
		t.Fatal("cutover retry changed records")
	}
}

func TestVectorPartitionLifecycleV1RejectsStaleWrongIdentityAndConflicts(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a", "group-b"},
		PreviousActiveGeneration: 6, MutationEpoch: 10,
	})
	ready := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "a")
	})
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, ready)

	stale := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.ExpectedRevision--
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-b", "b")
	})
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, stale); !errors.Is(err, ErrVectorPartitionLifecycleStale) {
		t.Fatalf("stale err=%v", err)
	}

	wrong := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Identity.Index.CollectionIncarnation++
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-b", "b")
	})
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, wrong); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
		t.Fatalf("wrong identity err=%v", err)
	}

	conflict := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "f")
	})
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, conflict); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("changed readiness err=%v", err)
	}

	extra := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-c", "c")
	})
	if _, err := ApplyVectorPartitionLifecycleCommandV1(record, extra); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
		t.Fatalf("extra readiness err=%v", err)
	}

	badDigest := strings.Repeat("f", 64)
	if err := record.CanPrepare(identity, badDigest); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("incomplete/bad ready set err=%v", err)
	}
}

func TestVectorPartitionLifecycleV1EveryIdentityFieldFailsClosed(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"}, MutationEpoch: 10,
	})
	mutations := []struct {
		name   string
		mutate func(*VectorPartitionLifecycleIdentityV1)
	}{
		{"database", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.Collection.Database = "other" }},
		{"catalog name", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.Collection.Catalog = "other" }},
		{"collection", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.Collection.Collection = "other" }},
		{"collection incarnation", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.CollectionIncarnation++ }},
		{"index name", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.IndexName = "other" }},
		{"index digest", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.IndexDefinitionDigest = strings.Repeat("b", 64) }},
		{"index epoch", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.IndexEpoch++ }},
		{"catalog epoch", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.CatalogEpoch++ }},
		{"catalog digest", func(x *VectorPartitionLifecycleIdentityV1) { x.Index.CatalogDigest = strings.Repeat("8", 64) }},
		{"source generation", func(x *VectorPartitionLifecycleIdentityV1) { x.Source.Generation++ }},
		{"source checksum", func(x *VectorPartitionLifecycleIdentityV1) { x.Source.Checksum++ }},
		{"source schema", func(x *VectorPartitionLifecycleIdentityV1) { x.Source.SchemaHash++ }},
		{"source rows", func(x *VectorPartitionLifecycleIdentityV1) { x.Source.RowCount++ }},
		{"generation", func(x *VectorPartitionLifecycleIdentityV1) { x.Generation++ }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			command := vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
				test.mutate(&c.Identity)
				c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "a")
			})
			if _, err := ApplyVectorPartitionLifecycleCommandV1(record, command); !errors.Is(err, ErrVectorPartitionLifecycleIdentity) {
				t.Fatalf("changed identity err=%v", err)
			}
		})
	}
}

func TestVectorPartitionLifecycleV1CommandStateMatrix(t *testing.T) {
	states := []VectorPartitionLifecycleStateV1{
		VectorPartitionLifecycleAbsentV1,
		VectorPartitionLifecycleBuildingV1,
		VectorPartitionLifecycleStagedV1,
		VectorPartitionLifecyclePreparedV1,
		VectorPartitionLifecycleActiveV1,
		VectorPartitionLifecycleInvalidatedV1,
		VectorPartitionLifecycleRetiredV1,
		VectorPartitionLifecycleCleanableV1,
	}
	tests := []struct {
		kind    VectorPartitionLifecycleCommandKindV1
		allowed map[VectorPartitionLifecycleStateV1]bool
	}{
		{VectorPartitionLifecycleBeginBuildV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleAbsentV1: true}},
		{VectorPartitionLifecycleRecordGroupReadyV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleBuildingV1: true, VectorPartitionLifecycleStagedV1: true}},
		{VectorPartitionLifecyclePrepareV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleStagedV1: true}},
		{VectorPartitionLifecycleActivateV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecyclePreparedV1: true}},
		{VectorPartitionLifecycleAbortBuildV1, map[VectorPartitionLifecycleStateV1]bool{
			VectorPartitionLifecycleBuildingV1: true, VectorPartitionLifecycleStagedV1: true, VectorPartitionLifecyclePreparedV1: true,
		}},
		{VectorPartitionLifecycleInvalidateV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleActiveV1: true}},
		{VectorPartitionLifecycleRetireV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleInvalidatedV1: true}},
		{VectorPartitionLifecycleMarkCleanableV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleRetiredV1: true}},
		{VectorPartitionLifecycleRecordGroupCleanupV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleCleanableV1: true}},
		{VectorPartitionLifecycleCompleteCleanupV1, map[VectorPartitionLifecycleStateV1]bool{VectorPartitionLifecycleCleanableV1: true}},
	}
	for _, test := range tests {
		for _, state := range states {
			t.Run(string(test.kind)+"/"+string(state), func(t *testing.T) {
				command := VectorPartitionLifecycleCommandV1{
					Kind: test.kind, ExpectedRevision: 1, ExpectedState: state,
					Identity: vectorPartitionLifecycleTestIdentityV1(),
				}
				switch test.kind {
				case VectorPartitionLifecycleBeginBuildV1:
					command.ExpectedRevision = 0
					command.RequiredGroups = []raftcluster.GroupID{"group-a"}
					command.MutationEpoch = 10
				case VectorPartitionLifecycleRecordGroupReadyV1:
					command.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "a")
				case VectorPartitionLifecyclePrepareV1:
					command.ReadySetDigest = strings.Repeat("a", 64)
				case VectorPartitionLifecycleActivateV1:
					command.MutationEpoch = 10
				case VectorPartitionLifecycleAbortBuildV1:
					command.Reason = "abort"
				case VectorPartitionLifecycleInvalidateV1:
					command.Reason, command.InvalidationEpoch = "mutation", 11
				case VectorPartitionLifecycleRecordGroupCleanupV1:
					command.GroupID = "group-a"
				}
				_, err := EncodeVectorPartitionLifecycleCommandV1(command)
				if test.allowed[state] && err != nil {
					t.Fatalf("allowed state rejected: %v", err)
				}
				if !test.allowed[state] && !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
					t.Fatalf("forbidden state err=%v", err)
				}
			})
		}
	}
}

func TestVectorPartitionLifecycleV1ReadySetDigestIsCanonicalAndExact(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	required := []raftcluster.GroupID{"group-b", "group-a"}
	ready := []VectorPartitionLifecycleGroupReadyV1{
		vectorPartitionLifecycleTestReadyV1("group-b", "b"),
		vectorPartitionLifecycleTestReadyV1("group-a", "a"),
	}
	left, err := VectorPartitionLifecycleReadySetDigestV1(identity, required, ready)
	if err != nil {
		t.Fatal(err)
	}
	right, err := VectorPartitionLifecycleReadySetDigestV1(identity,
		[]raftcluster.GroupID{"group-a", "group-b"},
		[]VectorPartitionLifecycleGroupReadyV1{ready[1], ready[0]})
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("unordered inputs produced %q and %q", left, right)
	}
	ready[0].AppliedIndex++
	changed, err := VectorPartitionLifecycleReadySetDigestV1(identity, required, ready)
	if err != nil {
		t.Fatal(err)
	}
	if changed == left {
		t.Fatal("changed applied proof did not change ready-set digest")
	}
	if _, err := VectorPartitionLifecycleReadySetDigestV1(identity, required, ready[:1]); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("missing group err=%v", err)
	}
}

func TestVectorPartitionLifecycleV1CanonicalCodecsAndLimits(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b", "group-a"},
		PreviousActiveGeneration: 6, MutationEpoch: 10,
	}
	first, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("command encoding is nondeterministic")
	}
	decoded := mustDecodeVectorPartitionLifecycleCommandV1(t, first)
	if !reflect.DeepEqual(decoded.RequiredGroups, []raftcluster.GroupID{"group-a", "group-b"}) {
		t.Fatalf("decoded required groups=%v", decoded.RequiredGroups)
	}

	rawNoncanonical, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionLifecycleCommandV1(rawNoncanonical); !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
		t.Fatalf("noncanonical command err=%v", err)
	}
	withUnknown := append(append([]byte(nil), first[:len(first)-1]...), []byte(`,"unknown":1}`)...)
	if _, err := DecodeVectorPartitionLifecycleCommandV1(withUnknown); !errors.Is(err, ErrInvalidVectorPartitionLifecycle) {
		t.Fatalf("unknown field err=%v", err)
	}

	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, command)
	recordBytes, err := EncodeVectorPartitionLifecycleRecordV1(record)
	if err != nil {
		t.Fatal(err)
	}
	decodedRecord, err := DecodeVectorPartitionLifecycleRecordV1(recordBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedRecord, record) {
		t.Fatalf("record round trip mismatch\ngot:  %+v\nwant: %+v", decodedRecord, record)
	}

	tooMany := make([]raftcluster.GroupID, MaxVectorPartitionLifecycleGroupsV1+1)
	for i := range tooMany {
		tooMany[i] = raftcluster.GroupID("g" + strings.Repeat("x", i/26) + string(rune('a'+i%26)))
	}
	command.RequiredGroups = tooMany
	if _, err := EncodeVectorPartitionLifecycleCommandV1(command); !errors.Is(err, ErrVectorPartitionLifecycleLimit) {
		t.Fatalf("oversized group set err=%v", err)
	}
}

func TestVectorPartitionLifecycleV1TransitionGuardsFailClosed(t *testing.T) {
	identity := vectorPartitionLifecycleTestIdentityV1()
	states := []VectorPartitionLifecycleStateV1{
		VectorPartitionLifecycleAbsentV1,
		VectorPartitionLifecycleBuildingV1,
		VectorPartitionLifecycleStagedV1,
		VectorPartitionLifecyclePreparedV1,
		VectorPartitionLifecycleActiveV1,
		VectorPartitionLifecycleInvalidatedV1,
		VectorPartitionLifecycleRetiredV1,
		VectorPartitionLifecycleCleanableV1,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			record := vectorPartitionLifecycleTestRecordAtStateV1(t, state)
			search := VectorPartitionLifecycleSearchProofV1{Identity: identity, ReadySetDigest: record.ReadySetDigest}
			err := record.CanSearch(search)
			if state == VectorPartitionLifecycleActiveV1 {
				if err != nil {
					t.Fatalf("active CanSearch: %v", err)
				}
			} else if !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
				t.Fatalf("state %q CanSearch err=%v", state, err)
			}
			err = record.CanClean(VectorPartitionLifecycleReferencesV1{})
			if state == VectorPartitionLifecycleRetiredV1 {
				if err != nil {
					t.Fatalf("retired CanClean: %v", err)
				}
			} else if !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
				t.Fatalf("state %q CanClean err=%v", state, err)
			}
		})
	}
}

func TestVectorPartitionLifecycleV1NoActiveMutationProof(t *testing.T) {
	record, err := canonicalVectorPartitionLifecycleRecordV1(VectorPartitionLifecycleRecordV1{})
	if err != nil {
		t.Fatal(err)
	}
	proof := VectorPartitionLifecycleMutationProofV1{IndexIdentity: vectorPartitionLifecycleTestIdentityV1().Index}
	if err := record.CanCommitRelevantMutation(proof); err != nil {
		t.Fatalf("absent lifecycle rejected no-active proof: %v", err)
	}
	proof.ActiveGeneration = 7
	if err := record.CanCommitRelevantMutation(proof); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("absent lifecycle accepted named active generation: %v", err)
	}
}

func vectorPartitionLifecycleTestRecordAtStateV1(t *testing.T, state VectorPartitionLifecycleStateV1) VectorPartitionLifecycleRecordV1 {
	t.Helper()
	identity := vectorPartitionLifecycleTestIdentityV1()
	if state == VectorPartitionLifecycleAbsentV1 {
		record, err := canonicalVectorPartitionLifecycleRecordV1(VectorPartitionLifecycleRecordV1{})
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"},
		PreviousActiveGeneration: 0, MutationEpoch: 10,
	})
	if state == VectorPartitionLifecycleBuildingV1 {
		return record
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "a")
	}))
	if state == VectorPartitionLifecycleStagedV1 {
		return record
	}
	digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.ReadySetDigest = digest
	}))
	if state == VectorPartitionLifecyclePreparedV1 {
		return record
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleActivateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.PreviousActiveGeneration, c.MutationEpoch = 0, 10
	}))
	if state == VectorPartitionLifecycleActiveV1 {
		return record
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleInvalidateV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.Reason, c.InvalidationEpoch = "test invalidation", 11
	}))
	if state == VectorPartitionLifecycleInvalidatedV1 {
		return record
	}
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRetireV1, nil))
	if state == VectorPartitionLifecycleRetiredV1 {
		return record
	}
	return applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleMarkCleanableV1, nil))
}

func vectorPartitionLifecycleTestCommandV1(record VectorPartitionLifecycleRecordV1, kind VectorPartitionLifecycleCommandKindV1, mutate func(*VectorPartitionLifecycleCommandV1)) VectorPartitionLifecycleCommandV1 {
	command := VectorPartitionLifecycleCommandV1{
		Kind: kind, ExpectedRevision: record.Revision, ExpectedState: record.State, Identity: record.Identity,
	}
	if mutate != nil {
		mutate(&command)
	}
	return command
}

func vectorPartitionLifecycleBuildPreparedV1(t *testing.T, identity VectorPartitionLifecycleIdentityV1, previousGeneration, mutationEpoch uint64) VectorPartitionLifecycleRecordV1 {
	t.Helper()
	record := applyVectorPartitionLifecycleTestCommandV1(t, VectorPartitionLifecycleRecordV1{}, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-a"},
		PreviousActiveGeneration: previousGeneration, MutationEpoch: mutationEpoch,
	})
	record = applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecycleRecordGroupReadyV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.GroupReady = vectorPartitionLifecycleTestReadyV1("group-a", "a")
	}))
	digest, err := VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	return applyVectorPartitionLifecycleTestCommandV1(t, record, vectorPartitionLifecycleTestCommandV1(record, VectorPartitionLifecyclePrepareV1, func(c *VectorPartitionLifecycleCommandV1) {
		c.ReadySetDigest = digest
	}))
}

func applyVectorPartitionLifecycleTestCommandV1(t *testing.T, record VectorPartitionLifecycleRecordV1, command VectorPartitionLifecycleCommandV1) VectorPartitionLifecycleRecordV1 {
	t.Helper()
	next, err := ApplyVectorPartitionLifecycleCommandV1(record, command)
	if err != nil {
		t.Fatalf("apply %q: %v", command.Kind, err)
	}
	return next
}

func mustDecodeVectorPartitionLifecycleCommandV1(t *testing.T, raw []byte) VectorPartitionLifecycleCommandV1 {
	t.Helper()
	command, err := DecodeVectorPartitionLifecycleCommandV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func vectorPartitionLifecycleTestReadyV1(group raftcluster.GroupID, digit string) VectorPartitionLifecycleGroupReadyV1 {
	return VectorPartitionLifecycleGroupReadyV1{
		GroupID: group, AppliedIndex: 100, AssetSetDigest: strings.Repeat(digit, 64),
	}
}

func vectorPartitionLifecycleTestIdentityV1() VectorPartitionLifecycleIdentityV1 {
	return VectorPartitionLifecycleIdentityV1{
		Index: VectorPartitionLifecycleIndexIdentityV1{
			Collection:            CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"},
			CollectionIncarnation: 3,
			IndexName:             "embedding",
			IndexDefinitionDigest: strings.Repeat("a", 64),
			IndexEpoch:            4,
			CatalogEpoch:          5,
			CatalogDigest:         strings.Repeat("9", 64),
		},
		Source: VectorPartitionLifecycleSourceIdentityV1{
			Generation: 11, Checksum: 12, SchemaHash: 13, RowCount: 100,
		},
		Generation: 7,
	}
}

func assertVectorPartitionLifecycleStateV1(t *testing.T, record VectorPartitionLifecycleRecordV1, state VectorPartitionLifecycleStateV1, revision uint64) {
	t.Helper()
	if record.State != state || record.Revision != revision {
		t.Fatalf("record state/revision=%q/%d want %q/%d", record.State, record.Revision, state, revision)
	}
}
