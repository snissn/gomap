package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestOrderedBuilderTransactionalValidationRollback(t *testing.T) {
	opts := orderedCompletionOptions(t.TempDir())
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := New(db)
	if err := store.AdvanceDiscardFloor(5, CommitDurable); err != nil {
		t.Fatal(err)
	}
	var b CommitBuilder
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	original := []byte{'a', 0, 'b'}
	key := bytes.Clone(original)
	if err := b.AppendGroup(CommitGroup{Timestamp: 6, Mutations: []Mutation{{Key: key, Value: []byte("owned")}}}); err != nil {
		t.Fatal(err)
	}
	copy(key, []byte("zzz"))
	cases := []struct {
		name  string
		group CommitGroup
		want  error
	}{
		{"invalid-key", CommitGroup{Timestamp: 7, Mutations: []Mutation{{Key: []byte("retry-invalid")}, {Key: bytes.Repeat([]byte("k"), 65536)}}}, ErrInvalidKey},
		{"duplicate-in-group", CommitGroup{Timestamp: 7, Mutations: []Mutation{{Key: []byte("retry-within")}, {Key: []byte("retry-within")}}}, ErrDuplicateKey},
		{"duplicate-prior-group", CommitGroup{Timestamp: 6, Mutations: []Mutation{{Key: []byte("retry-prior")}, {Key: original}}}, ErrDuplicateKey},
		{"below-floor", CommitGroup{Timestamp: 5, Mutations: []Mutation{{Key: []byte("retry-floor")}}}, ErrVersionBelowDiscardFloor},
	}
	for _, tc := range cases {
		tc.group.Mutations[0].Value = []byte("retry")
		t.Run(tc.name, func(t *testing.T) {
			before := len(b.seen)
			if err := b.AppendGroup(tc.group); !errors.Is(err, tc.want) || errors.Is(err, ErrStorage) {
				t.Fatalf("validation error=%v want=%v", err, tc.want)
			}
			if len(b.seen) != before {
				t.Fatalf("failed group changed identities: %d -> %d", before, len(b.seen))
			}
			if _, ok := b.seen[commitIdentity{string(original), 6}]; !ok {
				t.Fatal("rollback erased prior group or retained caller key alias")
			}
			retry := tc.group
			retry.Mutations = retry.Mutations[:1]
			if retry.Timestamp <= 5 {
				// The same below-floor identity must have been rolled back too.
				if _, exists := b.seen[commitIdentity{string(retry.Mutations[0].Key), retry.Timestamp}]; exists {
					t.Fatal("floor rejection retained identity")
				}
				retry.Timestamp = 8
			}
			if err := b.AppendGroup(retry); err != nil {
				t.Fatalf("fresh prefix could not be staged after rejection: %v", err)
			}
		})
	}
	if err := b.AppendGroup(CommitGroup{Timestamp: 6, Mutations: []Mutation{{Key: key, Value: []byte("distinct")}}}); err != nil {
		t.Fatal(err)
	}
	var c CommitCompletion
	if err := b.Submit(CommitDurable, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
	if len(b.seen) != 0 || len(b.mutations) != 0 {
		t.Fatal("completed builder retained identities or operands")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store = New(db)
	requireResult(t, store, original, 6, Present, 6, []byte("owned"))
	requireResult(t, store, key, 6, Present, 6, []byte("distinct"))
	for _, tc := range cases {
		ts := tc.group.Timestamp
		if ts <= 5 {
			ts = 8
		}
		requireResult(t, store, tc.group.Mutations[0].Key, ts, Present, ts, []byte("retry"))
	}
}

func TestOrderedBuilderScratchOwnershipAndRetention(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	var b CommitBuilder
	for round := 0; round < 2; round++ {
		if err := store.BeginCommitGroup(&b); err != nil {
			t.Fatal(err)
		}
		if err := b.AppendGroup(CommitGroup{Timestamp: 1, Mutations: []Mutation{{Key: []byte("small"), Value: []byte("value")}}}); err != nil {
			t.Fatal(err)
		}
		if len(b.mutations) != 0 || cap(b.mutations) == 0 {
			t.Fatal("small staging scratch was not reusable")
		}
		for _, m := range b.mutations[:cap(b.mutations)] {
			if m.Key != nil || m.Value != nil {
				t.Fatal("scratch retained borrowed caller bytes")
			}
		}
		if err := b.Abort(); err != nil {
			t.Fatal(err)
		}
		if len(b.seen) != 0 || b.seen == nil {
			t.Fatal("small identity table should be empty and reusable")
		}
	}
	// Fail after growing the table, then fail at the first key of a large
	// preallocated table. Both paths must relinquish oversized idle storage.
	for _, invalidFirst := range []bool{false, true} {
		if err := store.BeginCommitGroup(&b); err != nil {
			t.Fatal(err)
		}
		mutations := make([]Mutation, commitBuilderScratchRows+2)
		for i := range mutations {
			mutations[i].Key = []byte(fmt.Sprintf("key-%06d", i))
		}
		bad := len(mutations) - 1
		if invalidFirst {
			bad = 0
		}
		mutations[bad].Key = bytes.Repeat([]byte("k"), 65536)
		if err := b.AppendGroup(CommitGroup{Timestamp: 1, Mutations: mutations}); !errors.Is(err, ErrInvalidKey) {
			t.Fatal(err)
		}
		if len(b.seen) != 0 || b.mutations != nil {
			t.Fatal("failed oversized group retained identities or operand scratch")
		}
		if err := b.Abort(); err != nil {
			t.Fatal(err)
		}
		if b.seen != nil {
			t.Fatal("oversized table retained after abort")
		}
	}
}

func TestOrderedBuilderStageFailurePoisonsAndClearsBorrow(t *testing.T) {
	db, err := treedb.Open(orderedCompletionOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := New(db)
	if err := store.CommitAt(1, []Mutation{{Key: []byte("anchor"), Value: []byte("durable")}}, CommitDurable); err != nil {
		t.Fatal(err)
	}
	var b CommitBuilder
	if err := store.BeginCommitGroup(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	injected := errors.New("injected native staging failure")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterLogicalMVCCStaging {
			return injected
		}
		return nil
	})
	defer func() { restore() }()
	group := CommitGroup{Timestamp: 2, Mutations: []Mutation{{Key: []byte("unassigned"), Value: []byte("discard")}}}
	stageErr := b.AppendGroup(group)
	if !errors.Is(stageErr, ErrStorage) || !errors.Is(stageErr, injected) {
		t.Fatal(stageErr)
	}
	if len(b.seen) != 0 || len(b.mutations) != 0 {
		t.Fatal("failed staging retained speculative identities or operands")
	}
	for _, m := range b.mutations[:cap(b.mutations)] {
		if m.Key != nil || m.Value != nil {
			t.Fatal("poisoned scratch retained borrowed bytes")
		}
	}
	if err := b.AppendGroup(group); err != stageErr {
		t.Fatalf("poison not sticky: %v", err)
	}
	if err := b.Abort(); err != nil {
		t.Fatal(err)
	}
	restore()
	restore = func() {}
	requireResult(t, store, []byte("anchor"), 1, Present, 1, []byte("durable"))
	requireResult(t, store, []byte("unassigned"), 2, Absent, 0, nil)
}
