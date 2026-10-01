package mvcc

import (
	"fmt"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

// The native shared successor cannot exclude a batch that applies across
// multiple mutable shards. Store owns that publication fence. This checks the
// fence at the real public batch boundary, including before durability waits.
type groupAdmissionProbeDB struct {
	*treedb.DB
	probe func()
}

func (db *groupAdmissionProbeDB) NewBatchWithSize(size int) treedb.Batch {
	return &groupAdmissionProbeBatch{Batch: db.DB.NewBatchWithSize(size), probe: db.probe}
}

type groupAdmissionProbeBatch struct {
	treedb.Batch
	probe func()
}

func (b *groupAdmissionProbeBatch) Write() error     { b.probe(); return b.Batch.Write() }
func (b *groupAdmissionProbeBatch) WriteSync() error { b.probe(); return b.Batch.WriteSync() }

func TestCommitGroupAt_QualifiedSuccessorRequiresExclusiveAdmission(t *testing.T) {
	for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			wrapped := &groupAdmissionProbeDB{DB: openTestDB(t, t.TempDir(), treedb.DurabilityDurable)}
			defer wrapped.Close()
			store := newStore(wrapped)
			called := false
			wrapped.probe = func() {
				called = true
				if store.mu.TryRLock() {
					store.mu.RUnlock()
					t.Fatal("multi-record commit permits a Store reader to observe partial shard apply")
				}
			}
			if err := store.CommitGroupAt([]CommitGroup{{Timestamp: 10, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("one")}, {Key: []byte("b"), Value: []byte("two")}}}}, mode); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("batch probe not called")
			}
			requireResult(t, store, []byte("a"), 10, Present, 10, []byte("one"))
			requireResult(t, store, []byte("b"), 10, Present, 10, []byte("two"))
		})
	}
}

// Split only the test adapter's publication into real public point writes. The
// first record is visible in TreeDB while the second is paused, reproducing
// the cached batch's permitted shard prefix without a production test hook.
type splitPublicationDB struct {
	*treedb.DB
	applied chan struct{}
	resume  chan struct{}
}

func (db *splitPublicationDB) NewBatchWithSize(size int) treedb.Batch {
	return &splitPublicationBatch{Batch: db.DB.NewBatchWithSize(size), db: db}
}

type splitPublicationBatch struct {
	treedb.Batch
	db      *splitPublicationDB
	entries [][2][]byte
}

func (b *splitPublicationBatch) Set(key, value []byte) error {
	b.entries = append(b.entries, [2][]byte{append([]byte(nil), key...), append([]byte(nil), value...)})
	return nil
}
func (b *splitPublicationBatch) Write() error     { return b.write(false) }
func (b *splitPublicationBatch) WriteSync() error { return b.write(true) }
func (b *splitPublicationBatch) write(durable bool) error {
	for i, entry := range b.entries {
		var err error
		if durable {
			err = b.db.DB.SetSync(entry[0], entry[1])
		} else {
			err = b.db.DB.Set(entry[0], entry[1])
		}
		if err != nil {
			return err
		}
		if i == 0 {
			close(b.db.applied)
			<-b.db.resume
		}
	}
	return nil
}

func TestCommitGroupAt_QualifiedReadsCannotObservePublicationPrefix(t *testing.T) {
	for _, mode := range []CommitMode{CommitRelaxed, CommitDurable} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db := &splitPublicationDB{DB: openTestDB(t, t.TempDir(), treedb.DurabilityDurable), applied: make(chan struct{}), resume: make(chan struct{})}
			defer db.Close()
			var once sync.Once
			release := func() { once.Do(func() { close(db.resume) }) }
			defer release()
			store := newStore(db)
			commit := make(chan error, 1)
			go func() {
				commit <- store.CommitGroupAt([]CommitGroup{{Timestamp: 10, Mutations: []Mutation{{Key: []byte("a"), Value: []byte("one")}, {Key: []byte("b"), Value: []byte("two")}}}}, mode)
			}()
			<-db.applied
			pointStarted, snapshotStarted := make(chan struct{}), make(chan struct{})
			point := make(chan error, 1)
			snapshot := make(chan error, 1)
			go func() {
				close(pointStarted)
				result, err := store.GetAt([]byte("b"), 10)
				if err == nil && (result.State != Present || string(result.Value) != "two") {
					err = fmt.Errorf("point result: %+v", result)
				}
				point <- err
			}()
			go func() {
				close(snapshotStarted)
				it, err := store.IterateVersions(VersionIteratorOptions{ReadTimestamp: 10})
				if err == nil {
					defer it.Close()
					seen := map[string]string{}
					for ; it.Valid(); it.Next() {
						entry := it.Entry()
						seen[string(entry.Key)] = string(entry.Value)
					}
					err = it.Error()
					if err == nil && (len(seen) != 2 || seen["a"] != "one" || seen["b"] != "two") {
						err = fmt.Errorf("snapshot exposed publication prefix: %v", seen)
					}
				}
				snapshot <- err
			}()
			<-pointStarted
			<-snapshotStarted
			timer := time.NewTimer(30 * time.Millisecond)
			select {
			case err := <-point:
				timer.Stop()
				t.Fatalf("point read escaped partial publication fence: %v", err)
			case err := <-snapshot:
				timer.Stop()
				t.Fatalf("snapshot escaped partial publication fence: %v", err)
			case <-timer.C:
			}
			release()
			if err := <-commit; err != nil {
				t.Fatal(err)
			}
			if err := <-point; err != nil {
				t.Fatal(err)
			}
			if err := <-snapshot; err != nil {
				t.Fatal(err)
			}
		})
	}
}
