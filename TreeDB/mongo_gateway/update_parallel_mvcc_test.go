package mongogateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestUpdateCoalescerParallelMVCCDisjointW4Reopen(t *testing.T) {
	profiles := []struct {
		name    string
		profile treedb.Profile
	}{
		{name: "no_wal_fast", profile: treedb.ProfileNoWALFast},
		{name: "command_wal_relaxed", profile: treedb.ProfileCommandWALRelaxed},
	}
	for _, tc := range profiles {
		t.Run(tc.name, func(t *testing.T) {
			const (
				writers      = 4
				opsPerWriter = 40
			)
			dir := t.TempDir()
			s, collName := openUpdateCoalescerBenchServer(t, dir, tc.profile, writers*4)
			ns := "app." + collName

			ctx := context.Background()
			start := make(chan struct{})
			var wg sync.WaitGroup
			errCh := make(chan error, writers)
			for w := 0; w < writers; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 1; i <= opsPerWriter; i++ {
						cmd := buildUpdateOneCommand(t, collName, int64(w), int64(i))
						resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
						if err != nil {
							errCh <- fmt.Errorf("writer=%d op=%d: %w", w, i, err)
							return
						}
						if !mongoCommandResponseOK(resp) {
							errCh <- fmt.Errorf("writer=%d op=%d not ok: %s", w, i, resp)
							return
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(errCh)
			for err := range errCh {
				_ = s.Close()
				t.Fatal(err)
			}

			coalescer := s.Server.mongoUpdateCoalescer(ns)
			if coalescer == nil {
				_ = s.Close()
				t.Fatal("expected non-nil update coalescer")
			}
			if got := coalescer.inlineExecs.Load(); got != writers*opsPerWriter {
				_ = s.Close()
				t.Fatalf("inlineExecs=%d want %d (workerBatches=%d)", got, writers*opsPerWriter, coalescer.workerBatches.Load())
			}
			if got := coalescer.workerBatches.Load(); got != 0 {
				_ = s.Close()
				t.Fatalf("workerBatches=%d want 0 for disjoint w=4 parallel MVCC", got)
			}

			for w := 0; w < writers; w++ {
				if got := findDocN(t, s.Server, collName, int64(w)); got != opsPerWriter {
					_ = s.Close()
					t.Fatalf("pre-reopen doc[%d] n=%d want %d", w, got, opsPerWriter)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     dir,
				Profile: tc.profile,
			})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			for w := 0; w < writers; w++ {
				if got := findDocN(t, reopened.Server, collName, int64(w)); got != opsPerWriter {
					t.Fatalf("reopen doc[%d] n=%d want %d", w, got, opsPerWriter)
				}
			}
		})
	}
}

func TestUpdateCoalescerParallelMVCCHotKeyAndW8Escalation(t *testing.T) {
	t.Run("hot_key_same_id_w4", func(t *testing.T) {
		const (
			writers      = 4
			opsPerWriter = 50
		)
		dir := t.TempDir()
		s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileNoWALFast, 4)
		defer func() { _ = s.Close() }()

		ctx := context.Background()
		start := make(chan struct{})
		var wg sync.WaitGroup
		var okOps atomic.Int64
		errCh := make(chan error, writers)
		for w := 0; w < writers; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 1; i <= opsPerWriter; i++ {
					cmd := buildUpdateOneCommand(t, collName, 0, int64(w*1000+i))
					resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
					if err != nil {
						errCh <- fmt.Errorf("writer=%d op=%d: %w", w, i, err)
						return
					}
					if !mongoCommandResponseOK(resp) {
						errCh <- fmt.Errorf("writer=%d op=%d not ok: %s", w, i, resp)
						return
					}
					okOps.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatal(err)
		}
		if got := okOps.Load(); got != writers*opsPerWriter {
			t.Fatalf("completed ops=%d want %d", got, writers*opsPerWriter)
		}
		finalN := findDocN(t, s.Server, collName, 0)
		if finalN <= 0 {
			t.Fatalf("hot-key doc[0] final n=%d want > 0", finalN)
		}
	})

	t.Run("w8_escalates_to_coalescer", func(t *testing.T) {
		const (
			writers      = 8
			opsPerWriter = 60
		)
		dir := t.TempDir()
		s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALRelaxed, writers*2)
		ns := "app." + collName

		ctx := context.Background()
		start := make(chan struct{})
		var wg sync.WaitGroup
		errCh := make(chan error, writers)
		for w := 0; w < writers; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 1; i <= opsPerWriter; i++ {
					cmd := buildUpdateOneCommand(t, collName, int64(w), int64(i))
					resp, err := s.Server.commandResponse(ctx, "update", cmd, nil, 0)
					if err != nil {
						errCh <- fmt.Errorf("writer=%d op=%d: %w", w, i, err)
						return
					}
					if !mongoCommandResponseOK(resp) {
						errCh <- fmt.Errorf("writer=%d op=%d not ok: %s", w, i, resp)
						return
					}
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			_ = s.Close()
			t.Fatal(err)
		}

		coalescer := s.Server.mongoUpdateCoalescer(ns)
		if coalescer == nil || coalescer.workerBatches.Load() == 0 {
			_ = s.Close()
			t.Fatalf("expected w=8 to escalate to hot worker coalescer (workerBatches > 0)")
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened, err := OpenStandaloneServer(StandaloneOptions{
			Dir:     dir,
			Profile: treedb.ProfileCommandWALRelaxed,
		})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer func() { _ = reopened.Close() }()
		for w := 0; w < writers; w++ {
			if got := findDocN(t, reopened.Server, collName, int64(w)); got != opsPerWriter {
				t.Fatalf("reopen doc[%d] n=%d want %d", w, got, opsPerWriter)
			}
		}
	})
}

func TestUpdateCoalescerDurableNeverUsesParallelMVCC(t *testing.T) {
	dir := t.TempDir()
	s, collName := openUpdateCoalescerBenchServer(t, dir, treedb.ProfileCommandWALDurable, 4)
	defer func() { _ = s.Close() }()

	col, err := s.Collections.OpenCollection("app." + collName)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if col.CanParallelNativeBSONSet() {
		t.Fatal("command_wal_durable collection unexpectedly reported CanParallelNativeBSONSet=true")
	}
	coalescer := s.Server.mongoUpdateCoalescer("app." + collName)
	if coalescer == nil {
		t.Fatal("expected non-nil coalescer")
	}
	item := mongoUpdateItem{
		exactID:         true,
		bsonSetFieldsOK: true,
		bsonSetFields: []collections.BSONSetField{
			{Key: "n", Value: bson.RawValue{Type: bson.TypeInt64, Value: bsoncore.AppendInt64(nil, 1)}},
		},
	}
	if coalescer.canParallelNativeBSONSet(col, item) {
		t.Fatal("command_wal_durable coalescer unexpectedly allowed parallel MVCC bypass")
	}
}

func TestResolveGatewayCollectionNameBytes(t *testing.T) {
	s := NewServer()
	cases := []struct {
		name    string
		db      string
		col     string
		want    string
		wantErr bool
	}{
		{name: "valid", db: "bench", col: "docs", want: "bench.docs"},
		{name: "empty_db", db: "", col: "docs", wantErr: true},
		{name: "empty_col", db: "bench", col: "", wantErr: true},
		{name: "db_slash", db: "a/b", col: "docs", wantErr: true},
		{name: "db_colon", db: "a:b", col: "docs", wantErr: true},
		{name: "db_nul", db: "a\x00b", col: "docs", wantErr: true},
		{name: "col_nul", db: "bench", col: "d\x00s", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.resolveGatewayCollectionNameBytes([]byte(tc.db), []byte(tc.col))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for db=%q col=%q, got %q", tc.db, tc.col, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}

	dbBytes := []byte("bench")
	colBytes := []byte("docs")
	if got, err := s.resolveGatewayCollectionNameBytes(dbBytes, colBytes); err != nil || got != "bench.docs" {
		t.Fatalf("warm resolve: got=%q err=%v", got, err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		got, err := s.resolveGatewayCollectionNameBytes(dbBytes, colBytes)
		if err != nil || got != "bench.docs" {
			t.Fatalf("cached resolve: got=%q err=%v", got, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("cached resolveGatewayCollectionNameBytes allocs=%v want 0", allocs)
	}

	// Validate rawBSONStringBytes framing checks.
	good := bsoncore.Value{Type: bsoncore.TypeString, Data: bsoncore.AppendString(nil, "items")}
	if got, ok := rawBSONStringBytes(good); !ok || string(got) != "items" {
		t.Fatalf("rawBSONStringBytes(good) = (%q, %v), want (items, true)", got, ok)
	}
	badLen := bsoncore.Value{Type: bsoncore.TypeString, Data: []byte{0x05, 0x00, 0x00, 0x00, 'a', 0x00}}
	if _, ok := rawBSONStringBytes(badLen); ok {
		t.Fatal("rawBSONStringBytes accepted truncated length")
	}
	badNul := bsoncore.Value{Type: bsoncore.TypeString, Data: []byte{0x02, 0x00, 0x00, 0x00, 'a', 'b'}}
	if _, ok := rawBSONStringBytes(badNul); ok {
		t.Fatal("rawBSONStringBytes accepted missing NUL terminator")
	}
}
