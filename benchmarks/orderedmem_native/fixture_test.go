package orderedmemnative

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"sort"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

type operation struct {
	kind      byte
	timestamp uint64
	mutations []mvcc.Mutation
	key       []byte
	expected  record
	rows      []record
}

type fixture struct {
	keys       [][]byte
	seed       []operation
	operations []operation
	histories  [][]record // chronological per key, independent oracle
	held       []record   // key ascending, timestamp descending at initial cut
	seedTime   uint64
	trace      string
}

func fixtureKey(id int) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b, uint64(id))
	copy(b[8:], "ordered-native01")
	return b
}

func fixtureValue(id int, ts uint64, size int) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b, uint64(id))
	binary.BigEndian.PutUint64(b[8:], ts)
	for i := 16; i < len(b); i++ {
		b[i] = byte(uint64(i*31+id*17) + ts)
	}
	return b
}

func makeFixture(c config) (*fixture, error) {
	f := &fixture{keys: make([][]byte, c.KeyCount), histories: make([][]record, c.KeyCount)}
	for i := range f.keys {
		f.keys[i] = fixtureKey(i)
	}
	depth := 1
	if c.Case == "history" || c.Case == "range" {
		depth = c.HistoryDepth
	}
	f.seedTime = uint64(depth)
	// Seed order intentionally includes backfills. The oracle is sorted only
	// after insertion order is frozen; no benchmark shape hint reaches an engine.
	order := make([]int, depth)
	for i := range order {
		order[i] = i + 1
	}
	x := c.Seed
	for i := len(order) - 1; i > 0; i-- {
		x = splitmix(x)
		j := int(x % uint64(i+1))
		order[i], order[j] = order[j], order[i]
	}
	add := func(id int, ts uint64, deleted bool) mvcc.Mutation {
		m := mvcc.Mutation{Key: f.keys[id], Delete: deleted}
		r := record{Key: m.Key, Timestamp: ts, State: mvcc.Tombstone}
		if !deleted {
			m.Value = fixtureValue(id, ts, c.ValueBytes)
			r.Value, r.State = m.Value, mvcc.Present
		}
		f.histories[id] = append(f.histories[id], r)
		return m
	}
	// Common seed chunking is independent of the foreground batch size. Both
	// engines pay exactly these commits in the separately reported seed phase.
	const seedChunk = 256
	for _, timestamp := range order {
		ts := uint64(timestamp)
		for start := 0; start < c.KeyCount; start += seedChunk {
			end := min(start+seedChunk, c.KeyCount)
			op := operation{kind: 'c', timestamp: ts, mutations: make([]mvcc.Mutation, 0, end-start)}
			for id := start; id < end; id++ {
				deleted := depth > 1 && (id+timestamp)%17 == 0
				op.mutations = append(op.mutations, add(id, ts, deleted))
			}
			f.seed = append(f.seed, op)
		}
	}
	for id := range f.histories {
		sort.Slice(f.histories[id], func(i, j int) bool { return f.histories[id][i].Timestamp < f.histories[id][j].Timestamp })
		for j := len(f.histories[id]) - 1; j >= 0; j-- {
			f.held = append(f.held, f.histories[id][j])
		}
	}
	f.operations = make([]operation, c.Operations)
	currentTS := f.seedTime
	for i := range f.operations {
		id := int((uint64(i)*131 + c.Seed) % uint64(c.KeyCount))
		op := operation{timestamp: currentTS, key: f.keys[id]}
		switch c.Case {
		case "history":
			op.kind = 'g'
			// Vary the cutoff on subsequent key cycles so a key is not
			// permanently coupled to one historical read timestamp.
			op.timestamp = (uint64(i)*37+uint64(i/c.KeyCount))%uint64(depth) + 1
			op.expected = pointExpected(f.histories[id], op.timestamp)
		case "range":
			op.kind = 'r'
			op.timestamp = (uint64(i)*37+uint64(i/c.KeyCount))%uint64(depth) + 1
			for candidate := id; candidate < c.KeyCount && len(op.rows) < c.RangeWidth; candidate++ {
				r := pointExpected(f.histories[candidate], op.timestamp)
				if r.State == mvcc.Present {
					op.rows = append(op.rows, r)
				}
			}
		case "mixed":
			if i%10 < 6 {
				op.kind = 'g'
				op.expected = pointExpected(f.histories[id], currentTS)
				break
			}
			currentTS++
			op.kind, op.timestamp = 'c', currentTS
			op.mutations = []mvcc.Mutation{add(id, currentTS, i%10 >= 8)}
		default:
			currentTS++
			op.kind, op.timestamp = 'c', currentTS
			op.mutations = make([]mvcc.Mutation, c.BatchSize)
			for j := range op.mutations {
				keyID := (id + j) % c.KeyCount
				op.mutations[j] = add(keyID, currentTS, false)
			}
		}
		f.operations[i] = op
	}
	f.trace = fixtureDigest(c, f)
	return f, nil
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func pointExpected(history []record, ts uint64) record {
	i := sort.Search(len(history), func(i int) bool { return history[i].Timestamp > ts }) - 1
	if i < 0 || history[i].State != mvcc.Present {
		return record{State: mvcc.Absent}
	}
	return history[i]
}

func fixtureDigest(c config, f *fixture) string {
	h := sha256.New()
	// Route/profile/engine, directories and diagnostic settings do not alter
	// useful work and therefore are excluded from this cross-arm fingerprint.
	fmt.Fprintf(h, "%s/%d/%d/%d/%d/%d/%d/%d/%d/%d/%t/%d/", c.Case, c.Operations,
		c.BatchSize, c.ValueBytes, c.KeyCount, c.HistoryDepth, c.Workers, c.RangeWidth,
		c.FlushThreshold, c.CheckpointEvery, c.HoldCut, c.Seed)
	for _, ops := range [][]operation{f.seed, f.operations} {
		for _, op := range ops {
			h.Write([]byte{op.kind})
			writeUint(h, op.timestamp)
			writeBytes(h, op.key)
			writeUint(h, uint64(len(op.mutations)))
			for _, m := range op.mutations {
				writeBytes(h, m.Key)
				writeBytes(h, m.Value)
				if m.Delete {
					h.Write([]byte{1})
				} else {
					h.Write([]byte{0})
				}
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeUint(h hash.Hash, n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	h.Write(b[:])
}
func writeBytes(h hash.Hash, b []byte) { writeUint(h, uint64(len(b))); h.Write(b) }

func checkRecord(got, want record, key bool) error {
	if got.State != want.State || got.Timestamp != want.Timestamp || !bytes.Equal(got.Value, want.Value) || (key && !bytes.Equal(got.Key, want.Key)) {
		return fmt.Errorf("record mismatch key=%x ts=%d state=%d value=%x; want key=%x ts=%d state=%d value=%x",
			got.Key, got.Timestamp, got.State, got.Value, want.Key, want.Timestamp, want.State, want.Value)
	}
	return nil
}

func checkHistory(cursor historyCursor, expected []record) (count int, err error) {
	for cursor.Valid() {
		if count >= len(expected) {
			return count, fmt.Errorf("unexpected extra history after %d records", count)
		}
		got, e := cursor.Entry()
		if e != nil {
			return count, e
		}
		if e := checkRecord(got, expected[count], true); e != nil {
			return count, fmt.Errorf("history record %d: %w", count, e)
		}
		count++
		cursor.Next()
	}
	if e := cursor.Error(); e != nil {
		return count, e
	}
	if count != len(expected) {
		return count, fmt.Errorf("history count=%d want=%d", count, len(expected))
	}
	return count, nil
}

func (f *fixture) allHistory() []record {
	var result []record
	for _, versions := range f.histories {
		for j := len(versions) - 1; j >= 0; j-- {
			result = append(result, versions[j])
		}
	}
	return result
}
