package mvcc

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

type versionSeekBufferSource struct {
	treedb.Iterator
	keys    [][]byte
	values  [][]byte
	index   int
	reverse bool
	seeks   int
}

func (s *versionSeekBufferSource) Valid() bool   { return s.index >= 0 && s.index < len(s.keys) }
func (s *versionSeekBufferSource) Key() []byte   { return s.keys[s.index] }
func (s *versionSeekBufferSource) Value() []byte { return s.values[s.index] }
func (*versionSeekBufferSource) Error() error    { return nil }
func (*versionSeekBufferSource) Close() error    { return nil }
func (s *versionSeekBufferSource) Seek(key []byte) {
	s.seeks++
	if s.reverse {
		s.index = sort.Search(len(s.keys), func(i int) bool { return bytes.Compare(s.keys[i], key) > 0 }) - 1
	} else {
		s.index = sort.Search(len(s.keys), func(i int) bool { return bytes.Compare(s.keys[i], key) >= 0 })
	}
}
func (s *versionSeekBufferSource) Next() {
	if s.reverse {
		s.index--
	} else {
		s.index++
	}
}

func TestVersionIteratorReusesSeekEncodingWithoutBorrowingResult(t *testing.T) {
	logical := [][]byte{{}, {0}, {'a'}, {'a', 0}, {'a', 0, 'b'}, bytes.Repeat([]byte{'z', 0}, 300), {255}}
	for _, reverse := range []bool{false, true} {
		source := &versionSeekBufferSource{index: -1, reverse: reverse}
		for _, key := range logical {
			for _, timestamp := range []uint64{math.MaxUint64, 19, 1} {
				encoded, err := mvcckey.Encode(key, timestamp)
				if err != nil {
					t.Fatal(err)
				}
				source.keys = append(source.keys, encoded)
				source.values = append(source.values, []byte{recordValueV1, byte(timestamp)})
			}
		}
		it := &VersionIterator{raw: source}
		it.Seek(logical[len(logical)-2], 19) // grow both independent buffers first
		if !it.Valid() {
			t.Fatal("long seek")
		}
		seekCapacity := cap(it.seekBuf)
		var owned Version
		for _, key := range logical {
			it.Seek(key, 19)
			if !it.Valid() || !bytes.Equal(it.EntryView().Key, key) || it.EntryView().Timestamp != 19 {
				t.Fatalf("reverse=%t seek=%x result=%+v err=%v", reverse, key, it.EntryView(), it.Error())
			}
			owned = it.Entry()
			// EntryView's key is the independent decode buffer. It remains a
			// valid input when encoding the next seek into the reusable buffer.
			it.Seek(it.EntryView().Key, 1)
			if it.EntryView().Timestamp != 1 || owned.Timestamp != 19 || !bytes.Equal(owned.Key, key) {
				t.Fatal("seek input or owned result was overwritten")
			}
		}
		if allocations := testing.AllocsPerRun(10, func() {
			for _, key := range logical {
				it.Seek(key, 19)
			}
		}); allocations != 0 {
			t.Fatalf("warmed seeks allocate: %g", allocations)
		}
		if cap(it.seekBuf) != seekCapacity {
			t.Fatal("seek buffer unexpectedly grew")
		}
		before := source.seeks
		it.Seek([]byte("invalid"), 0)
		if !errors.Is(it.Error(), ErrInvalidKey) || source.seeks != before {
			t.Fatal("invalid timestamp reached raw source")
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if it.seekBuf != nil {
			t.Fatal("closed iterator retained seek storage")
		}
		if owned.Timestamp != 19 {
			t.Fatal("close changed an owned result")
		}
	}
}
