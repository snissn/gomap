package mvcckey

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		ts   uint64
	}{
		{name: "empty", key: nil, ts: 1},
		{name: "embedded zeros", key: []byte{0, 'a', 0, 0xff, 0}, ts: 42},
		{name: "physical marker as logical bytes", key: append([]byte(nil), namespaceV1[:]...), ts: math.MaxUint64},
		{name: "all bytes", key: allBytes(), ts: 1 << 63},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.key, tt.ts)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !InNamespace(encoded) {
				t.Fatalf("encoded key is outside namespace: %x", encoded)
			}
			decoded, ts, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !bytes.Equal(decoded, tt.key) || ts != tt.ts {
				t.Fatalf("Decode=(%x,%d), want (%x,%d)", decoded, ts, tt.key, tt.ts)
			}
		})
	}
}

func TestAppendPreservesPrefixAndErrorLeavesDestinationUnchanged(t *testing.T) {
	dst := []byte("caller:")
	got, err := Append(dst[:len(dst):len(dst)], []byte("key"), 9)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:len(dst)], dst) {
		t.Fatalf("prefix changed: %x", got[:len(dst)])
	}

	before := append([]byte(nil), dst...)
	got, err = Append(dst, []byte("key"), 0)
	if !errors.Is(err, ErrZeroTimestamp) || !bytes.Equal(got, before) {
		t.Fatalf("Append zero timestamp=(%x,%v), want unchanged,%v", got, err, ErrZeroTimestamp)
	}
}

func TestEncodedOrderingMatchesLogicalKeyAscendingTimestampDescending(t *testing.T) {
	rng := rand.New(rand.NewSource(3670))
	type item struct {
		key     []byte
		ts      uint64
		encoded []byte
	}
	items := make([]item, 0, 2000)
	for i := 0; i < 2000; i++ {
		key := make([]byte, rng.Intn(40))
		_, _ = rng.Read(key)
		if i%7 == 0 && len(key) != 0 {
			key[rng.Intn(len(key))] = 0
		}
		ts := rng.Uint64()
		if ts == 0 {
			ts = 1
		}
		encoded, err := Encode(key, ts)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{key: key, ts: ts, encoded: encoded})
	}

	byLogical := append([]item(nil), items...)
	sort.Slice(byLogical, func(i, j int) bool {
		if cmp := bytes.Compare(byLogical[i].key, byLogical[j].key); cmp != 0 {
			return cmp < 0
		}
		return byLogical[i].ts > byLogical[j].ts
	})
	byPhysical := append([]item(nil), items...)
	sort.Slice(byPhysical, func(i, j int) bool {
		return bytes.Compare(byPhysical[i].encoded, byPhysical[j].encoded) < 0
	})
	for i := range byLogical {
		if !bytes.Equal(byLogical[i].key, byPhysical[i].key) || byLogical[i].ts != byPhysical[i].ts {
			t.Fatalf("order mismatch at %d: logical=(%x,%d) physical=(%x,%d)", i, byLogical[i].key, byLogical[i].ts, byPhysical[i].key, byPhysical[i].ts)
		}
	}
}

func TestLogicalPrefixAndKeyVersionBounds(t *testing.T) {
	logicalKeys := [][]byte{nil, {0}, {0, 1}, {'a'}, {'a', 0}, {'a', 0, 'b'}, {'a', 'b'}, {'b'}}
	var physical [][]byte
	for _, key := range logicalKeys {
		for _, ts := range []uint64{1, 2, math.MaxUint64} {
			encoded, err := Encode(key, ts)
			if err != nil {
				t.Fatal(err)
			}
			physical = append(physical, encoded)
		}
	}

	for _, prefix := range logicalKeys {
		lower, err := AppendLogicalPrefixLower(nil, prefix)
		if err != nil {
			t.Fatal(err)
		}
		upper, err := AppendLogicalPrefixUpper(nil, prefix)
		if err != nil {
			t.Fatal(err)
		}
		for _, encoded := range physical {
			decoded, _, err := Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			inBounds := bytes.Compare(encoded, lower) >= 0 && bytes.Compare(encoded, upper) < 0
			if inBounds != bytes.HasPrefix(decoded, prefix) {
				t.Fatalf("prefix=%x key=%x bounds=%v want=%v lower=%x upper=%x", prefix, decoded, inBounds, bytes.HasPrefix(decoded, prefix), lower, upper)
			}
		}
	}

	for _, target := range logicalKeys {
		lower, err := AppendKeyVersionsLower(nil, target)
		if err != nil {
			t.Fatal(err)
		}
		upper, err := AppendKeyVersionsUpper(nil, target)
		if err != nil {
			t.Fatal(err)
		}
		for _, encoded := range physical {
			decoded, _, err := Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			inBounds := bytes.Compare(encoded, lower) >= 0 && bytes.Compare(encoded, upper) < 0
			if inBounds != bytes.Equal(decoded, target) {
				t.Fatalf("target=%x key=%x bounds=%v want=%v", target, decoded, inBounds, bytes.Equal(decoded, target))
			}
		}
	}
}

func TestNamespaceBounds(t *testing.T) {
	lower := AppendNamespaceLower(nil)
	upper := AppendNamespaceUpper(nil)
	if bytes.Compare(lower, upper) >= 0 {
		t.Fatalf("namespace bounds not increasing: %x >= %x", lower, upper)
	}
	encoded, err := Encode(nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(encoded, lower) < 0 || bytes.Compare(encoded, upper) >= 0 {
		t.Fatalf("encoded key %x outside [%x,%x)", encoded, lower, upper)
	}
	wrongVersion := append([]byte(nil), namespaceV1[:]...)
	wrongVersion[len(wrongVersion)-1]++
	wrongVersion = append(wrongVersion, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
	if _, _, err := Decode(wrongVersion); !errors.Is(err, ErrWrongNamespace) {
		t.Fatalf("Decode wrong version err=%v, want %v", err, ErrWrongNamespace)
	}
}

func TestDecodeRejectsMalformedKeys(t *testing.T) {
	valid, err := Encode([]byte{'a', 0, 'b'}, 7)
	if err != nil {
		t.Fatal(err)
	}
	zeroTimestamp := append([]byte(nil), valid...)
	for i := len(zeroTimestamp) - TimestampSize; i < len(zeroTimestamp); i++ {
		zeroTimestamp[i] = 0xff
	}

	tests := []struct {
		name string
		key  []byte
		want error
	}{
		{name: "empty", key: nil, want: ErrWrongNamespace},
		{name: "marker only", key: namespaceV1[:], want: ErrMalformedKey},
		{name: "truncated timestamp", key: valid[:len(valid)-1], want: ErrMalformedKey},
		{name: "extra timestamp byte", key: append(append([]byte(nil), valid...), 0), want: ErrMalformedKey},
		{name: "invalid escape", key: append(append([]byte(nil), namespaceV1[:]...), 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1), want: ErrMalformedKey},
		{name: "missing terminator", key: append(append([]byte(nil), namespaceV1[:]...), 'a', 0, 0, 0, 0, 0, 0, 0, 0), want: ErrMalformedKey},
		{name: "zero timestamp", key: zeroTimestamp, want: ErrZeroTimestamp},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := []byte("keep:")
			got, _, err := DecodeAppend(prefix, tt.key)
			if !errors.Is(err, tt.want) {
				t.Fatalf("DecodeAppend err=%v, want %v", err, tt.want)
			}
			if !bytes.Equal(got, prefix) {
				t.Fatalf("DecodeAppend changed destination on error: %x", got)
			}
		})
	}
}

func TestEncodedLengthMaximumBoundary(t *testing.T) {
	const fixed = len(namespaceV1) + 2 + TimestampSize
	maxNoZero := bytes.Repeat([]byte{'x'}, MaxEncodedKeySize-fixed)
	encoded, err := Encode(maxNoZero, 1)
	if err != nil {
		t.Fatalf("Encode exact maximum: %v", err)
	}
	if len(encoded) != MaxEncodedKeySize {
		t.Fatalf("encoded length=%d, want %d", len(encoded), MaxEncodedKeySize)
	}
	if _, err := Encode(append(maxNoZero, 'x'), 1); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Encode one over err=%v, want %v", err, ErrKeyTooLarge)
	}
	if _, err := AppendKeyVersionsLower(nil, maxNoZero); err != nil {
		t.Fatalf("AppendKeyVersionsLower exact maximum: %v", err)
	}
	if _, err := AppendKeyVersionsUpper(nil, append(maxNoZero, 'x')); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("AppendKeyVersionsUpper one over err=%v, want %v", err, ErrKeyTooLarge)
	}

	maxZeros := bytes.Repeat([]byte{0}, (MaxEncodedKeySize-fixed)/2)
	if _, err := Encode(maxZeros, 1); err != nil {
		t.Fatalf("Encode maximum all-zero key: %v", err)
	}
	if _, err := Encode(append(maxZeros, 0), 1); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Encode all-zero one over err=%v, want %v", err, ErrKeyTooLarge)
	}
}

func TestTimestampExtremesSortNewestFirst(t *testing.T) {
	oldest, err := Encode([]byte("k"), 1)
	if err != nil {
		t.Fatal(err)
	}
	newest, err := Encode([]byte("k"), math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(newest, oldest) >= 0 {
		t.Fatalf("newest key %x must sort before oldest %x", newest, oldest)
	}
	if _, err := Encode([]byte("k"), 0); !errors.Is(err, ErrZeroTimestamp) {
		t.Fatalf("zero timestamp err=%v, want %v", err, ErrZeroTimestamp)
	}
}

func TestVersionPrefixAndExactVersionRange(t *testing.T) {
	logical := []byte{'a', 0, 'b'}
	first, err := Encode(logical, 1)
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := Encode(logical, 99)
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	firstPrefix, ok := VersionPrefix(first)
	if !ok {
		t.Fatal("VersionPrefix(first) rejected valid key")
	}
	secondPrefix, ok := VersionPrefix(second)
	if !ok || !bytes.Equal(firstPrefix, secondPrefix) {
		t.Fatalf("version prefixes differ: %x vs %x", firstPrefix, secondPrefix)
	}
	if affinityPrefix, ok := VersionAffinityPrefix(first); !ok || !bytes.Equal(affinityPrefix, firstPrefix) {
		t.Fatalf("VersionAffinityPrefix(first)=(%x,%t), want %x,true", affinityPrefix, ok, firstPrefix)
	}
	upper, err := AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		t.Fatalf("AppendKeyVersionsUpper: %v", err)
	}
	gotPrefix, ok := ExactVersionRange(second, upper)
	if !ok || !bytes.Equal(gotPrefix, secondPrefix) {
		t.Fatalf("ExactVersionRange prefix=%x ok=%t want %x,true", gotPrefix, ok, secondPrefix)
	}

	malformedSuffix := append(append([]byte(nil), first...), 0)
	if affinityPrefix, ok := VersionAffinityPrefix(malformedSuffix); !ok || !bytes.Equal(affinityPrefix, firstPrefix) {
		t.Fatalf("VersionAffinityPrefix(malformed suffix)=(%x,%t), want %x,true", affinityPrefix, ok, firstPrefix)
	}
	oversizedMalformedSuffix := append(append([]byte(nil), first...), bytes.Repeat([]byte{0xa5}, MaxEncodedKeySize-len(first)+1)...)
	if len(oversizedMalformedSuffix) <= MaxEncodedKeySize {
		t.Fatalf("oversized malformed suffix length=%d want > %d", len(oversizedMalformedSuffix), MaxEncodedKeySize)
	}
	if affinityPrefix, ok := VersionAffinityPrefix(oversizedMalformedSuffix); !ok || !bytes.Equal(affinityPrefix, firstPrefix) {
		t.Fatalf("VersionAffinityPrefix(oversized malformed suffix)=(%x,%t), want %x,true", affinityPrefix, ok, firstPrefix)
	}
	for _, malformed := range [][]byte{
		nil,
		[]byte("raw"),
		append([]byte("raw-key"), 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe),
		append([]byte(nil), first[:len(first)-1]...),
		malformedSuffix,
	} {
		if prefix, ok := VersionPrefix(malformed); ok || prefix != nil {
			t.Fatalf("VersionPrefix(%x)=(%x,%t), want nil,false", malformed, prefix, ok)
		}
	}
	badUpper := append([]byte(nil), upper...)
	badUpper[len(badUpper)-1]++
	if prefix, ok := ExactVersionRange(second, badUpper); ok || prefix != nil {
		t.Fatalf("ExactVersionRange accepted noncanonical upper %x", badUpper)
	}
}

func allBytes() []byte {
	out := make([]byte, 256)
	for i := range out {
		out[i] = byte(i)
	}
	return out
}
