package main

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"

	"github.com/snissn/gomap/kvstore"
)

func (c quicksilverConfig) resolved() quicksilverConfig {
	if c.CommitMode == "" {
		c.CommitMode = "sync"
		if c.Case == "realistic" {
			c.CommitMode = "ordinary"
		}
	}
	c.BarrierPolicy = "initial/final Checkpoint; up to four separately timed concurrent Checkpoints at mutation-batch quarters"
	if c.Case == "realistic" {
		if c.Mixture == "" {
			c.Mixture = "primary"
		}
		if c.WorkingSet == "" {
			c.WorkingSet = "uniform"
		}
		c.Generation = "generic-v1"
		c.KeyDistribution = "namespace/hostname/opaque approximately 1/3 each; variable lengths; bijective scrambled interior identity"
		c.ValueDistribution = "80% 32..256 B; 18% 257..2048 B; 2% 2049..32768 B (seeded)"
		c.ContentDistribution = "50% structured; 50% opaque (seeded independent of length)"
		if c.Mixture == "holdout" {
			c.KeyDistribution = "50% namespace; 30% hostname; 20% opaque; variable lengths; bijective scrambled interior identity"
			c.ValueDistribution = "95% 32..256 B; 4% 257..2048 B; 1% 2049..32768 B (seeded)"
			c.ContentDistribution = "25% structured; 75% opaque (seeded independent of length)"
		}
		c.ValueDistribution += "; equally weighted logarithmic size bands within buckets"
	} else {
		c.Mixture = "historical"
		c.Seed = 24
		c.WorkingSet = "legacy-65536-trace"
		c.MissPercent = 0
		c.Generation = "legacy-v1"
		c.KeyDistribution = "shared_prefix24_be8"
		c.ValueDistribution = c.Case
		c.ContentDistribution = c.Case
	}
	return c
}
func quicksilverCommitBatch(b kvstore.Batch, c quicksilverConfig) error {
	if c.resolved().CommitMode == "ordinary" {
		return b.Commit()
	}
	return b.CommitSync()
}

// SplitMix64's xor shifts and odd multipliers are bijective on uint64. This
// scrambles identities without hashes/collisions or a fixed numeric key tail.
func quicksilverMix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func quicksilverKeyFamily(id uint64, seed int64, mixture string) int {
	if mixture == "holdout" {
		x := quicksilverMix(id^uint64(seed)^0x79c11a35) % 10
		if x < 5 {
			return 0
		}
		if x < 8 {
			return 1
		}
		return 2
	}
	return int(id % 3)
}
func quicksilverGenericKey(dst []byte, id uint64, seed int64, mixture string) []byte {
	x := quicksilverMix(id ^ uint64(seed))
	// Family identity stays disjoint; each family's token is injective and its
	// variable suffix cannot shift the token boundary.
	switch quicksilverKeyFamily(id, seed, mixture) {
	case 0:
		dst = append(dst, "namespace/"...)
		dst = strconv.AppendUint(dst, x, 16)
		dst = append(dst, "/settings/"...)
		const tail = "routing/feature/policy/configuration"
		return append(dst, tail[:1+int(x>>56)%len(tail)]...)
	case 1:
		dst = append(dst, 'h')
		dst = strconv.AppendUint(dst, x, 16)
		dst = append(dst, "."...)
		const tail = "edge-region.customer.example.net"
		return append(dst, tail[:8+int(x>>56)%24]...)
	default:
		dst = append(dst, 0x80)
		var token [8]byte
		binary.BigEndian.PutUint64(token[:], x)
		dst = append(dst, token[:]...)
		for j := 0; j < 1+int(x>>56)%55; j++ {
			dst = append(dst, byte(x>>uint(j%8*8))^byte(j))
		}
		return dst
	}
}
func quicksilverLookupKey(dst []byte, id uint64, seed int64, mixture string, arbitrary bool) []byte {
	if arbitrary {
		dst = append(dst, '!')
	}
	return quicksilverGenericKey(dst, id, seed, mixture)
}
func quicksilverRealisticSize(c *quicksilverConfig, id uint64) int {
	x := quicksilverMix(id ^ uint64(c.Seed) ^ 0xa513c789)
	small, medium := uint64(80), uint64(98)
	if c.Mixture == "holdout" {
		small, medium = 95, 99
	}
	base, bands := 32, 3
	switch bucket := x % 100; {
	case bucket < small:
	case bucket < medium:
		base = 256
	case bucket >= medium:
		base, bands = 2048, 4
	}
	lo := base << int((x>>8)%uint64(bands))
	// Include 32 in the smallest bucket; medium/large start above the prior bucket.
	if base == 32 {
		return lo + int(x>>16)%(lo+1)
	}
	return lo + 1 + int(x>>16)%lo
}
func quicksilverOpaque(c quicksilverConfig, id uint64) bool {
	x := quicksilverMix(id^uint64(c.Seed)^0x83a67cc5) % 4
	if c.Mixture == "holdout" {
		return x != 0
	}
	return x >= 2
}
func quicksilverRealisticValue(dst []byte, c quicksilverConfig, id, gen uint64) {
	if quicksilverOpaque(c, id) {
		x := quicksilverMix(id ^ uint64(c.Seed) ^ (gen+1)*0x9e3779b97f4a7c15)
		for p := 16; p < len(dst); p += 8 {
			x += 0x9e3779b97f4a7c15
			word := quicksilverMix(x)
			for j := 0; j < 8 && p+j < len(dst); j++ {
				dst[p+j] = byte(word >> uint(j*8))
			}
		}
	} else {
		var scratch [256]byte
		for p, block := 16, uint64(0); p < len(dst); block++ {
			x := quicksilverMix(id ^ uint64(c.Seed) ^ (gen+1)*0x9e3779b97f4a7c15 ^ block*0x517cc1b727220a95)
			record := append(scratch[:0], `{"zone":"`...)
			record = strconv.AppendUint(record, x, 16)
			record = append(record, `","route":"/api/`...)
			record = strconv.AppendUint(record, quicksilverMix(x), 16)
			record = append(record, `","ttl":`...)
			record = strconv.AppendUint(record, 60+x%3541, 10)
			record = append(record, ` ,"enabled":`...)
			if x&1 == 0 {
				record = append(record, "true"...)
			} else {
				record = append(record, "false"...)
			}
			record = append(record, ` ,"region":"`...)
			regions := [4]string{"us-east", "eu-west", "ap-south", "global"}
			record = append(record, regions[x%4]...)
			record = append(record, `","action":"`...)
			actions := [3]string{"allow", "challenge", "deny"}
			record = append(record, actions[x%3]...)
			record = append(record, '"', '}', '\n')
			p += copy(dst[p:], record)
		}
	}
	binary.BigEndian.PutUint64(dst, id)
	binary.BigEndian.PutUint64(dst[8:], gen)
}
func quicksilverWorkingKeys(c quicksilverConfig) int {
	switch c.WorkingSet {
	case "1%":
		return max(1, c.Keys/100)
	case "20%":
		return max(1, c.Keys/5)
	}
	return c.Keys
}
func quicksilverDeletedKeys(c quicksilverConfig) int { return max(1, c.Keys/100) }

func quicksilverAccess(c *quicksilverConfig, r *rand.Rand, mode, global, stride, offset int) (id uint64, absent bool, kind, distinct int) {
	i := int((int64(r.IntN(quicksilverWorkingKeys(*c)))*int64(stride) + int64(offset)) % int64(c.Keys))
	id = uint64(i) * 2
	absent = mode == 1 || (mode >= 2 && r.IntN(100) < c.MissPercent)
	distinct = i * 5
	if absent {
		kind = global % 3 // Balanced classes are independent of key/working-set draws.
		switch kind {
		case 0:
			distinct++ // Arbitrary: first byte replaced with reserved '!'.
		case 1:
			id++
			distinct += 2 // Same family/prefix, never loaded.
		case 2:
			i %= quicksilverDeletedKeys(*c)
			id = uint64(c.Keys+i) * 2
			distinct = i*5 + 3
		}
	} else if mode == 3 && global%7 == 0 && c.Updates >= 3 {
		// Readers also observe newly inserted identities, absent before publication.
		j := (i%((c.Updates+1)/4))*4 + 2
		id = uint64(2*c.Keys+j) * 2
		distinct = j*5 + 4
	}
	return
}

func quicksilverCheckRead(c *quicksilverConfig, id uint64, value []byte, absent, concurrent bool, state uint8) error {
	if c.Case != "realistic" {
		return quicksilverCheckValue(id, value, c.valueSize(), concurrent)
	}
	if absent {
		if value != nil {
			return fmt.Errorf("quicksilver: absent key %d returned bytes", id)
		}
		return nil
	}
	if concurrent && value == nil && (state == 255 || state == 5) {
		return nil
	}
	if len(value) != quicksilverRealisticSize(c, id) || binary.BigEndian.Uint64(value) != id {
		return fmt.Errorf("quicksilver: bad identity/length at %d", id)
	}
	gen := binary.BigEndian.Uint64(value[8:])
	if (!concurrent && gen != 0) || (concurrent && ((state == 0 || state == 255) && gen != 0 || state == 1 && gen > 1 || state == 4 && gen > 4 || state == 5 && gen != 1)) {
		return fmt.Errorf("quicksilver: bad generation %d at %d", gen, id)
	}
	return nil
}

type quicksilverMutations struct {
	Updates          int `json:"updates"`
	Deletes          int `json:"deletes"`
	Inserts          int `json:"inserts"`
	OverwriteTargets int `json:"overwrite_targets"`
	OverwriteSets    int `json:"overwrite_sets"`
}

func (m *quicksilverMutations) add(offset, count int) {
	for j := offset; j < offset+count; j++ {
		switch j % 4 {
		case 0:
			m.Updates++
		case 1:
			m.Deletes++
		case 2:
			m.Inserts++
		case 3:
			m.OverwriteTargets++
			m.OverwriteSets += 4
		}
	}
}

type quicksilverDistribution struct {
	KeyKinds         [3]int `json:"namespace_hostname_opaque_keys"`
	KeyBytes         int64  `json:"key_bytes"`
	MinKeyBytes      int    `json:"min_key_bytes"`
	MaxKeyBytes      int    `json:"max_key_bytes"`
	ValueBuckets     [3]int `json:"small_medium_large_values"`
	OpaqueValues     int    `json:"opaque_values"`
	StructuredValues int    `json:"structured_values"`
	ValueBytes       int64  `json:"value_bytes"`
	MinValueBytes    int    `json:"min_value_bytes"`
	MaxValueBytes    int    `json:"max_value_bytes"`
}

func quicksilverLoadedDistribution(c quicksilverConfig) (d quicksilverDistribution) {
	var scratch [128]byte
	d.MinKeyBytes, d.MinValueBytes = 128, 32768
	for i := 0; i < c.Keys; i++ {
		id := uint64(i) * 2
		k := quicksilverGenericKey(scratch[:0], id, c.Seed, c.Mixture)
		n := quicksilverRealisticSize(&c, id)
		d.KeyKinds[quicksilverKeyFamily(id, c.Seed, c.Mixture)]++
		d.KeyBytes += int64(len(k))
		d.ValueBytes += int64(n)
		d.MinKeyBytes = min(d.MinKeyBytes, len(k))
		d.MaxKeyBytes = max(d.MaxKeyBytes, len(k))
		d.MinValueBytes = min(d.MinValueBytes, n)
		d.MaxValueBytes = max(d.MaxValueBytes, n)
		bucket := 0
		if n > 256 {
			bucket = 1
		}
		if n > 2048 {
			bucket = 2
		}
		d.ValueBuckets[bucket]++
		if quicksilverOpaque(c, id) {
			d.OpaqueValues++
		} else {
			d.StructuredValues++
		}
	}
	return
}
func quicksilverRealisticWrite(db kvstore.DB, c quicksilverConfig, offset, count, stride int, update bool) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var key [128]byte
	value := make([]byte, 32768) // One group scratch; Batch.Set copies before reuse (no SetView).
	write := func(overwriteGen uint64) (err error) {
		b, err := db.(kvstore.Batcher).NewBatch()
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, b.Close()) }()
		for j := offset; j < offset+count; j++ {
			if overwriteGen != 0 && j%4 != 3 {
				continue
			}
			i := j
			gen := uint64(0)
			if update {
				i = int(int64(j) * int64(stride) % int64(c.Keys))
				gen = 1
			}
			if overwriteGen != 0 {
				gen = overwriteGen
			}
			id := uint64(i) * 2
			if update && j%4 == 2 {
				id = uint64(2*c.Keys+j) * 2
			}
			k := quicksilverGenericKey(key[:0], id, c.Seed, c.Mixture)
			if update && j%4 == 1 {
				if err = b.Delete(k); err != nil {
					return
				}
				continue
			}
			v := value[:quicksilverRealisticSize(&c, id)]
			quicksilverRealisticValue(v, c, id, gen)
			if err = b.Set(k, v); err != nil {
				return
			}
		}
		return quicksilverCommitBatch(b, c)
	}
	if err := write(0); err != nil {
		return err
	}
	// Distinct commits make overwrite generations visible and incur actual log
	// churn. Repeated Set calls inside one batch could be collapsed by an engine.
	if update && (offset+count)/4 > offset/4 {
		for gen := uint64(2); gen <= 4; gen++ {
			if err := write(gen); err != nil {
				return err
			}
		}
	}
	return nil
}
func quicksilverPrepareDeleted(db kvstore.DB, c quicksilverConfig, guard *benchGuard) (err error) {
	// A 1% disjoint population is inserted then deleted before measured reads.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var key [128]byte
	value := make([]byte, 32768)
	for off := 0; off < quicksilverDeletedKeys(c); off += 1000 {
		if err = guard.Checkpoint(); err != nil {
			return
		}
		b, e := db.(kvstore.Batcher).NewBatch()
		if e != nil {
			return e
		}
		for pass := 0; pass < 2; pass++ {
			for i := off; i < min(off+1000, quicksilverDeletedKeys(c)); i++ {
				id := uint64(c.Keys+i) * 2
				k := quicksilverGenericKey(key[:0], id, c.Seed, c.Mixture)
				if pass == 0 {
					v := value[:quicksilverRealisticSize(&c, id)]
					quicksilverRealisticValue(v, c, id, 0)
					err = b.Set(k, v)
				} else {
					err = b.Delete(k)
				}
				if err != nil {
					return errors.Join(err, b.Close())
				}
			}
			if err = quicksilverCommitBatch(b, c); err != nil {
				return errors.Join(err, b.Close())
			}
			if err = b.Close(); err != nil {
				return
			}
			if pass == 0 {
				b, err = db.(kvstore.Batcher).NewBatch()
				if err != nil {
					return
				}
			}
		}
	}
	return
}
func quicksilverRealisticVerify(db kvstore.DB, c quicksilverConfig, stride int, guard *benchGuard, mutated bool) (keys, misses int, err error) {
	changed := make([]uint8, c.Keys)
	if mutated {
		for j := 0; j < c.Updates; j++ {
			i := int(int64(j) * int64(stride) % int64(c.Keys))
			switch j % 4 {
			case 0:
				changed[i] = 1
			case 1:
				changed[i] = 255
			case 3:
				changed[i] = 4
			}
		}
	}
	var scratch [128]byte
	expected := make([]byte, 32768)
	check := func(id uint64, absent bool, gen uint64, arbitrary bool) error {
		k := quicksilverLookupKey(scratch[:0], id, c.Seed, c.Mixture, arbitrary)
		v, e := quicksilverGet(db.Get, k)
		if e != nil {
			return e
		}
		if absent {
			if v != nil {
				return fmt.Errorf("quicksilver: reopen absent key %d returned bytes", id)
			}
			misses++
			return nil
		}
		want := expected[:quicksilverRealisticSize(&c, id)]
		quicksilverRealisticValue(want, c, id, gen)
		if !bytes.Equal(v, want) {
			return fmt.Errorf("quicksilver: reopen byte mismatch key %d", id)
		}
		keys++
		return nil
	}
	for i := 0; i < c.Keys; i++ {
		if i%256 == 0 {
			if err = guard.Checkpoint(); err != nil {
				return
			}
		}
		id := uint64(i) * 2
		if err = check(id, changed[i] == 255, uint64(changed[i]), false); err != nil {
			return
		}
		if err = check(id+1, true, 0, false); err != nil {
			return
		}
		if err = check(id, true, 0, true); err != nil {
			return
		}
	}
	for i := 0; i < quicksilverDeletedKeys(c); i++ {
		if err = check(uint64(c.Keys+i)*2, true, 0, false); err != nil {
			return
		}
	}
	if mutated {
		for j := 2; j < c.Updates; j += 4 {
			if err = check(uint64(2*c.Keys+j)*2, false, 1, false); err != nil {
				return
			}
		}
	}
	return
}

type quicksilverCompressionGroup struct {
	Records         int     `json:"records"`
	RawBytes        int64   `json:"raw_bytes"`
	CompressedBytes int64   `json:"compressed_bytes"`
	Ratio           float64 `json:"compressed_to_raw_ratio"`
}
type quicksilverCompressibility struct {
	Codec         string                      `json:"codec"`
	Basis         string                      `json:"basis"`
	SampleBuckets [3]int                      `json:"sample_small_medium_large_records"`
	Total         quicksilverCompressionGroup `json:"total"`
	Structured    quicksilverCompressionGroup `json:"structured"`
	Opaque        quicksilverCompressionGroup `json:"opaque"`
}

func quicksilverMeasureCompressibility(c quicksilverConfig) (out quicksilverCompressibility, err error) {
	out.Codec = "stdlib DEFLATE BestSpeed"
	out.Basis = "up to 4096 distinct loaded records, seeded coprime-stride sample; each record compressed independently including identity/generation header; setup only, not an engine codec claim"
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.BestSpeed)
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	scratch := make([]byte, 32768)
	stride := quicksilverUpdateStride(c.Keys)
	offset := int(quicksilverMix(uint64(c.Seed)) % uint64(c.Keys))
	for j := 0; j < min(4096, c.Keys); j++ {
		id := uint64((int64(j)*int64(stride)+int64(offset))%int64(c.Keys)) * 2
		v := scratch[:quicksilverRealisticSize(&c, id)]
		quicksilverRealisticValue(v, c, id, 0)
		compressed.Reset()
		writer.Reset(&compressed)
		if _, err = writer.Write(v); err != nil {
			return
		}
		if err = writer.Close(); err != nil {
			return
		}
		group := &out.Structured
		if quicksilverOpaque(c, id) {
			group = &out.Opaque
		}
		for _, g := range []*quicksilverCompressionGroup{group, &out.Total} {
			g.Records++
			g.RawBytes += int64(len(v))
			g.CompressedBytes += int64(compressed.Len())
		}
		bucket := 0
		if len(v) > 256 {
			bucket = 1
		}
		if len(v) > 2048 {
			bucket = 2
		}
		out.SampleBuckets[bucket]++
	}
	for _, g := range []*quicksilverCompressionGroup{&out.Total, &out.Structured, &out.Opaque} {
		if g.RawBytes > 0 {
			g.Ratio = float64(g.CompressedBytes) / float64(g.RawBytes)
		}
	}
	return
}
