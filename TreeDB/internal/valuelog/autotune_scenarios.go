package valuelog

import "math/rand"

// AutotuneWorkload describes a deterministic value generator.
type AutotuneWorkload struct {
	Name string
	Make func(rng *rand.Rand, size int) []byte
}

// AutotuneWorkloads returns the standard deterministic workloads used by the
// autotune bench suite.
func AutotuneWorkloads() []AutotuneWorkload {
	makeRepeatTail := func(rng *rand.Rand, size, tail int, pattern []byte) []byte {
		buf := make([]byte, size)
		for i := 0; i < size; {
			n := copy(buf[i:], pattern)
			i += n
		}
		if tail > 0 && size > 0 {
			if tail > size {
				tail = size
			}
			rng.Read(buf[size-tail:])
		}
		return buf
	}
	makeSparseNoise := func(rng *rand.Rand, size, stride, noise int, pattern []byte) []byte {
		buf := makeRepeatTail(rng, size, 0, pattern)
		if stride <= 0 {
			stride = 256
		}
		if noise <= 0 {
			noise = 16
		}
		for off := 0; off < size; off += stride {
			end := off + noise
			if end > size {
				end = size
			}
			rng.Read(buf[off:end])
		}
		return buf
	}
	return []AutotuneWorkload{
		{
			Name: "highly_compressible_tail64",
			Make: func(rng *rand.Rand, size int) []byte {
				return makeRepeatTail(rng, size, 64, []byte("{\"key\":\"value\",\"active\":true}"))
			},
		},
		{
			Name: "template_friendly_mid",
			Make: func(rng *rand.Rand, size int) []byte {
				const (
					prefix = "templatedb:{\"type\":\"account\",\"data\":"
					suffix = ",\"state\":\"active\"}"
				)
				p := []byte(prefix)
				s := []byte(suffix)
				if size <= len(p)+len(s) {
					buf := make([]byte, size)
					copy(buf, p)
					if size > len(p) {
						copy(buf[len(p):], s[:size-len(p)])
					}
					return buf
				}
				midLen := size - len(p) - len(s)
				buf := make([]byte, size)
				copy(buf, p)
				// Deterministic variable middle.
				for i := 0; i < midLen; i++ {
					buf[len(p)+i] = byte('a' + rng.Intn(26))
				}
				copy(buf[len(p)+midLen:], s)
				return buf
			},
		},
		{
			Name: "medium_compressible_sparse",
			Make: func(rng *rand.Rand, size int) []byte {
				return makeSparseNoise(rng, size, 256, 16, []byte("abcd1234"))
			},
		},
		{
			Name: "incompressible",
			Make: func(rng *rand.Rand, size int) []byte {
				buf := make([]byte, size)
				rng.Read(buf)
				return buf
			},
		},
	}
}

// LookupAutotuneWorkload returns the workload with the given name.
func LookupAutotuneWorkload(name string) (AutotuneWorkload, bool) {
	for _, w := range AutotuneWorkloads() {
		if w.Name == name {
			return w, true
		}
	}
	return AutotuneWorkload{}, false
}

// GenerateAutotuneValues builds deterministic samples for a workload.
func GenerateAutotuneValues(workload AutotuneWorkload, size, count int, seed int64) [][]byte {
	if count <= 0 || size <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(seed))
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		values[i] = workload.Make(rng, size)
	}
	return values
}
