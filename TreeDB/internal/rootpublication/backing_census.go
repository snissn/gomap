package rootpublication

import (
	"math"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
)

// BackingCensus records allocation instances stamped at their actual birth.
// LiveClassBytes is retained typed backing, not heap or process RSS; Allocated*
// remain cumulative even after nodes are removed. It never treats len as bytes.
type BackingCensus struct {
	LiveClassBytes      uint64
	LiveAllocations     uint64
	AllocatedClassBytes uint64
	Allocations         uint64
}
type backingStamp struct {
	identity         uint64
	classBytes       uint64
	extraAllocations uint64
}

var nextBackingIdentity atomic.Uint64

// StableBackingClassBytes uses the pinned Go1.26.3 amd64 class/header/page rules.
// It fails closed on another target rather than exporting a false certificate.
// Strings/byte arrays are noscan; pointer-containing typed backing is scanned.
func StableBackingClassBytes(n uint64, scan bool) (uint64, error) {
	b, err := allocclass.ClassBytes(n, scan)
	if err != nil {
		return 0, ErrStableMetadataShapeUnsupported
	}
	return b, nil
}

func prepareBackingStamp(n uint64, scan bool, account StableMetadataAccount) (backingStamp, error) {
	b, e := StableBackingClassBytes(n, scan)
	if e != nil {
		return backingStamp{}, e
	}
	if account != nil {
		if e = account.ReserveStableMetadata(b); e != nil {
			return backingStamp{}, e
		}
	}
	if b == 0 {
		return backingStamp{}, nil
	}
	id := nextBackingIdentity.Add(1)
	if id == 0 {
		panic("stable backing allocation identity exhausted")
	}
	return backingStamp{identity: id, classBytes: b}, nil
}
func (c *BackingCensus) add(s backingStamp) {
	if s.identity == 0 {
		return
	}
	if c.AllocatedClassBytes > math.MaxUint64-s.classBytes || c.LiveClassBytes > math.MaxUint64-s.classBytes || c.Allocations == math.MaxUint64 || c.LiveAllocations == math.MaxUint64 {
		panic("stable backing census overflow")
	}
	c.LiveClassBytes += s.classBytes
	c.LiveAllocations++
	c.AllocatedClassBytes += s.classBytes
	c.Allocations++
}
func (c *BackingCensus) remove(s backingStamp) {
	if s.identity == 0 {
		return
	}
	if c.LiveClassBytes < s.classBytes || c.LiveAllocations < 1+s.extraAllocations {
		panic("stable backing census imbalance")
	}
	c.LiveClassBytes -= s.classBytes
	c.LiveAllocations -= 1 + s.extraAllocations
}

func (c *BackingCensus) addGroup(bytes, count uint64) {
	if bytes == 0 && count == 0 {
		return
	}
	if c.LiveClassBytes > math.MaxUint64-bytes || c.AllocatedClassBytes > math.MaxUint64-bytes || c.LiveAllocations > math.MaxUint64-count || c.Allocations > math.MaxUint64-count {
		panic("stable backing census overflow")
	}
	c.LiveClassBytes += bytes
	c.LiveAllocations += count
	c.AllocatedClassBytes += bytes
	c.Allocations += count
}
func backingStringPlan(values ...string) (uint64, uint64, error) {
	var b, n uint64
	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		x, e := StableBackingClassBytes(uint64(len(v)), false)
		if e != nil {
			return 0, 0, e
		}
		b, e = finiteStableAdd(b, x)
		if e != nil {
			return 0, 0, e
		}
		n++
	}
	return b, n, nil
}

type backingLayout struct {
	census BackingCensus
	err    error
}

func (p *backingLayout) add(n uint64, scan bool) {
	if p.err != nil || n == 0 {
		return
	}
	var s backingStamp
	s, p.err = prepareBackingStamp(n, scan, nil)
	if p.err == nil {
		p.census.add(s)
	}
}
func (p *backingLayout) string(s string) { p.add(uint64(len(s)), false) }
func (p *backingLayout) file(f *os.File) {
	if f == nil {
		return
	}
	p.add(uint64(unsafe.Sizeof(os.File{})), true)
	p.add(uint64(unsafe.Sizeof(finiteLinuxOSFile{})), true)
	p.string(f.Name())
}
func (p *backingLayout) merge(c BackingCensus) {
	if p.census.LiveClassBytes > math.MaxUint64-c.LiveClassBytes || p.census.LiveAllocations > math.MaxUint64-c.LiveAllocations || p.census.AllocatedClassBytes > math.MaxUint64-c.AllocatedClassBytes || p.census.Allocations > math.MaxUint64-c.Allocations {
		panic("stable backing census overflow")
	}
	p.census.LiveClassBytes += c.LiveClassBytes
	p.census.LiveAllocations += c.LiveAllocations
	p.census.AllocatedClassBytes += c.AllocatedClassBytes
	p.census.Allocations += c.Allocations
}
func stableTokenRetainedCensus(t *StableResourceToken) BackingCensus {
	var p backingLayout
	p.add(uint64(unsafe.Sizeof(StableResourceToken{})), true)
	p.file(t.pinned)
	p.add(uint64(unsafe.Sizeof(atomic.Int64{})), true)
	if t.identityPin != nil {
		p.add(uint64(unsafe.Sizeof(IdentityPin{})), true)
	}
	p.string(string(t.kind))
	p.string(t.logicalLane)
	p.string(t.resourceID)
	p.string(t.diagnosticPath)
	p.string(string(t.reachability))
	return p.census
}
func stableNamespaceRetainedCensus(t *StableNamespaceToken) BackingCensus {
	var p backingLayout
	p.add(uint64(unsafe.Sizeof(StableNamespaceToken{})), true)
	p.file(t.parent)
	if t.persistence != t.parent {
		p.file(t.persistence)
	}
	p.string(t.oldName)
	p.string(t.newName)
	p.string(t.diagnosticPath)
	return p.census
}
func finiteStableClassAdd(a, n uint64, scan bool) (uint64, error) {
	b, e := StableBackingClassBytes(n, scan)
	if e != nil {
		return 0, e
	}
	return finiteStableAdd(a, b)
}

// A clone owns its descriptor strings and token/pin wrappers; FD/refcounter,
// kind and namespace backing remain explicit retained source aliases.
func stableClonedTokenRetainedCensus(t *StableResourceToken) BackingCensus {
	var p backingLayout
	p.add(uint64(unsafe.Sizeof(StableResourceToken{})), true)
	if t.identityPin != nil {
		p.add(uint64(unsafe.Sizeof(IdentityPin{})), true)
	}
	p.string(t.logicalLane)
	p.string(t.resourceID)
	p.string(t.diagnosticPath)
	p.string(string(t.reachability))
	return p.census
}
