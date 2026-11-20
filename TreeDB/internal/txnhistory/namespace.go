package txnhistory

import (
	"bytes"
	"cmp"
	"errors"
)

// NamespaceKind separates arbitrary raw bytes from semantic keyspaces. The
// zero Namespace is exclusively the raw keyspace; no raw key prefix is reserved
// for conflict bookkeeping.
type NamespaceKind uint8

const (
	RawNamespace NamespaceKind = iota
	CatalogNamespace
	CollectionNamespace
)

// Namespace uses durable logical identities, never physical RootIDs or manager
// addresses. Catalog names use CatalogNamespace with zero Collection/Domain.
// A collection uses its monotone incarnation, a stable logical domain, and the
// schema interpretation epoch. Ordinary writes and physical handoff preserve it.
type Namespace struct {
	Kind                               NamespaceKind
	Collection, Domain, Interpretation uint64
}

// OnlyPointChanged requires an actual write to point and excludes changes to
// other dependencies, covering ranges and namespace replacement barriers.
// Callers serialize it with validation and accepted publication.
func (j *History) OnlyPointChanged(start uint64, point Dependency, dependencies, effects []Dependency) bool {
	if j == nil || j.Check(start) != nil || point.Kind != PointEffect || point.Namespace.Kind != CollectionNamespace ||
		j.index[j.slotIn(point.Namespace, string(point.Key))].seq <= start || j.pointCoveredByRange(start, point.Namespace, point.Key) {
		return false
	}
	for _, set := range [2][]Dependency{dependencies, effects} {
		for _, d := range set {
			if d.Kind == PointEffect && d.Namespace == point.Namespace && bytes.Equal(d.Key, point.Key) {
				continue
			}
			if changed, err := j.DependenciesChanged(start, []Dependency{d}); changed || err != nil {
				return false
			}
		}
	}
	return true
}

func compareNamespace(a, b Namespace) int {
	if c := cmp.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Collection, b.Collection); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Domain, b.Domain); c != 0 {
		return c
	}
	return cmp.Compare(a.Interpretation, b.Interpretation)
}

type EffectKind uint8

const (
	PointEffect EffectKind = iota + 1
	RangeEffect
	BarrierEffect
)

// Effect is a logical mutation. A point records intent including absent deletes
// and same-value writes. Range is the full half-open interval (nil End is
// unbounded). Barrier changes interpretation or conservatively represents an
// oversized mutation in this namespace alone.
type Effect struct {
	Namespace Namespace
	Kind      EffectKind
	Key, End  []byte
}

// Dependency has the same interval representation as an effect. Catalog absent
// lookup is a point dependency, listing is a range, and schema/full replacement
// is a barrier. Bytes belong to the transaction until validation completes.
type Dependency = Effect

var (
	ErrInvalidEffect  = errors.New("treedb: invalid transaction history effect")
	ErrEffectCapacity = errors.New("treedb: transaction history capacity exhausted")
)

// DependenciesChanged validates tracked reads and blind-write dependencies
// against the SAME journal. Check distinguishes expired history before probing.
func (j *History) DependenciesChanged(start uint64, dependencies []Dependency) (bool, error) {
	if err := j.Check(start); err != nil {
		return false, err
	}
	for _, d := range dependencies {
		switch d.Kind {
		case PointEffect:
			if j.PointChangedIn(start, d.Namespace, d.Key) {
				return true, nil
			}
		case RangeEffect:
			if j.RangeChangedIn(start, d.Namespace, d.Key, d.End) {
				return true, nil
			}
		case BarrierEffect:
			if j.RangeChangedIn(start, d.Namespace, nil, nil) {
				return true, nil
			}
		default:
			return false, ErrInvalidEffect
		}
	}
	return false, nil
}

// ValidateCurrentDependencies checks dependencies when the caller holds the
// publication sequencer and knows no installed sequence exceeds start.
func (j *History) ValidateCurrentDependencies(start uint64, dependencies []Dependency) error {
	if err := j.Check(start); err != nil {
		return err
	}
	for _, d := range dependencies {
		switch d.Kind {
		case PointEffect, RangeEffect, BarrierEffect:
		default:
			return ErrInvalidEffect
		}
	}
	return nil
}
