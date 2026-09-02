// Package mappedresource defines TreeDB's shared vocabulary for immutable
// file-backed resource views. It is intentionally small: subsystem-specific
// caches may remain specialized while using these keys, scopes, handles, stats,
// and maintenance pins as the common contract.
package mappedresource

import (
	"errors"
	"fmt"
)

// Class identifies the resource family. It is coarse enough for shared
// accounting but does not replace subsystem-specific formats.
type Class string

const (
	ClassValueLog         Class = "value_log"
	ClassLeafLog          Class = "leaf_log"
	ClassTypedRowAsset    Class = "typed_row_asset"
	ClassTypedColumnAsset Class = "typed_column_asset"
	ClassExternalAsset    Class = "external_asset"
)

// Section identifies a logical sub-range within an immutable asset. Column-part
// assets use this to address descriptor, dictionary, vector, adjacency, and
// scalar payload sections without requiring one OS file per section.
type Section struct {
	Kind     string
	Category string
	Name     string
	Column   string
	Ordinal  uint32
}

// Empty reports whether the section identity is absent.
func (s Section) Empty() bool {
	return s == Section{}
}

// Key is the stable identity for a mapped or copied resource range.
//
// Offset/Length are a physical range in FileID/object identity. Section is an
// optional logical refinement for internally sectioned assets. Checksum is a
// generic content checksum/digest slot; callers may use the lower bits when the
// source format stores uint32 checksums.
type Key struct {
	Class      Class
	Namespace  string
	Kind       string
	Generation uint64
	PartID     uint64
	FileID     uint32
	Offset     int64
	Length     int64
	Checksum   uint64
	Version    uint16
	Encoding   string
	Section    Section
}

// Validate rejects incomplete or unsafe resource identities. It is deliberately
// format-agnostic; subsystem adapters may apply stricter checks before calling
// into the manager.
func (k Key) Validate() error {
	switch k.Class {
	case ClassValueLog, ClassLeafLog, ClassTypedRowAsset, ClassTypedColumnAsset, ClassExternalAsset:
	case "":
		return errors.New("mappedresource: missing resource class")
	default:
		return fmt.Errorf("mappedresource: unsupported resource class %q", k.Class)
	}
	if k.Offset < 0 {
		return fmt.Errorf("mappedresource: negative resource offset %d", k.Offset)
	}
	if k.Length < 0 {
		return fmt.Errorf("mappedresource: negative resource length %d", k.Length)
	}
	if k.Offset > 0 && k.Length > maxInt64-k.Offset {
		return fmt.Errorf("mappedresource: resource range offset=%d length=%d overflows", k.Offset, k.Length)
	}
	if k.Class == ClassTypedRowAsset || k.Class == ClassTypedColumnAsset || k.Class == ClassValueLog || k.Class == ClassLeafLog {
		if k.Namespace == "" {
			return fmt.Errorf("mappedresource: %s resource requires namespace", k.Class)
		}
		if k.FileID == 0 {
			return fmt.Errorf("mappedresource: %s resource requires file id", k.Class)
		}
	}
	return nil
}

// Equal reports exact key equality.
func (k Key) Equal(other Key) bool {
	return k == other
}

// ScopeKind identifies the logical lifetime that owns a resource handle.
type ScopeKind string

const (
	ScopeDB                 ScopeKind = "db"
	ScopeSnapshot           ScopeKind = "snapshot"
	ScopeCollectionReadView ScopeKind = "collection_read_view"
	ScopeManifestGeneration ScopeKind = "manifest_generation"
	ScopeMaintenance        ScopeKind = "maintenance"
	ScopePreparedQuery      ScopeKind = "prepared_query"
	ScopePreparedSearch     ScopeKind = "prepared_search"
	ScopeTypedRowReader     ScopeKind = "typed_row_reader"
	ScopeColumnPartReader   ScopeKind = "column_part_reader"
)

// Scope ties a physical resource handle to a logical lifetime. Snapshot/read
// view scopes should anchor ordinary query/search handles; maintenance scopes
// anchor destructive planning and protection checks.
type Scope struct {
	Kind       ScopeKind
	ID         string
	ParentID   string
	Collection string
	Namespace  string
	Generation uint64
	Reason     string
}

// Validate rejects lifetime-less scopes. A non-empty ID makes ownership and
// tests explicit even when the scope is only process-local.
func (s Scope) Validate() error {
	switch s.Kind {
	case ScopeDB, ScopeSnapshot, ScopeCollectionReadView, ScopeManifestGeneration, ScopeMaintenance, ScopePreparedQuery, ScopePreparedSearch, ScopeTypedRowReader, ScopeColumnPartReader:
	case "":
		return errors.New("mappedresource: missing lifetime scope kind")
	default:
		return fmt.Errorf("mappedresource: unsupported lifetime scope kind %q", s.Kind)
	}
	if s.ID == "" {
		return fmt.Errorf("mappedresource: %s scope requires id", s.Kind)
	}
	return nil
}

// ValidateForKey checks both the scope and key. It also catches accidental
// namespace mismatches when both sides carry a namespace.
func (s Scope) ValidateForKey(k Key) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := k.Validate(); err != nil {
		return err
	}
	if s.Namespace != "" && k.Namespace != "" && s.Namespace != k.Namespace {
		return fmt.Errorf("mappedresource: scope namespace=%q does not match key namespace=%q", s.Namespace, k.Namespace)
	}
	return nil
}

// NewScope constructs a scope with the minimum required fields.
func NewScope(kind ScopeKind, id string) Scope {
	return Scope{Kind: kind, ID: id}
}

const maxInt64 = int64(^uint64(0) >> 1)
