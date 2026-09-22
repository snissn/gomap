package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	maxCatalogMetaJSONKeyBytesV1                = 32
	maxCatalogMetaJSONNumberBytesV1             = 20
	maxCatalogMetaJSONObjectsV1                 = 25000
	maxCatalogMetaJSONArraysV1                  = 5000
	maxCatalogMetaJSONNestedElementsV1          = MaxCatalogMetaFeaturesV1 + MaxCatalogMetaGroupsV1 + MaxCatalogMetaPlacementsV1 + MaxCatalogMetaPartitionsV1 + MaxCatalogMetaGroupsV1*MaxCatalogMetaMembersPerGroupV1
	maxCatalogMetaSnapshotFieldBytesV1          = ((MaxCatalogMetaCommandBytesV1 + 2) / 3) * 4
	maxCatalogMetaLifecycleSnapshotFieldBytesV1 = ((MaxCatalogMetaSnapshotBytesV1 + 2) / 3) * 4
	catalogMetaPreflightCommandV1               = "command"
	catalogMetaPreflightRecordV1                = "record"
	catalogMetaPreflightSnapshotV1              = "snapshot"
)

// catalogMetaJSONPreflightV1 walks the complete JSON token stream before
// encoding/json is allowed to populate any catalog slice. The schema-specific
// walk caps every repeated field and catches duplicate keys and identities
// without constructing the command, record, or snapshot object graph.
type catalogMetaJSONPreflightV1 struct {
	decoder  *json.Decoder
	depth    int
	objects  int
	arrays   int
	elements int
}

func preflightCatalogMetaCommandJSONV1(raw []byte) error {
	p, err := newCatalogMetaJSONPreflightV1(raw)
	if err != nil {
		return err
	}
	if err := p.command(); err != nil {
		return err
	}
	return p.finish(catalogMetaPreflightCommandV1)
}

func preflightCatalogMetaRecordJSONV1(raw []byte) error {
	p, err := newCatalogMetaJSONPreflightV1(raw)
	if err != nil {
		return err
	}
	if err := p.record(); err != nil {
		return err
	}
	return p.finish(catalogMetaPreflightRecordV1)
}

func preflightCatalogMetaSnapshotJSONV1(raw []byte) error {
	p, err := newCatalogMetaJSONPreflightV1(raw)
	if err != nil {
		return err
	}
	if err := p.snapshot(); err != nil {
		return err
	}
	return p.finish(catalogMetaPreflightSnapshotV1)
}

func newCatalogMetaJSONPreflightV1(raw []byte) (*catalogMetaJSONPreflightV1, error) {
	if !utf8.Valid(raw) {
		return nil, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("JSON is not valid UTF-8"))
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return &catalogMetaJSONPreflightV1{decoder: d}, nil
}

func (p *catalogMetaJSONPreflightV1) finish(kind string) error {
	if _, err := p.decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("trailing %s data", kind))
		}
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s JSON: %w", kind, err))
	}
	return nil
}

func (p *catalogMetaJSONPreflightV1) command() error {
	return p.object([]string{"format", "expected_epoch", "record"}, func(key string) error {
		switch key {
		case "format":
			return p.format()
		case "expected_epoch":
			_, err := p.unsigned(^uint64(0), "expected_epoch")
			return err
		case "record":
			return p.record()
		default:
			return p.unknownField(catalogMetaPreflightCommandV1, key)
		}
	})
}

func (p *catalogMetaJSONPreflightV1) record() error {
	return p.object([]string{"format", "epoch", "catalog", "digest"}, func(key string) error {
		switch key {
		case "format":
			return p.format()
		case "epoch":
			_, err := p.unsigned(^uint64(0), "epoch")
			return err
		case "catalog":
			return p.catalog()
		case "digest":
			_, err := p.string(MaxCatalogMetaDigestBytesV1, "digest")
			return err
		default:
			return p.unknownField(catalogMetaPreflightRecordV1, key)
		}
	})
}

func (p *catalogMetaJSONPreflightV1) snapshot() error {
	return p.object([]string{"format", "applied_index", "record", "last_command"}, func(key string) error {
		switch key {
		case "format":
			return p.format()
		case "applied_index":
			_, err := p.unsigned(^uint64(0), "applied_index")
			return err
		case "record", "last_command":
			_, _, err := p.nullableString(maxCatalogMetaSnapshotFieldBytesV1, key)
			return err
		case "vector_partition_lifecycle":
			_, _, err := p.nullableString(maxCatalogMetaLifecycleSnapshotFieldBytesV1, key)
			return err
		default:
			return p.unknownField(catalogMetaPreflightSnapshotV1, key)
		}
	})
}

func (p *catalogMetaJSONPreflightV1) format() error {
	format, err := p.unsigned(uint64(^uint16(0)), "format")
	if err != nil {
		return err
	}
	if format != uint64(CatalogMetaFormatV1) {
		return errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedVersion, fmt.Errorf("format %d", format))
	}
	return nil
}

func (p *catalogMetaJSONPreflightV1) catalog() error {
	groupIDs := make(map[string]struct{})
	placementIDs := make(map[CollectionRefV1]struct{})
	featureNames := make(map[string]struct{})
	partitions := 0
	return p.object([]string{"Features", "Groups", "Placements"}, func(key string) error {
		switch key {
		case "Features":
			return p.features(featureNames)
		case "Groups":
			return p.array(MaxCatalogMetaGroupsV1, "Groups", func(_ int) error {
				id, err := p.group()
				if err != nil {
					return err
				}
				if _, exists := groupIDs[id]; exists {
					return errors.Join(ErrInvalidCatalogMeta, ErrDuplicateGroup, fmt.Errorf("group %q appears more than once", id))
				}
				groupIDs[id] = struct{}{}
				return nil
			})
		case "Placements":
			return p.array(MaxCatalogMetaPlacementsV1, "Placements", func(_ int) error {
				ref, count, err := p.placement()
				if err != nil {
					return err
				}
				if _, exists := placementIDs[ref]; exists {
					return errors.Join(ErrInvalidCatalogMeta, ErrDuplicatePlacement, fmt.Errorf("placement for %s/%s/%s appears more than once", ref.Database, ref.Catalog, ref.Collection))
				}
				placementIDs[ref] = struct{}{}
				if count > MaxCatalogMetaPartitionsV1-partitions {
					return p.limit("aggregate TokenPartitions count exceeds %d", MaxCatalogMetaPartitionsV1)
				}
				partitions += count
				return nil
			})
		default:
			return p.unknownField("catalog", key)
		}
	})
}

func (p *catalogMetaJSONPreflightV1) features(featureNames map[string]struct{}) error {
	return p.object([]string{"ConfigVersion", "Required"}, func(key string) error {
		switch key {
		case "ConfigVersion":
			version, err := p.version()
			if err != nil {
				return err
			}
			if version != (raftcluster.Version{}) && (version.Major != SupportedCatalogVersion.Major || version.Minor > SupportedCatalogVersion.Minor) {
				return errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedVersion, fmt.Errorf("catalog version %d.%d", version.Major, version.Minor))
			}
			return nil
		case "Required":
			return p.array(MaxCatalogMetaFeaturesV1, "Required", func(_ int) error {
				name, version, err := p.requiredFeature()
				if err != nil {
					return err
				}
				if _, exists := featureNames[name]; exists {
					return errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedFeature, fmt.Errorf("required feature %q appears more than once", name))
				}
				featureNames[name] = struct{}{}
				floor, ok := SupportedFeatureFloors[raftcluster.FeatureName(name)]
				if !ok {
					return errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedFeature, fmt.Errorf("required feature %q", name))
				}
				if version.Major != floor.Major || version.Minor > floor.Minor {
					return errors.Join(ErrInvalidCatalogMeta, ErrUnsupportedVersion, fmt.Errorf("required feature %q version %d.%d", name, version.Major, version.Minor))
				}
				return nil
			})
		default:
			return p.unknownField("Features", key)
		}
	})
}

func (p *catalogMetaJSONPreflightV1) requiredFeature() (string, raftcluster.Version, error) {
	var name string
	var version raftcluster.Version
	err := p.object([]string{"Name", "Version"}, func(key string) error {
		switch key {
		case "Name":
			value, err := p.string(MaxCatalogMetaStringBytesV1, "feature Name")
			name = value
			return err
		case "Version":
			value, err := p.version()
			version = value
			return err
		default:
			return p.unknownField("Required feature", key)
		}
	})
	return name, version, err
}

func (p *catalogMetaJSONPreflightV1) version() (raftcluster.Version, error) {
	var version raftcluster.Version
	err := p.object([]string{"Major", "Minor"}, func(key string) error {
		if key != "Major" && key != "Minor" {
			return p.unknownField("Version", key)
		}
		value, err := p.unsigned(uint64(^uint16(0)), key)
		if err != nil {
			return err
		}
		switch key {
		case "Major":
			version.Major = uint16(value)
		case "Minor":
			version.Minor = uint16(value)
		}
		return nil
	})
	return version, err
}

func (p *catalogMetaJSONPreflightV1) group() (string, error) {
	var id string
	members := make(map[string]struct{})
	err := p.object([]string{"ID", "Members", "LeaderHint"}, func(key string) error {
		switch key {
		case "ID":
			value, err := p.string(MaxCatalogMetaStringBytesV1, "group ID")
			id = value
			return err
		case "Members":
			return p.array(MaxCatalogMetaMembersPerGroupV1, "Members", func(_ int) error {
				member, err := p.string(MaxCatalogMetaStringBytesV1, "member ID")
				if err != nil {
					return err
				}
				if _, exists := members[member]; exists {
					return errors.Join(ErrInvalidCatalogMeta, ErrDuplicateMember, fmt.Errorf("member %q appears more than once", member))
				}
				members[member] = struct{}{}
				return nil
			})
		case "LeaderHint":
			_, err := p.string(MaxCatalogMetaStringBytesV1, "leader hint")
			return err
		default:
			return p.unknownField("Group", key)
		}
	})
	return id, err
}

func (p *catalogMetaJSONPreflightV1) placement() (CollectionRefV1, int, error) {
	var ref CollectionRefV1
	partitions := 0
	partitionIDs := make(map[string]struct{})
	err := p.object([]string{"Collection", "GroupID", "Mode", "RouteKey", "TokenPartitions"}, func(key string) error {
		switch key {
		case "Collection":
			value, err := p.collectionRef()
			ref = value
			return err
		case "GroupID", "Mode", "RouteKey":
			_, err := p.string(MaxCatalogMetaStringBytesV1, key)
			return err
		case "TokenPartitions":
			return p.array(MaxCatalogMetaPartitionsPerPlacementV1, "TokenPartitions", func(_ int) error {
				id, err := p.tokenPartition()
				if err != nil {
					return err
				}
				if _, exists := partitionIDs[id]; exists {
					return errors.Join(ErrInvalidCatalogMeta, ErrDuplicateTokenPartition, fmt.Errorf("token partition %q appears more than once", id))
				}
				partitionIDs[id] = struct{}{}
				partitions++
				return nil
			})
		default:
			return p.unknownField("Placement", key)
		}
	})
	return ref, partitions, err
}

func (p *catalogMetaJSONPreflightV1) collectionRef() (CollectionRefV1, error) {
	var ref CollectionRefV1
	err := p.object([]string{"Database", "Catalog", "Collection"}, func(key string) error {
		if key != "Database" && key != "Catalog" && key != "Collection" {
			return p.unknownField("Collection", key)
		}
		value, err := p.string(MaxCatalogMetaStringBytesV1, key)
		if err != nil {
			return err
		}
		switch key {
		case "Database":
			ref.Database = value
		case "Catalog":
			ref.Catalog = value
		case "Collection":
			ref.Collection = value
		}
		return nil
	})
	return ref, err
}

func (p *catalogMetaJSONPreflightV1) tokenPartition() (string, error) {
	var id string
	err := p.object([]string{"ID", "GroupID", "Start", "End"}, func(key string) error {
		switch key {
		case "ID", "GroupID":
			value, err := p.string(MaxCatalogMetaStringBytesV1, key)
			if key == "ID" {
				id = value
			}
			return err
		case "Start", "End":
			_, err := p.unsigned(^uint64(0), key)
			return err
		default:
			return p.unknownField("TokenPartition", key)
		}
	})
	return id, err
}

func (p *catalogMetaJSONPreflightV1) object(required []string, field func(string) error) error {
	if err := p.open('{'); err != nil {
		return err
	}
	defer p.closeContainer()
	seen := make(map[string]struct{}, len(required))
	for p.decoder.More() {
		token, err := p.decoder.Token()
		if err != nil {
			return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("object key: %w", err))
		}
		key, ok := token.(string)
		if !ok {
			return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("object key is %T", token))
		}
		if len(key) > maxCatalogMetaJSONKeyBytesV1 {
			return p.limit("object key is %d bytes", len(key))
		}
		if _, exists := seen[key]; exists {
			return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("duplicate JSON key %q", key))
		}
		seen[key] = struct{}{}
		if err := field(key); err != nil {
			return err
		}
	}
	if err := p.end('}'); err != nil {
		return err
	}
	for _, key := range required {
		if _, ok := seen[key]; !ok {
			return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("missing JSON field %q", key))
		}
	}
	return nil
}

func (p *catalogMetaJSONPreflightV1) array(limit int, label string, item func(int) error) error {
	token, err := p.decoder.Token()
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s: %w", label, err))
	}
	if token == nil {
		return nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is not an array or null", label))
	}
	if err := p.enterContainer('['); err != nil {
		return err
	}
	defer p.closeContainer()
	count := 0
	for p.decoder.More() {
		count++
		if count > limit {
			return p.limit("%s has more than %d elements", label, limit)
		}
		p.elements++
		if p.elements > maxCatalogMetaJSONNestedElementsV1 {
			return p.limit("aggregate nested element count exceeds %d", maxCatalogMetaJSONNestedElementsV1)
		}
		if err := item(count - 1); err != nil {
			return err
		}
	}
	return p.end(']')
}

func (p *catalogMetaJSONPreflightV1) open(want json.Delim) error {
	token, err := p.decoder.Token()
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("open %q: %w", want, err))
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != want {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("got %v, want %q", token, want))
	}
	return p.enterContainer(want)
}

func (p *catalogMetaJSONPreflightV1) enterContainer(kind json.Delim) error {
	p.depth++
	if p.depth > MaxCatalogMetaNestingDepthV1 {
		return p.limit("JSON nesting exceeds %d", MaxCatalogMetaNestingDepthV1)
	}
	switch kind {
	case '{':
		p.objects++
		if p.objects > maxCatalogMetaJSONObjectsV1 {
			return p.limit("JSON object count exceeds %d", maxCatalogMetaJSONObjectsV1)
		}
	case '[':
		p.arrays++
		if p.arrays > maxCatalogMetaJSONArraysV1 {
			return p.limit("JSON array count exceeds %d", maxCatalogMetaJSONArraysV1)
		}
	}
	return nil
}

func (p *catalogMetaJSONPreflightV1) closeContainer() {
	p.depth--
}

func (p *catalogMetaJSONPreflightV1) end(want json.Delim) error {
	token, err := p.decoder.Token()
	if err != nil {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("close %q: %w", want, err))
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != want {
		return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("got %v, want %q", token, want))
	}
	return nil
}

func (p *catalogMetaJSONPreflightV1) string(limit int, label string) (string, error) {
	value, isNull, err := p.nullableString(limit, label)
	if err != nil {
		return "", err
	}
	if isNull {
		return "", errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is null", label))
	}
	return value, nil
}

func (p *catalogMetaJSONPreflightV1) nullableString(limit int, label string) (string, bool, error) {
	token, err := p.decoder.Token()
	if err != nil {
		return "", false, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s: %w", label, err))
	}
	if token == nil {
		return "", true, nil
	}
	value, ok := token.(string)
	if !ok {
		return "", false, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is not a string", label))
	}
	if len(value) > limit {
		return "", false, p.limit("%s is %d bytes", label, len(value))
	}
	return value, false, nil
}

func (p *catalogMetaJSONPreflightV1) unsigned(max uint64, label string) (uint64, error) {
	token, err := p.decoder.Token()
	if err != nil {
		return 0, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s: %w", label, err))
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is not an unsigned integer", label))
	}
	text := string(number)
	if len(text) == 0 || len(text) > maxCatalogMetaJSONNumberBytesV1 {
		return 0, p.limit("%s numeric token is %d bytes", label, len(text))
	}
	if text != "0" && text[0] == '0' {
		return 0, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is not a canonical unsigned integer", label))
	}
	for i := range text {
		if text[i] < '0' || text[i] > '9' {
			return 0, errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("%s is not a canonical unsigned integer", label))
		}
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil || value > max {
		return 0, p.limit("%s integer overflows its field", label)
	}
	return value, nil
}

func (p *catalogMetaJSONPreflightV1) unknownField(object, key string) error {
	return errors.Join(ErrInvalidCatalogMeta, fmt.Errorf("unknown %s field %q", object, key))
}

func (p *catalogMetaJSONPreflightV1) limit(format string, args ...any) error {
	return errors.Join(ErrCatalogMetaLimit, fmt.Errorf(format, args...))
}
