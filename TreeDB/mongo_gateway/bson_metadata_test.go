package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Frozen independent oracles from f5bf0f1988f5768123e6ed1eafeea027ae3ff5ca.
// Keep their parsing and semantic order separate from the production helpers.
func baselineMetadataProjection(projection wire.Document) (projectionMode, map[string]struct{}, bool, bool, error) {
	elements, err := bson.Raw(projection).Elements()
	if err != nil {
		return projectionNone, nil, true, false, err
	}
	fields := make(map[string]struct{}, len(elements))
	mode := projectionNone
	includeID := true
	idSpecified := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		include, err := baselineMetadataProjectionValue(elem.Value())
		if err != nil {
			return projectionNone, nil, true, false, err
		}
		if key == "_id" {
			includeID = include
			idSpecified = true
			continue
		}
		// _id has deliberately narrow root-only projection semantics.  Treating
		// _id.part as an ordinary path would either return the whole root _id
		// through the inclusion default or silently drop it through an explicit
		// _id:0.  Until embedded _id traversal has a complete Mongo-compatible
		// contract, reject it before a scan or cursor can be admitted.
		if strings.HasPrefix(key, "_id.") {
			return projectionNone, nil, true, false, fmt.Errorf("Mongo gateway projection does not support dotted paths rooted at _id")
		}
		if key == "" || strings.HasPrefix(key, "$") {
			return projectionNone, nil, true, false, fmt.Errorf("Mongo gateway projection has invalid path %q", key)
		}
		nextMode := projectionExclude
		if include {
			nextMode = projectionInclude
		}
		if mode != projectionNone && mode != nextMode {
			return projectionNone, nil, true, false, errors.New("Mongo gateway projection cannot mix include and exclude fields")
		}
		mode = nextMode
		fields[key] = struct{}{}
	}
	return mode, fields, includeID, idSpecified, nil
}

func baselineMetadataProjectionValue(value bson.RawValue) (bool, error) {
	if v, ok := value.BooleanOK(); ok {
		return v, nil
	}
	if v, ok := value.Int32OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.Int64OK(); ok {
		return v != 0, nil
	}
	if v, ok := value.DoubleOK(); ok {
		return v != 0, nil
	}
	return false, errors.New("Mongo gateway projection values must be boolean or numeric")
}

func baselineMetadataReadConcern(command wire.Document) error {
	elements, err := bson.Raw(command).Elements()
	if err != nil {
		return mongoReadConcernFailedToParse("Mongo command is malformed: %v", err)
	}
	seenReadConcern := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return mongoReadConcernFailedToParse("Mongo command is malformed: %v", err)
		}
		if key != "readConcern" {
			continue
		}
		if seenReadConcern {
			return mongoReadConcernBadValue("Mongo command field \"readConcern\" is duplicated")
		}
		seenReadConcern = true
		if err := baselineMetadataReadConcernValue(elem.Value()); err != nil {
			return err
		}
	}
	return nil
}

func baselineMetadataReadConcernValue(value bson.RawValue) error {
	readConcern, ok := value.DocumentOK()
	if !ok {
		return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" must be a document")
	}
	elements, err := readConcern.Elements()
	if err != nil {
		return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" is malformed: %v", err)
	}
	seenLevel := false
	for _, elem := range elements {
		key, err := elem.KeyErr()
		if err != nil {
			return mongoReadConcernFailedToParse("Mongo command field \"readConcern\" is malformed: %v", err)
		}
		switch key {
		case "level":
			if seenLevel {
				return mongoReadConcernBadValue("Mongo readConcern field \"level\" is duplicated")
			}
			seenLevel = true
			level, ok := elem.Value().StringValueOK()
			if !ok {
				return mongoReadConcernFailedToParse("Mongo command field \"readConcern.level\" must be a string")
			}
			if !(level == "local" || level == "available") {
				return mongoReadConcernBadValue("Mongo gateway readConcern level %q is not supported; only local-stale reads are supported", level)
			}
		case "afterClusterTime", "atClusterTime":
			return mongoReadConcernBadValue("Mongo gateway readConcern does not support %q", key)
		default:
			return mongoReadConcernBadValue("Mongo gateway readConcern does not support %q", key)
		}
	}
	return nil
}

func metadataErrorSignature(err error) string {
	if err == nil {
		return ""
	}
	var rc mongoReadConcernParseError
	if errors.As(err, &rc) {
		return fmt.Sprintf("%T:%d:%s:%s", err, rc.code, rc.codeName, rc.message)
	}
	return fmt.Sprintf("%T:%s", err, err.Error())
}

func TestBSONMetadataProjectionBaselineDifferential(t *testing.T) {
	fixtures := []wire.Document{nil, {}, {0}, {5, 0, 0, 0, 0}}
	for _, width := range []int{0, 1, 11, 16, 17, 64} {
		var d bson.D
		for i := 0; i < width; i++ {
			d = append(d, bson.E{Key: fmt.Sprintf("field%d", i), Value: i%2 == 0})
		}
		// Uniform inclusion also covers successful wide projections and spill.
		uniform := bson.D{}
		for i := 0; i < width; i++ {
			uniform = append(uniform, bson.E{Key: fmt.Sprintf("field%d", i), Value: true})
		}
		fixtures = append(fixtures, marshalProjectionFixture(t, d), marshalProjectionFixture(t, uniform))
	}
	for _, d := range []bson.D{
		{{Key: "field", Value: true}, {Key: "field", Value: true}, {Key: "_id", Value: false}},
		{{Key: "_id", Value: false}, {Key: "_id", Value: true}},
		{{Key: "field", Value: "invalid"}, {Key: "later", Value: true}},
		{{Key: "field", Value: int32(1)}, {Key: "other", Value: int64(1)}, {Key: "double", Value: 1.0}},
		{{Key: "_id.part", Value: true}}, {{Key: "$invalid", Value: true}}, {{Key: "", Value: true}},
		{{Key: strings.Repeat("é", 100), Value: true}},
	} {
		fixtures = append(fixtures, marshalProjectionFixture(t, d))
	}
	check := func(raw wire.Document) {
		t.Helper()
		m, f, id, specified, err := parseProjection(raw)
		wm, wf, wid, ws, we := baselineMetadataProjection(raw)
		if m != wm || !reflect.DeepEqual(f, wf) || id != wid || specified != ws || metadataErrorSignature(err) != metadataErrorSignature(we) {
			t.Fatalf("projection %x: got %v/%v/%v/%v/%s want %v/%v/%v/%v/%s", raw, m, f, id, specified, metadataErrorSignature(err), wm, wf, wid, ws, metadataErrorSignature(we))
		}
	}
	for _, raw := range fixtures {
		metadataMutations(raw, check)
	}
}

func metadataMutations(raw wire.Document, check func(wire.Document)) {
	check(raw)
	for n := 0; n < len(raw); n++ {
		check(raw[:n])
	}
	if len(raw) == 0 {
		return
	}
	for _, pos := range []int{0, 3, 4, len(raw) - 1} {
		if pos >= len(raw) {
			continue
		}
		for _, value := range []byte{0, 1, 0xff} {
			copyRaw := append(wire.Document(nil), raw...)
			copyRaw[pos] = value
			check(copyRaw)
		}
	}
	// Corrupt later elements while keeping the outer length intact. This
	// distinguishes structural-before-semantic precedence from early returns.
	if elems, err := bson.Raw(raw).Elements(); err == nil {
		pos := 4
		for _, elem := range elems {
			if pos+len(elem) > len(raw) {
				break
			}
			badType := append(wire.Document(nil), raw...)
			badType[pos] = 0x7f
			check(badType)
			if end := bytes.IndexByte(elem[1:], 0); end >= 0 {
				badKey := append(wire.Document(nil), raw...)
				badKey[pos+1+end] = 0xff
				check(badKey)
			}
			pos += len(elem)
		}
	}
	check(append(append(wire.Document(nil), raw...), 0, 1, 2))
}

func TestBSONMetadataReadConcernBaselineDifferential(t *testing.T) {
	fixtures := []wire.Document{nil, {}, {0}, {5, 0, 0, 0, 0}}
	concerns := []any{bson.D{}, bson.D{{Key: "level", Value: "local"}}, bson.D{{Key: "level", Value: "available"}},
		bson.D{{Key: "level", Value: "majority"}}, bson.D{{Key: "level", Value: 1}},
		bson.D{{Key: "level", Value: "local"}, {Key: "level", Value: "local"}},
		bson.D{{Key: "afterClusterTime", Value: 1}}, bson.D{{Key: "unknown", Value: true}}, "invalid"}
	for _, concern := range concerns {
		for _, d := range []bson.D{
			{{Key: "find", Value: "rows"}, {Key: "readConcern", Value: concern}, {Key: "unknown", Value: 1}},
			{{Key: "readConcern", Value: concern}, {Key: "readConcern", Value: bson.D{}}},
		} {
			fixtures = append(fixtures, marshalProjectionFixture(t, d))
		}
	}
	for _, width := range []int{0, 1, 11, 16, 17, 64} {
		d := bson.D{}
		for i := 0; i < width; i++ {
			d = append(d, bson.E{Key: fmt.Sprintf("other%d", i), Value: i})
		}
		fixtures = append(fixtures, marshalProjectionFixture(t, d))
	}
	check := func(raw wire.Document) {
		t.Helper()
		got, want := parseMongoReadConcern(raw), baselineMetadataReadConcern(raw)
		if metadataErrorSignature(got) != metadataErrorSignature(want) {
			t.Fatalf("readConcern %x: got %s want %s", raw, metadataErrorSignature(got), metadataErrorSignature(want))
		}
	}
	for _, raw := range fixtures {
		metadataMutations(raw, check)
	}
}

func TestBSONMetadataScratchSpillAndOwnedKeys(t *testing.T) {
	for _, width := range []int{0, 1, 11, 16, 17, 64} {
		d := bson.D{}
		for i := 0; i < width; i++ {
			d = append(d, bson.E{Key: fmt.Sprintf("field%d", i), Value: true})
		}
		raw := marshalProjectionFixture(t, d)
		var scratch [16]bson.RawElement
		got, err := bsonElementsWithScratch(bson.Raw(raw), scratch[:])
		if err != nil || len(got) != width {
			t.Fatalf("width=%d len=%d err=%v", width, len(got), err)
		}
		if width > 16 && cap(got) <= 16 {
			t.Fatalf("width %d did not spill", width)
		}
	}
	key := strings.Repeat("longé", 40)
	command := marshalProjectionFixture(t, bson.D{{Key: key, Value: true}, {Key: "_id", Value: false}})
	compiled, err := compileProjection(command)
	if err != nil {
		t.Fatal(err)
	}
	plan := cloneFindPlanForCursor(findPlan{projection: compiled})
	charge := findPlanCursorRetainedBytes(plan)
	clear(command)
	row := marshalProjectionFixture(t, bson.D{{Key: key, Value: "first"}, {Key: key, Value: "second"}, {Key: "_id", Value: 1}})
	want := marshalProjectionFixture(t, bson.D{{Key: key, Value: "first"}, {Key: key, Value: "second"}})
	got, err := projectDocumentWithProjection(row, plan.projection)
	if err != nil {
		t.Fatal(err)
	}
	clear(row)
	if !bytes.Equal(got, want) {
		t.Fatalf("owned duplicate/UTF8 projection mismatch: %x", got)
	}
	if findPlanCursorRetainedBytes(plan) != charge {
		t.Fatal("retained selector charge changed")
	}
}
