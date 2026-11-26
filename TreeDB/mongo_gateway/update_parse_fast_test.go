package mongogateway

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshalUpdateDoc(t testing.TB, doc any) wire.Document {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	return wire.Document(raw)
}

func TestParseMongoUpdateItemPureSetFastPathParity(t *testing.T) {
	t.Run("basic_pure_set", func(t *testing.T) {
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(42)}, {Key: "city", Value: "nyc"}}}}},
			{Key: "multi", Value: false},
			{Key: "upsert", Value: false},
		})
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			t.Fatalf("parseMongoUpdateItem: %v", err)
		}
		if !item.pureSet || !item.setFieldsOK || !item.bsonSetFieldsOK || !item.exactID {
			t.Fatalf("unexpected flags: pureSet=%v setFieldsOK=%v bsonSetFieldsOK=%v exactID=%v", item.pureSet, item.setFieldsOK, item.bsonSetFieldsOK, item.exactID)
		}
		if len(item.bsonSetFields) != 2 || item.bsonSetFields[0].Key != "score" || item.bsonSetFields[1].Key != "city" {
			t.Fatalf("unexpected bsonSetFields: %+v", item.bsonSetFields)
		}
	})

	t.Run("duplicate_keys_last_wins_first_order", func(t *testing.T) {
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-dup"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "a", Value: int32(1)},
				{Key: "b", Value: int32(2)},
				{Key: "a", Value: int32(99)},
			}}}},
		})
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			t.Fatalf("parseMongoUpdateItem: %v", err)
		}
		if len(item.bsonSetFields) != 2 || item.bsonSetFields[0].Key != "a" || item.bsonSetFields[1].Key != "b" {
			t.Fatalf("unexpected order: %+v", item.bsonSetFields)
		}
		if got, ok := item.bsonSetFields[0].Value.Int32OK(); !ok || got != 99 {
			t.Fatalf("expected last value 99 for key a, got %v (ok=%v)", got, ok)
		}
	})

	t.Run("over_16_fields_seen_map_promotion", func(t *testing.T) {
		setDoc := make(bson.D, 0, 22)
		for i := 0; i < 20; i++ {
			setDoc = append(setDoc, bson.E{Key: fmt.Sprintf("f%02d", i), Value: int32(i)})
		}
		setDoc = append(setDoc, bson.E{Key: "f03", Value: int32(300)})
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-many"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: setDoc}}},
		})
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			t.Fatalf("parseMongoUpdateItem: %v", err)
		}
		if len(item.bsonSetFields) != 20 {
			t.Fatalf("expected 20 unique fields, got %d", len(item.bsonSetFields))
		}
		if got, ok := item.bsonSetFields[3].Value.Int32OK(); !ok || got != 300 {
			t.Fatalf("expected f03=300, got %v (ok=%v)", got, ok)
		}
	})

	t.Run("bson_only_type_keeps_bson_fast_path", func(t *testing.T) {
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-bin"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{
				{Key: "ts", Value: bson.NewDateTimeFromTime(time.Unix(1700000000, 0))},
				{Key: "blob", Value: bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
			}}}},
		})
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			t.Fatalf("parseMongoUpdateItem: %v", err)
		}
		if !item.pureSet || item.setFieldsOK || !item.bsonSetFieldsOK {
			t.Fatalf("unexpected flags: pureSet=%v setFieldsOK=%v bsonSetFieldsOK=%v", item.pureSet, item.setFieldsOK, item.bsonSetFieldsOK)
		}
	})

	t.Run("unsupported_options_precedence", func(t *testing.T) {
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "hint", Value: bson.D{{Key: "_id", Value: 1}}},
			{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}},
			{Key: "arrayFilters", Value: bson.A{}},
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "score", Value: int32(1)}}}}},
		})
		_, err := parseMongoUpdateItem(2, raw)
		if err == nil || !strings.Contains(err.Error(), "arrayFilters") {
			t.Fatalf("expected arrayFilters precedence error, got %v", err)
		}
	})

	t.Run("unique_index_touch_without_setFields_map", func(t *testing.T) {
		raw := mustMarshalUpdateDoc(t, bson.D{
			{Key: "q", Value: bson.D{{Key: "_id", Value: "doc-1"}}},
			{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "profile", Value: int32(31)}}}}},
		})
		item, err := parseMongoUpdateItem(0, raw)
		if err != nil {
			t.Fatalf("parseMongoUpdateItem: %v", err)
		}
		if item.setFields != nil || len(item.bsonSetFields) != 1 {
			t.Fatalf("expected fast path with nil setFields map and 1 bsonSetField, got setFields=%v len=%d", item.setFields, len(item.bsonSetFields))
		}
		metaNonTouching := collections.CollectionMeta{
			Indexes: []collections.IndexDefinition{
				{Name: "email_1", Field: "email", Unique: true},
				{Name: "city_1", Field: "city", Unique: false},
			},
		}
		if mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(metaNonTouching, item) {
			t.Fatalf("expected profile not to touch email unique index")
		}
		metaTouching := collections.CollectionMeta{
			Indexes: []collections.IndexDefinition{
				{Name: "profile_1", Field: "profile.email", Unique: true},
			},
		}
		if !mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(metaTouching, item) {
			t.Fatalf("expected profile to conservatively match top-level unique index prefix profile")
		}
		metaCompound := collections.CollectionMeta{
			Indexes: []collections.IndexDefinition{
				{Name: "compound_1", Unique: true, Components: []collections.IndexComponent{{Field: "email"}, {Field: "profile.name"}}},
			},
		}
		if !mongoUpdateSetFieldsTouchSecondaryUniqueIndexMeta(metaCompound, item) {
			t.Fatalf("expected profile to match compound unique index component prefix profile")
		}
	})
}
