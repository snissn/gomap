package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestCatalogMetaCanonicalCommandAndExactRetry(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatalf("NewCatalogMetaRecordV1: %v", err)
	}
	command, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	again, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1 again: %v", err)
	}
	if string(command) != string(again) {
		t.Fatalf("command bytes are not deterministic")
	}

	a := NewCatalogMetaAuthorityV1()
	first, err := a.applyCommittedCatalogMetaV1(command, 11)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if first.Epoch != 1 || first.AppliedIndex != 11 || first.Digest != record.Digest {
		t.Fatalf("first status=%+v", first)
	}
	retry, err := a.applyCommittedCatalogMetaV1(command, 12)
	if err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if retry.Epoch != 1 || retry.AppliedIndex != 11 {
		t.Fatalf("retry status=%+v", retry)
	}
}

func TestCatalogMetaRejectsStaleSkippedAndConflictingEpoch(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	first := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.applyCommittedCatalogMetaV1(first, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 2, validCatalog()), 2); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale err=%v", err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 3, validCatalog()), 3); !errors.Is(err, ErrCatalogMetaSkippedEpoch) {
		t.Fatalf("skipped err=%v", err)
	}
	c := validCatalog()
	c.Groups[0].LeaderHint = ""
	if _, err := a.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, c), 3); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestCatalogMetaRouteAdmissionFailsClosed(t *testing.T) {
	a := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := a.applyCommittedCatalogMetaV1(command, 7); err != nil {
		t.Fatal(err)
	}
	status, ok := a.Status()
	if !ok {
		t.Fatal("missing status")
	}
	request := RouteRequestV1{Collection: CollectionRefV1{Database: DefaultDatabase, Catalog: DefaultCatalog, Collection: "users"}, Shape: RouteShapeCollectionV1}
	if _, err := a.Route(context.Background(), CatalogProofV1{}, request); !errors.Is(err, ErrCatalogMetaProofMissing) {
		t.Fatalf("missing proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 2, Digest: status.Digest}, request); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("stale proof err=%v", err)
	}
	if _, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: "00"}, request); !errors.Is(err, ErrCatalogMetaDigestMismatch) {
		t.Fatalf("digest proof err=%v", err)
	}
	decision, err := a.Route(context.Background(), CatalogProofV1{Epoch: 1, Digest: status.Digest}, request)
	if err != nil || decision.GroupID() != "group-a" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestCatalogMetaSnapshotInstallNeverMovesBackward(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	if _, err := source.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 0, 1, validCatalog()), 9); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	target := NewCatalogMetaAuthorityV1()
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); err != nil {
		t.Fatalf("same snapshot: %v", err)
	}
	if _, err := target.applyCommittedCatalogMetaV1(mustCatalogMetaCommand(t, 1, 2, validCatalog()), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := target.InstallCatalogMetaSnapshotV1(snapshot); !errors.Is(err, ErrCatalogMetaStaleEpoch) {
		t.Fatalf("rollback snapshot err=%v", err)
	}
}

func TestCatalogMetaDecodeRejectsUnknownAndOversized(t *testing.T) {
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCatalogMetaCommandV1(append(append([]byte{}, b[:len(b)-1]...), []byte(`,"unknown":true}`)...)); !errors.Is(err, ErrInvalidCatalogMeta) {
		t.Fatalf("unknown err=%v", err)
	}
	if _, err := DecodeCatalogMetaCommandV1(make([]byte, MaxCatalogMetaCommandBytesV1+1)); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("limit err=%v", err)
	}
}

func TestCatalogMetaDecodePreflightRejectsMalformedBeforeTypedDecode(t *testing.T) {
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	record, err := NewCatalogMetaRecordV1(1, validCatalog())
	if err != nil {
		t.Fatal(err)
	}
	recordBytes, err := encodeCatalogMetaRecordV1(record)
	if err != nil {
		t.Fatal(err)
	}
	duplicateGroups := bytes.Replace(command, []byte(`"ID":"group-b"`), []byte(`"ID":"group-a"`), 1)
	duplicateMembers := bytes.Replace(command, []byte(`"Members":["node-a","node-c"]`), []byte(`"Members":["node-a","node-a"]`), 1)
	duplicatePlacements := bytes.Replace(command, []byte(`"Collection":"orders"`), []byte(`"Collection":"users"`), 1)
	duplicatePartitions := bytes.Replace(catalogMetaPartitionsShapeV1(2), []byte(`"ID":"p1"`), []byte(`"ID":"p0"`), 1)
	feature := `{"Name":"` + string(FeatureCollectionGroups) + `","Version":{"Major":1,"Minor":0}}`
	duplicateFeatures := bytes.Replace(
		catalogMetaCommandShapeV1(`null`, `null`),
		[]byte(`"Required":null`),
		[]byte(`"Required":[`+feature+`,`+feature+`]`),
		1,
	)
	badDigest := bytes.Clone(command)
	digestAt := bytes.Index(badDigest, []byte(`"digest":"`))
	if digestAt < 0 {
		t.Fatal("missing digest field")
	}
	digestAt += len(`"digest":"`)
	if badDigest[digestAt] == '0' {
		badDigest[digestAt] = '1'
	} else {
		badDigest[digestAt] = '0'
	}
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "truncated", raw: command[:len(command)-1], want: ErrInvalidCatalogMeta},
		{name: "duplicate command key", raw: bytes.Replace(command, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
		{name: "duplicate nested key", raw: bytes.Replace(command, []byte(`"Groups":`), []byte(`"Groups":null,"Groups":`), 1), want: ErrInvalidCatalogMeta},
		{name: "unknown exact-case field", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"Expected_Epoch":0`), 1), want: ErrInvalidCatalogMeta},
		{name: "unknown format", raw: bytes.Replace(command, []byte(`{"format":1`), []byte(`{"format":2`), 1), want: ErrUnsupportedVersion},
		{name: "uint64 overflow", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"expected_epoch":18446744073709551616`), 1), want: ErrCatalogMetaLimit},
		{name: "non-integer numeric", raw: bytes.Replace(command, []byte(`"expected_epoch":0`), []byte(`"expected_epoch":0.0`), 1), want: ErrInvalidCatalogMeta},
		{name: "overlong string", raw: bytes.Replace(command, []byte(`"ID":"group-a"`), []byte(`"ID":"`+strings.Repeat("a", MaxCatalogMetaStringBytesV1+1)+`"`), 1), want: ErrCatalogMetaLimit},
		{name: "digest mismatch", raw: badDigest, want: ErrCatalogMetaDigestMismatch},
		{name: "duplicate group identity", raw: duplicateGroups, want: ErrDuplicateGroup},
		{name: "duplicate member identity", raw: duplicateMembers, want: ErrDuplicateMember},
		{name: "duplicate placement identity", raw: duplicatePlacements, want: ErrDuplicatePlacement},
		{name: "duplicate partition identity", raw: duplicatePartitions, want: ErrDuplicateTokenPartition},
		{name: "duplicate feature identity", raw: duplicateFeatures, want: ErrUnsupportedFeature},
		{name: "record duplicate key", raw: bytes.Replace(recordBytes, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got error
			if strings.HasPrefix(tc.name, "record ") {
				_, got = decodeCatalogMetaRecordV1(tc.raw)
			} else {
				_, got = DecodeCatalogMetaCommandV1(tc.raw)
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("error=%v want errors.Is(%v)", got, tc.want)
			}
		})
	}
}

func TestCatalogMetaRecordConstructionEnforcesWireStringLimit(t *testing.T) {
	catalog := validCatalog()
	longID := raftcluster.GroupID(strings.Repeat("g", MaxCatalogMetaStringBytesV1+1))
	catalog.Groups[0].ID = longID
	catalog.Placements[0].GroupID = longID
	if _, err := NewCatalogMetaRecordV1(1, catalog); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("NewCatalogMetaRecordV1 error=%v want ErrCatalogMetaLimit", err)
	}
}

func TestCatalogMetaDecodePreflightElementLimits(t *testing.T) {
	tests := []struct {
		name  string
		count int
		build func(int) []byte
		want  error
	}{
		{name: "groups minimum", count: 1, build: catalogMetaGroupsShapeV1},
		{name: "groups maximum", count: MaxCatalogMetaGroupsV1, build: catalogMetaGroupsShapeV1},
		{name: "groups one over", count: MaxCatalogMetaGroupsV1 + 1, build: catalogMetaGroupsShapeV1, want: ErrCatalogMetaLimit},
		{name: "members minimum", count: 1, build: catalogMetaMembersShapeV1},
		{name: "members maximum", count: MaxCatalogMetaMembersPerGroupV1, build: catalogMetaMembersShapeV1},
		{name: "members one over", count: MaxCatalogMetaMembersPerGroupV1 + 1, build: catalogMetaMembersShapeV1, want: ErrCatalogMetaLimit},
		{name: "placements minimum", count: 1, build: catalogMetaPlacementsShapeV1},
		{name: "placements maximum", count: MaxCatalogMetaPlacementsV1, build: catalogMetaPlacementsShapeV1},
		{name: "placements one over", count: MaxCatalogMetaPlacementsV1 + 1, build: catalogMetaPlacementsShapeV1, want: ErrCatalogMetaLimit},
		{name: "partitions minimum", count: 1, build: catalogMetaPartitionsShapeV1},
		{name: "partitions maximum", count: MaxCatalogMetaPartitionsPerPlacementV1, build: catalogMetaPartitionsShapeV1},
		{name: "partitions one over", count: MaxCatalogMetaPartitionsPerPlacementV1 + 1, build: catalogMetaPartitionsShapeV1, want: ErrCatalogMetaLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.build(tc.count)
			if tc.want == nil && len(raw) > MaxCatalogMetaCommandBytesV1 {
				t.Fatalf("test shape is %d bytes, beyond command cap", len(raw))
			}
			err := preflightCatalogMetaCommandJSONV1(raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("preflight error=%v want errors.Is(%v); bytes=%d", err, tc.want, len(raw))
			}
		})
	}
}

func TestCatalogMetaDecodePreflightAggregatePartitionLimit(t *testing.T) {
	raw := catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1([]int{
		MaxCatalogMetaPartitionsV1 / 2,
		MaxCatalogMetaPartitionsV1 - MaxCatalogMetaPartitionsV1/2 + 1,
	}))
	if len(raw) > MaxCatalogMetaCommandBytesV1 {
		t.Fatalf("aggregate test shape is %d bytes, beyond command cap", len(raw))
	}
	if err := preflightCatalogMetaCommandJSONV1(raw); !errors.Is(err, ErrCatalogMetaLimit) {
		t.Fatalf("preflight error=%v want ErrCatalogMetaLimit", err)
	}
}

func TestCatalogMetaMaximumPlacementShapeRoundTrip(t *testing.T) {
	catalog := CatalogV1{
		Groups: []GroupV1{{ID: "g", Members: []raftcluster.NodeID{"n"}, LeaderHint: "n"}},
	}
	catalog.Placements = make([]CollectionPlacementV1, MaxCatalogMetaPlacementsV1)
	for i := range catalog.Placements {
		catalog.Placements[i] = CollectionPlacementV1{
			Collection: CollectionRefV1{Database: "d", Catalog: "c", Collection: fmt.Sprintf("x%04d", i)},
			GroupID:    "g",
		}
	}
	command := mustCatalogMetaCommand(t, 0, 1, catalog)
	if len(command) > MaxCatalogMetaCommandBytesV1 {
		t.Fatalf("maximum placement command is %d bytes", len(command))
	}
	decoded, err := DecodeCatalogMetaCommandV1(command)
	if err != nil {
		t.Fatalf("DecodeCatalogMetaCommandV1: %v", err)
	}
	if len(decoded.Record.Catalog.Placements) != MaxCatalogMetaPlacementsV1 {
		t.Fatalf("placements=%d want %d", len(decoded.Record.Catalog.Placements), MaxCatalogMetaPlacementsV1)
	}
	again, err := EncodeCatalogMetaCommandV1(decoded)
	if err != nil {
		t.Fatalf("EncodeCatalogMetaCommandV1: %v", err)
	}
	if !bytes.Equal(command, again) {
		t.Fatal("maximum placement command did not round trip exactly")
	}
}

func TestCatalogMetaSnapshotBytesPreflightAndCanonicalRestore(t *testing.T) {
	source := NewCatalogMetaAuthorityV1()
	command := mustCatalogMetaCommand(t, 0, 1, validCatalog())
	if _, err := source.applyCommittedCatalogMetaV1(command, 42); err != nil {
		t.Fatal(err)
	}
	raw, err := source.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	target := NewCatalogMetaAuthorityV1()
	if err := target.InstallCatalogMetaSnapshotBytesV1(raw); err != nil {
		t.Fatalf("canonical restore: %v", err)
	}
	status, ok := target.Status()
	if !ok || status.Epoch != 1 || status.AppliedIndex != 42 {
		t.Fatalf("restored status=%+v ok=%t", status, ok)
	}

	var snapshot CatalogMetaSnapshotV1
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Record = []byte(`{"format":1,"format":1}`)
	badNested, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	tooLong := []byte(`{"format":1,"applied_index":1,"record":"` + strings.Repeat("A", maxCatalogMetaSnapshotFieldBytesV1+1) + `","last_command":null}`)
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "duplicate wrapper key", raw: bytes.Replace(raw, []byte(`{"format":1`), []byte(`{"format":1,"format":1`), 1), want: ErrInvalidCatalogMeta},
		{name: "non canonical whitespace", raw: append(append([]byte{}, raw...), '\n'), want: ErrInvalidCatalogMeta},
		{name: "nested record duplicate key", raw: badNested, want: ErrInvalidCatalogMeta},
		{name: "encoded record over limit", raw: tooLong, want: ErrCatalogMetaLimit},
		{name: "total bytes over limit", raw: make([]byte, MaxCatalogMetaSnapshotBytesV1+1), want: ErrCatalogMetaLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewCatalogMetaAuthorityV1().InstallCatalogMetaSnapshotBytesV1(tc.raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want errors.Is(%v)", err, tc.want)
			}
		})
	}
}

func FuzzDecodeCatalogMetaCommandV1(f *testing.F) {
	valid := mustCatalogMetaCommand(f, 0, 1, validCatalog())
	f.Add(valid)
	f.Add(valid[:len(valid)-1])
	f.Add([]byte(`{"format":1,"expected_epoch":18446744073709551616}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxCatalogMetaCommandBytesV1 {
			return
		}
		command, err := DecodeCatalogMetaCommandV1(raw)
		if err != nil {
			return
		}
		again, err := EncodeCatalogMetaCommandV1(command)
		if err != nil {
			t.Fatalf("accepted command failed re-encode: %v", err)
		}
		if !bytes.Equal(raw, again) {
			t.Fatal("accepted command was not canonical")
		}
	})
}

func catalogMetaCommandShapeV1(groups, placements string) []byte {
	return []byte(`{"format":1,"expected_epoch":0,"record":{"format":1,"epoch":1,"catalog":{"Features":{"ConfigVersion":{"Major":0,"Minor":0},"Required":null},"Groups":` + groups + `,"Placements":` + placements + `},"digest":"` + strings.Repeat("0", MaxCatalogMetaDigestBytesV1) + `"}}`)
}

func catalogMetaGroupsShapeV1(count int) []byte {
	var groups strings.Builder
	groups.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			groups.WriteByte(',')
		}
		fmt.Fprintf(&groups, `{"ID":"g%d","Members":["n"],"LeaderHint":"n"}`, i)
	}
	groups.WriteByte(']')
	return catalogMetaCommandShapeV1(groups.String(), `null`)
}

func catalogMetaMembersShapeV1(count int) []byte {
	var members strings.Builder
	members.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			members.WriteByte(',')
		}
		fmt.Fprintf(&members, `"n%d"`, i)
	}
	members.WriteByte(']')
	groups := `[{"ID":"g","Members":` + members.String() + `,"LeaderHint":"n0"}]`
	return catalogMetaCommandShapeV1(groups, `null`)
}

func catalogMetaPlacementsShapeV1(count int) []byte {
	counts := make([]int, count)
	return catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1(counts))
}

func catalogMetaPartitionsShapeV1(count int) []byte {
	return catalogMetaCommandShapeV1(`null`, catalogMetaPlacementArrayV1([]int{count}))
}

func catalogMetaPlacementArrayV1(partitionCounts []int) string {
	var placements strings.Builder
	placements.WriteByte('[')
	for i, count := range partitionCounts {
		if i > 0 {
			placements.WriteByte(',')
		}
		placements.WriteString(`{"Collection":{"Database":"d","Catalog":"c","Collection":"x`)
		placements.WriteString(strconv.Itoa(i))
		placements.WriteString(`"},"GroupID":"g","Mode":"","RouteKey":"","TokenPartitions":`)
		if count == 0 {
			placements.WriteString(`null}`)
			continue
		}
		placements.WriteByte('[')
		for j := 0; j < count; j++ {
			if j > 0 {
				placements.WriteByte(',')
			}
			fmt.Fprintf(&placements, `{"ID":"p%d","GroupID":"g","Start":0,"End":0}`, j)
		}
		placements.WriteString(`]}`)
	}
	placements.WriteByte(']')
	return placements.String()
}

func mustCatalogMetaCommand(t testing.TB, expected, epoch uint64, catalog CatalogV1) []byte {
	t.Helper()
	record, err := NewCatalogMetaRecordV1(epoch, catalog)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	b, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: expected, Record: record})
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	return b
}
