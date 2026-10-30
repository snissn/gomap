//go:build ignore

// A separately labeled exact-value control; it is not the timed YCSB workload.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/internal/ycsbevidence"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var collection = "ycsb_correctness"
var database = "ycsb_correctness"

type identity struct {
	SchemaVersion int    `json:"schema_version"`
	SourceID      string `json:"source_id"`
	FixtureID     string `json:"fixture_id"`
	Profile       string `json:"profile"`
	Target        string `json:"target"`
	Format        string `json:"format"`
}
type value struct {
	Type    string `json:"type"`
	Value   string `json:"value,omitempty"`
	Subtype *byte  `json:"subtype,omitempty"`
	Base64  string `json:"base64,omitempty"`
}
type row struct {
	ID     string           `json:"id"`
	Fields map[string]value `json:"fields"`
}
type update struct {
	ID       string `json:"id"`
	Worker   int    `json:"worker"`
	Sequence int    `json:"sequence"`
	Field    string `json:"field"`
	Value    value  `json:"value"`
}
type manifest struct {
	identity
	Load    []row    `json:"load"`
	Updates []update `json:"updates"`
}
type outcome struct {
	ID           string `json:"id"`
	Sequence     int    `json:"sequence"`
	Acknowledged bool   `json:"acknowledged"`
	ReadbackOK   bool   `json:"readback_ok"`
	Error        string `json:"error"`
}
type snapshot struct {
	identity
	Stage      string    `json:"stage"`
	Rows       []row     `json:"rows"`
	Operations []outcome `json:"operations"`
}

type driver interface {
	insert(context.Context, row) error
	update(context.Context, update) error
	get(context.Context, string) (row, error)
	scan(context.Context) ([]row, error)
	close(context.Context) error
}

func typed(b []byte, format string) value {
	if format == "json" {
		return value{Type: "string", Value: base64.StdEncoding.EncodeToString(b)}
	}
	zero := byte(0)
	return value{Type: "binary", Subtype: &zero, Base64: base64.StdEncoding.EncodeToString(b)}
}
func raw(v value) ([]byte, error) {
	if v.Type == "string" {
		return base64.StdEncoding.DecodeString(v.Value)
	}
	return base64.StdEncoding.DecodeString(v.Base64)
}
func deterministicBytes(prefix string) []byte {
	b := bytes.Repeat([]byte{'x'}, 100)
	copy(b, prefix)
	return b
}
func makeManifest(id identity) manifest {
	m := manifest{identity: id}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("user%04d", i)
		fields := map[string]value{}
		for f := 0; f < 10; f++ {
			fields[fmt.Sprintf("field%d", f)] = typed(deterministicBytes(fmt.Sprintf("load-%04d-%02d", i, f)), id.Format)
		}
		m.Load = append(m.Load, row{key, fields})
		for s := 1; s <= 2; s++ {
			m.Updates = append(m.Updates, update{key, i / 16, s, fmt.Sprintf("field%d", s-1), typed(deterministicBytes(fmt.Sprintf("update-%04d-%d", i, s)), id.Format)})
		}
	}
	return m
}
func writeJSON(path string, obj any) error {
	b, e := json.MarshalIndent(obj, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	data := append(b, '\n')
	n, e := f.Write(data)
	if e == nil && n != len(data) {
		e = errors.New("short manifest write")
	}
	return errors.Join(e, f.Close())
}

// Sync only fixture artifacts, never the service database or a global sync.
func syncArtifact(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = errors.Join(f.Sync(), f.Close())
	d, openErr := os.Open(filepath.Dir(path))
	if openErr != nil {
		return errors.Join(err, openErr)
	}
	return errors.Join(err, d.Sync(), d.Close())
}
func durableJSON(path string, obj any) error {
	if err := writeJSON(path, obj); err != nil {
		return err
	}
	return syncArtifact(path)
}
func crashManifest(id identity) manifest {
	m := makeManifest(id)
	m.Updates = []update{}
	return m
}

type crashOutcome struct {
	SchemaVersion int                 `json:"schema_version"`
	Ordinal       int                 `json:"ordinal"`
	ID            string              `json:"id"`
	Acknowledged  bool                `json:"acknowledged"`
	RequestedACK  string              `json:"requested_ack"`
	Write         ycsbevidence.Result `json:"write"`
}

// Sequential whole-document inserts give relaxed WAL an unambiguous ACK order.
// Each actual returned outcome is fsynced before another request is issued.
func crashWrites(ctx context.Context, d driver, m manifest, journal string) (retErr error) {
	f, err := os.OpenFile(journal, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	if err = syncArtifact(journal); err != nil {
		return err
	}
	ack := "AckVisible"
	if m.Target == "mongo" {
		ack = "w:1,j:absent"
	}
	for n, row := range m.Load {
		var cause error
		result := ycsbevidence.Call(func() error { cause = d.insert(ctx, row); return cause })
		outcome := crashOutcome{1, n, row.ID, cause == nil, ack, result}
		data, marshalErr := json.Marshal(outcome)
		if marshalErr != nil {
			return errors.Join(cause, marshalErr)
		}
		data = append(data, '\n')
		count, writeErr := f.Write(data)
		if count != len(data) && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if err = errors.Join(cause, writeErr, f.Sync()); err != nil {
			return err
		}
	}
	return nil
}

func crashScan(ctx context.Context, d driver) ([]row, error) {
	// A lost namespace is an allowed empty state only under the loss profiles;
	// check public metadata explicitly rather than swallowing arbitrary errors.
	found := false
	switch d := d.(type) {
	case *nativeDriver:
		metas, err := d.c.ListCollections(ctx)
		if err != nil {
			return nil, err
		}
		for _, meta := range metas {
			if meta.Name == collection {
				found = true
			}
		}
	case *mongoDriver:
		names, err := d.c.Database(database).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			if name == collection {
				found = true
			}
		}
	default:
		return nil, errors.New("crash scan requires a public driver")
	}
	if !found {
		return []row{}, nil
	}
	return d.scan(ctx)
}
func crashRowsValid(m manifest, actual []row) bool {
	wanted := map[string]row{}
	for _, r := range m.Load {
		wanted[r.ID] = r
	}
	seen := map[string]bool{}
	for _, r := range actual {
		if seen[r.ID] || !reflect.DeepEqual(wanted[r.ID], r) {
			return false
		}
		seen[r.ID] = true
	}
	if m.Profile == "command_wal_durable" {
		return len(actual) == len(m.Load)
	}
	if m.Profile == "command_wal_relaxed" {
		for n, r := range m.Load {
			if seen[r.ID] != (n < len(actual)) {
				return false
			}
		}
	}
	return true
}
func persistOutcomes(path string, id identity, stage string, outcomes []outcome, cause error) error {
	return errors.Join(cause, writeJSON(path, snapshot{id, stage, []row{}, outcomes}))
}
func sameRows(a, b []row) bool {
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	sort.Slice(b, func(i, j int) bool { return b[i].ID < b[j].ID })
	return reflect.DeepEqual(a, b)
}

func checkedCheckpoint(ctx context.Context, client interface{ Checkpoint(context.Context) error }) (ycsbevidence.Result, error) {
	var err error
	result := ycsbevidence.Call(func() error { err = client.Checkpoint(ctx); return err })
	return result, err
}
func finalRows(m manifest) []row {
	out := make([]row, len(m.Load))
	for i, r := range m.Load {
		f := map[string]value{}
		for k, v := range r.Fields {
			f[k] = v
		}
		out[i] = row{r.ID, f}
	}
	idx := map[string]int{}
	for i, r := range out {
		idx[r.ID] = i
	}
	for _, u := range m.Updates {
		out[idx[u.ID]].Fields[u.Field] = u.Value
	}
	return out
}
func indexOf(rows []row, id string) int {
	for i, r := range rows {
		if r.ID == id {
			return i
		}
	}
	return -1
}
func parallel(items []row, drivers []driver, fn func(driver, row) (bool, error)) ([]outcome, error) {
	results := make(chan outcome, len(items))
	var wg sync.WaitGroup
	for n, d := range drivers {
		wg.Add(1)
		go func(n int, d driver) {
			defer wg.Done()
			for i := n * 16; i < (n+1)*16; i++ {
				acked, e := fn(d, items[i])
				x := outcome{ID: items[i].ID, Acknowledged: acked, ReadbackOK: acked && e == nil}
				if e != nil {
					x.Error = e.Error()
				}
				results <- x
			}
		}(n, d)
	}
	wg.Wait()
	close(results)
	out := []outcome{}
	var err error
	for x := range results {
		out = append(out, x)
		if !x.Acknowledged || !x.ReadbackOK {
			err = errors.Join(err, errors.New(x.Error))
		}
	}
	return out, err
}
func parallelUpdates(items []update, drivers []driver, fn func(driver, update) (bool, error)) ([]outcome, error) {
	results := make(chan outcome, len(items))
	var wg sync.WaitGroup
	for n, d := range drivers {
		wg.Add(1)
		go func(n int, d driver) {
			defer wg.Done()
			for i := n * 32; i < (n+1)*32; i++ {
				u := items[i]
				acked, e := fn(d, u)
				x := outcome{ID: u.ID, Sequence: u.Sequence, Acknowledged: acked, ReadbackOK: acked && e == nil}
				if e != nil {
					x.Error = e.Error()
				}
				results <- x
				if e != nil {
					return
				}
			}
		}(n, d)
	}
	wg.Wait()
	close(results)
	out := []outcome{}
	var err error
	for x := range results {
		out = append(out, x)
		if !x.Acknowledged || !x.ReadbackOK {
			err = errors.Join(err, errors.New(x.Error))
		}
	}
	if len(out) != len(items) {
		err = errors.Join(err, errors.New("updates incomplete"))
	}
	return out, err
}

type nativeDriver struct {
	c      *nativewire.Client
	format string
}

func nativeOpen(ctx context.Context, addr, format string) (*nativeDriver, error) {
	conn, e := net.DialTimeout("tcp", addr, 5*time.Second)
	if e != nil {
		return nil, e
	}
	c := nativewire.NewClient(conn)
	if e = c.Hello(ctx); e != nil {
		_ = c.Close()
		return nil, e
	}
	return &nativeDriver{c, format}, nil
}
func (d *nativeDriver) insert(ctx context.Context, r row) error {
	doc := map[string]any{"_ycsb_key": r.ID}
	for k, v := range r.Fields {
		b, e := raw(v)
		if e != nil {
			return e
		}
		doc[k] = b
	}
	var data []byte
	var e error
	if d.format == "json" {
		data, e = json.Marshal(doc)
	} else {
		data, e = bson.Marshal(doc)
	}
	if e != nil {
		return e
	}
	return d.c.InsertBatchNoResult(ctx, collection, collections.DocumentFormat(d.format), [][]byte{[]byte(r.ID)}, [][]byte{data}, nativewire.AckVisible)
}
func (d *nativeDriver) update(ctx context.Context, u update) error {
	b, e := raw(u.Value)
	if e != nil {
		return e
	}
	if d.format == "bson" {
		typ, data, e := bson.MarshalValue(b)
		if e != nil {
			return e
		}
		matched, modified, e := d.c.UpdateBSONSet(ctx, collection, []byte(u.ID), []collections.BSONSetField{{Key: u.Field, Value: bson.RawValue{Type: typ, Value: data}}}, nativewire.AckVisible)
		if e != nil {
			return e
		}
		if matched != 1 || modified != 1 {
			return fmt.Errorf("update %s matched=%d modified=%d", u.ID, matched, modified)
		}
		return nil
	}
	docs, present, e := d.c.GetMany(ctx, collection, [][]byte{[]byte(u.ID)})
	if e != nil {
		return e
	}
	if len(present) != 1 || !present[0] {
		return fmt.Errorf("missing key %s", u.ID)
	}
	var doc map[string]any
	if e = json.Unmarshal(docs[0], &doc); e != nil {
		return e
	}
	doc[u.Field] = base64.StdEncoding.EncodeToString(b)
	data, e := json.Marshal(doc)
	if e != nil {
		return e
	}
	matched, modified, e := d.c.ReplaceBatch(ctx, collection, collections.DocumentFormatJSON, [][]byte{[]byte(u.ID)}, [][]byte{data}, nativewire.AckVisible)
	if e != nil {
		return e
	}
	if matched != 1 || modified != 1 {
		return fmt.Errorf("replace %s matched=%d modified=%d", u.ID, matched, modified)
	}
	return nil
}
func decodeJSON(id string, data []byte) (row, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return row{}, errors.New("JSON document must be an object")
	}
	doc := map[string]any{}
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return row{}, err
		}
		key, ok := token.(string)
		if !ok {
			return row{}, errors.New("invalid JSON field key")
		}
		if _, exists := doc[key]; exists {
			return row{}, fmt.Errorf("duplicate JSON field %s", key)
		}
		var v any
		if err = decoder.Decode(&v); err != nil {
			return row{}, err
		}
		doc[key] = v
	}
	if _, err = decoder.Token(); err != nil {
		return row{}, err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return row{}, errors.New("trailing JSON document data")
	}
	if doc["_ycsb_key"] != id {
		return row{}, errors.New("native ID mismatch")
	}
	delete(doc, "_ycsb_key")
	f := map[string]value{}
	for k, v := range doc {
		s, ok := v.(string)
		if !ok {
			return row{}, fmt.Errorf("field %s not string", k)
		}
		f[k] = value{Type: "string", Value: s}
	}
	return row{id, f}, nil
}
func decodeBSON(id, key string, data []byte) (row, error) {
	doc := bson.Raw(data)
	if e := doc.Validate(); e != nil {
		return row{}, e
	}
	keyValue, ok := doc.Lookup(key).StringValueOK()
	if !ok || keyValue != id {
		return row{}, errors.New("document ID mismatch")
	}
	els, e := doc.Elements()
	if e != nil {
		return row{}, e
	}
	f := map[string]value{}
	seen := map[string]bool{}
	for _, el := range els {
		if seen[el.Key()] {
			return row{}, fmt.Errorf("duplicate field %s", el.Key())
		}
		seen[el.Key()] = true
		if el.Key() == key {
			continue
		}
		subtype, data, ok := el.Value().BinaryOK()
		if !ok {
			return row{}, fmt.Errorf("field %s not binary", el.Key())
		}
		f[el.Key()] = value{Type: "binary", Subtype: &subtype, Base64: base64.StdEncoding.EncodeToString(data)}
	}
	return row{id, f}, nil
}
func (d *nativeDriver) scan(ctx context.Context) ([]row, error) {
	result, e := d.c.OpenScan(ctx, collection, nativewire.CursorLimits{MaxItems: 128})
	out := []row{}
	for {
		if e != nil {
			return nil, e
		}
		if result.Truncated || len(result.IDs) != len(result.Docs) {
			return nil, errors.New("truncated or malformed scan")
		}
		for i, id := range result.IDs {
			var r row
			if d.format == "json" {
				r, e = decodeJSON(string(id), result.Docs[i])
			} else {
				r, e = decodeBSON(string(id), "_ycsb_key", result.Docs[i])
			}
			if e != nil {
				return nil, e
			}
			out = append(out, r)
		}
		if !result.Cursor.HasMore {
			break
		}
		result, e = d.c.CursorNext(ctx, result.Cursor.CursorID, nativewire.CursorLimits{MaxItems: 128})
	}
	return out, nil
}
func (d *nativeDriver) get(ctx context.Context, id string) (row, error) {
	docs, present, err := d.c.GetMany(ctx, collection, [][]byte{[]byte(id)})
	if err != nil {
		return row{}, err
	}
	if len(present) != 1 || !present[0] {
		return row{}, fmt.Errorf("missing key %s", id)
	}
	if d.format == "json" {
		return decodeJSON(id, docs[0])
	}
	return decodeBSON(id, "_ycsb_key", docs[0])
}
func (d *nativeDriver) close(ctx context.Context) error { return d.c.Close() }

type mongoDriver struct {
	c    *mongo.Client
	coll *mongo.Collection
}

func mongoOpen(addr string) (*mongoDriver, error) {
	c, e := mongo.Connect(options.Client().ApplyURI("mongodb://" + addr))
	if e != nil {
		return nil, e
	}
	return &mongoDriver{c, c.Database(database).Collection(collection, options.Collection().SetWriteConcern(writeconcern.W1()))}, nil
}
func (d *mongoDriver) insert(ctx context.Context, r row) error {
	doc := bson.M{"_id": r.ID}
	for k, v := range r.Fields {
		b, e := raw(v)
		if e != nil {
			return e
		}
		doc[k] = b
	}
	_, e := d.coll.InsertOne(ctx, doc)
	return e
}
func (d *mongoDriver) update(ctx context.Context, u update) error {
	b, e := raw(u.Value)
	if e != nil {
		return e
	}
	r, e := d.coll.UpdateOne(ctx, bson.M{"_id": u.ID}, bson.M{"$set": bson.M{u.Field: b}})
	if e != nil {
		return e
	}
	if r.MatchedCount != 1 || r.ModifiedCount != 1 {
		return fmt.Errorf("update %s matched=%d modified=%d", u.ID, r.MatchedCount, r.ModifiedCount)
	}
	return nil
}
func (d *mongoDriver) scan(ctx context.Context) (out []row, retErr error) {
	cursor, e := d.coll.Find(ctx, bson.D{})
	if e != nil {
		return nil, e
	}
	defer func() { retErr = errors.Join(retErr, cursor.Close(ctx)) }()
	out = []row{}
	for cursor.Next(ctx) {
		var raw bson.Raw
		if e = cursor.Decode(&raw); e != nil {
			return nil, e
		}
		id, ok := raw.Lookup("_id").StringValueOK()
		if !ok {
			return nil, errors.New("missing string _id")
		}
		r, e := decodeBSON(id, "_id", raw)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, cursor.Err()
}
func (d *mongoDriver) get(ctx context.Context, id string) (row, error) {
	var doc bson.Raw
	if err := d.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc); err != nil {
		return row{}, err
	}
	return decodeBSON(id, "_id", doc)
}
func (d *mongoDriver) close(ctx context.Context) error { return d.c.Disconnect(ctx) }

func run() (retErr error) {
	target := flag.String("target", "", "native or mongo")
	addr := flag.String("addr", "", "server address")
	format := flag.String("format", "", "json or bson")
	profile := flag.String("profile", "", "storage profile")
	mode := flag.String("mode", "", "write, verify, snapshot, crash-write, or crash-verify")
	expected := flag.String("expected", "", "exclusive expected manifest on write, existing manifest on verify")
	observed := flag.String("observed", "", "exclusive updated/reopened snapshot")
	receipt := flag.String("receipt", "", "exclusive fixture client cleanup receipt")
	source := flag.String("source-id", "", "bound source identity")
	fixture := flag.String("fixture-id", "", "fresh fixture identity")
	stage := flag.String("stage", "", "random snapshot stage: loaded, preclose, or reopened")
	checkpoint := flag.Bool("checkpoint", false, "explicit checkpoint after random snapshot")
	ownerCheckpointDir := flag.String("owner-checkpoint-dir", "", "private opt-in gateway owner evidence directory; not a Mongo API")
	flag.Parse()
	crash := *mode == "crash-write" || *mode == "crash-verify"
	if *addr == "" || *source == "" || *fixture == "" || *expected == "" || *observed == "" || *receipt == "" || (*target != "native" && *target != "mongo") || (*format != "json" && *format != "bson") || (*mode != "write" && *mode != "verify" && *mode != "snapshot" && !crash) || (*profile != "command_wal_durable" && *profile != "command_wal_relaxed" && *profile != "no_wal_fast") {
		return errors.New("all flags and valid target/format/profile/mode are required")
	}
	if *target == "mongo" && *format == "json" {
		return errors.New("Mongo JSON storage with byte-valued fields is an unsupported fixture shape")
	}
	if *mode == "snapshot" {
		if *stage != "loaded" && *stage != "preclose" && *stage != "reopened" {
			return errors.New("invalid random snapshot stage")
		}
		if *checkpoint && *target != "native" && *ownerCheckpointDir == "" {
			return errors.New("no supported Mongo checkpoint API; refusing unproven checkpoint")
		}
		collection = "usertable"
		database = "ycsb"
	} else if *stage != "" || *checkpoint {
		return errors.New("random snapshot options cannot alter deterministic fixture")
	}
	if *ownerCheckpointDir != "" && (*target != "mongo" || (*mode != "write" && !(*mode == "snapshot" && *checkpoint && *stage == "preclose"))) {
		return errors.New("private checkpoint control only applies to Mongo write or preclose snapshot")
	}
	if crash {
		collection = "ycsb_crash_recovery"
		database = "ycsb_crash_recovery"
	}
	id := identity{1, *source, *fixture, *profile, *target, *format}
	var m manifest
	if *mode == "crash-write" {
		m = crashManifest(id)
		if e := durableJSON(*expected, m); e != nil {
			return e
		}
	} else if *mode == "write" {
		m = makeManifest(id)
		if e := writeJSON(*expected, m); e != nil {
			return e
		}
	} else if *mode == "verify" || *mode == "crash-verify" {
		b, e := os.ReadFile(*expected)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &m); e != nil {
			return e
		}
		want := makeManifest(id)
		if crash {
			want = crashManifest(id)
		}
		if m.identity != id || !reflect.DeepEqual(m, want) {
			return errors.New("expected manifest does not match deterministic fixture identity and intents")
		}
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var ds []driver
	checkpointAttempted, checkpointSucceeded := false, false
	var checkpointResult any
	var ownerCheckpoint *ycsbevidence.CheckpointReceipt
	privateCheckpoint := func() error {
		proof, err := ycsbevidence.RequestCheckpoint(ctx, *ownerCheckpointDir, *source, *fixture, *observed)
		ownerCheckpoint = &proof
		checkpointResult = proof.Result
		checkpointAttempted = proof.Checkpoint.Attempted
		checkpointSucceeded = err == nil && proof.Checkpoint.Success && proof.Result.Success
		return err
	}
	nativeCheckpoint := func() error {
		result, err := checkedCheckpoint(ctx, ds[0].(*nativeDriver).c)
		checkpointResult = result
		checkpointAttempted, checkpointSucceeded = result.Attempted, result.Success
		return err
	}
	defer func() {
		var cerr error
		for _, d := range ds {
			cerr = errors.Join(cerr, d.close(ctx))
		}
		retErr = errors.Join(retErr, cerr)
		status := ""
		if retErr != nil {
			status = retErr.Error()
		}
		kind := "deterministic_fixture_client_close"
		if *mode == "snapshot" {
			kind = "random_snapshot_client_close"
		}
		if crash {
			kind = "public_process_crash_client_close"
		}
		artifacts := map[string]string{}
		for _, path := range []string{*expected, *observed, *observed + ".load.json", *observed + ".acknowledged.jsonl"} {
			file, err := os.Open(path)
			if err != nil {
				continue
			}
			hash := sha256.New()
			_, err = io.Copy(hash, file)
			err = errors.Join(err, file.Close())
			if err != nil {
				retErr = errors.Join(retErr, err)
				status = retErr.Error()
				continue
			}
			artifacts[filepath.Base(path)] = hex.EncodeToString(hash.Sum(nil))
		}
		e := writeJSON(*receipt, map[string]any{"schema_version": 1, "artifacts_sha256": artifacts, "checkpoint": checkpointResult, "owner_checkpoint": ownerCheckpoint, "kind": kind, "fixture_id": *fixture, "source_id": *source, "mode": *mode, "pid": os.Getpid(), "started_utc": started.Format(time.RFC3339Nano), "ended_utc": time.Now().UTC().Format(time.RFC3339Nano), "checkpoint_attempted": checkpointAttempted, "checkpoint_succeeded": checkpointSucceeded, "close_error": func() string {
			if cerr != nil {
				return cerr.Error()
			}
			return ""
		}(), "result_error": status})
		retErr = errors.Join(retErr, e)
		if crash && e == nil {
			retErr = errors.Join(retErr, syncArtifact(*receipt))
		}
	}()
	clients := 4
	if *mode == "snapshot" || crash {
		clients = 1
	}
	for i := 0; i < clients; i++ {
		if *target == "native" {
			d, e := nativeOpen(ctx, *addr, *format)
			if e != nil {
				return e
			}
			ds = append(ds, d)
		} else {
			d, e := mongoOpen(*addr)
			if e != nil {
				return e
			}
			ds = append(ds, d)
		}
	}
	if crash {
		if *mode == "crash-write" {
			if *target == "native" {
				_, err := ds[0].(*nativeDriver).c.CreateCollection(ctx, collections.CollectionMeta{Name: collection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormat(*format)}})
				if err != nil {
					return err
				}
			}
			if err := crashWrites(ctx, ds[0], m, *observed+".acknowledged.jsonl"); err != nil {
				return err
			}
		}
		actual, err := crashScan(ctx, ds[0])
		if err != nil {
			return err
		}
		sort.Slice(actual, func(i, j int) bool { return actual[i].ID < actual[j].ID })
		if (*mode == "crash-write" && !sameRows(actual, m.Load)) || !crashRowsValid(m, actual) {
			return errors.New("public process crash state violates profile guarantee")
		}
		return durableJSON(*observed, snapshot{id, *mode, actual, []outcome{}})
	}
	if *mode == "snapshot" {
		actual, e := ds[0].scan(ctx)
		if e != nil {
			return e
		}
		sort.Slice(actual, func(i, j int) bool { return actual[i].ID < actual[j].ID })
		if e = writeJSON(*observed, snapshot{id, "random_" + *stage, actual, []outcome{}}); e != nil {
			return e
		}
		if *checkpoint {
			if *target == "mongo" {
				return privateCheckpoint()
			}
			return nativeCheckpoint()
		}
		return nil
	}

	if *mode == "verify" {
		actual, e := ds[0].scan(ctx)
		if e != nil {
			return e
		}
		if !sameRows(actual, finalRows(m)) {
			return errors.New("reopened exact state mismatch")
		}
		return writeJSON(*observed, snapshot{id, "reopened", actual, []outcome{}})
	}
	if *target == "native" {
		_, e := ds[0].(*nativeDriver).c.CreateCollection(ctx, collections.CollectionMeta{Name: collection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormat(*format)}})
		if e != nil {
			return e
		}
	}
	load, e := parallel(m.Load, ds, func(d driver, r row) (bool, error) {
		if err := d.insert(ctx, r); err != nil {
			return false, err
		}
		got, err := d.get(ctx, r.ID)
		if err != nil {
			return true, err
		}
		if !reflect.DeepEqual(got, r) {
			return true, fmt.Errorf("acknowledged insert readback mismatch %s", r.ID)
		}
		return true, nil
	})
	if resultErr := persistOutcomes(*observed+".load-operations.json", id, "load_operations", load, e); resultErr != nil {
		return resultErr
	}
	actual, e := ds[0].scan(ctx)
	if e != nil {
		return e
	}
	if !sameRows(actual, m.Load) {
		return errors.New("load exact state mismatch")
	}
	if e = writeJSON(*observed+".load.json", snapshot{id, "load", actual, load}); e != nil {
		return e
	}
	expectedByStep := map[string]row{}
	for _, initial := range m.Load {
		fields := map[string]value{}
		for k, v := range initial.Fields {
			fields[k] = v
		}
		for seq := 1; seq <= 2; seq++ {
			u := m.Updates[2*indexOf(m.Load, initial.ID)+seq-1]
			fields[u.Field] = u.Value
			copied := map[string]value{}
			for k, v := range fields {
				copied[k] = v
			}
			expectedByStep[fmt.Sprintf("%s/%d", initial.ID, seq)] = row{initial.ID, copied}
		}
	}
	updated, e := parallelUpdates(m.Updates, ds, func(d driver, u update) (bool, error) {
		if err := d.update(ctx, u); err != nil {
			return false, err
		}
		got, err := d.get(ctx, u.ID)
		if err != nil {
			return true, err
		}
		if !reflect.DeepEqual(got, expectedByStep[fmt.Sprintf("%s/%d", u.ID, u.Sequence)]) {
			return true, fmt.Errorf("acknowledged update readback mismatch %s/%d", u.ID, u.Sequence)
		}
		return true, nil
	})
	if resultErr := persistOutcomes(*observed+".update-operations.json", id, "update_operations", updated, e); resultErr != nil {
		return resultErr
	}
	actual, e = ds[0].scan(ctx)
	if e != nil {
		return e
	}
	if !sameRows(actual, finalRows(m)) {
		return errors.New("updated exact state mismatch")
	}
	if e = writeJSON(*observed, snapshot{id, "updated", actual, updated}); e != nil {
		return e
	}
	if *target == "native" {
		return nativeCheckpoint()
	} else if *ownerCheckpointDir != "" {
		return privateCheckpoint()
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
