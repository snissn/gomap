//go:build ignore

// A separately labeled exact-value control; it is not the timed YCSB workload.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const collection = "ycsb_correctness"

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
func sameRows(a, b []row) bool {
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	sort.Slice(b, func(i, j int) bool { return b[i].ID < b[j].ID })
	return reflect.DeepEqual(a, b)
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
	var doc map[string]any
	if e := json.Unmarshal(data, &doc); e != nil {
		return row{}, e
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
	return &mongoDriver{c, c.Database("ycsb_correctness").Collection(collection, options.Collection().SetWriteConcern(writeconcern.W1()))}, nil
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
	mode := flag.String("mode", "", "write or verify")
	expected := flag.String("expected", "", "exclusive expected manifest on write, existing manifest on verify")
	observed := flag.String("observed", "", "exclusive updated/reopened snapshot")
	receipt := flag.String("receipt", "", "exclusive fixture client cleanup receipt")
	source := flag.String("source-id", "", "bound source identity")
	fixture := flag.String("fixture-id", "", "fresh fixture identity")
	flag.Parse()
	if *addr == "" || *source == "" || *fixture == "" || *expected == "" || *observed == "" || *receipt == "" || (*target != "native" && *target != "mongo") || (*format != "json" && *format != "bson") || (*mode != "write" && *mode != "verify") || (*profile != "command_wal_durable" && *profile != "command_wal_relaxed" && *profile != "no_wal_fast") {
		return errors.New("all flags and valid target/format/profile/mode are required")
	}
	if *target == "mongo" && *format == "json" {
		return errors.New("Mongo JSON storage with byte-valued fields is an unsupported fixture shape")
	}
	id := identity{1, *source, *fixture, *profile, *target, *format}
	var m manifest
	if *mode == "write" {
		m = makeManifest(id)
		if e := writeJSON(*expected, m); e != nil {
			return e
		}
	} else {
		b, e := os.ReadFile(*expected)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &m); e != nil {
			return e
		}
		if m.identity != id || !reflect.DeepEqual(m, makeManifest(id)) {
			return errors.New("expected manifest does not match deterministic fixture identity and intents")
		}
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var ds []driver
	checkpointAttempted, checkpointSucceeded := false, false
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
		e := writeJSON(*receipt, map[string]any{"schema_version": 1, "kind": "deterministic_fixture_client_close", "fixture_id": *fixture, "source_id": *source, "mode": *mode, "pid": os.Getpid(), "started_utc": started.Format(time.RFC3339Nano), "ended_utc": time.Now().UTC().Format(time.RFC3339Nano), "checkpoint_attempted": checkpointAttempted, "checkpoint_succeeded": checkpointSucceeded, "close_error": func() string {
			if cerr != nil {
				return cerr.Error()
			}
			return ""
		}(), "result_error": status})
		retErr = errors.Join(retErr, e)
	}()
	for i := 0; i < 4; i++ {
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
	if e != nil {
		return e
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
	if e != nil {
		return e
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
		checkpointAttempted = true
		if e = ds[0].(*nativeDriver).c.Checkpoint(ctx); e != nil {
			return e
		}
		checkpointSucceeded = true
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
