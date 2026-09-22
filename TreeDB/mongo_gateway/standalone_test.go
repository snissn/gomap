package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const standaloneShutdownTimeout = 5 * time.Second

func TestNormalizeStandaloneOptionsDefaultsAndValidation(t *testing.T) {
	opts, err := NormalizeStandaloneOptions(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NormalizeStandaloneOptions defaults: %v", err)
	}
	if opts.Profile != treedb.ProfileCommandWALDurable {
		t.Fatalf("profile=%q want %q", opts.Profile, treedb.ProfileCommandWALDurable)
	}
	if opts.DefaultCollectionOptions.DocumentFormat != collections.DocumentFormatBSON {
		t.Fatalf("document format=%q want bson", opts.DefaultCollectionOptions.DocumentFormat)
	}
	if opts.DefaultCollectionOptions.DataRootStoragePolicy != collections.RootStorageDefault {
		t.Fatalf("data root storage=%q want default", opts.DefaultCollectionOptions.DataRootStoragePolicy)
	}

	normalized, err := NormalizeStandaloneOptions(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALRelaxed,
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormat(" BSON "),
		},
	})
	if err != nil {
		t.Fatalf("NormalizeStandaloneOptions normalized strings: %v", err)
	}
	if normalized.Profile != treedb.ProfileCommandWALRelaxed {
		t.Fatalf("normalized profile=%q want %q", normalized.Profile, treedb.ProfileCommandWALRelaxed)
	}
	commandWAL, err := NormalizeStandaloneOptions(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALDurable,
	})
	if err != nil {
		t.Fatalf("NormalizeStandaloneOptions command WAL profile: %v", err)
	}
	if commandWAL.Profile != treedb.ProfileCommandWALDurable {
		t.Fatalf("command WAL profile=%q want %q", commandWAL.Profile, treedb.ProfileCommandWALDurable)
	}
	if normalized.DefaultCollectionOptions.DocumentFormat != collections.DocumentFormatBSON {
		t.Fatalf("normalized document format=%q want bson", normalized.DefaultCollectionOptions.DocumentFormat)
	}
	if got, err := normalizeStandaloneRootStoragePolicy(collections.RootStoragePolicy(" FAST ")); err != nil || got != collections.RootStorageFast {
		t.Fatalf("normalized root storage=%q err=%v want fast", got, err)
	}

	cases := []struct {
		name string
		opts StandaloneOptions
		want string
	}{
		{
			name: "missing dir",
			opts: StandaloneOptions{},
			want: "Dir is required",
		},
		{
			name: "bad profile",
			opts: StandaloneOptions{Dir: t.TempDir(), Profile: treedb.Profile("unsafe")},
			want: "allowed: " + treedb.ProfileFlagHelp,
		},
		{
			name: "deprecated profile",
			opts: StandaloneOptions{Dir: t.TempDir(), Profile: treedb.Profile("WAL_ON_FAST")},
			want: "allowed: " + treedb.ProfileFlagHelp,
		},
		{
			name: "bad document format",
			opts: StandaloneOptions{Dir: t.TempDir(), DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormat("yaml")}},
			want: "unsupported collection document format",
		},
		{
			name: "bad data root storage",
			opts: StandaloneOptions{Dir: t.TempDir(), DefaultCollectionOptions: collections.CollectionOptions{DataRootStoragePolicy: collections.RootStoragePolicy("archive")}},
			want: "data root storage policy",
		},
		{
			name: "bad index root storage",
			opts: StandaloneOptions{Dir: t.TempDir(), DefaultIndexStoragePolicy: collections.RootStoragePolicy("archive")},
			want: "index root storage policy",
		},
		{
			name: "bad max message length",
			opts: StandaloneOptions{Dir: t.TempDir(), MaxMessageLength: -1},
			want: "MaxMessageLength must be >= 0",
		},
		{
			name: "bad max find scan documents",
			opts: StandaloneOptions{Dir: t.TempDir(), MaxFindScanDocuments: -1},
			want: "MaxFindScanDocuments must be >= 0",
		},
		{
			name: "bad max cursor retained bytes",
			opts: StandaloneOptions{Dir: t.TempDir(), MaxCursorRetainedBytes: -1},
			want: "MaxCursorRetainedBytes must be >= 0",
		},
		{
			name: "bad max open cursors",
			opts: StandaloneOptions{Dir: t.TempDir(), MaxOpenCursors: -1},
			want: "MaxOpenCursors must be >= 0",
		},
		{
			name: "bad update coalescing max batch",
			opts: StandaloneOptions{Dir: t.TempDir(), UpdateCoalescingMaxBatch: -1},
			want: "UpdateCoalescingMaxBatch must be >= 0",
		},
		{
			name: "bad insert coalescing max batch",
			opts: StandaloneOptions{Dir: t.TempDir(), InsertCoalescingMaxBatch: -1},
			want: "InsertCoalescingMaxBatch must be >= 0",
		},
		{
			name: "cluster submitter missing catalog version",
			opts: StandaloneOptions{Dir: t.TempDir(), ClusterSubmitter: &mongoClusterFakeSubmitter{}},
			want: "ClusterCatalogVersion is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeStandaloneOptions(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestServerServeContextCancelDoesNotCloseOtherServeConnections(t *testing.T) {
	server := NewServer()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = ln1.Close()
		t.Fatalf("listen 2: %v", err)
	}
	serveErr1 := make(chan error, 1)
	serveErr2 := make(chan error, 1)
	go func() {
		serveErr1 <- server.Serve(ctx1, ln1)
	}()
	go func() {
		serveErr2 <- server.Serve(ctx2, ln2)
	}()

	conn1, err := net.Dial("tcp", ln1.Addr().String())
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	defer func() { _ = conn1.Close() }()
	conn2, err := net.Dial("tcp", ln2.Addr().String())
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer func() { _ = conn2.Close() }()
	servePing := func(conn net.Conn, requestID int32) {
		t.Helper()
		ping := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}})
		req, err := wire.AppendMsgMessage(nil, requestID, 0, 0, ping)
		if err != nil {
			t.Fatalf("AppendMsgMessage: %v", err)
		}
		if _, err := conn.Write(req); err != nil {
			t.Fatalf("write ping: %v", err)
		}
		assertOK(t, readMsgResponse(t, readOneMessageBytes(t, conn), requestID))
	}
	servePing(conn1, 7201)
	servePing(conn2, 7202)

	cancel1()
	select {
	case err := <-serveErr1:
		if err != nil {
			t.Fatalf("Serve 1 returned error after cancel: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		t.Fatal("Serve 1 did not stop after context cancellation")
	}
	servePing(conn2, 7203)

	cancel2()
	select {
	case err := <-serveErr2:
		if err != nil {
			t.Fatalf("Serve 2 returned error after cancel: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		t.Fatal("Serve 2 did not stop after context cancellation")
	}
}

func TestServerServeClosesListenerOnAcceptError(t *testing.T) {
	wantErr := errors.New("accept failed")
	ln := &acceptErrorListener{err: wantErr}

	if err := NewServer().Serve(context.Background(), ln); !errors.Is(err, wantErr) {
		t.Fatalf("Serve err=%v want %v", err, wantErr)
	}
	if !ln.closed.Load() {
		t.Fatal("Serve did not close listener after accept error")
	}
}

type acceptErrorListener struct {
	err    error
	closed atomic.Bool
}

func (l *acceptErrorListener) Accept() (net.Conn, error) {
	return nil, l.err
}

func (l *acceptErrorListener) Close() error {
	l.closed.Store(true)
	return nil
}

func (l *acceptErrorListener) Addr() net.Addr {
	return dummyAddr("accept-error-listener")
}

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func readOneMessageBytes(tb testing.TB, conn net.Conn) []byte {
	tb.Helper()
	h, body, err := wire.ReadMessage(conn, 0)
	if err != nil {
		tb.Fatalf("ReadMessage: %v", err)
	}
	msg, err := wire.AppendMessage(nil, h.RequestID, h.ResponseTo, h.OpCode, body)
	if err != nil {
		tb.Fatalf("AppendMessage: %v", err)
	}
	return msg
}

func TestStandaloneServerServeNilClosesListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var standalone *StandaloneServer
	if err := standalone.Serve(context.Background(), ln); !errors.Is(err, errServerClosed) {
		t.Fatalf("Serve err=%v want errServerClosed", err)
	}
	if err := ln.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener was not closed by nil StandaloneServer.Serve; second close err=%v", err)
	}
}

func TestServerServeAcceptsWireClientsAndStopsOnContextCancel(t *testing.T) {
	server := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(ctx, ln)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ping := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}})
	req, err := wire.AppendMsgMessage(nil, 7101, 0, 0, ping)
	if err != nil {
		cancel()
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		cancel()
		t.Fatalf("write ping: %v", err)
	}
	h, body, err := wire.ReadMessage(conn, 0)
	if err != nil {
		cancel()
		t.Fatalf("read ping response: %v", err)
	}
	if h.OpCode != wire.OpMsg || h.ResponseTo != 7101 {
		cancel()
		t.Fatalf("response header=%+v", h)
	}
	msg, err := wire.ParseMsg(body)
	if err != nil {
		cancel()
		t.Fatalf("ParseMsg: %v", err)
	}
	assertOK(t, msg.Body)

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		_ = ln.Close()
		t.Fatal("Serve did not stop after context cancellation")
	}
}

func TestServerCloseStopsServe(t *testing.T) {
	server := NewServer()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(context.Background(), ln)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = server.Close()
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ping := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}})
	req, err := wire.AppendMsgMessage(nil, 7102, 0, 0, ping)
	if err != nil {
		_ = server.Close()
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		_ = server.Close()
		t.Fatalf("write ping: %v", err)
	}
	if _, _, err := wire.ReadMessage(conn, 0); err != nil {
		_ = server.Close()
		t.Fatalf("read ping response: %v", err)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve returned error after Close: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		_ = ln.Close()
		t.Fatal("Serve did not stop after Server.Close")
	}
}

func TestServerListenAndServeRejectsInvalidAddress(t *testing.T) {
	err := NewServer().ListenAndServe(context.Background(), "127.0.0.1:bad-port")
	if err == nil {
		t.Fatal("ListenAndServe returned nil for invalid address")
	}
}

func TestServerListenAndServeClosedDoesNotBind(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	var nilServer *Server
	if err := nilServer.ListenAndServe(context.Background(), ln.Addr().String()); !errors.Is(err, errServerClosed) {
		t.Fatalf("nil ListenAndServe err=%v want errServerClosed", err)
	}

	server := NewServer()
	if err := server.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}
	if err := server.ListenAndServe(context.Background(), ln.Addr().String()); !errors.Is(err, errServerClosed) {
		t.Fatalf("closed ListenAndServe err=%v want errServerClosed", err)
	}
}

func TestStandaloneServerListenAndServeClosedDoesNotBind(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	var nilStandalone *StandaloneServer
	if err := nilStandalone.ListenAndServe(context.Background(), ln.Addr().String()); !errors.Is(err, errServerClosed) {
		t.Fatalf("nil ListenAndServe err=%v want errServerClosed", err)
	}

	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("standalone close: %v", err)
	}
	if err := standalone.ListenAndServe(context.Background(), ln.Addr().String()); !errors.Is(err, errServerClosed) {
		t.Fatalf("closed ListenAndServe err=%v want errServerClosed", err)
	}
}

func TestStandaloneServerCloseWaitsForServe(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = standalone.Close()
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- standalone.Serve(context.Background(), ln)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = standalone.Close()
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ping := mustDocument(t, bson.D{{Key: "ping", Value: int32(1)}, {Key: "$db", Value: "admin"}})
	req, err := wire.AppendMsgMessage(nil, 7103, 0, 0, ping)
	if err != nil {
		_ = standalone.Close()
		t.Fatalf("AppendMsgMessage: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		_ = standalone.Close()
		t.Fatalf("write ping: %v", err)
	}
	if _, _, err := wire.ReadMessage(conn, 0); err != nil {
		_ = standalone.Close()
		t.Fatalf("read ping response: %v", err)
	}

	if err := standalone.Close(); err != nil {
		t.Fatalf("standalone close: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve returned error after Close: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		t.Fatal("StandaloneServer.Close returned before Serve finished")
	}
}

func TestStandaloneServerOfficialDriverCRUDAndReopen(t *testing.T) {
	dir := t.TempDir()
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: dir})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = standalone.Close()
		t.Fatalf("listen: %v", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- standalone.Serve(ctx, ln)
	}()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + ln.Addr().String()).
		SetDirect(true).
		SetServerSelectionTimeout(time.Second))
	if err != nil {
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("mongo connect: %v", err)
	}

	opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer opCancel()
	if err := client.Ping(opCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("driver ping: %v", err)
	}

	coll := client.Database("app").Collection("users")
	if _, err := coll.InsertOne(opCtx, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "name", Value: "ada"},
		{Key: "age", Value: int64(37)},
	}); err != nil {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("driver insert one: %v", err)
	}

	var got bson.M
	if err := coll.FindOne(opCtx, bson.D{{Key: "_id", Value: "u1"}}).Decode(&got); err != nil {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("driver find one: %v", err)
	}
	if got["name"] != "ada" || got["age"] != int64(37) {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("decoded doc=%v want name=ada age=37", got)
	}

	gatewayCollection, err := standalone.Collections.OpenCollection("app.users")
	if err != nil {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("open TreeDB collection: %v", err)
	}
	if format := gatewayCollection.Meta().Options.DocumentFormat; format != collections.DocumentFormatBSON {
		_ = client.Disconnect(context.Background())
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("collection format=%q want bson", format)
	}

	if err := client.Disconnect(context.Background()); err != nil {
		cancel()
		_ = ln.Close()
		_ = standalone.Close()
		t.Fatalf("disconnect: %v", err)
	}
	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			_ = standalone.Close()
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(standaloneShutdownTimeout):
		_ = standalone.Close()
		t.Fatal("Serve did not stop")
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("standalone close: %v", err)
	}

	reopened, err := OpenStandaloneServer(StandaloneOptions{Dir: dir})
	if err != nil {
		t.Fatalf("reopen standalone: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := reopened.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	key, _, err := prepareInsertDocument(mustDocument(t, bson.D{{Key: "_id", Value: "u1"}}), collections.DocumentFormatBSON)
	if err != nil {
		t.Fatalf("prepare key: %v", err)
	}
	stored, err := reopenedCollection.Get(key)
	if err != nil {
		t.Fatalf("get stored doc after reopen: %v", err)
	}
	raw := bson.Raw(stored)
	if err := raw.Validate(); err != nil {
		t.Fatalf("stored BSON validation: %v", err)
	}
	name, ok := raw.Lookup("name").StringValueOK()
	if !ok || name != "ada" {
		t.Fatalf("stored name=%q ok=%v want ada", name, ok)
	}
}

func TestStandaloneServerCommandWALBSONInsertReadUpdateVisibility(t *testing.T) {
	profiles := []treedb.Profile{
		treedb.ProfileCommandWALRelaxed,
		treedb.ProfileCommandWALDurable,
	}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{
				Dir:     t.TempDir(),
				Profile: profile,
				DefaultCollectionOptions: collections.CollectionOptions{
					DocumentFormat: collections.DocumentFormatBSON,
				},
			})
			if err != nil {
				t.Fatalf("OpenStandaloneServer: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				_ = standalone.Close()
				t.Fatalf("listen: %v", err)
			}
			serveErr := make(chan error, 1)
			go func() {
				serveErr <- standalone.Serve(ctx, ln)
			}()

			client, err := mongo.Connect(options.Client().
				ApplyURI("mongodb://" + ln.Addr().String()).
				SetDirect(true).
				SetServerSelectionTimeout(time.Second))
			if err != nil {
				cancel()
				_ = ln.Close()
				_ = standalone.Close()
				t.Fatalf("mongo connect: %v", err)
			}

			const requestTimeout = 10 * time.Second
			scenarioStarted := time.Now()
			pingStarted := time.Now()
			pingCtx, pingCancel := context.WithTimeout(context.Background(), requestTimeout)
			err = client.Ping(pingCtx, nil)
			pingElapsed := time.Since(pingStarted)
			pingCancel()
			if err != nil {
				_ = client.Disconnect(context.Background())
				cancel()
				_ = ln.Close()
				_ = standalone.Close()
				t.Fatalf("driver ping after %s (request limit %s): %v", pingElapsed, requestTimeout, err)
			}

			coll := client.Database("ycsb").Collection("usertable")
			const docs = 512
			const workers = 16
			ycsbValues := func(i int) bson.D {
				values := make(bson.D, 0, 11)
				values = append(values, bson.E{Key: "_id", Value: "user" + strconv.Itoa(i)})
				for field := 0; field < 10; field++ {
					values = append(values, bson.E{Key: "field" + strconv.Itoa(field), Value: []byte("value" + strconv.Itoa(i) + "-" + strconv.Itoa(field))})
				}
				return values
			}
			ycsbUpdate := func(i int) bson.D {
				values := make(bson.D, 0, 10)
				for field := 0; field < 10; field++ {
					values = append(values, bson.E{Key: "field" + strconv.Itoa(field), Value: []byte("updated" + strconv.Itoa(i) + "-" + strconv.Itoa(field))})
				}
				return values
			}
			projection := bson.D{{Key: "_id", Value: false}}
			for field := 0; field < 10; field++ {
				projection = append(projection, bson.E{Key: "field" + strconv.Itoa(field), Value: true})
			}

			type insertError struct {
				index   int
				elapsed time.Duration
				err     error
			}
			errCh := make(chan insertError, 1)
			insertTimingCh := make(chan time.Duration, docs)
			workCh := make(chan int)
			insertCtx, insertCancel := context.WithCancel(context.Background())
			var insertWG sync.WaitGroup
			sendInsertErr := func(i int, elapsed time.Duration, err error) {
				if err == nil {
					return
				}
				select {
				case errCh <- insertError{index: i, elapsed: elapsed, err: err}:
				default:
				}
				insertCancel()
			}
			insertStarted := time.Now()
			for worker := 0; worker < workers; worker++ {
				insertWG.Add(1)
				go func() {
					defer insertWG.Done()
					for {
						select {
						case <-insertCtx.Done():
							return
						case i, ok := <-workCh:
							if !ok {
								return
							}
							requestStarted := time.Now()
							requestCtx, requestCancel := context.WithTimeout(insertCtx, requestTimeout)
							_, err := coll.InsertOne(requestCtx, ycsbValues(i))
							requestElapsed := time.Since(requestStarted)
							requestCancel()
							insertTimingCh <- requestElapsed
							sendInsertErr(i, requestElapsed, err)
						}
					}
				}()
			}
			go func() {
				defer close(workCh)
				for i := 0; i < docs; i++ {
					select {
					case <-insertCtx.Done():
						return
					case workCh <- i:
					}
				}
			}()
			insertDone := make(chan struct{})
			go func() {
				insertWG.Wait()
				close(insertTimingCh)
				close(insertDone)
			}()
			select {
			case insertErr := <-errCh:
				insertCancel()
				<-insertDone
				_ = client.Disconnect(context.Background())
				cancel()
				_ = ln.Close()
				_ = standalone.Close()
				t.Fatalf("insert %d after %s (request limit %s): %v", insertErr.index, insertErr.elapsed, requestTimeout, insertErr.err)
			case <-insertDone:
				select {
				case insertErr := <-errCh:
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("insert %d after %s (request limit %s): %v", insertErr.index, insertErr.elapsed, requestTimeout, insertErr.err)
				default:
				}
			}
			insertCancel()
			insertElapsed := time.Since(insertStarted)
			var insertMax time.Duration
			for elapsed := range insertTimingCh {
				if elapsed > insertMax {
					insertMax = elapsed
				}
			}

			var initialReadElapsed time.Duration
			var initialReadMax time.Duration
			var updateElapsed time.Duration
			var updateMax time.Duration
			var finalReadElapsed time.Duration
			var finalReadMax time.Duration
			for i := 0; i < docs; i++ {
				id := "user" + strconv.Itoa(i)
				var got map[string][]byte
				requestStarted := time.Now()
				requestCtx, requestCancel := context.WithTimeout(context.Background(), requestTimeout)
				err := coll.FindOne(requestCtx, bson.D{{Key: "_id", Value: id}}, options.FindOne().SetProjection(projection)).Decode(&got)
				requestElapsed := time.Since(requestStarted)
				requestCancel()
				initialReadElapsed += requestElapsed
				if requestElapsed > initialReadMax {
					initialReadMax = requestElapsed
				}
				if err != nil {
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("find after insert %s after %s (request limit %s): %v", id, requestElapsed, requestTimeout, err)
				}
				requestStarted = time.Now()
				requestCtx, requestCancel = context.WithTimeout(context.Background(), requestTimeout)
				result, err := coll.UpdateOne(requestCtx,
					bson.D{{Key: "_id", Value: id}},
					bson.D{{Key: "$set", Value: ycsbUpdate(i)}},
				)
				requestElapsed = time.Since(requestStarted)
				requestCancel()
				updateElapsed += requestElapsed
				if requestElapsed > updateMax {
					updateMax = requestElapsed
				}
				if err != nil {
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("update %s after %s (request limit %s): %v", id, requestElapsed, requestTimeout, err)
				}
				if result.MatchedCount != 1 || result.ModifiedCount != 1 {
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("update %s matched=%d modified=%d, want 1/1", id, result.MatchedCount, result.ModifiedCount)
				}
				got = nil
				requestStarted = time.Now()
				requestCtx, requestCancel = context.WithTimeout(context.Background(), requestTimeout)
				err = coll.FindOne(requestCtx, bson.D{{Key: "_id", Value: id}}, options.FindOne().SetProjection(projection)).Decode(&got)
				requestElapsed = time.Since(requestStarted)
				requestCancel()
				finalReadElapsed += requestElapsed
				if requestElapsed > finalReadMax {
					finalReadMax = requestElapsed
				}
				if err != nil {
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("find after update %s after %s (request limit %s): %v", id, requestElapsed, requestTimeout, err)
				}
				if string(got["field0"]) != "updated"+strconv.Itoa(i)+"-0" {
					_ = client.Disconnect(context.Background())
					cancel()
					_ = ln.Close()
					_ = standalone.Close()
					t.Fatalf("find after update %s field0=%q want updated", id, got["field0"])
				}
			}
			t.Logf("command-WAL BSON visibility profile=%s docs=%d workers=%d request_limit=%s ping=%s insert_wall=%s insert_max=%s initial_read_total=%s initial_read_max=%s update_total=%s update_max=%s final_read_total=%s final_read_max=%s scenario_wall=%s",
				profile, docs, workers, requestTimeout, pingElapsed, insertElapsed, insertMax, initialReadElapsed, initialReadMax, updateElapsed, updateMax, finalReadElapsed, finalReadMax, time.Since(scenarioStarted))

			if err := client.Disconnect(context.Background()); err != nil {
				cancel()
				_ = ln.Close()
				_ = standalone.Close()
				t.Fatalf("disconnect: %v", err)
			}
			cancel()
			_ = ln.Close()
			select {
			case err := <-serveErr:
				if err != nil {
					_ = standalone.Close()
					t.Fatalf("Serve returned error: %v", err)
				}
			case <-time.After(standaloneShutdownTimeout):
				_ = standalone.Close()
				t.Fatal("Serve did not stop")
			}
			if err := standalone.Close(); err != nil {
				t.Fatalf("standalone close: %v", err)
			}
		})
	}
}

func TestStandaloneServerCloseIsIdempotent(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOpenStandaloneServerPreservesDefaultUpdateCoalescingBatch(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() { _ = standalone.Close() }()

	if standalone.Server.UpdateCoalescingMaxBatch != defaultUpdateCoalescingBatch {
		t.Fatalf("UpdateCoalescingMaxBatch=%d want default %d", standalone.Server.UpdateCoalescingMaxBatch, defaultUpdateCoalescingBatch)
	}
	if standalone.Server.InsertCoalescingMaxBatch != defaultInsertCoalescingBatch {
		t.Fatalf("InsertCoalescingMaxBatch=%d want default %d", standalone.Server.InsertCoalescingMaxBatch, defaultInsertCoalescingBatch)
	}
}

func TestOpenStandaloneServerAppliesExplicitZeroUpdateCoalescingBatch(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:                         t.TempDir(),
		UpdateCoalescingMaxBatchSet: true,
		UpdateCoalescingMaxBatch:    0,
		InsertCoalescingMaxBatchSet: true,
		InsertCoalescingMaxBatch:    0,
	})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() { _ = standalone.Close() }()

	if standalone.Server.UpdateCoalescingMaxBatch != 0 {
		t.Fatalf("UpdateCoalescingMaxBatch=%d want explicit zero", standalone.Server.UpdateCoalescingMaxBatch)
	}
	if standalone.Server.InsertCoalescingMaxBatch != 0 {
		t.Fatalf("InsertCoalescingMaxBatch=%d want explicit zero", standalone.Server.InsertCoalescingMaxBatch)
	}
}

func TestStandaloneServerGoRunHelp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}

	cases := []struct {
		name string
		dir  string
		args []string
	}{
		{
			name: "package dir",
			args: []string{"run", "./server.go", "-help"},
		},
		{
			name: "short help",
			args: []string{"run", "./server.go", "-h"},
		},
		{
			name: "repo root documented path",
			dir:  "../..",
			args: []string{"run", "./TreeDB/mongo_gateway/server.go", "-help"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("go", tc.args...)
			cmd.Dir = tc.dir
			cmd.Env = append(os.Environ(), "GOWORK=off")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("go %s: %v\n%s", strings.Join(tc.args, " "), err, out)
			}
			if !bytes.Contains(out, []byte("TreeDB root directory")) || !bytes.Contains(out, []byte("-document-format")) {
				t.Fatalf("help output missing expected flags:\n%s", out)
			}
		})
	}
}

func TestStandaloneServerGoRunDefaultDirUsesMongoGatewayDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}
	cmd := exec.Command("go", "run", "./server.go", "-help")
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"MONGO_GATEWAY_DIR=/tmp/mongo-gateway-env",
		"TREEDB_MONGO_GATEWAY_DIR=/tmp/legacy-treedb-mongo-gateway-env",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go run ./server.go -help: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte(`/tmp/mongo-gateway-env`)) {
		t.Fatalf("help output missing MONGO_GATEWAY_DIR default:\n%s", out)
	}
}

func TestStandaloneServerGoRunMissingDirError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}
	cmd := exec.Command("go", "run", "./server.go", "-dir", "")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("go run ./server.go -dir \"\" succeeded unexpectedly:\n%s", out)
	}
	if !bytes.Contains(out, []byte("TreeDB root directory -dir is required")) {
		t.Fatalf("missing-dir output missing context:\n%s", out)
	}
	if got := bytes.Count(out, []byte("mongo gateway server:")); got != 1 {
		t.Fatalf("missing-dir output has %d top-level prefixes, want 1:\n%s", got, out)
	}
}

func TestStandaloneServerGoRunRejectsNegativeIntegerLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}
	cmd := exec.Command("go", "run", "./server.go", "-max-open-cursors", "-1")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("go run ./server.go -max-open-cursors -1 succeeded unexpectedly:\n%s", out)
	}
	if !bytes.Contains(out, []byte("-max-open-cursors must be >= 0")) {
		t.Fatalf("negative limit output missing context:\n%s", out)
	}
	if got := bytes.Count(out, []byte("mongo gateway server:")); got != 1 {
		t.Fatalf("negative limit output has %d top-level prefixes, want 1:\n%s", got, out)
	}
}

func TestStandaloneServerGoRunListenErrorDoesNotCreateDir(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	dir := filepath.Join(t.TempDir(), "db")
	cmd := exec.Command("go", "run", "./server.go", "-addr", ln.Addr().String(), "-dir", dir)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("go run ./server.go with occupied addr succeeded unexpectedly:\n%s", out)
	}
	if !bytes.Contains(out, []byte("mongo gateway server: listen on")) {
		t.Fatalf("listen failure output missing context:\n%s", out)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("TreeDB dir stat err=%v want not-exist", err)
	}
}

func TestStandaloneServerGoRunUnknownFlagErrorPrintedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping go run smoke test in short mode")
	}
	cmd := exec.Command("go", "run", "./server.go", "-unknown")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("go run ./server.go -unknown succeeded unexpectedly:\n%s", out)
	}
	if got := bytes.Count(out, []byte("flag provided but not defined: -unknown")); got != 1 {
		t.Fatalf("unknown flag output has %d parse errors, want 1:\n%s", got, out)
	}
	if got := bytes.Count(out, []byte("mongo gateway server:")); got != 1 {
		t.Fatalf("unknown flag output has %d top-level prefixes, want 1:\n%s", got, out)
	}
}

func TestStandaloneServerServeNilReturnsClosed(t *testing.T) {
	var standalone *StandaloneServer
	if err := standalone.Serve(context.Background(), nil); !errors.Is(err, errServerClosed) {
		t.Fatalf("nil standalone Serve err=%v want errServerClosed", err)
	}
}

func TestStandaloneServerServeNilReturnsValidationError(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = standalone.Close() }()
	if err := standalone.Serve(context.Background(), nil); err == nil {
		t.Fatal("Serve(nil) succeeded")
	}
}
