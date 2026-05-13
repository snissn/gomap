package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
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
	if opts.Profile != treedb.ProfileDurable {
		t.Fatalf("profile=%q want %q", opts.Profile, treedb.ProfileDurable)
	}
	if opts.DefaultCollectionOptions.DocumentFormat != collections.DocumentFormatBSON {
		t.Fatalf("document format=%q want bson", opts.DefaultCollectionOptions.DocumentFormat)
	}
	if opts.DefaultCollectionOptions.DataRootStoragePolicy != collections.RootStorageDefault {
		t.Fatalf("data root storage=%q want default", opts.DefaultCollectionOptions.DataRootStoragePolicy)
	}

	normalized, err := NormalizeStandaloneOptions(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.Profile(" WAL_ON_FAST "),
		DefaultCollectionOptions: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormat(" BSON "),
		},
	})
	if err != nil {
		t.Fatalf("NormalizeStandaloneOptions normalized strings: %v", err)
	}
	if normalized.Profile != treedb.ProfileWALOnFast {
		t.Fatalf("normalized profile=%q want %q", normalized.Profile, treedb.ProfileWALOnFast)
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
			want: "unsupported TreeDB profile",
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
}

func TestOpenStandaloneServerAppliesExplicitZeroUpdateCoalescingBatch(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{
		Dir:                         t.TempDir(),
		UpdateCoalescingMaxBatchSet: true,
		UpdateCoalescingMaxBatch:    0,
	})
	if err != nil {
		t.Fatalf("OpenStandaloneServer: %v", err)
	}
	defer func() { _ = standalone.Close() }()

	if standalone.Server.UpdateCoalescingMaxBatch != 0 {
		t.Fatalf("UpdateCoalescingMaxBatch=%d want explicit zero", standalone.Server.UpdateCoalescingMaxBatch)
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
