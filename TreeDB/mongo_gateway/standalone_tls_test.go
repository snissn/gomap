package mongogateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestStandaloneServerRejectsPlaintextNonLoopbackListener(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Serve(context.Background(), ln); err == nil {
		t.Fatal("plaintext wildcard listener unexpectedly accepted")
	}
	if err := ln.Close(); err == nil {
		t.Fatal("rejected listener was not closed")
	}
}

func TestStandaloneServerInsecureRemoteOverrideStatus(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AllowInsecureRemote: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := standalone.ValidateListener(ln); err != nil {
		t.Fatal(err)
	}
	status := standalone.TransportStatus()
	if status.Mode != "plaintext-insecure-remote" || !status.InsecureRemoteOverride {
		t.Fatalf("status=%+v", status)
	}
}

func TestStandaloneServerRejectsPlaintextRemotePasswordAuthentication(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AuthenticationEnabled: true, AllowInsecureRemote: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Serve(context.Background(), ln); err == nil {
		t.Fatal("plaintext remote password authentication unexpectedly accepted")
	}
}

func TestStandaloneServerOfficialDriverTLS(t *testing.T) {
	certFile, keyFile, pool := writeTLSMaterial(t, false)
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetTLSConfig(&tls.Config{RootCAs: pool, ServerName: "localhost"}).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = client.Ping(pingCtx, nil)
	pingCancel()
	if err != nil {
		t.Fatalf("trusted TLS driver ping: %v", err)
	}
	if err := client.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for metrics := standalone.TransportMetrics(); (metrics.ConnectionsAccepted == 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted || metrics.ActiveConnections != 0) && time.Now().Before(deadline); metrics = standalone.TransportMetrics() {
		time.Sleep(time.Millisecond)
	}
	if metrics := standalone.TransportMetrics(); metrics.ConnectionsAccepted == 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted || metrics.ActiveConnections != 0 {
		t.Fatalf("connection metrics after trusted disconnect=%+v", metrics)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	if got := standalone.TransportStatus(); got.Mode != "tls" || got.TLSCertificateNotAfter.IsZero() {
		t.Fatalf("status=%+v", got)
	}
}

func TestStandaloneServerOfficialDriverSCRAMSHA256(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: "alice", Password: "correct horse battery staple", AuthSource: "admin"}).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		t.Fatalf("SCRAM driver ping: %v", err)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneServerOfficialDriverAuthorizationRoleBoundary(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	catalog := standalone.Server.AuthCatalog
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "reader", []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	connect := func(username, password string) *mongo.Client {
		t.Helper()
		client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: username, Password: password, AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}).SetServerSelectionTimeout(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
		return client
	}
	root := connect("root", "root password")
	reader := connect("reader", "reader password")
	opCtx, opCancel := context.WithCancel(ctx)
	opTimer := time.AfterFunc(10*time.Second, opCancel)
	defer opTimer.Stop()
	defer opCancel()
	collection := root.Database("app").Collection("items")
	if _, err := collection.InsertOne(opCtx, bson.D{{Key: "_id", Value: "visible"}}); err != nil {
		t.Fatal(err)
	}
	var found bson.M
	if err := reader.Database("app").Collection("items").FindOne(opCtx, bson.D{{Key: "_id", Value: "visible"}}).Decode(&found); err != nil {
		t.Fatalf("read role find: %v", err)
	}
	_, err = reader.Database("app").Collection("items").InsertOne(opCtx, bson.D{{Key: "_id", Value: "denied"}})
	var commandErr mongo.CommandError
	if !errors.As(err, &commandErr) || commandErr.Code != 13 {
		t.Fatalf("read role insert error=%v want Unauthorized code 13", err)
	}
	count, err := collection.CountDocuments(opCtx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("documents after denied insert=%d want 1", count)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneServerOfficialDriverSCRAMSHA256SASLprepUnicode(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	// U+00A0 is mapped to ordinary space by SASLprep. This proves the stored
	// verifier is compatible with the official driver's SCRAM implementation.
	if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "unicode", []byte("p\u00e4ss\u00a0word")); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: "unicode", Password: "p\u00e4ss\u00a0word", AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	pingCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := client.Ping(pingCtx, nil); err != nil {
		t.Fatalf("Unicode SCRAM driver ping: %v", err)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneServerOfficialDriverSCRAMFailureRotationAndReconnect(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	catalog := standalone.Server.AuthCatalog
	if err := catalog.UpsertPassword("admin", "alice", []byte("first password")); err != nil {
		t.Fatal(err)
	}
	addBackupAuthAdministrator(t, catalog)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	ping := func(password, authDB string) error {
		client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: "alice", Password: password, AuthSource: authDB, AuthMechanism: "SCRAM-SHA-256"}).SetServerSelectionTimeout(time.Second))
		if err != nil {
			return err
		}
		defer client.Disconnect(context.Background())
		pingCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		return client.Ping(pingCtx, nil)
	}
	if err := ping("wrong password", "admin"); err == nil {
		t.Fatal("official driver accepted wrong password")
	}
	if err := ping("first password", "other"); err == nil {
		t.Fatal("official driver accepted wrong auth DB")
	}
	if err := catalog.SetEnabled("admin", "alice", false); err != nil {
		t.Fatal(err)
	}
	if err := ping("first password", "admin"); err == nil {
		t.Fatal("official driver authenticated disabled user")
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("second password")); err != nil {
		t.Fatal(err)
	}
	if err := ping("first password", "admin"); err == nil {
		t.Fatal("official driver accepted rotated password")
	}
	if err := ping("second password", "admin"); err != nil {
		t.Fatalf("official driver reconnect after rotation: %v", err)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneServerSCRAMCatalogReopenAndConcurrentAuthentication(t *testing.T) {
	dir := t.TempDir()
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	standalone, err = OpenStandaloneServer(StandaloneOptions{Dir: dir, AuthenticationEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- standalone.Serve(ctx, ln) }()
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: "alice", Password: "correct horse battery staple", AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}).SetServerSelectionTimeout(3 * time.Second))
			if err == nil {
				pingCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
				err = client.Ping(pingCtx, nil)
				stop()
				_ = client.Disconnect(context.Background())
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SCRAM authentication: %v", err)
		}
	}
	cancel()
	_ = ln.Close()
	if err := <-served; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneServerTLSHandshakeTimeoutAndClose(t *testing.T) {
	certFile, keyFile, pool := writeTLSMaterial(t, false)
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile, TLSHandshakeTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(context.Background(), ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetTLSConfig(&tls.Config{RootCAs: pool, ServerName: "localhost"}).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := client.Ping(pingCtx, nil); err != nil {
		t.Fatalf("stalled TLS client blocked trusted driver: %v", err)
	}
	pingCancel()
	_ = client.Disconnect(context.Background())
	if metrics := standalone.TransportMetrics(); metrics.HandshakesSucceeded == 0 || metrics.ActiveHandshakes == 0 || metrics.HandshakeTotalNanoseconds == 0 || metrics.HandshakeMaxNanoseconds == 0 || metrics.ConnectionsAccepted < 2 || metrics.ActiveConnections < metrics.ActiveHandshakes {
		t.Fatalf("metrics=%+v want succeeded and stalled active handshake", metrics)
	}
	closeErr := make(chan error, 1)
	go func() { closeErr <- standalone.Close() }()
	deadline := time.Now().Add(time.Second)
	for metrics := standalone.TransportMetrics(); (metrics.ActiveHandshakes != 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted || metrics.ActiveConnections != 0) && time.Now().Before(deadline); metrics = standalone.TransportMetrics() {
		time.Sleep(time.Millisecond)
	}
	if metrics := standalone.TransportMetrics(); metrics.ActiveHandshakes != 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted || metrics.ActiveConnections != 0 {
		t.Fatalf("connection metrics after shutdown=%+v", metrics)
	}
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * standaloneShutdownTimeout):
		t.Fatal("standalone Close did not finish after transport shutdown")
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve leaked after TLS handshake timeout")
	}
}

func TestStandaloneServerRejectsConcurrentListenerWithoutMaskingRemoteStatus(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), AllowInsecureRemote: true})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, signalAcceptListener{Listener: remote, started: started}) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("remote listener did not reach Accept")
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.ValidateListener(probe); err == nil {
		t.Fatal("ValidateListener unexpectedly changed an active listener policy")
	}
	_ = probe.Close()
	if got := standalone.TransportStatus(); got.Mode != "plaintext-insecure-remote" || !got.InsecureRemoteOverride {
		t.Fatalf("ValidateListener masked active remote status: %+v", got)
	}
	loopback, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := standalone.Serve(context.Background(), loopback); err == nil {
		t.Fatal("second listener unexpectedly accepted")
	}
	if got := standalone.TransportStatus(); got.Mode != "plaintext-insecure-remote" || !got.InsecureRemoteOverride {
		t.Fatalf("second listener masked active remote status: %+v", got)
	}
	cancel()
	_ = remote.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("remote serve: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneServerPlaintextConnectionMetrics(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for metrics := standalone.TransportMetrics(); metrics.ConnectionsClosed == 0 && time.Now().Before(deadline); metrics = standalone.TransportMetrics() {
		time.Sleep(time.Millisecond)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	if metrics := standalone.TransportMetrics(); metrics.ConnectionsAccepted != 1 || metrics.ConnectionsClosed != 1 || metrics.ActiveConnections != 0 || metrics.HandshakesStarted != 0 {
		t.Fatalf("plaintext connection metrics=%+v", metrics)
	}
}

func TestStandaloneServerValidateListenerRejectsClosedServer(t *testing.T) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	before := standalone.TransportStatus()
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := standalone.ValidateListener(ln); !errors.Is(err, errServerClosed) {
		t.Fatalf("ValidateListener after Close error=%v, want errServerClosed", err)
	}
	if got := standalone.TransportStatus(); got != before {
		t.Fatalf("ValidateListener after Close changed status: before=%+v after=%+v", before, got)
	}
}

type signalAcceptListener struct {
	net.Listener
	started chan<- struct{}
}

func (l signalAcceptListener) Accept() (net.Conn, error) {
	select {
	case l.started <- struct{}{}:
	default:
	}
	return l.Listener.Accept()
}

func TestStandaloneTLSStartupValidation(t *testing.T) {
	certFile, keyFile, _ := writeTLSMaterial(t, false)
	otherCert, otherKey, _ := writeTLSMaterial(t, false)
	for _, tc := range []struct {
		name string
		opts StandaloneOptions
	}{
		{"missing-key", StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile}},
		{"missing-client-ca", StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile, RequireClientCert: true}},
		{"client-cert-without-server-tls", StandaloneOptions{Dir: t.TempDir(), TLSCAFile: certFile, RequireClientCert: true}},
		{"bad-min-version", StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile, TLSMinVersion: tls.VersionTLS11}},
		{"mismatched-key", StandaloneOptions{Dir: t.TempDir(), TLSCertFile: otherCert, TLSKeyFile: keyFile}},
		{"unused-other-key", StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: otherKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := OpenStandaloneServer(tc.opts); err == nil {
				t.Fatal("OpenStandaloneServer unexpectedly succeeded")
			}
		})
	}
}

func TestStandaloneTLSRejectsHostnameTrustAndMinimumVersionBeforeMongo(t *testing.T) {
	certFile, keyFile, pool := writeTLSMaterial(t, false)
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile, TLSMinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	for _, tc := range []struct {
		name string
		cfg  *tls.Config
	}{
		{"hostname", &tls.Config{RootCAs: pool, ServerName: "wrong.example"}},
		{"trust", &tls.Config{ServerName: "localhost"}},
		{"min-version", &tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", ln.Addr().String(), tc.cfg)
			if err == nil {
				_ = conn.Close()
				t.Fatal("TLS dial unexpectedly succeeded")
			}
		})
	}
	deadline := time.Now().Add(time.Second)
	for standalone.TransportMetrics().HandshakesFailed < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	metrics := standalone.TransportMetrics()
	if metrics.HandshakesFailed < 3 || metrics.HandshakeTotalNanoseconds == 0 {
		t.Fatalf("metrics=%+v", metrics)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestTransportMetricsRecordsMinimumCompletedHandshakeDuration(t *testing.T) {
	metrics := &transportMetrics{}
	metrics.recordHandshakeDuration(0)
	if got := metrics.totalNS.Load(); got != 1 {
		t.Fatalf("total handshake nanoseconds=%d want 1 for completed zero-duration seam", got)
	}
	if got := metrics.maxNS.Load(); got != 1 {
		t.Fatalf("max handshake nanoseconds=%d want 1 for completed zero-duration seam", got)
	}
	metrics.recordHandshakeDuration(7 * time.Nanosecond)
	if got := metrics.totalNS.Load(); got != 8 {
		t.Fatalf("total handshake nanoseconds=%d want 8", got)
	}
	if got := metrics.maxNS.Load(); got != 7 {
		t.Fatalf("max handshake nanoseconds=%d want 7", got)
	}
}

func TestStandaloneTLSRejectsExpiredCertificateBeforeMongo(t *testing.T) {
	certFile, keyFile, pool := writeTLSMaterial(t, true)
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err == nil {
		_ = conn.Close()
		t.Fatal("expired certificate unexpectedly connected")
	}
	deadline := time.Now().Add(time.Second)
	for metrics := standalone.TransportMetrics(); (metrics.HandshakesFailed == 0 || metrics.HandshakeTotalNanoseconds == 0 || metrics.ActiveHandshakes != 0 || metrics.ActiveConnections != 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted) && time.Now().Before(deadline); metrics = standalone.TransportMetrics() {
		time.Sleep(time.Millisecond)
	}
	if metrics := standalone.TransportMetrics(); metrics.HandshakesFailed == 0 || metrics.HandshakeTotalNanoseconds == 0 || metrics.ActiveHandshakes != 0 || metrics.ActiveConnections != 0 || metrics.ConnectionsAccepted == 0 || metrics.ConnectionsClosed != metrics.ConnectionsAccepted {
		t.Fatalf("metrics=%+v", metrics)
	}
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestStandaloneTLSRequiresVerifiedClientCertificate(t *testing.T) {
	certFile, keyFile, pool := writeTLSMaterial(t, false)
	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), TLSCertFile: certFile, TLSKeyFile: keyFile, TLSCAFile: certFile, RequireClientCert: true})
	if err != nil {
		t.Fatal(err)
	}
	defer standalone.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- standalone.Serve(ctx, ln) }()
	if conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: pool, ServerName: "localhost"}); err == nil {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var one [1]byte
		_, readErr := conn.Read(one[:])
		_ = conn.Close()
		if readErr == nil {
			t.Fatal("missing client certificate unexpectedly connected")
		}
	}
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetTLSConfig(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}, ServerName: "localhost"}).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = client.Ping(pingCtx, nil)
	pingCancel()
	if err != nil {
		t.Fatalf("verified client certificate ping: %v", err)
	}
	_ = client.Disconnect(context.Background())
	cancel()
	_ = ln.Close()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func writeTLSMaterial(t testing.TB, expired bool) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	notAfter := now.Add(time.Hour)
	if expired {
		notAfter = now.Add(-time.Hour)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: now.Add(-time.Hour), NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return certFile, keyFile, pool
}

// BenchmarkStandaloneTransportHandshake measures fresh official-driver
// Connect, Ping, and Disconnect lifecycle cost. For TLS it also reports the
// server-observed handshake duration separately; the reused FindOne benchmark
// measures the pooled CRUD read path.
func BenchmarkStandaloneTransportHandshake(b *testing.B) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plaintext"
		if tlsEnabled {
			name = "tls"
		}
		b.Run(name, func(b *testing.B) {
			certFile, keyFile, pool := writeTLSMaterial(b, false)
			opts := StandaloneOptions{Dir: b.TempDir()}
			if tlsEnabled {
				opts.TLSCertFile, opts.TLSKeyFile = certFile, keyFile
			}
			standalone, err := OpenStandaloneServer(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer standalone.Close()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- standalone.Serve(ctx, ln) }()
			defer func() { cancel(); _ = ln.Close(); <-done }()
			b.ReportAllocs()
			before := standalone.TransportMetrics()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cfg := options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetServerSelectionTimeout(time.Second)
				if tlsEnabled {
					cfg.SetTLSConfig(&tls.Config{RootCAs: pool, ServerName: "localhost"})
				}
				client, err := mongo.Connect(cfg)
				if err != nil {
					b.Fatal(err)
				}
				pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
				err = client.Ping(pingCtx, nil)
				pingCancel()
				_ = client.Disconnect(context.Background())
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if tlsEnabled {
				after := standalone.TransportMetrics()
				handshakes := after.HandshakesSucceeded - before.HandshakesSucceeded
				if handshakes == 0 {
					b.Fatal("no successful TLS handshakes observed")
				}
				b.ReportMetric(float64(after.HandshakeTotalNanoseconds-before.HandshakeTotalNanoseconds)/float64(handshakes), "server-handshake-ns/op")
			}
		})
	}
}

// BenchmarkStandaloneSCRAMSHA256Handshake measures the fresh official-driver
// password-authentication boundary. Reused CRUD is intentionally excluded: it
// must not re-run PBKDF2 after the connection identity is established.
func BenchmarkStandaloneSCRAMSHA256Handshake(b *testing.B) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: b.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		b.Fatal(err)
	}
	defer standalone.Close()
	if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "bench", []byte("correct horse battery staple")); err != nil {
		b.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- standalone.Serve(ctx, ln) }()
	defer func() { cancel(); _ = ln.Close(); <-done }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetAuth(options.Credential{Username: "bench", Password: "correct horse battery staple", AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}).SetServerSelectionTimeout(time.Second))
		if err != nil {
			b.Fatal(err)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Ping(pingCtx, nil)
		pingCancel()
		_ = client.Disconnect(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStandaloneSCRAMSHA256ReusedFindOne is the steady-state companion:
// client setup and its one SCRAM exchange finish before timing begins.
func BenchmarkStandaloneSCRAMSHA256ReusedFindOne(b *testing.B) {
	standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: b.TempDir(), AuthenticationEnabled: true})
	if err != nil {
		b.Fatal(err)
	}
	defer standalone.Close()
	if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "bench", []byte("correct horse battery staple")); err != nil {
		b.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- standalone.Serve(ctx, ln) }()
	defer func() { cancel(); _ = ln.Close(); <-done }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetServerSelectionTimeout(3 * time.Second).SetAuth(options.Credential{Username: "bench", Password: "correct horse battery staple", AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"}))
	if err != nil {
		b.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	coll := client.Database("bench").Collection("items")
	if _, err := coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out bson.M
		if err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}).Decode(&out); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStandaloneAuthVsPlaintextReusedFindOne compares the same pooled
// official-driver FindOne shape. Connection setup and, for SCRAM, the initial
// authentication exchange finish before the timer starts.
func BenchmarkStandaloneAuthVsPlaintextReusedFindOne(b *testing.B) {
	for _, authenticationEnabled := range []bool{false, true} {
		name := "plaintext"
		if authenticationEnabled {
			name = "scram"
		}
		b.Run(name, func(b *testing.B) {
			standalone, err := OpenStandaloneServer(StandaloneOptions{Dir: b.TempDir(), AuthenticationEnabled: authenticationEnabled})
			if err != nil {
				b.Fatal(err)
			}
			defer standalone.Close()
			if authenticationEnabled {
				if err := standalone.Server.AuthCatalog.UpsertPassword("admin", "bench", []byte("correct horse battery staple")); err != nil {
					b.Fatal(err)
				}
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- standalone.Serve(ctx, ln) }()
			defer func() { cancel(); _ = ln.Close(); <-done }()
			cfg := options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).SetServerSelectionTimeout(3 * time.Second)
			if authenticationEnabled {
				cfg.SetAuth(options.Credential{Username: "bench", Password: "correct horse battery staple", AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256"})
			}
			client, err := mongo.Connect(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = client.Ping(pingCtx, nil)
			pingCancel()
			if err != nil {
				b.Fatal(err)
			}
			coll := client.Database("bench").Collection("items")
			if _, err := coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}); err != nil {
				b.Fatal(err)
			}
			var fixture bson.M
			if err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}).Decode(&fixture); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bson.M
				if err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}).Decode(&out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStandaloneTransportReusedFindOne measures a pooled official-driver
// request/response after connection setup; listener, database, and client
// construction are outside the timed region.
func BenchmarkStandaloneTransportReusedFindOne(b *testing.B) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plaintext"
		if tlsEnabled {
			name = "tls"
		}
		b.Run(name, func(b *testing.B) {
			certFile, keyFile, pool := writeTLSMaterial(b, false)
			opts := StandaloneOptions{Dir: b.TempDir()}
			if tlsEnabled {
				opts.TLSCertFile, opts.TLSKeyFile = certFile, keyFile
			}
			standalone, err := OpenStandaloneServer(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer standalone.Close()
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- standalone.Serve(ctx, ln) }()
			defer func() { cancel(); _ = ln.Close(); <-done }()
			cfg := options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true)
			if tlsEnabled {
				cfg.SetTLSConfig(&tls.Config{RootCAs: pool, ServerName: "localhost"})
			}
			client, err := mongo.Connect(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Disconnect(context.Background())
			coll := client.Database("bench").Collection("items")
			if _, err := coll.InsertOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}, {Key: "n", Value: int32(1)}}); err != nil {
				b.Fatal(err)
			}
			var fixture bson.M
			if err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}).Decode(&fixture); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out bson.M
				if err := coll.FindOne(context.Background(), bson.D{{Key: "_id", Value: "fixture"}}).Decode(&out); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
