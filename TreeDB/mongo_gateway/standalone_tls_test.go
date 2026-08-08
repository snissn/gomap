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
	if metrics := standalone.TransportMetrics(); metrics.HandshakesSucceeded == 0 || metrics.ActiveHandshakes == 0 || metrics.HandshakeTotalNanoseconds == 0 || metrics.HandshakeMaxNanoseconds == 0 || metrics.ConnectionsAccepted < 2 || metrics.ActiveConnections < 2 {
		t.Fatalf("metrics=%+v want succeeded and stalled active handshake", metrics)
	}
	started := time.Now()
	if err := standalone.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close waited %s for stalled TLS handshake", elapsed)
	}
	if metrics := standalone.TransportMetrics(); metrics.ConnectionsClosed != metrics.ConnectionsAccepted || metrics.ActiveConnections != 0 {
		t.Fatalf("connection metrics after shutdown=%+v", metrics)
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
