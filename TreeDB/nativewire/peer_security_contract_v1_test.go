package nativewire

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestPeerSecurityIdentityAndGroupV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Catalog.Peers = config.Catalog.Peers[:1]
	config.Groups = config.Groups[:1]
	config.ClusterID = "identity-group"
	ca := newPeerCAFixtureV1(t)
	now := time.Now()
	issue := func(cluster, node string) *PeerCredentialsV1 {
		return ca.issue(t, cluster, node, now.Add(-time.Hour), now.Add(time.Hour))
	}
	config.Credentials = issue(config.ClusterID, string(config.NodeID))
	runtime, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	consumer := config
	consumer.NodeID = "owner-1"
	consumer.ListenAddress = config.Nodes[1].Address
	consumer.RaftListen = nil
	consumer.Credentials = issue(config.ClusterID, string(consumer.NodeID))
	consumer.DataRoot = filepath.Join(t.TempDir(), "data")
	consumer.RaftRoot = filepath.Join(t.TempDir(), "raft")
	client, err := NewFixedPeerTCPClientV1(consumer)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	status, err := client.Status(ctx, config.NodeID)
	if err != nil || status.NodeID != config.NodeID {
		t.Fatalf("configured consumer status: %+v error=%v", status, err)
	}
	if _, err := client.PublishCatalog(ctx, config.NodeID, nil); !errors.Is(err, errPeerAuthenticationV1) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("non-voter catalog publication must be a definite authorization refusal: %v", err)
	}
	if conn, err := client.security.dial(ctx, config.ListenAddress, "owner-2"); err == nil {
		conn.Close()
		t.Fatal("accepted a valid certificate for the wrong destination node")
	}
	for name, credentials := range map[string]*PeerCredentialsV1{
		"unknown":       issue(config.ClusterID, "unknown"),
		"wrong-cluster": issue("another-cluster", "owner-1"),
		"expired":       ca.issue(t, config.ClusterID, "owner-1", now.Add(-time.Hour), now.Add(-time.Minute)),
	} {
		t.Run(name, func(t *testing.T) {
			certificate, err := tls.LoadX509KeyPair(credentials.CertificateFile, credentials.PrivateKeyFile)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			raw, err := os.ReadFile(credentials.TrustRootsFile)
			if err != nil || !roots.AppendCertsFromPEM(raw) {
				t.Fatalf("roots: %v", err)
			}
			transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}}}
			defer transport.CloseIdleConnections()
			request, err := http.NewRequestWithContext(ctx, "POST", "https://"+config.ListenAddress+"/v1/status", strings.NewReader("{}"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("X-TreeDB-Node", string(config.NodeID))
			request.Header.Set("X-TreeDB-Config", runtime.client.digest)
			response, err := (&http.Client{Transport: transport, Timeout: time.Second}).Do(request)
			if response != nil {
				response.Body.Close()
			}
			if err == nil {
				t.Fatal("unauthorized certificate reached HTTP application response")
			}
		})
	}
	// This consumer has a valid known-node credential, but is not a voter in
	// either hosted group. It must not reach either Raft or snapshot decoding.
	probe, err := newFixedPeerTCPTransportWithSecurityV1("127.0.0.1:0", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, time.Second, client.security, config.Catalog.Peers)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	defer probe.CloseStreams()
	var reply hraft.AppendEntriesResponse
	request := hraft.AppendEntriesRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax}}
	if err := probe.AppendEntries(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[config.Catalog.ID]), &request, &reply); err == nil {
		t.Fatal("nonmember credential entered catalog Raft group")
	}
	// The same actual Raft RPC succeeds with an authorized group credential.
	authorized, err := newFixedPeerTCPTransportWithSecurityV1("127.0.0.1:0", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}, time.Second, runtime.client.security, config.Catalog.Peers)
	if err != nil {
		t.Fatal(err)
	}
	defer authorized.Close()
	defer authorized.CloseStreams()
	if err := authorized.AppendEntries(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[config.Catalog.ID]), &request, &reply); err != nil {
		t.Fatalf("authorized Raft peer refused: %v", err)
	}
}

func TestPeerSecurityRotationReopenAndDowngradeRefusalV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Nodes = config.Nodes[:1]
	config.Catalog.Peers = config.Catalog.Peers[:1]
	config.Groups = config.Groups[:1]
	config.ClusterID = "rotation-reopen"
	config.Credentials = peerCredentialsFixtureV1(t, config.ClusterID, string(config.NodeID))
	runtime, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { runtime.Close() }()
	oldClient, err := NewFixedPeerTCPClientV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer oldClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := oldClient.Status(ctx, config.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	// Replace the three configured files while the node is drained. The
	// manifest retains exact paths, while a new CA/leaf is loaded on restart.
	replacement := peerCredentialsFixtureV1(t, config.ClusterID, string(config.NodeID))
	for destination, source := range map[string]string{
		config.Credentials.TrustRootsFile:  replacement.TrustRootsFile,
		config.Credentials.CertificateFile: replacement.CertificateFile,
		config.Credentials.PrivateKeyFile:  replacement.PrivateKeyFile,
	} {
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	runtime, err = OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	oldClient.Close()
	if _, err := oldClient.Status(ctx, config.NodeID); err == nil {
		t.Fatal("retired trust material rejoined after rotation")
	}
	newClient, err := NewFixedPeerTCPClientV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer newClient.Close()
	if status, err := newClient.Status(ctx, config.NodeID); err != nil || status.RecoveryState != "reopened" {
		t.Fatalf("rotated credential did not rejoin exact store identity: %+v error=%v", status, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	downgraded := config
	downgraded.Credentials = nil
	if node, err := OpenFixedPeerTCPRuntimeV1(downgraded); err == nil {
		node.Close()
		t.Fatal("secure persisted node silently reopened without TLS")
	}
}

func TestPeerCredentialDeadlineCannotOutliveCertificateV1(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	guard := &peerCredentialDeadlineConnV1{Conn: left, expires: time.Now().Add(30 * time.Millisecond)}
	if err := guard.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := guard.Read(one[:]); err == nil {
		t.Fatal("idle certificate expiry did not interrupt blocked read")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("expiry error=%v", err)
	}
}
