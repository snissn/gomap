package nativewire

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func peerCredentialsFixtureV1(t testing.TB, cluster, node string) *PeerCredentialsV1 {
	return newPeerCAFixtureV1(t).issue(t, cluster, node, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
}

type peerCAFixtureV1 struct {
	key         *ecdsa.PrivateKey
	certificate *x509.Certificate
	der         []byte
}

func newPeerCAFixtureV1(t testing.TB) *peerCAFixtureV1 {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test cluster CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &peerCAFixtureV1{key: caKey, certificate: ca, der: caDER}
}

func (ca *peerCAFixtureV1) issue(t testing.TB, cluster, node string, notBefore, notAfter time.Time) *PeerCredentialsV1 {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identity := &url.URL{Scheme: "spiffe", Host: "treedb", Path: "/cluster/" + cluster + "/node/" + node}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: serial, NotBefore: notBefore, NotAfter: notAfter, URIs: []*url.URL{identity}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca.certificate, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	credentials := &PeerCredentialsV1{TrustRootsFile: filepath.Join(root, "ca.pem"), CertificateFile: filepath.Join(root, "node.pem"), PrivateKeyFile: filepath.Join(root, "node-key.pem")}
	for path, block := range map[string]*pem.Block{
		credentials.TrustRootsFile:  {Type: "CERTIFICATE", Bytes: ca.der},
		credentials.CertificateFile: {Type: "CERTIFICATE", Bytes: leafDER},
		credentials.PrivateKeyFile:  {Type: "PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return credentials
}

func TestUnknownPeerCannotSubmitOrInstallSnapshotV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Nodes = config.Nodes[:1]
	config.Catalog.Peers = config.Catalog.Peers[:1]
	config.Groups = config.Groups[:1]
	config.ClusterID = "security-conformance"
	config.Credentials = peerCredentialsFixtureV1(t, config.ClusterID, string(config.NodeID))
	node, err := OpenFixedPeerTCPRuntimeV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixedPeerWaitV1(t, ctx, func() bool {
		s, e := node.Status(ctx)
		return e == nil && s.CatalogRaft.State == "Leader" && len(s.Groups) == 1 && s.Groups[0].State == "Leader"
	})
	catalog := raftplacement.CatalogV1{
		Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{config.NodeID}}},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "unauthorized"}, GroupID: "group-a"}},
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	// Fixture publication uses the actual local production provider; it does
	// not grant the credential-free network client any authority.
	if _, _, err := node.meta.SubmitCatalogMetaCommandV1(ctx, command); err != nil {
		t.Fatal(err)
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "unauthorized", Shape: ClusterRouteShapeCollection}
	route, err := node.route(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	status, err := node.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
	ApplyClusterRouteMetadata(&metadata, request, route)
	body, err := json.Marshal(fixedPeerRequestV1{Entry: fixedPeerCreateEntryV1(t, "unauthorized", status.Groups[0].CatalogVersion), Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	// The inventory/configuration digest is intentionally known to the
	// attacker. It must never substitute for an authenticated certificate.
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+config.ListenAddress+"/v1/submit", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-TreeDB-Node", string(config.NodeID))
	req.Header.Set("X-TreeDB-Config", node.client.digest)
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: time.Second}).Do(req)
	if err == nil {
		defer response.Body.Close()
		var reply fixedPeerReplyV1
		if response.StatusCode == http.StatusOK && json.NewDecoder(io.LimitReader(response.Body, fixedPeerMaxRPCBytesV1)).Decode(&reply) == nil && reply.Error == "" && reply.Submit.Evidence.Index != 0 {
			t.Errorf("credential-free client committed a durable mutation at index %d", reply.Submit.Evidence.Index)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stream := &sparseCatalogRecordingStreamV1{Listener: listener, dialed: make(chan net.Conn, 1)}
	unauthorized := hraft.NewNetworkTransport(stream, 1, time.Second, io.Discard)
	defer unauthorized.Close()
	defer unauthorized.CloseStreams()
	// A stale-term snapshot is harmless but still elicits a normal Raft
	// response on the old unauthenticated transport. TLS must reject before
	// even this snapshot header reaches the consensus consumer.
	snapshot := hraft.InstallSnapshotRequest{RPCHeader: hraft.RPCHeader{ProtocolVersion: hraft.ProtocolVersionMax}, Term: 0, LastLogIndex: 1, LastLogTerm: 1, Size: 0}
	var installed hraft.InstallSnapshotResponse
	if err := unauthorized.InstallSnapshot(hraft.ServerID(config.NodeID), hraft.ServerAddress(config.RaftListen[config.Catalog.ID]), &snapshot, &installed, bytes.NewReader(nil)); err == nil {
		t.Error("credential-free client entered the Raft snapshot RPC")
	}
}
