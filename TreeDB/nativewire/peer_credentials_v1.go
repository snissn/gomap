package nativewire

// PeerCredentialsV1 names node-local PEM files for mutually authenticated
// cluster transport. The credential paths are not cluster authority.
type PeerCredentialsV1 struct {
	TrustRootsFile  string
	CertificateFile string
	PrivateKeyFile  string
}
