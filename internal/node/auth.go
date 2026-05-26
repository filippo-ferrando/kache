package node

import (
	"crypto/rsa"
	"crypto/x509"

	p2pProtocol "github.com/libp2p/go-libp2p/core/protocol"
)

const AuthProtocolID = p2pProtocol.ID("/kache/cluster-auth/1.0.0")

type ClusterAuthenticator struct {
	RootCACert *x509.Certificate
	NodeCert   *x509.Certificate
	NodeKey    *rsa.PrivateKey
}

type AuthChallenge struct {
	Nonce []byte `json:"nonce"`
}

type AuthResponse struct {
	Certificate []byte `json:"certificate"`
	Signature   []byte `json:"signature"`
}
