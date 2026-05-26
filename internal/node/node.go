// Package node - provides the core network runtime and local peer discovery mechanisms for the mini-dcdn private mesh network.
package node

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const (
	ServiceName = "kache-private-mesh"
)

type discoveryNotifee struct {
	host     host.Host
	nodeKey  *rsa.PrivateKey
	nodeCert *x509.Certificate
}

func (auth *ClusterAuthenticator) AuthenticatePeer(stream network.Stream) error {
	defer stream.Close()

	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}

	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(AuthChallenge{Nonce: nonce}); err != nil {
		return err
	}

	var resp AuthResponse
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&resp); err != nil {
		return err
	}

	peerCert, err := x509.ParseCertificate(resp.Certificate)
	if err != nil {
		return fmt.Errorf("bad node verification certificate: %w", err)
	}

	roots := x509.NewCertPool()
	roots.AddCert(auth.RootCACert)
	opts := x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	if _, err := peerCert.Verify(opts); err != nil {
		return fmt.Errorf("untrusted certificate source chain rejected: %w", err)
	}

	hashedNonce := sha256.Sum256(nonce)
	pubKey, ok := peerCert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return errors.New("unsupported key format inside asset payload")
	}

	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hashedNonce[:], resp.Signature); err != nil {
		return errors.New("cryptographic fraud signature mismatch detected")
	}

	return nil
}

func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.host.ID() {
		return
	}

	log.Printf("[Node Discovery] Local peer localized via mDNS: %s", pi.ID.ShortString())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := n.host.Connect(ctx, pi); err != nil {
		log.Printf("[Node Discovery] Warning: Failed to connect to discovered peer %s: %v", pi.ID.ShortString(), err)
		return
	}

	authStream, err := n.host.NewStream(ctx, pi.ID, AuthProtocolID)
	if err != nil {
		log.Printf("[Node Discovery] Warning: Failed to open authentication stream to peer %s: %v", pi.ID.ShortString(), err)
		_ = n.host.Network().ClosePeer(pi.ID)
		return
	}
	defer authStream.Close()

	var challenge AuthChallenge
	decoder := json.NewDecoder(authStream)
	if err := decoder.Decode(&challenge); err != nil {
		log.Printf("[Node Discovery] Security Error: Failed to decode challenge from peer %s: %v", pi.ID.ShortString(), err)
		_ = n.host.Network().ClosePeer(pi.ID)
		return
	}

	hashedNonce := sha256.Sum256(challenge.Nonce)

	signature, err := rsa.SignPKCS1v15(rand.Reader, n.nodeKey, crypto.SHA256, hashedNonce[:])
	if err != nil {
		log.Printf("[Node Discovery] Internal Error: Failed to sign authentication challenge: %v", err)
		_ = n.host.Network().ClosePeer(pi.ID)
		return
	}

	resp := AuthResponse{
		Certificate: n.nodeCert.Raw,
		Signature:   signature,
	}

	encoder := json.NewEncoder(authStream)
	if err := encoder.Encode(resp); err != nil {
		log.Printf("[Node Discovery] Security Error: Failed to transmit auth response to peer %s: %v", pi.ID.ShortString(), err)
		_ = n.host.Network().ClosePeer(pi.ID)
		return
	}

	time.Sleep(100 * time.Millisecond)

	if len(n.host.Network().ConnsToPeer(pi.ID)) == 0 {
		log.Printf("[Node Discovery] Security Rejection: Remote peer %s disconnected us after authentication verification", pi.ID.ShortString())
		return
	}

	log.Printf("[Node Discovery] Connection verified. Secure swarm pipe opened to: %s", pi.ID.ShortString())
}

func MakeHost(listenPort int, auth *ClusterAuthenticator) (host.Host, mdns.Service, error) {
	tcpAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)
	quicAddr := fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort)

	log.Printf("[Node Architecture] Building transport bounds | TCP: %s | QUIC: %s", tcpAddr, quicAddr)

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(tcpAddr, quicAddr),
		libp2p.NATPortMap(),
		libp2p.DefaultConnectionManager,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("[Node Architecture] failed to construct libp2p host container: %w", err)
	}

	h.SetStreamHandler(AuthProtocolID, func(stream network.Stream) {
		if err := auth.AuthenticatePeer(stream); err != nil {
			log.Printf("[Node Layer] Authentication failure from peer %s: %v", stream.Conn().RemotePeer().ShortString(), err)
			stream.Conn().Close()
		} else {
			log.Printf("[Node Layer] Authentication success from peer %s: secure connection established", stream.Conn().RemotePeer().ShortString())
		}
	})

	log.Printf("[Node Layer] Host initialized successfully | PeerID: %s", h.ID().String())

	notifee := &discoveryNotifee{
		host:     h,
		nodeKey:  auth.NodeKey,
		nodeCert: auth.NodeCert,
	}

	disc := mdns.NewMdnsService(h, ServiceName, notifee)
	if err := disc.Start(); err != nil {
		_ = h.Close()
		return nil, nil, fmt.Errorf("[Node Layer] failed to start local mDNS background thread: %w", err)
	}

	log.Println("[Node Layer] mDNS background multicast engine online and listening.")
	return h, disc, nil
}
