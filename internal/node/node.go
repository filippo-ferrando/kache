// Package node - provides the core network runtime and local peer discovery mechanisms for the mini-dcdn private mesh network.
package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const ServiceName = "kache-private-mesh"

type discoveryNotifee struct {
	host host.Host
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

	log.Printf("[Node Discovery] Connection verified. Secure swarm pipe opened to: %s", pi.ID.ShortString())
}

func MakeHost(listenPort int) (host.Host, mdns.Service, error) {
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

	log.Printf("[Node Layer] Host initialized successfully | PeerID: %s", h.ID().String())

	notifee := &discoveryNotifee{host: h}

	disc := mdns.NewMdnsService(h, ServiceName, notifee)
	if err := disc.Start(); err != nil {
		_ = h.Close()
		return nil, nil, fmt.Errorf("[Node Layer] failed to start local mDNS background thread: %w", err)
	}

	log.Println("[Node Layer] mDNS background multicast engine online and listening.")
	return h, disc, nil
}
