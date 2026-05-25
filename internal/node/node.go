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

// ServiceName establishes the unique broadcast namespace for this private CDN network segment
const ServiceName = "mini-dcdn-private-mesh"

// discoveryNotifee implements the libp2p mdns.Notifee interface
type discoveryNotifee struct {
	host host.Host
}

// HandlePeerFound is triggered automatically by the mDNS subsystem when a peer broadcasts its availability
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// Filter out self-referential discovery signals
	if pi.ID == n.host.ID() {
		return
	}

	log.Printf("[Node Discovery] Local peer localized via mDNS: %s", pi.ID.ShortString())

	// Establish an aggressive connection timeout boundary to maintain non-blocking execution paths
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt connection handshake across discovered multiaddresses
	if err := n.host.Connect(ctx, pi); err != nil {
		log.Printf("[Node Discovery] Warning: Failed to connect to discovered peer %s: %v", pi.ID.ShortString(), err)
		return
	}

	log.Printf("[Node Discovery] Connection verified. Secure swarm pipe opened to: %s", pi.ID.ShortString())
}

// MakeHost initializes the network runtime, identities, and mDNS multi-cast channels
func MakeHost(listenPort int) (host.Host, mdns.Service, error) {
	// Configure transport endpoints. Setting the port to 0 triggers automatic dynamic assignment.
	tcpAddr := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort)
	quicAddr := fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", listenPort)

	log.Printf("[Node Architecture] Building transport bounds | TCP: %s | QUIC: %s", tcpAddr, quicAddr)

	// 1. Initialize the libp2p host stack
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(tcpAddr, quicAddr),
		// Enable automated UPnP/NAT-PMP port forwarding definitions for routers
		libp2p.NATPortMap(),
		// Enables default connection pooling management protections
		libp2p.DefaultConnectionManager,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to construct libp2p host container: %w", err)
	}

	log.Printf("[Node Layer] Host initialized successfully | PeerID: %s", h.ID().String())

	// 2. Wire up the local mDNS discovery engine
	notifee := &discoveryNotifee{host: h}

	// Interval defines how frequently this node actively broadcasts its own presence
	// and checks for changes in local multicast segments.
	disc := mdns.NewMdnsService(h, ServiceName, notifee)
	if err := disc.Start(); err != nil {
		// Clean up the opened host resources if the discovery stack crashes immediately
		_ = h.Close()
		return nil, nil, fmt.Errorf("failed to start local mDNS background thread: %w", err)
	}

	log.Println("[Node Layer] mDNS background multicast engine online and listening.")
	return h, disc, nil
}
