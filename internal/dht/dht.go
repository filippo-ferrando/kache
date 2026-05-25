package dht

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

type DHTEngine struct {
	Kademlia *dht.IpfsDHT
	host     host.Host
	ctx      context.Context

	mu         sync.RWMutex
	localItems map[string]cid.Cid
}

// NewDHTEngine instantiates the Kademlia DHT routing table for a private network
func NewDHTEngine(ctx context.Context, h host.Host) (*DHTEngine, error) {
	// Configure the DHT in Server mode so it can store and serve records within your private cluster
	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate private Kademlia DHT: %w", err)
	}

	engine := &DHTEngine{
		Kademlia:   kdht,
		host:       h,
		ctx:        ctx,
		localItems: make(map[string]cid.Cid),
	}

	// Initialize the local routing table state
	engine.bootstrapPrivateNetwork()

	// Start background worker to re-advertise content periodically
	go engine.startReprovideWorker(12 * time.Hour)

	return engine, nil
}

// Advertise registers this node's PeerID as an active provider for a content hash
func (d *DHTEngine) Advertise(ctx context.Context, assetID string) error {
	targetCID, err := d.toCid(assetID)
	if err != nil {
		return fmt.Errorf("invalid asset string format: %w", err)
	}

	// Fail fast if we try to advertise but have absolutely zero connections yet
	if d.host.Peerstore().Peers().Len() == 0 {
		log.Printf("[DHT Warning] Advertising '%s' locally, but node is currently isolated. Peer discovery pending.", assetID)
	} else {
		log.Printf("[DHT Engine] Broadcasting provider record for CID: %s", targetCID.String())
	}

	// Provide tells the network that this host node holds the specified block.
	// If alone, this populates our local datastore until other peers query us.
	if err := d.Kademlia.Provide(ctx, targetCID, true); err != nil {
		return fmt.Errorf("failed to publish provider statement: %w", err)
	}

	d.mu.Lock()
	d.localItems[assetID] = targetCID
	d.mu.Unlock()

	return nil
}

// LocateProviders queries the private Kademlia mesh to find external addresses caching the target asset
func (d *DHTEngine) LocateProviders(ctx context.Context, assetID string) ([]peer.AddrInfo, error) {
	targetCID, err := d.toCid(assetID)
	if err != nil {
		return nil, fmt.Errorf("invalid asset validation lookup: %w", err)
	}

	log.Printf("[DHT Engine] Searching private routing table for CID: %s", targetCID.String())

	// If we have no peers, don't waste CPU cycles iterating through an empty routing table
	if d.host.Peerstore().Peers().Len() == 0 {
		return nil, errors.New("lookup aborted: node is completely isolated with 0 connected peers")
	}

	providersChan := d.Kademlia.FindProvidersAsync(ctx, targetCID, 20)

	var providers []peer.AddrInfo
	for p := range providersChan {
		if p.ID == d.host.ID() {
			continue // Skip self
		}
		providers = append(providers, p)
	}

	if len(providers) == 0 {
		return nil, errors.New("routing search completed: content not found on any external peer")
	}

	return providers, nil
}

// toCid converts an arbitrary string identifier safely into a cryptographically compliant CIDv1
func (d *DHTEngine) toCid(input string) (cid.Cid, error) {
	if c, err := cid.Decode(input); err == nil {
		return c, nil
	}

	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum([]byte(input))
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

// bootstrapPrivateNetwork sets up the local Kademlia routing matrix
func (d *DHTEngine) bootstrapPrivateNetwork() {
	log.Println("[DHT Engine] Initializing Kademlia routing sub-system...")

	// Running Bootstrap on an empty node tells the DHT engine to prepare its internal
	// routing loops and structures. It will gracefully sit and wait for incoming connections.
	if err := d.Kademlia.Bootstrap(d.ctx); err != nil {
		log.Printf("[DHT Warning] Core routing table bootstrap reported initial quirks: %v", err)
	}

	log.Println("[DHT Engine] Private network mode active. Standing by for local mDNS / manual peer connections...")
}

// startReprovideWorker runs in the background to ensure data records don't expire
func (d *DHTEngine) startReprovideWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// If we are still completely alone, skip the re-provide step until a peer arrives
			if d.host.Peerstore().Peers().Len() == 0 {
				continue
			}

			d.mu.RLock()
			if len(d.localItems) == 0 {
				d.mu.RUnlock()
				continue
			}

			itemsToRefresh := make([]cid.Cid, 0, len(d.localItems))
			for _, c := range d.localItems {
				itemsToRefresh = append(itemsToRefresh, c)
			}
			d.mu.RUnlock()

			log.Printf("[DHT Engine] Executing scheduled re-provider cycle for %d cached items...", len(itemsToRefresh))
			for _, targetCid := range itemsToRefresh {
				ctx, cancel := context.WithTimeout(d.ctx, 15*time.Second)
				if err := d.Kademlia.Provide(ctx, targetCid, true); err != nil {
					log.Printf("[DHT Warning] Reprovide failed for %s: %v", targetCid, err)
				}
				cancel()
			}
		case <-d.ctx.Done():
			return
		}
	}
}
