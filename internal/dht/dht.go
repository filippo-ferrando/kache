// Package dht - implementation of the private Kademlia DHT engine responsible for peer discovery and content routing within the local network cluster.
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

func NewDHTEngine(ctx context.Context, h host.Host) (*DHTEngine, error) {
	kdht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		return nil, fmt.Errorf("[DHT Engine] Failed to instantiate private Kademlia DHT: %w", err)
	}

	engine := &DHTEngine{
		Kademlia:   kdht,
		host:       h,
		ctx:        ctx,
		localItems: make(map[string]cid.Cid),
	}

	engine.bootstrapPrivateNetwork()

	go engine.startReprovideWorker(12 * time.Hour)

	return engine, nil
}

func (d *DHTEngine) Advertise(ctx context.Context, assetID string) error {
	targetCID, err := d.toCid(assetID)
	if err != nil {
		return fmt.Errorf("[DHT Engine] Invalid asset string format: %w", err)
	}

	// fail-fast logic for advertising when 0 peers present
	if d.host.Peerstore().Peers().Len() == 0 {
		log.Printf("[DHT Engine] Advertising '%s' locally, but node is currently isolated. Peer discovery pending.", assetID)
	} else {
		log.Printf("[DHT Engine] Broadcasting provider record for CID: %s", targetCID.String())
	}

	if err := d.Kademlia.Provide(ctx, targetCID, true); err != nil {
		return fmt.Errorf("[DHT Engine] Failed to publish provider statement: %w", err)
	}

	d.mu.Lock()
	d.localItems[assetID] = targetCID
	d.mu.Unlock()

	return nil
}

func (d *DHTEngine) LocateProviders(ctx context.Context, assetID string) ([]peer.AddrInfo, error) {
	targetCID, err := d.toCid(assetID)
	if err != nil {
		return nil, fmt.Errorf("[DHT Engine] Invalid asset validation lookup: %w", err)
	}

	log.Printf("[DHT Engine] Searching private routing table for CID: %s", targetCID.String())

	if d.host.Peerstore().Peers().Len() == 0 {
		return nil, errors.New("[DHT engine error] Lookup aborted: node is completely isolated with 0 connected peers")
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
		return nil, errors.New("[DHT engine warning] Routing search completed: content not found on any external peer")
	}

	return providers, nil
}

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

func (d *DHTEngine) bootstrapPrivateNetwork() {
	log.Println("[DHT Engine] Initializing Kademlia routing sub-system...")

	if err := d.Kademlia.Bootstrap(d.ctx); err != nil {
		log.Printf("[DHT Engine] Core routing table bootstrap reported initial quirks: %v", err)
	}

	log.Println("[DHT Engine] Private network mode active. Standing by for local mDNS / manual peer connections...")
}

func (d *DHTEngine) startReprovideWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
					log.Printf("[DHT Engine] Reprovide failed for %s: %v", targetCid, err)
				}
				cancel()
			}
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *DHTEngine) GetClosestPeers(ctx context.Context, assetID string) ([]peer.ID, error) {
	targetCID, err := d.toCid(assetID)
	if err != nil {
		return nil, fmt.Errorf("[DHT Engine] Failed to parse asset format for XOR comparison: %w", err)
	}

	closest, err := d.Kademlia.GetClosestPeers(ctx, targetCID.String())
	if err != nil {
		return nil, fmt.Errorf("[DHT Engine] Failed to retrieve closest peers: %w", err)
	}

	return closest, nil
}
