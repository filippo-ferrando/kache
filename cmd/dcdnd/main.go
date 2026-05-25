package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kache/internal/api"
	"kache/internal/cache"
	"kache/internal/dht"
	"kache/internal/node"
)

func main() {
	apiAddr := flag.String("api", "127.0.0.1:8080", "Address configuration binding for local control operations")
	cacheDir := flag.String("cache", "./dcdn_cache", "Target directory system path used for disk allocations")
	cacheRetention := flag.Duration("cache-ttl", 24*time.Hour, "Default time-to-live duration for cached assets (e.g., 24h, 72h)")
	cacheSize := flag.Int64("cache-size", 500*1024*1024, "Maximum cache capacity in bytes (e.g., 524288000 for 500MB)")
	p2pPort := flag.Int("p2p-port", 0, "Port for p2p swarm connections (0 triggers auto-assign)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheCfg := cache.Config{
		CacheDir:    *cacheDir,
		MaxCapacity: *cacheSize,      // 500MB hard limit boundary
		DefaultTTL:  *cacheRetention, // 24 hours default lifespan for cached items
		CleanPeriod: 5 * time.Minute,
	}

	storage, err := cache.NewLocalCacheManager(ctx, cacheCfg)
	if err != nil {
		log.Fatalf("Critical system fault constructing physical cache blocks: %v", err)
	}

	hostInstance, discoveryService, err := node.MakeHost(*p2pPort)
	if err != nil {
		log.Fatalf("Network identity assignment failures detected: %v", err)
	}

	dhtEngine, err := dht.NewDHTEngine(ctx, hostInstance)
	if err != nil {
		log.Fatalf("Failed to register Kademlia core structures: %v", err)
	}

	controlServer := api.NewAPIServer(hostInstance, dhtEngine, storage)
	go func() {
		log.Printf("[Initialization Master] Upstream API engine binding directly to target %s", *apiAddr)
		if err := controlServer.Start(*apiAddr); err != nil {
			log.Fatalf("API transport layer crashed: %v", err)
		}
	}()

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	<-stopSignal

	log.Println("[Shutdown Runtime] Tearing down transport layers...")
	if discoveryService != nil {
		_ = discoveryService.Close()
	}
	_ = dhtEngine.Kademlia.Close()
	_ = hostInstance.Close()
}
