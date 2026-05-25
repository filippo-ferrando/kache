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
	p2pPort := flag.Int("p2p-port", 0, "Port for p2p swarm connections (0 triggers auto-assign)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Storage setup: Pass the context and construct the full Config structure
	cacheCfg := cache.Config{
		CacheDir:    *cacheDir,
		MaxCapacity: 500 * 1024 * 1024, // 500MB hard limit boundary
		DefaultTTL:  24 * time.Hour,
		CleanPeriod: 5 * time.Minute,
	}

	storage, err := cache.NewLocalCacheManager(ctx, cacheCfg)
	if err != nil {
		log.Fatalf("Critical system fault constructing physical cache blocks: %v", err)
	}

	// 2. Network setup: Pass the p2pPort integer to fulfill MakeHost(int)
	hostInstance, discoveryService, err := node.MakeHost(*p2pPort)
	if err != nil {
		log.Fatalf("Network identity assignment failures detected: %v", err)
	}

	// 3. Routing engine instantiation
	dhtEngine, err := dht.NewDHTEngine(ctx, hostInstance)
	if err != nil {
		log.Fatalf("Failed to register Kademlia core structures: %v", err)
	}

	// 4. API wiring
	controlServer := api.NewAPIServer(hostInstance, dhtEngine, storage)
	go func() {
		log.Printf("[Initialization Master] Upstream API engine binding directly to target %s", *apiAddr)
		if err := controlServer.Start(*apiAddr); err != nil {
			log.Fatalf("API transport layer crashed: %v", err)
		}
	}()

	// Graceful intercept
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
