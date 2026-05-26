package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
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
	rootCACertPath := flag.String("root-ca", "", "File path to PEM-encoded root CA certificate for cluster authentication")
	nodeCACertPath := flag.String("node-cert", "", "File path to PEM-encoded node certificate for cluster authentication")
	nodeCAKeyPath := flag.String("node-key", "", "File path to PEM-encoded node private key for cluster authentication")
	flag.Parse()

	// Enforce credential presence before proceeding with system initialization
	if *rootCACertPath == "" || *nodeCACertPath == "" || *nodeCAKeyPath == "" {
		log.Fatalf("Critical Configuration Failure: Security fields --root-ca, --node-cert, and --node-key are required flags.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheCfg := cache.Config{
		CacheDir:    *cacheDir,
		MaxCapacity: *cacheSize,      // 500MB hard limit boundary
		DefaultTTL:  *cacheRetention, // 24 hours default lifespan for cached items
		CleanPeriod: 5 * time.Minute,
	}

	rootCACert, nodeCert, nodeKey, err := loadAuthCredentials(*rootCACertPath, *nodeCACertPath, *nodeCAKeyPath)
	if err != nil {
		log.Fatalf("Authentication credential loading failure: %v", err)
	}

	authCfg := &node.ClusterAuthenticator{
		RootCACert: rootCACert,
		NodeCert:   nodeCert,
		NodeKey:    nodeKey,
	}

	storage, err := cache.NewLocalCacheManager(ctx, cacheCfg)
	if err != nil {
		log.Fatalf("Critical system fault constructing physical cache blocks: %v", err)
	}

	hostInstance, discoveryService, err := node.MakeHost(*p2pPort, authCfg)
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

// loadAuthCredentials reads and decodes the cryptographic PEM assets from the host filesystem
func loadAuthCredentials(rootCAPath, nodeCertPath, nodeKeyPath string) (*x509.Certificate, *x509.Certificate, *rsa.PrivateKey, error) {
	// 1. Load and Parse Root CA Certificate
	rootPEM, err := os.ReadFile(rootCAPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read root CA file: %w", err)
	}
	rootBlock, _ := pem.Decode(rootPEM)
	if rootBlock == nil || rootBlock.Type != "CERTIFICATE" {
		return nil, nil, nil, errors.New("invalid or missing root CA certificate PEM block")
	}
	rootCert, err := x509.ParseCertificate(rootBlock.Bytes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse root CA x509 structure: %w", err)
	}

	// 2. Load and Parse Node Certificate
	nodePEM, err := os.ReadFile(nodeCertPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read node certificate file: %w", err)
	}
	nodeBlock, _ := pem.Decode(nodePEM)
	if nodeBlock == nil || nodeBlock.Type != "CERTIFICATE" {
		return nil, nil, nil, errors.New("invalid or missing node certificate PEM block")
	}
	// FIXED: Parse the DER bytes into a *x509.Certificate object
	nodeCert, err := x509.ParseCertificate(nodeBlock.Bytes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse node x509 structure: %w", err)
	}

	// 3. Load and Decode Node Private Key
	keyPEM, err := os.ReadFile(nodeKeyPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read node private key file: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, nil, errors.New("invalid or missing private key PEM block")
	}

	var privateKey *rsa.PrivateKey
	switch keyBlock.Type {
	case "RSA PRIVATE KEY": // PKCS#1 Format
		privateKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to parse PKCS1 RSA private key: %w", err)
		}
	case "PRIVATE KEY": // PKCS#8 Format
		parsedKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to parse PKCS8 private key: %w", err)
		}
		var ok bool
		privateKey, ok = parsedKey.(*rsa.PrivateKey)
		if !ok {
			return nil, nil, nil, errors.New("loaded key is valid PKCS8, but it is not an RSA key")
		}
	default:
		return nil, nil, nil, fmt.Errorf("unsupported private key PEM label type: %s", keyBlock.Type)
	}

	return rootCert, nodeCert, privateKey, nil
}
