package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	cacheKeyPath := flag.String("cache-key", "", "File path to a 32-byte key for encrypting cached data (optional, will be auto-generated if not provided)")
	p2pPort := flag.Int("p2p-port", 0, "Port for p2p swarm connections (0 triggers auto-assign)")
	rootCACertPath := flag.String("root-ca", "", "File path to PEM-encoded root CA certificate for cluster authentication")
	nodeCACertPath := flag.String("node-cert", "", "File path to PEM-encoded node certificate for cluster authentication")
	nodeCAKeyPath := flag.String("node-key", "", "File path to PEM-encoded node private key for cluster authentication")
	flag.Parse()

	if *rootCACertPath == "" || *nodeCACertPath == "" || *nodeCAKeyPath == "" {
		log.Fatalf("Critical Configuration Failure: Security fields --root-ca, --node-cert, and --node-key are required flags.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	secretKey, err := getOrCreateCacheKey(*cacheKeyPath)
	if err != nil {
		log.Fatalf("failed to obtain cache encryption key: %s", err)
	}

	cacheCfg := cache.Config{
		CacheDir:    *cacheDir,
		MaxCapacity: *cacheSize,
		DefaultTTL:  *cacheRetention,
		CleanPeriod: 5 * time.Minute,
		SecretKey:   secretKey,
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

func loadAuthCredentials(rootCAPath, nodeCertPath, nodeKeyPath string) (*x509.Certificate, *x509.Certificate, *rsa.PrivateKey, error) {
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

	nodePEM, err := os.ReadFile(nodeCertPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read node certificate file: %w", err)
	}
	nodeBlock, _ := pem.Decode(nodePEM)
	if nodeBlock == nil || nodeBlock.Type != "CERTIFICATE" {
		return nil, nil, nil, errors.New("invalid or missing node certificate PEM block")
	}
	nodeCert, err := x509.ParseCertificate(nodeBlock.Bytes)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse node x509 structure: %w", err)
	}

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
	case "RSA PRIVATE KEY":
		privateKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to parse PKCS1 RSA private key: %w", err)
		}
	case "PRIVATE KEY":
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

func getOrCreateCacheKey(cacheDir string) ([]byte, error) {
	if cacheDir == "" {
		log.Println("[Cache Engine] Operating with an empty cache directory. Generating temporary in-memory key...")
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("failed to generate random in-memory key: %w", err)
		}
		return key, nil
	}

	keyPath := filepath.Join(cacheDir, ".cache.key")
	if data, err := os.ReadFile(keyPath); err == nil {
		if len(data) == 32 {
			return data, nil
		}
		return nil, fmt.Errorf("malformed key file found at %s (must be 32 bytes)", keyPath)
	}

	// 3. File doesn't exist yet; create the directory and save a new persistent key
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory layout: %w", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}

	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		return nil, fmt.Errorf("failed to save persistent cache key file: %w", err)
	}

	log.Printf("[Cache Engine] Persistent 32-byte master key initialized and saved to %s", keyPath)
	return key, nil
}
