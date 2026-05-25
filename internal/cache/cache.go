package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config dictates storage limits, directories, and retention rules for the CDN node
type Config struct {
	CacheDir    string        // Physical directory path on disk
	MaxCapacity int64         // Hard upper boundary limit in bytes (e.g., 500 * 1024 * 1024 for 500MB)
	DefaultTTL  time.Duration // Lifespan of an asset if a specific TTL is not specified
	CleanPeriod time.Duration // Frequency at which the background janitor purges expired records
}

// CacheItem tracks system metadata for an individual asset on disk
type CacheItem struct {
	CID        string    // Content Identifier
	Path       string    // Absolute filesystem location
	Size       int64     // File allocation footprint in bytes
	LastAccess time.Time // Tracked for LRU capacity eviction calculations
	ExpiresAt  time.Time // Tracked for temporal retention policy calculations
}

// IsExpired evaluates if an item has exceeded its allowed retention timeframe
func (item *CacheItem) IsExpired() bool {
	return time.Now().After(item.ExpiresAt)
}

type LocalCacheManager struct {
	mu          sync.RWMutex
	cfg         Config
	currentSize int64
	registry    map[string]*CacheItem
}

// NewLocalCacheManager initializes the physical storage path and spins up the retention subsystem
func NewLocalCacheManager(ctx context.Context, cfg Config) (*LocalCacheManager, error) {
	if cfg.CacheDir == "" {
		return nil, errors.New("invalid storage configuration: CacheDir cannot be empty")
	}
	if cfg.MaxCapacity <= 0 {
		return nil, errors.New("invalid storage configuration: MaxCapacity must be greater than 0")
	}
	if cfg.CleanPeriod <= 0 {
		cfg.CleanPeriod = 5 * time.Minute // Default guard fallback
	}

	// Create storage layout safely
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to mount physical cache directory layout: %w", err)
	}

	cm := &LocalCacheManager{
		cfg:      cfg,
		registry: make(map[string]*CacheItem),
	}

	// Start the background retention custodian worker
	go cm.startJanitor(ctx)

	return cm, nil
}

// Put commits data to disk, managing capacity constraints via LRU eviction if necessary
func (cm *LocalCacheManager) Put(cid string, data []byte, customTTL time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	itemSize := int64(len(data))
	if itemSize > cm.cfg.MaxCapacity {
		return fmt.Errorf("rejected: object size (%d bytes) exceeds total node capability allocation (%d bytes)", itemSize, cm.cfg.MaxCapacity)
	}

	// 1. Enforce Capacity Guardrails via LRU Eviction Loops
	for cm.currentSize+itemSize > cm.cfg.MaxCapacity {
		log.Printf("[Cache Engine] Storage threshold reached (%d/%d bytes). Initializing eviction pass...", cm.currentSize, cm.cfg.MaxCapacity)
		if !cm.evictBestCandidate() {
			return errors.New("write failure: capacity sub-routines aborted due to locked data assertions")
		}
	}

	// 2. Commit asset stream securely to disk
	filePath := filepath.Join(cm.cfg.CacheDir, cid)
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write block stream to storage node: %w", err)
	}

	// 3. Formulate asset retention lifespan values
	ttl := cm.cfg.DefaultTTL
	if customTTL > 0 {
		ttl = customTTL
	}

	// 4. Update memory mapping registers
	cm.registry[cid] = &CacheItem{
		CID:        cid,
		Path:       filePath,
		Size:       itemSize,
		LastAccess: time.Now(),
		ExpiresAt:  time.Now().Add(ttl),
	}
	cm.currentSize += itemSize

	log.Printf("[Cache Engine] Stored asset '%s' (%d bytes) | Retention TTL: %v", cid, itemSize, ttl)
	return nil
}

// Get handles high-concurrency read operations while conducting passive, on-the-fly expiration validation
func (cm *LocalCacheManager) Get(cid string) ([]byte, error) {
	// Acquire a read lock to quickly evaluate existence and retention health
	cm.mu.RLock()
	item, exists := cm.registry[cid]
	if !exists {
		cm.mu.RUnlock()
		return nil, errors.New("cache miss: content signature absent from cache system")
	}

	// Passive (Lazy) Exiction Check: If the asset expired but the background sweeper hasn't run yet
	if item.IsExpired() {
		cm.mu.RUnlock()

		// Upgrade lock to write permission to safely clean up the expired asset
		cm.mu.Lock()
		cm.removeAssetFile(cid)
		cm.mu.Unlock()

		return nil, errors.New("cache miss: content matches retention expiration signatures")
	}

	// Update the access stamp for LRU ordering
	item.LastAccess = time.Now()
	cm.mu.RUnlock()

	// Read from disk without holding global locks to prevent blocking concurrent lookups
	data, err := os.ReadFile(item.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open cached object data frame: %w", err)
	}

	return data, nil
}

// Status returns runtime cache metrics for monitoring via the CLI
func (cm *LocalCacheManager) Status() (int64, int64, int) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.currentSize, cm.cfg.MaxCapacity, len(cm.registry)
}

// evictBestCandidate executes combined strategic evictions (purges expired entries first, then drops oldest LRU)
func (cm *LocalCacheManager) evictBestCandidate() bool {
	var bestCandidate *CacheItem
	var oldestAccess time.Time
	foundCandidate := false

	// Pass 1: Search for an expired item to evict immediately
	for _, item := range cm.registry {
		if item.IsExpired() {
			cm.removeAssetFile(item.CID)
			return true // Immediate execution pass completed successfully
		}

		// Pass 2: Track LRU candidate if no explicitly expired metrics match
		if !foundCandidate || item.LastAccess.Before(oldestAccess) {
			oldestAccess = item.LastAccess
			bestCandidate = item
			foundCandidate = true
		}
	}

	// Execute LRU eviction step if active tracking constraints require it
	if foundCandidate && bestCandidate != nil {
		log.Printf("[Eviction Subsystem] Evicting asset '%s' via LRU (Last accessed: %s)", bestCandidate.CID, bestCandidate.LastAccess.Format(time.Kitchen))
		cm.removeAssetFile(bestCandidate.CID)
		return true
	}

	return false
}

// removeAssetFile safely handles internal registry adjustments and unlinks file targets
func (cm *LocalCacheManager) removeAssetFile(cid string) {
	item, exists := cm.registry[cid]
	if !exists {
		return
	}

	_ = os.Remove(item.Path)
	cm.currentSize -= item.Size
	delete(cm.registry, cid)
	log.Printf("[Cache Engine] Eviction finalized for: %s", cid)
}

// ListRegistry returns a decoupled snapshot array containing all non-expired cached asset metadata structures
func (cm *LocalCacheManager) ListRegistry() []CacheItem {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	list := make([]CacheItem, 0, len(cm.registry))
	for _, item := range cm.registry {
		if !item.IsExpired() {
			list = append(list, *item)
		}
	}
	return list
}

// startJanitor runs as a background worker to enforce active data retention
func (cm *LocalCacheManager) startJanitor(ctx context.Context) {
	ticker := time.NewTicker(cm.cfg.CleanPeriod)
	defer ticker.Stop()

	log.Printf("[Retention Subsystem] Automated custodian worker active. Scan interval: %v", cm.cfg.CleanPeriod)

	for {
		select {
		case <-ticker.C:
			cm.mu.Lock()
			expiredCounter := 0
			initialCount := len(cm.registry)

			for cid, item := range cm.registry {
				if item.IsExpired() {
					cm.removeAssetFile(cid)
					expiredCounter++
				}
			}

			if expiredCounter > 0 {
				log.Printf("[Retention Subsystem] Sweeper cycle complete. Cleaned %d expired assets out of %d active nodes.", expiredCounter, initialCount)
			}
			cm.mu.Unlock()

		case <-ctx.Done():
			log.Println("[Retention Subsystem] Deactivating background sweeping channels safely...")
			return
		}
	}
}
