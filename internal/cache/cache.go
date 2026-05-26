// Package cache - implements a local disk-based caching layer for a CDN node, managing asset storage, retrieval, and retention policies with LRU eviction and background sweeping mechanisms.
package cache

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	CacheDir    string
	MaxCapacity int64
	DefaultTTL  time.Duration
	CleanPeriod time.Duration
	SecretKey   []byte
}

type CacheItem struct {
	CID        string
	Path       string
	Size       int64
	LastAccess time.Time
	ExpiresAt  time.Time
}

func (item *CacheItem) IsExpired() bool {
	return time.Now().After(item.ExpiresAt)
}

type LocalCacheManager struct {
	mu          sync.RWMutex
	cfg         Config
	currentSize int64
	registry    map[string]*CacheItem
}

func (cm *LocalCacheManager) encrypt(plainText []byte) ([]byte, error) {
	if len(cm.cfg.SecretKey) != 32 {
		return nil, errors.New("[Cache Engine] Encryption error: SecretKey must be 32 bytes for AES-256")
	}

	block, err := aes.NewCipher(cm.cfg.SecretKey)
	if err != nil {
		return nil, errors.New("[Cache Engine] Encryption error: failed to create cipher block")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("[Cache Engine] Encryption error: failed to create GCM cipher")
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.New("[Cache Engine] Encryption error: failed to generate nonce")
	}

	return gcm.Seal(nonce, nonce, plainText, nil), nil
}

func (cm *LocalCacheManager) decrypt(cipherText []byte) ([]byte, error) {
	if len(cm.cfg.SecretKey) != 32 {
		return nil, errors.New("[Cache Engine] Decryption error: SecretKey must be 32 bytes for AES-256")
	}

	block, err := aes.NewCipher(cm.cfg.SecretKey)
	if err != nil {
		return nil, errors.New("[Cache Engine] Decryption error: failed to create cipher block")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("[Cache Engine] Decryption error: failed to create GCM cipher")
	}

	nonceSize := gcm.NonceSize()
	if len(cipherText) < nonceSize {
		return nil, errors.New("[Cache Engine] Decryption error: cipherText too short")
	}

	nonce, actualCipherText := cipherText[:nonceSize], cipherText[nonceSize:]
	return gcm.Open(nil, nonce, actualCipherText, nil)
}

func NewLocalCacheManager(ctx context.Context, cfg Config) (*LocalCacheManager, error) {
	if cfg.CacheDir == "" {
		return nil, errors.New("[Cache Engine] Invalid storage configuration: CacheDir cannot be empty")
	}
	if cfg.MaxCapacity <= 0 {
		return nil, errors.New("[Cache Engine] Invalid storage configuration: MaxCapacity must be greater than 0")
	}
	if cfg.CleanPeriod <= 0 {
		cfg.CleanPeriod = 5 * time.Minute
	}

	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("[Cache Engine] Failed to mount physical cache directory layout: %w", err)
	}

	cm := &LocalCacheManager{
		cfg:      cfg,
		registry: make(map[string]*CacheItem),
	}

	go cm.startJanitor(ctx)

	return cm, nil
}

func (cm *LocalCacheManager) Put(cid string, data []byte, customTTL time.Duration) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	itemSize := int64(len(data))
	if itemSize > cm.cfg.MaxCapacity {
		return fmt.Errorf("[Cache Engine] Rejected: object size (%d bytes) exceeds total node capability allocation (%d bytes)", itemSize, cm.cfg.MaxCapacity)
	}

	for cm.currentSize+itemSize > cm.cfg.MaxCapacity {
		log.Printf("[Cache Engine] Storage threshold reached (%d/%d bytes). Initializing eviction pass...", cm.currentSize, cm.cfg.MaxCapacity)
		if !cm.evictBestCandidate() {
			return errors.New("[Cache Engine] Write failure: capacity sub-routines aborted due to locked data assertions")
		}
	}

	encryptedData, err := cm.encrypt(data)
	if err != nil {
		return fmt.Errorf("[Cache Engine] Failed to encrypt data for CID '%s': %w", cid, err)
	}

	filePath := filepath.Join(cm.cfg.CacheDir, cid)
	if err := os.WriteFile(filePath, encryptedData, 0o600); err != nil {
		return fmt.Errorf("[Cache Engine] Failed to write block stream to storage node: %w", err)
	}

	ttl := cm.cfg.DefaultTTL
	if customTTL > 0 {
		ttl = customTTL
	}

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

func (cm *LocalCacheManager) Get(cid string) ([]byte, error) {
	cm.mu.RLock()
	item, exists := cm.registry[cid]
	if !exists {
		cm.mu.RUnlock()
		return nil, errors.New("[Cache Engine] Cache miss: content signature absent from cache system")
	}

	if item.IsExpired() {
		cm.mu.RUnlock()

		cm.mu.Lock()
		cm.removeAssetFile(cid)
		cm.mu.Unlock()

		return nil, errors.New("[Cache Engine] Cache miss: content matches retention expiration signatures")
	}

	item.LastAccess = time.Now()
	cm.mu.RUnlock()

	encryptedData, err := os.ReadFile(item.Path)
	if err != nil {
		return nil, fmt.Errorf("[Cache Engine] Failed to open cached object data frame: %w", err)
	}

	data, err := cm.decrypt(encryptedData)
	if err != nil {
		return nil, fmt.Errorf("[Cache Engine] Failed to decrypt cached data for CID '%s': %w", cid, err)
	}

	return data, nil
}

func (cm *LocalCacheManager) Status() (int64, int64, int) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.currentSize, cm.cfg.MaxCapacity, len(cm.registry)
}

func (cm *LocalCacheManager) evictBestCandidate() bool {
	var bestCandidate *CacheItem
	var oldestAccess time.Time
	foundCandidate := false

	for _, item := range cm.registry {
		if item.IsExpired() {
			cm.removeAssetFile(item.CID)
			return true
		}

		if !foundCandidate || item.LastAccess.Before(oldestAccess) {
			oldestAccess = item.LastAccess
			bestCandidate = item
			foundCandidate = true
		}
	}

	if foundCandidate && bestCandidate != nil {
		log.Printf("[Cache Engine] Evicting asset '%s' via LRU (Last accessed: %s)", bestCandidate.CID, bestCandidate.LastAccess.Format(time.Kitchen))
		cm.removeAssetFile(bestCandidate.CID)
		return true
	}

	return false
}

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

func (cm *LocalCacheManager) startJanitor(ctx context.Context) {
	ticker := time.NewTicker(cm.cfg.CleanPeriod)
	defer ticker.Stop()

	log.Printf("[Cache Engine] Automated custodian worker active. Scan interval: %v", cm.cfg.CleanPeriod)

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
				log.Printf("[Cache Engine] Sweeper cycle complete. Cleaned %d expired assets out of %d active nodes.", expiredCounter, initialCount)
			}
			cm.mu.Unlock()

		case <-ctx.Done():
			log.Println("[Cache Engine] Deactivating background sweeping channels safely...")
			return
		}
	}
}
