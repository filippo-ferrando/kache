package api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"kache/internal/cache"
	"kache/internal/dht"
	"kache/pkg/protocol"

	"github.com/gin-gonic/gin"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	p2pProtocol "github.com/libp2p/go-libp2p/core/protocol"
)

const DataProtocolID = p2pProtocol.ID("/mini-dcdn/data/1.0.0")

type APIServer struct {
	host  host.Host
	dht   *dht.DHTEngine
	cache *cache.LocalCacheManager
}

func NewAPIServer(h host.Host, d *dht.DHTEngine, c *cache.LocalCacheManager) *APIServer {
	server := &APIServer{host: h, dht: d, cache: c}
	h.SetStreamHandler(DataProtocolID, server.handleIncomingP2PStream)
	return server
}

func (s *APIServer) Start(listenAddr string) error {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.GET("/status", s.handleStatus)
	r.GET("/content/list", s.handleContentList)
	r.GET("/content/stream/:cid", s.handleStreamToClient) // NEW: Streaming payload endpoint
	r.POST("/content/advertise", s.handleAdvertise)
	r.POST("/content/download", s.handleDownload)
	r.POST("/content/upload", s.handleUpload)

	return http.ListenAndServe(listenAddr, r)
}

func (s *APIServer) handleStatus(c *gin.Context) {
	addrs := s.host.Addrs()
	addrStrings := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrings[i] = a.String()
	}

	peerList := s.host.Peerstore().Peers()
	swarmPeers := make([]string, 0)
	for _, p := range peerList {
		if p != s.host.ID() {
			swarmPeers = append(swarmPeers, p.String())
		}
	}

	c.JSON(http.StatusOK, protocol.StatusResponse{
		PeerID:    s.host.ID().String(),
		Addresses: addrStrings,
		Swarm:     swarmPeers,
	})
}

// ENHANCED: Now maps active Kademlia DHT providers for each listed item
func (s *APIServer) handleContentList(c *gin.Context) {
	items := s.cache.ListRegistry()
	fileList := make([]protocol.FileInfo, 0, len(items))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, item := range items {
		// Start with itself since it exists in the local node cache registry
		providersList := []string{s.host.ID().String()}

		// Query Kademlia to see if external nodes have replicated this item
		if remoteProviders, err := s.dht.LocateProviders(ctx, item.CID); err == nil {
			for _, p := range remoteProviders {
				providersList = append(providersList, p.ID.String())
			}
		}

		fileList = append(fileList, protocol.FileInfo{
			CID:        item.CID,
			Size:       item.Size,
			ExpiresAt:  item.ExpiresAt.Format(time.RFC3339),
			LastAccess: item.LastAccess.Format(time.RFC3339),
			Providers:  providersList,
		})
	}
	c.JSON(http.StatusOK, fileList)
}

// NEW: Streams data from the CDN directly back to your local laptop filesystem
func (s *APIServer) handleStreamToClient(c *gin.Context) {
	targetCID := c.Param("cid")
	if targetCID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Content identity hash parameter missing"})
		return
	}

	// 1. Check local cache first
	data, err := s.cache.Get(targetCID)
	if err != nil {
		// 2. Cache Miss: Fetch data from the P2P network first
		log.Printf("[API Gateway] Cache miss for client download request '%s'. Fetching from network...", targetCID)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		providers, err := s.dht.LocateProviders(ctx, targetCID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Hash missing from network indexes: %v", err)})
			return
		}

		stream, err := s.host.NewStream(ctx, providers[0].ID, DataProtocolID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("P2P stream failed: %v", err)})
			return
		}
		defer stream.Close()

		_, _ = stream.Write([]byte(targetCID + "\n"))
		fetchedData, err := io.ReadAll(stream)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Network data transfer failure"})
			return
		}

		// Save to edge cache allocations so this node behaves as an active replicator
		if err := s.cache.Put(targetCID, fetchedData, 0); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal write storage constraints hit"})
			return
		}
		_ = s.dht.Advertise(ctx, targetCID)
		data = fetchedData
	}

	// 3. Pipe binary octet-stream back down to your laptop client execution loop
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", targetCID))
	c.Data(http.StatusOK, "application/octet-stream", data)
}

func (s *APIServer) handleAdvertise(c *gin.Context) {
	var req protocol.AdvertiseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, err := os.ReadFile(req.LocalPath)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("failed to read local file: %v", err)})
		return
	}

	if err := s.cache.Put(req.CID, data, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("cache rejection: %v", err)})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.dht.Advertise(ctx, req.CID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("DHT routing failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "File indexed and advertised to swarm"})
}

// Updated handleDownload that utilizes the "FETCH\n" wire protocol header
func (s *APIServer) handleDownload(c *gin.Context) {
	var req protocol.DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 1. If it's already in our local cache, return a cache hit early
	if data, err := s.cache.Get(req.CID); err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "hit", "message": "File already present in local cache", "size": len(data)})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 2. Query Kademlia DHT to find which external peers have advertised this hash
	providers, err := s.dht.LocateProviders(ctx, req.CID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("content not found in network: %v", err)})
		return
	}

	// Select the first discovered provider from the swarm
	targetPeer := providers[0]

	// 3. Establish a direct P2P connection stream to the provider node
	stream, err := s.host.NewStream(ctx, targetPeer.ID, DataProtocolID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to connect to provider stream: %v", err)})
		return
	}
	defer stream.Close()

	log.Printf("[API Gateway] Sending FETCH wire command to peer %s for CID: %s", targetPeer.ID.ShortString(), req.CID)

	// 4. Write the upgraded wire protocol headers: FETCH\n<cid>\n
	_, err = stream.Write([]byte("FETCH\n" + req.CID + "\n"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to write command headers to peer: %v", err)})
		return
	}

	// 5. Read the streaming data payload sent back from the remote peer
	fetchedData, err := io.ReadAll(stream)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("download interrupted midway: %v", err)})
		return
	}

	// 6. Save the newly acquired file into our local cache engine
	if err := s.cache.Put(req.CID, fetchedData, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to commit downloaded file to cache: %v", err)})
		return
	}

	// 7. Automatically advertise ourselves as a new provider for this content hash
	_ = s.dht.Advertise(ctx, req.CID)

	c.JSON(http.StatusOK, gin.H{
		"status":           "success",
		"downloaded_bytes": len(fetchedData),
		"source_peer":      targetPeer.ID.String(),
	})
}

// Updated handleUpload that triggers instant replication to the swarm
func (s *APIServer) handleUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	uploadedFile, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer uploadedFile.Close()

	fileBytes, err := io.ReadAll(uploadedFile)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read upload data"})
		return
	}

	hasher := sha256.New()
	hasher.Write(fileBytes)
	generatedCID := fmt.Sprintf("%x", hasher.Sum(nil))

	// 1. Save to local storage
	if err := s.cache.Put(generatedCID, fileBytes, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 2. Advertise ownership to the Kademlia DHT routing table
	if err := s.dht.Advertise(ctx, generatedCID); err != nil {
		log.Printf("[API Warning] DHT advertise failed: %v", err)
	}

	// 3. NEW: Proactively copy this data to neighbors instantly
	go s.replicateToSwarm(generatedCID, fileBytes)

	c.JSON(http.StatusOK, gin.H{"status": "uploaded & replicated", "cid": generatedCID, "size": file.Size})
}

// Background worker that pushes files to any currently connected swarm peers
func (s *APIServer) replicateToSwarm(cid string, data []byte) {
	peers := s.host.Peerstore().Peers()
	if len(peers) <= 1 {
		log.Printf("[Replication] Node is currently alone. Skipping instant replication.")
		return
	}

	replicatedCount := 0
	for _, peerID := range peers {
		if peerID == s.host.ID() {
			continue // Skip self
		}

		log.Printf("[Replication] Proactively pushing copy of %s to peer %s", cid, peerID.ShortString())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := s.host.NewStream(ctx, peerID, DataProtocolID)
		if err != nil {
			cancel()
			continue
		}

		// Write our new wire protocol header: STORE\n<cid>\n<bytes>
		_, _ = stream.Write([]byte("STORE\n" + cid + "\n"))
		_, _ = stream.Write(data)
		stream.Close()
		cancel()

		replicatedCount++
		if replicatedCount >= 2 { // Cap replication factor to 2 neighbors for this test
			break
		}
	}
	log.Printf("[Replication] Instant replication pass complete. Pushed data to %d peers.", replicatedCount)
}

// Upgraded P2P Stream Handler capable of handling both Pulls and proactive Pushes
func (s *APIServer) handleIncomingP2PStream(stream network.Stream) {
	defer stream.Close()

	// Read the command action (either "FETCH" or "STORE")
	var command string
	_, err := fmt.Fscanln(stream, &command)
	if err != nil {
		log.Printf("[Data Plane] Failed to parse stream command header: %v", err)
		return
	}

	var requestedCID string
	_, err = fmt.Fscanln(stream, &requestedCID)
	if err != nil {
		log.Printf("[Data Plane] Failed to parse CID from stream: %v", err)
		return
	}

	switch command {
	case "FETCH":
		log.Printf("[Data Plane] Peer %s is FETCHING CID: %s", stream.Conn().RemotePeer().ShortString(), requestedCID)
		data, err := s.cache.Get(requestedCID)
		if err != nil {
			log.Printf("[Data Plane] Cache miss for requested asset %s: %v", requestedCID, err)
			return
		}
		_, _ = stream.Write(data)

	case "STORE":
		log.Printf("[Data Plane] Peer %s is pushing a proactive replication STORE for CID: %s", stream.Conn().RemotePeer().ShortString(), requestedCID)
		// Read the raw binary data being pushed by the remote peer
		pushedData, err := io.ReadAll(stream)
		if err != nil {
			log.Printf("[Data Plane] Proactive replication stream interrupted: %v", err)
			return
		}

		// Save it to our local cache completely unsolicited!
		if err := s.cache.Put(requestedCID, pushedData, 0); err != nil {
			log.Printf("[Data Plane] Failed to save replicated payload: %v", err)
			return
		}
		// Advertise that we now hold a copy too
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.dht.Advertise(ctx, requestedCID)
		cancel()
	}
}
