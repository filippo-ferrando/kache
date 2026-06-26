// Package api - implements the HTTP API server for the Kache edge caching system. It provides endpoints for clients to interact with the cache and orchestrates P2P data transfers and DHT operations under the hood. The API server also handles incoming P2P streams for data fetching and replication, ensuring seamless integration between the HTTP interface and the libp2p network layer.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"kache/internal/cache"
	"kache/internal/dht"
	"kache/pkg/protocol"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	p2pProtocol "github.com/libp2p/go-libp2p/core/protocol"
)

const DataProtocolID = p2pProtocol.ID("/kache/data/1.0.0")

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
	r.Use(cors.Default())
	r.GET("/status", s.handleStatus)
	r.GET("/content/list", s.handleContentList)
	r.GET("/content/stream/:cid", s.handleStreamToClient)
	r.GET("/swarm/matrix", s.handleSwarmMatrix)
	r.POST("/content/advertise", s.handleAdvertise)
	r.POST("/content/download", s.handleDownload)
	r.POST("/content/upload", s.handleUpload)

	return http.ListenAndServe(listenAddr, r)
}

func (s *APIServer) sortProvidersByLatency(providers []peer.AddrInfo) {
	sort.Slice(providers, func(i, j int) bool {
		latencyI := s.host.Peerstore().LatencyEWMA(providers[i].ID)
		latencyJ := s.host.Peerstore().LatencyEWMA(providers[j].ID)

		if latencyI == 0 {
			latencyI = time.Hour
		}
		if latencyJ == 0 {
			latencyJ = time.Hour
		}

		return latencyI < latencyJ
	})
}

func (s *APIServer) sortPeersByLatency(peers []peer.ID) {
	sort.Slice(peers, func(i, j int) bool {
		latencyI := s.host.Peerstore().LatencyEWMA(peers[i])
		latencyJ := s.host.Peerstore().LatencyEWMA(peers[j])

		if latencyI == 0 {
			latencyI = time.Hour
		}
		if latencyJ == 0 {
			latencyJ = time.Hour
		}

		return latencyI < latencyJ
	})
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

func (s *APIServer) handleContentList(c *gin.Context) {
	items := s.cache.ListRegistry()
	fileList := make([]protocol.FileInfo, 0, len(items))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, item := range items {
		providersList := []string{s.host.ID().String()}

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

func (s *APIServer) handleStreamToClient(c *gin.Context) {
	targetCID := c.Param("cid")
	if targetCID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Content identity hash parameter missing"})
		return
	}

	data, err := s.cache.Get(targetCID)
	if err != nil {
		log.Printf("[API Gateway] Cache miss for client download request '%s'. Fetching from network...", targetCID)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		providers, err := s.dht.LocateProviders(ctx, targetCID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Hash missing from network indexes: %v", err)})
			return
		}

		s.sortProvidersByLatency(providers)

		log.Printf("[API Gateway] Routing stream lookup request to physically closest provider: %s", providers[0].ID.ShortString())

		stream, err := s.host.NewStream(ctx, providers[0].ID, DataProtocolID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("P2P stream failed: %v", err)})
			return
		}
		defer stream.Close()

		_, err = stream.Write([]byte("FETCH\n" + targetCID + "\n"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to push unified command wire payload"})
			return
		}

		fetchedData, err := io.ReadAll(stream)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Network data transfer failure"})
			return
		}

		if err := s.cache.Put(targetCID, fetchedData, 0); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal write storage constraints hit"})
			return
		}
		_ = s.dht.Advertise(ctx, targetCID)
		data = fetchedData
	}

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

func (s *APIServer) handleDownload(c *gin.Context) {
	var req protocol.DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data, err := s.cache.Get(req.CID); err == nil {
		c.JSON(http.StatusOK, gin.H{"status": "hit", "message": "File already present in local cache", "size": len(data)})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	providers, err := s.dht.LocateProviders(ctx, req.CID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("content not found in network: %v", err)})
		return
	}

	s.sortProvidersByLatency(providers)
	targetPeer := providers[0]

	stream, err := s.host.NewStream(ctx, targetPeer.ID, DataProtocolID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to connect to provider stream: %v", err)})
		return
	}
	defer stream.Close()

	log.Printf("[API Gateway] Sending FETCH wire command to nearest edge peer %s for CID: %s", targetPeer.ID.ShortString(), req.CID)

	_, err = stream.Write([]byte("FETCH\n" + req.CID + "\n"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to write command headers to peer: %v", err)})
		return
	}

	fetchedData, err := io.ReadAll(stream)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("download interrupted midway: %v", err)})
		return
	}

	if err := s.cache.Put(req.CID, fetchedData, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to commit downloaded file to cache: %v", err)})
		return
	}

	_ = s.dht.Advertise(ctx, req.CID)

	c.JSON(http.StatusOK, gin.H{
		"status":           "success",
		"downloaded_bytes": len(fetchedData),
		"source_peer":      targetPeer.ID.String(),
	})
}

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

	if err := s.cache.Put(generatedCID, fileBytes, 0); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.dht.Advertise(ctx, generatedCID); err != nil {
		log.Printf("[API Warning] DHT advertise failed: %v", err)
	}

	go s.replicateToSwarm(generatedCID, fileBytes)

	c.JSON(http.StatusOK, gin.H{"status": "uploaded & replicated", "cid": generatedCID, "size": file.Size})
}

func (s *APIServer) replicateToSwarm(cid string, data []byte) {
	peers := s.host.Peerstore().Peers()
	if len(peers) <= 1 {
		log.Printf("[Replication] Node is currently alone. Skipping instant replication.")
		return
	}

	closestPeers, err := s.dht.GetClosestPeers(context.Background(), cid)
	if err != nil {
		log.Printf("[Kademlia replication error] Failed to find closest peers for replication: %v", err)
		return
	}

	s.sortPeersByLatency(closestPeers)

	replicatedCount := 0
	for _, peerID := range closestPeers {
		if peerID == s.host.ID() {
			continue
		}

		log.Printf("[Replication] Proactively pushing copy of %s to physically nearest XOR peer %s", cid, peerID.ShortString())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := s.host.NewStream(ctx, peerID, DataProtocolID)
		if err != nil {
			cancel()
			continue
		}

		_, _ = stream.Write([]byte("STORE\n" + cid + "\n"))
		_, _ = stream.Write(data)
		stream.Close()
		cancel()

		replicatedCount++
		if replicatedCount >= 2 {
			break
		}
	}
	log.Printf("[Replication] Instant replication pass complete. Pushed data to %d peers.", replicatedCount)
}

func (s *APIServer) handleIncomingP2PStream(stream network.Stream) {
	defer stream.Close()

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
		pushedData, err := io.ReadAll(stream)
		if err != nil {
			log.Printf("[Data Plane] Proactive replication stream interrupted: %v", err)
			return
		}

		if err := s.cache.Put(requestedCID, pushedData, 0); err != nil {
			log.Printf("[Data Plane] Failed to save replicated payload: %v", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.dht.Advertise(ctx, requestedCID)
		cancel()

	case "MATRIX":
		log.Printf("[Topology] Peer %s requested our local network latency matrix map", stream.Conn().RemotePeer().ShortString())

		localPeerstoreViews := make(map[string]string)
		for _, p := range s.host.Network().Peers() {
			ewma := s.host.Peerstore().LatencyEWMA(p)
			if ewma > 0 {
				localPeerstoreViews[p.String()] = ewma.String()
			} else {
				localPeerstoreViews[p.String()] = "Connected/Unmeasured"
			}
		}

		encoder := json.NewEncoder(stream)
		_ = encoder.Encode(localPeerstoreViews)
		return
	}
}

func (s *APIServer) handleSwarmMatrix(c *gin.Context) {
	peers := s.host.Network().Peers()
	responseMatrix := make(map[string]protocol.PeerLatencyInfo)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, peerID := range peers {
		localEWMA := s.host.Peerstore().LatencyEWMA(peerID)
		latencyStr := "Unmeasured"
		if localEWMA > 0 {
			latencyStr = localEWMA.String()
		}

		info := protocol.PeerLatencyInfo{
			PeerID:        peerID.String(),
			LatencyFromUs: latencyStr,
			TargetViews:   make(map[string]string),
		}

		stream, err := s.host.NewStream(ctx, peerID, DataProtocolID)
		if err != nil {
			info.TargetViews["status"] = "Offline/Unreachable for matrix query"
			responseMatrix[peerID.String()] = info
			continue
		}

		_, _ = stream.Write([]byte("MATRIX\nAll\n"))

		var remoteViews map[string]string
		decoder := json.NewDecoder(stream)
		if err := decoder.Decode(&remoteViews); err == nil {
			info.TargetViews = remoteViews
		} else {
			info.TargetViews["error"] = "Failed to parse remote matrix dataset"
		}
		stream.Close()

		responseMatrix[peerID.String()] = info
	}

	c.JSON(http.StatusOK, protocol.SwarmMatrixResponse{
		LocalNodeID:  s.host.ID().String(),
		ClusterNodes: responseMatrix,
	})
}
