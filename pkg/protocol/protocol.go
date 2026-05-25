package protocol

type StatusResponse struct {
	PeerID    string   `json:"peer_id"`
	Addresses []string `json:"addresses"`
	Swarm     []string `json:"swarm_peers"`
}

type FileInfo struct {
	CID        string   `json:"cid"`
	Size       int64    `json:"size_bytes"`
	ExpiresAt  string   `json:"expires_at"`
	LastAccess string   `json:"last_access"`
	Providers  []string `json:"providers"` // Track all network nodes caching this specific asset
}

type AdvertiseRequest struct {
	CID       string `json:"cid" binding:"required"`
	LocalPath string `json:"local_path" binding:"required"`
}

type DownloadRequest struct {
	CID string `json:"cid" binding:"required"`
}
