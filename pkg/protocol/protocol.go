// Package protocol - interface definition for the API endpoints and data structures used in the communication between the laptop and the local node.
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
	Providers  []string `json:"providers"`
}

type AdvertiseRequest struct {
	CID       string `json:"cid" binding:"required"`
	LocalPath string `json:"local_path" binding:"required"`
}

type DownloadRequest struct {
	CID string `json:"cid" binding:"required"`
}

type PeerLatencyInfo struct {
	PeerID        string            `json:"peer_id"`
	LatencyFromUs string            `json:"latency_from_local_node"`
	TargetViews   map[string]string `json:"latencies_to_other_peers"`
}

type SwarmMatrixResponse struct {
	LocalToDaemonLatency string                     `json:"local_to_daemon_latency"`
	LocalNodeID          string                     `json:"local_node_id"`
	ClusterNodes         map[string]PeerLatencyInfo `json:"cluster_nodes"`
}
