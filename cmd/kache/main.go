package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"kache/pkg/protocol"

	"github.com/spf13/cobra"
)

var apiTarget string

func main() {
	rootCmd := &cobra.Command{
		Use:   "kachectl",
		Short: "kachectl controls and interacts with your decentralized CDN edge network",
	}

	rootCmd.PersistentFlags().StringVar(&apiTarget, "endpoint", "http://127.0.0.1:8080", "Target URL of the local dCDN node control API")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Fetch current connectivity status metrics from pointed cdn node",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(apiTarget + "/status")
			if err != nil {
				fmt.Printf("[Error] Target node daemon is unreachable at %s: %v\n", apiTarget, err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println("=== Swarm Network Status ===")
			fmt.Println(formatJSON(body))
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all files currently pinned or cached on poitend CDN edge node",
		Run: func(cmd *cobra.Command, args []string) {
			resp, err := http.Get(apiTarget + "/content/list")
			if err != nil {
				fmt.Printf("[Error] Failed to connect to node daemon: %v\n", err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println("=== Locally Cached/Indexed Contents ===")
			fmt.Println(formatJSON(body))
		},
	}

	advertiseCmd := &cobra.Command{
		Use:   "advertise [cid] [local_file_path]",
		Short: "Index a file already present on the node's machine and announce it to the Kademlia DHT",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			targetCID := args[0]
			filePath := args[1]

			payload, _ := json.Marshal(map[string]string{
				"cid":        targetCID,
				"local_path": filePath,
			})

			resp, err := http.Post(apiTarget+"/content/advertise", "application/json", bytes.NewBuffer(payload))
			if err != nil {
				fmt.Printf("[Error] Network transport failed: %v\n", err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println(formatJSON(body))
		},
	}

	downloadCmd := &cobra.Command{
		Use:   "download [cid]",
		Short: "Instruct the CDN network node to locate, pull, and cache a file locally",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			targetCID := args[0]

			payload, _ := json.Marshal(map[string]string{
				"cid": targetCID,
			})

			fmt.Printf("[Action] Querying Kademlia DHT for provider nodes hosting hash: %s...\n", targetCID)
			resp, err := http.Post(apiTarget+"/content/download", "application/json", bytes.NewBuffer(payload))
			if err != nil {
				fmt.Printf("[Error] Failed to initiate download command: %v\n", err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			fmt.Println("=== Fetch Execution Summary ===")
			fmt.Println(formatJSON(body))
		},
	}

	uploadCmd := &cobra.Command{
		Use:   "upload [file_path]",
		Short: "Upload local file into the CDN network",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			filePath := args[0]

			file, err := os.Open(filePath)
			if err != nil {
				fmt.Printf("[File Error] Unable to open target local file: %v\n", err)
				return
			}
			defer file.Close()

			body := &bytes.Buffer{}
			writer := multipart.NewWriter(body)

			part, err := writer.CreateFormFile("file", filepath.Base(filePath))
			if err != nil {
				fmt.Printf("[Form Error] Failed to generate form-data headers: %v\n", err)
				return
			}

			_, err = io.Copy(part, file)
			if err != nil {
				fmt.Printf("[Payload Error] Failed to stream file data to buffer: %v\n", err)
				return
			}
			writer.Close()

			req, err := http.NewRequest("POST", apiTarget+"/content/upload", body)
			if err != nil {
				fmt.Printf("[Request Error] Failed to prepare HTTP client request: %v\n", err)
				return
			}
			req.Header.Set("Content-Type", writer.FormDataContentType())

			fmt.Println("[Action] Streaming file payload across the control interface gateway...")
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("[Network Error] Node failed to accept inbound data multi-part: %v\n", err)
				return
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(resp.Body)
			fmt.Println("=== Ingestion / Publication Response ===")
			fmt.Println(formatJSON(respBody))
		},
	}

	getCmd := &cobra.Command{
		Use:   "get [cid] [destination_path]",
		Short: "Download CDN file on local machine",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			targetCID := args[0]
			destPath := args[1]

			url := fmt.Sprintf("%s/content/stream/%s", apiTarget, targetCID)
			fmt.Printf("[Local Action] Initializing edge file sync stream for CID: %s...\n", targetCID)

			resp, err := http.Get(url)
			if err != nil {
				fmt.Printf("[Transport Error] Failed to connect to gateway edge node: %v\n", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errMsg, _ := io.ReadAll(resp.Body)
				fmt.Printf("[Node Refusal] Download request failed (Status %d): %s\n", resp.StatusCode, string(errMsg))
				return
			}

			outFile, err := os.Create(destPath)
			if err != nil {
				fmt.Printf("[Filesystem Error] Cannot open local output path: %v\n", err)
				return
			}
			defer outFile.Close()

			bytesWritten, err := io.Copy(outFile, resp.Body)
			if err != nil {
				fmt.Printf("[Stream Error] Network connection dropped during active collection loop: %v\n", err)
				return
			}

			fmt.Printf("[Success] Sync operation completed successfully! Saved to '%s' (%d total bytes verified)\n", destPath, bytesWritten)
		},
	}

	nodesCmd := &cobra.Command{
		Use:   "nodes",
		Short: "Print a complete latency topology map of all nodes across the CDN swarm",
		Run: func(cmd *cobra.Command, args []string) {
			startTime := time.Now()
			resp, err := http.Get(apiTarget + "/swarm/matrix")
			localLatency := time.Since(startTime)

			if err != nil {
				fmt.Printf("[Error] Unable to reach local control node: %v\n", err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			var matrix protocol.SwarmMatrixResponse
			if err := json.Unmarshal(body, &matrix); err != nil {
				fmt.Println("Raw Server Payload:", string(body))
				return
			}

			matrix.LocalToDaemonLatency = localLatency.String()

			fmt.Printf("=== CDN NETWORK WEATHER MAP ===\n\n")
			fmt.Printf("local host -> Edge Daemon [%s]: %s\n\n", matrix.LocalNodeID[:12], matrix.LocalToDaemonLatency)

			if len(matrix.ClusterNodes) == 0 {
				fmt.Println(" No external peer nodes currently registered.")
				return
			}

			for peerID, info := range matrix.ClusterNodes {
				fmt.Printf("▪ Node ID: %s\n", peerID)
				fmt.Printf("  └─ Latency from local host: %s\n", info.LatencyFromUs)
				fmt.Printf("  └─ Latency from node %s :\n", peerID)

				if len(info.TargetViews) == 0 {
					fmt.Println("     (No active outward peer views mapped)")
				}
				for targetID, rtt := range info.TargetViews {
					shortTarget := targetID
					if len(targetID) > 16 {
						shortTarget = targetID[:16] + "..."
					}
					fmt.Printf("     ├── To Peer [%s]: %s\n", shortTarget, rtt)
				}
				fmt.Println()
			}
		},
	}

	rootCmd.AddCommand(statusCmd, listCmd, advertiseCmd, downloadCmd, uploadCmd, getCmd, nodesCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func formatJSON(input []byte) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, input, "", "  "); err != nil {
		return string(input)
	}
	return prettyJSON.String()
}
