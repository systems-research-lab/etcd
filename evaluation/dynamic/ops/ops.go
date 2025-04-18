package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Endpoints   []string   `yaml:"endpoints"`
	Operation   string     `yaml:"operation"`
	Mode        string     `yaml:"mode,omitempty"`
	AddPeers    []string   `yaml:"addPeers,omitempty"`
	RemovePeers []string   `yaml:"removePeers,omitempty"`
	SubClusters [][]string `yaml:"subClusters,omitempty"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	return &cfg, err
}

func initClients(endpoints []string) (map[string]*clientv3.Client, []uint64, *clientv3.Client, map[string]uint64) {
	memberIDs := []uint64{}
	clients := make(map[string]*clientv3.Client)
	idMap := make(map[string]uint64)
	var leaderClient *clientv3.Client

	for _, ep := range endpoints {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ep},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			log.Fatalf("Failed to create client for %s: %v", ep, err)
		}

		resp, err := client.Status(context.TODO(), ep)
		if err != nil {
			log.Fatalf("Status failed for %s: %v", ep, err)
		}

		memberIDs = append(memberIDs, resp.Header.MemberId)
		idMap[ep] = resp.Header.MemberId

		if resp.Leader == resp.Header.MemberId {
			leaderClient = client
		}
		clients[ep] = client
	}
	return clients, memberIDs, leaderClient, idMap
}

func getEndpointFromID(id uint64, idMap map[string]uint64) (string, error) {
	for endpoint, memberID := range idMap {
		if memberID == id {
			return endpoint, nil
		}
	}
	return "", fmt.Errorf("no endpoint found for ID: %d", id)
}

func createMemberList(memberIDs []uint64, idMap map[string]uint64) etcdserverpb.MemberList {
	members := make([]etcdserverpb.Member, len(memberIDs))
	for i, id := range memberIDs {
		endpoint, _ := getEndpointFromID(id, idMap)
		members[i] = etcdserverpb.Member{
			ID:       id,
			PeerURLs: []string{endpoint},
		}
	}
	return etcdserverpb.MemberList{Members: members}
}

func handleSplit(ctx context.Context, client *clientv3.Client, subClusters [][]string, idMap map[string]uint64) {
	var clusters []etcdserverpb.MemberList
	for _, group := range subClusters {
		var ids []uint64
		for _, ep := range group {
			id, ok := idMap[ep]
			if !ok {
				log.Fatalf("Unknown endpoint in cluster: %s", ep)
			}
			ids = append(ids, id)
		}
		clusters = append(clusters, createMemberList(ids, idMap))
	}

	resp, err := client.MemberSplit(ctx, clusters, false, false)
	if err != nil {
		log.Fatalf("MemberSplit failed: %v", err)
	}
	fmt.Println("Split successful:", resp)
}

func handleMerge(
	ctx context.Context,
	clients map[string]*clientv3.Client,
	subClusters [][]string,
	idMap map[string]uint64,
	endpoints []string,
) {
	clusters := make(map[uint64]etcdserverpb.MemberList)

	for _, clusterEndpoints := range subClusters {
		if len(clusterEndpoints) == 0 {
			continue
		}

		firstEP := clusterEndpoints[0]
		client, ok := clients[firstEP]
		if !ok {
			log.Fatalf("Client not found for endpoint %s", firstEP)
		}

		statusResp, err := client.Status(ctx, firstEP)
		if err != nil {
			log.Fatalf("Failed to fetch cluster ID from %s: %v", firstEP, err)
		}

		clusterID := statusResp.Header.ClusterId

		var memberIDs []uint64
		for _, ep := range clusterEndpoints {
			id, exists := idMap[ep]
			if !exists {
				log.Fatalf("Member ID not found for %s", ep)
			}
			memberIDs = append(memberIDs, id)
		}

		clusters[clusterID] = createMemberList(memberIDs, idMap)
	}

	// Step 2: Send MemberMerge to one client
	firstEndpoint := endpoints[0]
	fmt.Printf("Sending MemberMerge to %s\n", firstEndpoint)
	resp, err := clients[firstEndpoint].MemberMerge(ctx, clusters)
	if err != nil {
		log.Printf("MemberMerge failed for %s: %v", firstEndpoint, err)
	} else {
		fmt.Printf("Merge successful on %s: %v\n", firstEndpoint, resp)
	}

}

func handleJoint(ctx context.Context, client *clientv3.Client, cfg *Config) {
	var removeIDs []uint64
	for _, hexID := range cfg.RemovePeers {
		var id uint64
		fmt.Sscanf(hexID, "%x", &id)
		removeIDs = append(removeIDs, id)
	}
	print(cfg.AddPeers)
	var resp *clientv3.MemberJointResponse
	var err error
	if len(cfg.AddPeers) > 0 && len(removeIDs) == 0 {
		// Add peers
		resp, err = client.MemberJoint(ctx, cfg.AddPeers, nil, cfg.Mode)
	} else if len(cfg.AddPeers) == 0 && len(removeIDs) > 0 {
		// Remove peers
		resp, err = client.MemberJoint(ctx, nil, removeIDs, cfg.Mode)
	} else {
		log.Fatal("Invalid joint config: either addPeers or removePeers must be non-empty, not both")
	}

	if err != nil {
		log.Fatalf("MemberJoint failed: %v", err)
	}
	fmt.Println("Joint consensus operation successful", resp)
}

func handleLeaveJoint(ctx context.Context, client *clientv3.Client) {
	resp, err := client.MemberLeaveJoint(ctx)
	if err != nil {
		log.Fatalf("MemberLeaveJoint failed: %v", err)
	}
	fmt.Println("Leave joint consensus successful", resp)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <config.yaml>")
	}
	cfgPath := os.Args[1]

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	ctx := context.TODO()

	clients, _, leaderClient, idMap := initClients(cfg.Endpoints)

	switch strings.ToLower(cfg.Operation) {
	case "split":
		handleSplit(ctx, leaderClient, cfg.SubClusters, idMap)
	case "merge":
		handleMerge(ctx, clients, cfg.SubClusters, idMap, cfg.Endpoints)
	case "joint_add", "joint_remove":
		handleJoint(ctx, leaderClient, cfg)
	case "leave_joint":
		handleLeaveJoint(ctx, leaderClient)
	default:
		log.Fatalf("Unsupported operation: %s", cfg.Operation)
	}
}
