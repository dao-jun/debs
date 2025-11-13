package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/debs/debs/config"
	"github.com/debs/debs/pkg/cloud"
	"github.com/debs/debs/pkg/metadata"
	"github.com/debs/debs/pkg/server"
	"github.com/debs/debs/pkg/shard"
	"github.com/debs/debs/pkg/volume"
	pb "github.com/debs/debs/proto"
)

func main() {
	// Load configuration
	cfg := config.DefaultConfig()

	// Override from environment variables if needed
	if nodeID := os.Getenv("NODE_ID"); nodeID != "" {
		cfg.Node.NodeID = nodeID
	}
	if instanceID := os.Getenv("INSTANCE_ID"); instanceID != "" {
		cfg.Node.InstanceID = instanceID
	}
	if region := os.Getenv("AWS_REGION"); region != "" {
		cfg.Cloud.Region = region
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize cloud provider
	var volumeProvider cloud.VolumeProvider
	var err error

	switch cfg.Cloud.Provider {
	case "aws":
		volumeProvider, err = cloud.NewAWSEBSProvider(ctx, cfg.Cloud.Region)
		if err != nil {
			log.Fatalf("Failed to create AWS EBS provider: %v", err)
		}
	default:
		log.Fatalf("Unsupported cloud provider: %s", cfg.Cloud.Provider)
	}

	// Initialize metadata store
	// NOTE: This is a placeholder - actual implementation depends on the chosen metadata store
	var metadataStore metadata.MetadataStore
	mockStore := NewMockMetadataStore() // For development/testing

	// Wrap the metadata store with retry logic to handle transient failures
	metadataStore = metadata.NewRetryWrapper(mockStore, metadata.DefaultRetryConfig())

	// Initialize volume manager
	volumeManager := volume.NewVolumeManager(
		volumeProvider,
		metadataStore,
		cfg.Node.NodeID,
		cfg.Node.InstanceID,
	)

	// Initialize shard manager
	shardManager := shard.NewShardManager(volumeManager, metadataStore)

	// Initialize pebble cache
	shard.InitializePebbleCache()
	// Load all shards from mounted volumes
	if err := shardManager.LoadAllShards(ctx); err != nil {
		log.Printf("Warning: Failed to load all shards: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	storageServer := server.NewStorageServer(shardManager, volumeManager, metadataStore)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	// Start listening
	listener, err := net.Listen("tcp", cfg.Server.Address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down server...")
		// todo shutdown shards and unregister the node
		err := metadataStore.UnregisterNode(context.Background(), cfg.Node.NodeID)
		if err != nil {
			log.Printf("Error unregistering node: %v", err)
		}
		grpcServer.GracefulStop()
		err = shardManager.Close()
		if err != nil {
			log.Printf("Error closing shards: %v", err)
		}
		shard.ReleasePebbleCache()
		cancel()
	}()

	// Start server
	log.Printf("Storage service listening on %s", cfg.Server.Address)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	// Register the server to metadata store
	err = metadataStore.RegisterNode(ctx, metadata.NodeInfo{
		NodeID:  cfg.Node.NodeID,
		Address: cfg.Server.Address,
	})
	if err != nil {
		// TODO handle the error
		return
	}
}

var _ metadata.MetadataStore = (*MockMetadataStore)(nil)

// MockMetadataStore is a simple in-memory metadata store for development/testing
type MockMetadataStore struct {
	shards      map[uint64]*metadata.ShardInfo
	volumes     map[string]*metadata.VolumeInfo
	nodes       map[string]*metadata.NodeInfo
	nextShardID uint64
}

func (m *MockMetadataStore) RegisterNode(ctx context.Context, info metadata.NodeInfo) error {
	m.nodes[info.NodeID] = &info
	return nil
}

func (m *MockMetadataStore) UnregisterNode(ctx context.Context, nodeID string) error {
	delete(m.nodes, nodeID)
	return nil
}

func NewMockMetadataStore() *MockMetadataStore {
	return &MockMetadataStore{
		shards:      make(map[uint64]*metadata.ShardInfo),
		volumes:     make(map[string]*metadata.VolumeInfo),
		nextShardID: 1,
	}
}

func (m *MockMetadataStore) GenerateShardID(ctx context.Context) (uint64, error) {
	id := m.nextShardID
	m.nextShardID++
	return id, nil
}

func (m *MockMetadataStore) CreateShard(ctx context.Context, shard *metadata.ShardInfo) error {
	m.shards[shard.ShardID] = shard
	return nil
}

func (m *MockMetadataStore) GetShard(ctx context.Context, shardID uint64) (*metadata.ShardInfo, error) {
	shard, exists := m.shards[shardID]
	if !exists {
		return nil, fmt.Errorf("shard not found")
	}
	return shard, nil
}

func (m *MockMetadataStore) UpdateShardStatus(ctx context.Context, shardID uint64, status metadata.ShardStatus) error {
	shardInfo, exists := m.shards[shardID]
	if !exists {
		return fmt.Errorf("shardInfo not found")
	}
	shardInfo.Status = status
	return nil
}

func (m *MockMetadataStore) DeleteShard(ctx context.Context, shardID uint64) error {
	delete(m.shards, shardID)
	return nil
}

func (m *MockMetadataStore) RegisterVolume(ctx context.Context, volume *metadata.VolumeInfo) error {
	m.volumes[volume.VolumeID] = volume
	return nil
}

func (m *MockMetadataStore) GetVolume(ctx context.Context, volumeID string) (*metadata.VolumeInfo, error) {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return nil, fmt.Errorf("volume not found")
	}
	return volume, nil
}

func (m *MockMetadataStore) UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) error {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return fmt.Errorf("volume not found")
	}
	volume.NodeID = newPrimaryNodeID
	volume.IsPrimary = true
	return nil
}

func (m *MockMetadataStore) ListVolumesByNode(ctx context.Context, nodeID string) ([]*metadata.VolumeInfo, error) {
	var volumes []*metadata.VolumeInfo
	for _, v := range m.volumes {
		if v.NodeID == nodeID {
			volumes = append(volumes, v)
		}
	}
	return volumes, nil
}

func (m *MockMetadataStore) AddBackupNode(ctx context.Context, volumeID string, nodeID string) error {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return fmt.Errorf("volume not found")
	}
	volume.BackupNodes = append(volume.BackupNodes, nodeID)
	return nil
}

func (m *MockMetadataStore) RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) error {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return fmt.Errorf("volume not found")
	}
	for i, n := range volume.BackupNodes {
		if n == nodeID {
			volume.BackupNodes = append(volume.BackupNodes[:i], volume.BackupNodes[i+1:]...)
			break
		}
	}
	return nil
}

func (m *MockMetadataStore) UnregisterVolume(ctx context.Context, volumeID string) error {
	delete(m.volumes, volumeID)
	return nil
}
