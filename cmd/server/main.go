package main

import (
	"context"
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
	mockStore := metadata.NewMockMetadataStore() // For development/testing

	// Wrap the metadata store with retry logic to handle transient failures
	metadataStore = metadata.NewRetryWrapper(mockStore, metadata.DefaultRetryConfig())

	// Initialize volume manager
	volumeManager := volume.NewVolumeManager(
		volumeProvider,
		metadataStore,
		cfg.Node.NodeID,
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
