package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/debs/debs/pkg/metadata"
	"github.com/debs/debs/pkg/shard"
	"github.com/debs/debs/pkg/volume"
	pb "github.com/debs/debs/proto"
)

// StorageServer implements the StorageService gRPC service
type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	shardManager  *shard.ShardManager
	volumeManager *volume.VolumeManager
	metadataStore metadata.MetadataStore
}

// NewStorageServer creates a new StorageServer
func NewStorageServer(
	shardManager *shard.ShardManager,
	volumeManager *volume.VolumeManager,
	metadataStore metadata.MetadataStore,
) *StorageServer {
	return &StorageServer{
		shardManager:  shardManager,
		volumeManager: volumeManager,
		metadataStore: metadataStore,
	}
}

// Put stores a single entry
func (s *StorageServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.ClientId == "" {
		return &pb.PutResponse{Code: pb.Code_INVALID_ARGUMENT}, nil
	}

	// Convert indexes
	indexes := make(map[string][]byte)
	for _, idx := range req.Indexes {
		indexes[idx.Key] = idx.Value
	}

	err := s.shardManager.Put(ctx, req.ShardId, req.ClientId, req.EntryId, req.Value, indexes)
	if err != nil {
		// Check for specific errors
		if errors.Is(err, shard.ErrNotFound) {
			return &pb.PutResponse{Code: pb.Code_NOT_FOUND}, nil
		}
		return nil, fmt.Errorf("failed to put entry: %w", err)
	}

	return &pb.PutResponse{Code: pb.Code_OK}, nil
}

// BatchPut stores multiple entries
func (s *StorageServer) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	if req.ClientId == "" {
		return nil, fmt.Errorf("clientId is required")
	}

	responses := make(map[uint64]*pb.PutResponse)

	for i, putReq := range req.Requests {
		// Use the batch clientId if individual request doesn't have one
		clientID := putReq.ClientId
		if clientID == "" {
			clientID = req.ClientId
		}

		// Convert indexes
		indexes := make(map[string][]byte)
		for _, idx := range putReq.Indexes {
			indexes[idx.Key] = idx.Value
		}

		err := s.shardManager.Put(ctx, putReq.ShardId, clientID, putReq.EntryId, putReq.Value, indexes)

		var code pb.Code
		if err != nil {
			if errors.Is(err, shard.ErrNotFound) {
				code = pb.Code_NOT_FOUND
			} else {
				code = pb.Code_INVALID_ARGUMENT
			}
		} else {
			code = pb.Code_OK
		}

		responses[uint64(i)] = &pb.PutResponse{Code: code}
	}

	return &pb.BatchPutResponse{Responses: responses}, nil
}

// Get retrieves a single entry
func (s *StorageServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := s.shardManager.Get(ctx, req.ShardId, req.EntryId)
	if err != nil {
		if errors.Is(err, shard.ErrNotFound) {
			return &pb.GetResponse{
				Code:    pb.Code_NOT_FOUND,
				ShardId: req.ShardId,
				EntryId: req.EntryId,
			}, nil
		}
		return nil, fmt.Errorf("failed to get entry: %w", err)
	}

	return &pb.GetResponse{
		Code:    pb.Code_OK,
		ShardId: req.ShardId,
		EntryId: req.EntryId,
		Value:   value,
	}, nil
}

// BatchGet retrieves multiple entries
func (s *StorageServer) BatchGet(req *pb.BatchGetRequest, stream pb.StorageService_BatchGetServer) error {
	results, err := s.shardManager.BatchGet(stream.Context(), req.ShardId, req.EntryIds)
	if err != nil {
		return fmt.Errorf("failed to batch get: %w", err)
	}

	// Stream results
	for entryID, value := range results {
		resp := &pb.GetResponse{
			Code:    pb.Code_OK,
			ShardId: req.ShardId,
			EntryId: entryID,
			Value:   value,
		}

		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}

	// Send NOT_FOUND for missing entries
	for _, entryID := range req.EntryIds {
		if _, found := results[entryID]; !found {
			resp := &pb.GetResponse{
				Code:    pb.Code_NOT_FOUND,
				ShardId: req.ShardId,
				EntryId: entryID,
			}

			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("failed to send response: %w", err)
			}
		}
	}

	return nil
}

// NewShard creates a new shard
func (s *StorageServer) NewShard(ctx context.Context, req *pb.NewShardRequest) (*pb.NewShardResponse, error) {
	if req.ClientId == "" {
		return &pb.NewShardResponse{Code: pb.Code_INVALID_ARGUMENT}, nil
	}

	shardID, err := s.shardManager.CreateShard(ctx, req.ClientId)
	if err != nil {
		return nil, fmt.Errorf("failed to create shard: %w", err)
	}

	return &pb.NewShardResponse{
		Code:    pb.Code_OK,
		ShardId: shardID,
	}, nil
}

// DeleteShard deletes a shard
func (s *StorageServer) DeleteShard(ctx context.Context, req *pb.DeleteShardRequest) (*pb.DeleteShardResponse, error) {
	err := s.shardManager.DeleteShard(ctx, req.ShardId)
	if err != nil {
		if errors.Is(err, shard.ErrNotFound) {
			return &pb.DeleteShardResponse{Code: pb.Code_NOT_FOUND}, nil
		}
		return nil, fmt.Errorf("failed to delete shard: %w", err)
	}

	return &pb.DeleteShardResponse{Code: pb.Code_OK}, nil
}

// CloseShard closes a shard (marks it read-only)
func (s *StorageServer) CloseShard(ctx context.Context, req *pb.CloseShardRequest) (*pb.CloseShardResponse, error) {
	err := s.shardManager.CloseShard(ctx, req.ShardId)
	if err != nil {
		if errors.Is(err, shard.ErrNotFound) {
			return &pb.CloseShardResponse{Code: pb.Code_NOT_FOUND}, nil
		}
		return nil, fmt.Errorf("failed to close shard: %w", err)
	}

	return &pb.CloseShardResponse{Code: pb.Code_OK}, nil
}
