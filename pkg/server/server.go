package server

import (
	"context"
	"errors"
	"log/slog"

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
		return &pb.PutResponse{Code: pb.Code_CLIENT_ID_REQUIRED}, nil
	}

	// Convert indexes
	indexes := make(map[string][]byte)
	for _, idx := range req.Indexes {
		indexes[idx.Key] = idx.Value
	}

	err := s.shardManager.Put(ctx, req.ShardId, req.ClientId, req.EntryId, req.Value, indexes)
	code := ParseErr(err)
	return &pb.PutResponse{Code: code}, nil
}

// BatchPut stores multiple entries
func (s *StorageServer) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	if req.ClientId == "" {
		resp := make(map[uint32]*pb.PutResponse)
		for _, r := range req.Requests {
			resp[r.EntryId] = &pb.PutResponse{Code: pb.Code_CLIENT_ID_REQUIRED}
		}
		return &pb.BatchPutResponse{Responses: resp}, nil
	}

	responses := make(map[uint32]*pb.PutResponse)

	for _, putReq := range req.Requests {
		// Convert indexes
		indexes := make(map[string][]byte)
		for _, idx := range putReq.Indexes {
			indexes[idx.Key] = idx.Value
		}

		err := s.shardManager.Put(ctx, putReq.ShardId, req.ClientId, putReq.EntryId, putReq.Value, indexes)
		var code = ParseErr(err)
		responses[putReq.EntryId] = &pb.PutResponse{Code: code}
	}

	return &pb.BatchPutResponse{Responses: responses}, nil
}

// Get retrieves a single entry
func (s *StorageServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := s.shardManager.Get(ctx, req.ShardId, req.EntryId)
	var code = ParseErr(err)
	return &pb.GetResponse{
		Code:    code,
		ShardId: req.ShardId,
		EntryId: req.EntryId,
		Value:   value,
	}, nil
}

// BatchGet retrieves multiple entries
func (s *StorageServer) BatchGet(req *pb.BatchGetRequest, stream pb.StorageService_BatchGetServer) error {
	shardId := req.ShardId
	for _, entryId := range req.EntryIds {
		value, err := s.shardManager.Get(stream.Context(), shardId, entryId)
		if err != nil {
			return err
		}
		var code = ParseErr(err)
		var resp = &pb.GetResponse{
			Code:    code,
			ShardId: shardId,
			EntryId: entryId,
			Value:   value,
		}
		if err := stream.SendMsg(resp); err != nil {
			slog.Error("failed to send response", "error", err)
			return err
		}
	}
	return nil
}

// NewShard creates a new shard
func (s *StorageServer) NewShard(ctx context.Context, req *pb.NewShardRequest) (*pb.NewShardResponse, error) {
	if req.ClientId == "" {
		return &pb.NewShardResponse{Code: pb.Code_CLIENT_ID_REQUIRED}, nil
	}

	shardID, err := s.shardManager.CreateShard(ctx, req.ClientId)
	var code = ParseErr(err)
	return &pb.NewShardResponse{
		Code:    code,
		ShardId: shardID,
	}, nil
}

// DeleteShard deletes a shard
func (s *StorageServer) DeleteShard(ctx context.Context, req *pb.DeleteShardRequest) (*pb.DeleteShardResponse, error) {
	err := s.shardManager.DeleteShard(ctx, req.ShardId)
	var code = ParseErr(err)
	return &pb.DeleteShardResponse{Code: code}, nil
}

// CloseShard closes a shard (marks it read-only)
func (s *StorageServer) CloseShard(ctx context.Context, req *pb.CloseShardRequest) (*pb.CloseShardResponse, error) {
	err := s.shardManager.CloseShard(ctx, req.ShardId)
	var code = ParseErr(err)
	return &pb.CloseShardResponse{Code: code}, nil
}

func ParseErr(err error) pb.Code {
	if err == nil {
		return pb.Code_OK
	} else if errors.Is(err, shard.ErrEntryNotFound) {
		return pb.Code_ENTRY_NOT_FOUND
	} else if errors.Is(err, shard.ErrShardNotFound) {
		return pb.Code_SHARD_NOT_FOUND
	} else if errors.Is(err, metadata.ErrMetadata) {
		return pb.Code_METADATA_EXCEPTION
	} else if errors.Is(err, shard.ErrIO) {
		return pb.Code_DEVICE_EXCEPTION
	} else if errors.Is(err, shard.ErrReadFailed) {
		return pb.Code_READ_EXCEPTION
	} else if errors.Is(err, shard.ErrWriteFailed) {
		return pb.Code_WRITE_EXCEPTION
	} else if errors.Is(err, shard.ErrFenced) {
		return pb.Code_SHARD_FENCED
	} else if errors.Is(err, shard.ErrNotShardLeader) {
		return pb.Code_NOT_SHARD_LEADER
	}
	// CONVERT MORE errors

	return pb.Code_OK
}
