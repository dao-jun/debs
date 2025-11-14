package metadata

import (
	"context"
	"errors"
)

// ShardInfo contains information about a shard
type ShardInfo struct {
	ShardID  uint64
	VolumeID string
	ClientID string
	Status   ShardStatus
}

// ShardStatus represents the status of a shard
type ShardStatus string

const (
	ShardStatusActive   ShardStatus = "active"
	ShardStatusReadOnly ShardStatus = "readonly"
	ShardStatusDeleted  ShardStatus = "deleted"
)

// VolumeInfo contains information about a volume
type VolumeInfo struct {
	VolumeID    string
	NodeID      string
	IsPrimary   bool
	MountPath   string
	BackupNodes []string
}

type NodeInfo struct {
	NodeID  string
	Address string
}

type MetadataError error

var ErrMetadata MetadataError = errors.New("operate metadata store failed")
var ErrNotFound MetadataError = errors.New("not found")

// MetadataStore is the interface for metadata operations
// This should be implemented using external consensus systems like etcd, ZooKeeper, etc.
type MetadataStore interface {
	// Node operations

	// RegisterNode registers a new node in the metadata store
	RegisterNode(ctx context.Context, info NodeInfo) MetadataError

	// UnregisterNode removes a node from the metadata store
	UnregisterNode(ctx context.Context, nodeID string) MetadataError

	// Shard operations

	// GenerateShardID generates a globally unique, incrementing shard ID
	GenerateShardID(ctx context.Context) (uint64, MetadataError)

	// CreateShard registers a new shard in the metadata store
	CreateShard(ctx context.Context, shard *ShardInfo) MetadataError

	// GetShard retrieves shard information by shard ID
	GetShard(ctx context.Context, shardID uint64) (*ShardInfo, MetadataError)

	// UpdateShardStatus updates the status of a shard
	UpdateShardStatus(ctx context.Context, shardID uint64, status ShardStatus) MetadataError

	// DeleteShard removes shard metadata
	DeleteShard(ctx context.Context, shardID uint64) MetadataError

	// Volume operations

	// RegisterVolume registers a volume with its primary node
	RegisterVolume(ctx context.Context, volume *VolumeInfo) MetadataError

	// GetVolume retrieves volume information by volume ID
	GetVolume(ctx context.Context, volumeID string) (*VolumeInfo, MetadataError)

	// UpdateVolumePrimary updates the primary node for a volume (for failover)
	UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) MetadataError

	// ListVolumesByNode lists all volumes attached to a specific node
	ListVolumesByNode(ctx context.Context, nodeID string) ([]*VolumeInfo, MetadataError)

	// AddBackupNode adds a backup node to a volume
	AddBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError

	// RemoveBackupNode removes a backup node from a volume
	RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError

	// UnregisterVolume removes volume metadata
	UnregisterVolume(ctx context.Context, volumeID string) MetadataError
}
