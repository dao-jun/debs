package metadata

import (
	"context"
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

// MetadataStore is the interface for metadata operations
// This should be implemented using external consensus systems like etcd, ZooKeeper, etc.
//
// Note: It's recommended to wrap MetadataStore implementations with RetryWrapper
// to handle transient failures automatically. Example:
//
//	store := NewYourMetadataStoreImpl()
//	retryStore := NewRetryWrapper(store, DefaultRetryConfig())
type MetadataStore interface {
	// Shard operations

	// GenerateShardID generates a globally unique, incrementing shard ID
	GenerateShardID(ctx context.Context) (uint64, error)

	// CreateShard registers a new shard in the metadata store
	CreateShard(ctx context.Context, shard *ShardInfo) error

	// GetShard retrieves shard information by shard ID
	GetShard(ctx context.Context, shardID uint64) (*ShardInfo, error)

	// UpdateShardStatus updates the status of a shard
	UpdateShardStatus(ctx context.Context, shardID uint64, status ShardStatus) error

	// DeleteShard removes shard metadata
	DeleteShard(ctx context.Context, shardID uint64) error

	// Volume operations

	// RegisterVolume registers a volume with its primary node
	RegisterVolume(ctx context.Context, volume *VolumeInfo) error

	// GetVolume retrieves volume information by volume ID
	GetVolume(ctx context.Context, volumeID string) (*VolumeInfo, error)

	// UpdateVolumePrimary updates the primary node for a volume (for failover)
	UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) error

	// ListVolumesByNode lists all volumes attached to a specific node
	ListVolumesByNode(ctx context.Context, nodeID string) ([]*VolumeInfo, error)

	// AddBackupNode adds a backup node to a volume
	AddBackupNode(ctx context.Context, volumeID string, nodeID string) error

	// RemoveBackupNode removes a backup node from a volume
	RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) error

	// UnregisterVolume removes volume metadata
	UnregisterVolume(ctx context.Context, volumeID string) error
}
