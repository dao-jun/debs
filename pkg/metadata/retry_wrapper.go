package metadata

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// RetryConfig holds the configuration for retry logic
type RetryConfig struct {
	// MaxElapsedTime is the maximum time to retry, default 10 seconds
	MaxElapsedTime time.Duration
	// InitialInterval is the initial retry interval, default 100ms
	InitialInterval time.Duration
	// MaxInterval is the maximum retry interval, default 2 seconds
	MaxInterval time.Duration
	// Multiplier is the exponential backoff multiplier, default 1.5
	Multiplier float64
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxElapsedTime:  10 * time.Second,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     2 * time.Second,
		Multiplier:      1.5,
	}
}

// RetryWrapper wraps a MetadataStore with retry logic
type RetryWrapper struct {
	store  MetadataStore
	config *RetryConfig
}

// NewRetryWrapper creates a new RetryWrapper with the given store and config
func NewRetryWrapper(store MetadataStore, config *RetryConfig) *RetryWrapper {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &RetryWrapper{
		store:  store,
		config: config,
	}
}

// newBackoff creates a new exponential backoff with the configured parameters
func (r *RetryWrapper) newBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = r.config.InitialInterval
	b.MaxInterval = r.config.MaxInterval
	b.Multiplier = r.config.Multiplier
	b.MaxElapsedTime = r.config.MaxElapsedTime
	return b
}

// GenerateShardID generates a globally unique, incrementing shard ID with retry
func (r *RetryWrapper) GenerateShardID(ctx context.Context) (uint64, error) {
	var shardID uint64
	operation := func() error {
		var err error
		shardID, err = r.store.GenerateShardID(ctx)
		return err
	}

	err := backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
	return shardID, err
}

// CreateShard registers a new shard in the metadata store with retry
func (r *RetryWrapper) CreateShard(ctx context.Context, shard *ShardInfo) error {
	operation := func() error {
		return r.store.CreateShard(ctx, shard)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// GetShard retrieves shard information by shard ID with retry
func (r *RetryWrapper) GetShard(ctx context.Context, shardID uint64) (*ShardInfo, error) {
	var shardInfo *ShardInfo
	operation := func() error {
		var err error
		shardInfo, err = r.store.GetShard(ctx, shardID)
		return err
	}

	err := backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
	return shardInfo, err
}

// UpdateShardStatus updates the status of a shard with retry
func (r *RetryWrapper) UpdateShardStatus(ctx context.Context, shardID uint64, status ShardStatus) error {
	operation := func() error {
		return r.store.UpdateShardStatus(ctx, shardID, status)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// DeleteShard removes shard metadata with retry
func (r *RetryWrapper) DeleteShard(ctx context.Context, shardID uint64) error {
	operation := func() error {
		return r.store.DeleteShard(ctx, shardID)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// RegisterVolume registers a volume with its primary node with retry
func (r *RetryWrapper) RegisterVolume(ctx context.Context, volume *VolumeInfo) error {
	operation := func() error {
		return r.store.RegisterVolume(ctx, volume)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// GetVolume retrieves volume information by volume ID with retry
func (r *RetryWrapper) GetVolume(ctx context.Context, volumeID string) (*VolumeInfo, error) {
	var volumeInfo *VolumeInfo
	operation := func() error {
		var err error
		volumeInfo, err = r.store.GetVolume(ctx, volumeID)
		return err
	}

	err := backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
	return volumeInfo, err
}

// UpdateVolumePrimary updates the primary node for a volume (for failover) with retry
func (r *RetryWrapper) UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) error {
	operation := func() error {
		return r.store.UpdateVolumePrimary(ctx, volumeID, newPrimaryNodeID)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// ListVolumesByNode lists all volumes attached to a specific node with retry
func (r *RetryWrapper) ListVolumesByNode(ctx context.Context, nodeID string) ([]*VolumeInfo, error) {
	var volumes []*VolumeInfo
	operation := func() error {
		var err error
		volumes, err = r.store.ListVolumesByNode(ctx, nodeID)
		return err
	}

	err := backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
	return volumes, err
}

// AddBackupNode adds a backup node to a volume with retry
func (r *RetryWrapper) AddBackupNode(ctx context.Context, volumeID string, nodeID string) error {
	operation := func() error {
		return r.store.AddBackupNode(ctx, volumeID, nodeID)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// RemoveBackupNode removes a backup node from a volume with retry
func (r *RetryWrapper) RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) error {
	operation := func() error {
		return r.store.RemoveBackupNode(ctx, volumeID, nodeID)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}

// UnregisterVolume removes volume metadata with retry
func (r *RetryWrapper) UnregisterVolume(ctx context.Context, volumeID string) error {
	operation := func() error {
		return r.store.UnregisterVolume(ctx, volumeID)
	}

	return backoff.Retry(operation, backoff.WithContext(r.newBackoff(), ctx))
}
