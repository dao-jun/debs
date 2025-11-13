package shard

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/debs/debs/pkg/metadata"
	"github.com/debs/debs/pkg/volume"
)

// ShardManager manages shards on the local node
type ShardManager struct {
	mu            sync.RWMutex
	shards        map[uint64]*Shard // shardID -> Shard
	volumeManager *volume.VolumeManager
	metadataStore metadata.MetadataStore
}

// NewShardManager creates a new ShardManager
func NewShardManager(
	volumeManager *volume.VolumeManager,
	metadataStore metadata.MetadataStore,
) *ShardManager {
	return &ShardManager{
		shards:        make(map[uint64]*Shard),
		volumeManager: volumeManager,
		metadataStore: metadataStore,
	}
}

// CreateShard creates a new shard on a volume
func (sm *ShardManager) CreateShard(ctx context.Context, clientID string, volumeID string) (uint64, error) {
	// Generate a new shard ID
	shardID, err := sm.metadataStore.GenerateShardID(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to generate shard ID: %w", err)
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(volumeID, shardID)
	if err != nil {
		return 0, fmt.Errorf("failed to get shard path: %w", err)
	}

	// Create the shard directory
	if err := os.MkdirAll(shardPath, 0755); err != nil {
		return 0, fmt.Errorf("failed to create shard directory: %w", err)
	}

	// Create the shard
	shard, err := NewShard(shardPath, shardID, clientID, false)
	if err != nil {
		return 0, fmt.Errorf("failed to create shard: %w", err)
	}

	// Register in metadata
	shardInfo := &metadata.ShardInfo{
		ShardID:  shardID,
		VolumeID: volumeID,
		ClientID: clientID,
		Status:   metadata.ShardStatusActive,
	}

	if err := sm.metadataStore.CreateShard(ctx, shardInfo); err != nil {
		// Clean up on failure
		shard.Delete()
		os.RemoveAll(shardPath)
		return 0, fmt.Errorf("failed to register shard in metadata: %w", err)
	}

	// Add to local cache
	sm.mu.Lock()
	sm.shards[shardID] = shard
	sm.mu.Unlock()

	return shardID, nil
}

// GetShard gets a shard by ID, loading it if necessary
func (sm *ShardManager) GetShard(ctx context.Context, shardID uint64) (*Shard, error) {
	sm.mu.RLock()
	shard, exists := sm.shards[shardID]
	sm.mu.RUnlock()

	if exists {
		return shard, nil
	}

	// Shard not loaded, load it
	return sm.loadShard(ctx, shardID)
}

// loadShard loads a shard from disk
func (sm *ShardManager) loadShard(ctx context.Context, shardID uint64) (*Shard, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check again after acquiring lock
	if shard, exists := sm.shards[shardID]; exists {
		return shard, nil
	}

	// Get shard metadata
	shardInfo, err := sm.metadataStore.GetShard(ctx, shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard metadata: %w", err)
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(shardInfo.VolumeID, shardID)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard path: %w", err)
	}

	// Check if shard directory exists
	if _, err := os.Stat(shardPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("shard directory does not exist: %s", shardPath)
	}

	// Determine if this shard should be read-only
	readOnly := shardInfo.Status == metadata.ShardStatusReadOnly

	// Check if this node is primary for the volume
	if !sm.volumeManager.IsPrimaryForVolume(shardInfo.VolumeID) {
		readOnly = true
	}

	// Open the shard
	shard, err := NewShard(shardPath, shardID, shardInfo.ClientID, readOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard: %w", err)
	}

	sm.shards[shardID] = shard

	return shard, nil
}

// CloseShard closes a shard and marks it as read-only
func (sm *ShardManager) CloseShard(ctx context.Context, shardID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shard, exists := sm.shards[shardID]
	if !exists {
		return fmt.Errorf("shard %d not found", shardID)
	}

	// Close the shard (marks it as read-only)
	if err := shard.Close(); err != nil {
		return fmt.Errorf("failed to close shard: %w", err)
	}

	// Update metadata
	if err := sm.metadataStore.UpdateShardStatus(ctx, shardID, metadata.ShardStatusReadOnly); err != nil {
		return fmt.Errorf("failed to update shard status: %w", err)
	}

	return nil
}

// DeleteShard deletes a shard
func (sm *ShardManager) DeleteShard(ctx context.Context, shardID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Get shard metadata first
	shardInfo, err := sm.metadataStore.GetShard(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to get shard metadata: %w", err)
	}

	// Close and remove from cache if loaded
	if shard, exists := sm.shards[shardID]; exists {
		if err := shard.Delete(); err != nil {
			return fmt.Errorf("failed to delete shard: %w", err)
		}
		delete(sm.shards, shardID)
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(shardInfo.VolumeID, shardID)
	if err != nil {
		return fmt.Errorf("failed to get shard path: %w", err)
	}

	// Remove shard directory
	if err := os.RemoveAll(shardPath); err != nil {
		return fmt.Errorf("failed to remove shard directory: %w", err)
	}

	// Update metadata
	if err := sm.metadataStore.UpdateShardStatus(ctx, shardID, metadata.ShardStatusDeleted); err != nil {
		return fmt.Errorf("failed to update shard status: %w", err)
	}

	return nil
}

// Put stores a value in a shard
func (sm *ShardManager) Put(ctx context.Context, shardID uint64, clientID string, entryID uint32, value []byte, indexes map[string][]byte) error {
	shard, err := sm.GetShard(ctx, shardID)
	if err != nil {
		return err
	}

	return shard.Put(clientID, entryID, value, indexes)
}

// Get retrieves a value from a shard
func (sm *ShardManager) Get(ctx context.Context, shardID uint64, entryID uint32) ([]byte, error) {
	shard, err := sm.GetShard(ctx, shardID)
	if err != nil {
		return nil, err
	}

	return shard.Get(entryID)
}

// BatchGet retrieves multiple values from a shard
func (sm *ShardManager) BatchGet(ctx context.Context, shardID uint64, entryIDs []uint32) (map[uint32][]byte, error) {
	shard, err := sm.GetShard(ctx, shardID)
	if err != nil {
		return nil, err
	}

	return shard.BatchGet(entryIDs)
}

// ListShards returns all loaded shards
func (sm *ShardManager) ListShards() []uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	shardIDs := make([]uint64, 0, len(sm.shards))
	for shardID := range sm.shards {
		shardIDs = append(shardIDs, shardID)
	}

	return shardIDs
}

// UnloadShard unloads a shard from memory
func (sm *ShardManager) UnloadShard(shardID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shard, exists := sm.shards[shardID]
	if !exists {
		return nil // Already unloaded
	}

	if err := shard.Delete(); err != nil {
		return fmt.Errorf("failed to close shard: %w", err)
	}

	delete(sm.shards, shardID)

	return nil
}

// LoadAllShards loads all shards from mounted volumes
func (sm *ShardManager) LoadAllShards(ctx context.Context) error {
	mountedVolumes := sm.volumeManager.ListMountedVolumes()

	for _, vol := range mountedVolumes {
		// List all shard directories in this volume
		entries, err := os.ReadDir(vol.MountPath)
		if err != nil {
			return fmt.Errorf("failed to read volume directory %s: %w", vol.MountPath, err)
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			// Check if this is a shard directory (shard-{shardID})
			var shardID uint64
			if _, err := fmt.Sscanf(entry.Name(), "shard-%d", &shardID); err != nil {
				continue // Not a shard directory
			}

			// Load the shard
			if _, err := sm.GetShard(ctx, shardID); err != nil {
				// Log error but continue
				fmt.Printf("Failed to load shard %d: %v\n", shardID, err)
			}
		}
	}

	return nil
}
