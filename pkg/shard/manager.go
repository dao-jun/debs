package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/debs/debs/pkg/metadata"
	"github.com/debs/debs/pkg/volume"
	"go.uber.org/multierr"
)

var ErrShardManagerClosed = errors.New("shard manager closed")

// ShardManager manages shards on the local node
type ShardManager struct {
	mu            sync.RWMutex
	shards        map[uint64]*Shard // shardID -> Shard
	volumeManager *volume.VolumeManager
	metadataStore metadata.MetadataStore
	closed        atomic.Bool
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
		closed:        atomic.Bool{},
	}
}

func (sm *ShardManager) listShardsByVolume(volumeId string) []*Shard {
	shards := make([]*Shard, len(sm.shards))
	for _, shard := range sm.shards {
		if shard.GetVolumeId() == volumeId {
			shards = append(shards, shard)
		}
	}
	return shards
}

// selectBestVolume selects the best volume for creating a new shard
// It chooses the volume with the most available space and fewest active shards
func (sm *ShardManager) selectBestVolume() (string, error) {
	volumes := sm.volumeManager.ListMountedVolumes()
	if len(volumes) == 0 {
		return "", volume.ErrNoEnoughVolume
	}

	var bestVolume string
	var bestScore float64 = -1

	for _, vol := range volumes {
		// Only consider primary volumes (read-write)
		if !vol.IsPrimary {
			continue
		}

		// Get volume statistics
		stats, err := sm.volumeManager.GetVolumeStats(vol.VolumeID)
		if err != nil {
			// Skip volumes where we can't get stats
			continue
		}

		// Check if volume has enough space (at least 100MB available)
		minSpaceRequired := uint64(100 * 1024 * 1024) // 100MB
		if stats.AvailableBytes < minSpaceRequired {
			continue
		}

		// Count active shards on this volume
		shards := sm.listShardsByVolume(vol.VolumeID)
		activeShardCount := 0
		for _, shard := range shards {
			if !shard.readOnly {
				activeShardCount++
			}
		}

		// Calculate a score: higher is better
		// Score is based on available space (weighted more) and inverse of active shard count
		// Normalize available space to GB for scoring
		availableGB := float64(stats.AvailableBytes) / (1024 * 1024 * 1024)
		// Weight: 70% available space, 30% inverse of shard count
		// Add 1 to shard count to avoid division by zero
		score := (availableGB * 0.7) + (100.0/(float64(activeShardCount)+1))*0.3

		if score > bestScore {
			bestScore = score
			bestVolume = vol.VolumeID
		}
	}

	if bestVolume == "" {
		return "", volume.ErrNoEnoughVolume
	}

	return bestVolume, nil
}

// CreateShard creates a new shard on a volume
// If volumeID is empty, automatically selects the best volume based on available space and active shard count
func (sm *ShardManager) CreateShard(ctx context.Context, clientID string) (uint64, error) {
	if sm.closed.Load() {
		return 0, ErrShardManagerClosed
	}
	// If no volume specified, select the best one
	volumeID, err := sm.selectBestVolume()
	if err != nil {
		return 0, err
	}

	// Generate a new shard ID
	shardID, err := sm.metadataStore.GenerateShardID(ctx)
	if err != nil {
		return 0, metadata.ErrMetadata
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(volumeID, shardID)
	if err != nil {
		return 0, err
	}

	// Create the shard directory
	if err := os.MkdirAll(shardPath, 0755); err != nil {
		return 0, ErrIO
	}

	// Create the shard
	shard, err := NewShard(volumeID, shardPath, shardID, clientID, false)
	if err != nil {
		return 0, err
	}

	// Register in metadata
	shardInfo := &metadata.ShardInfo{
		ShardID:  shardID,
		VolumeID: volumeID,
		ClientID: clientID,
		Status:   metadata.ShardStatusActive,
	}

	if err = sm.metadataStore.CreateShard(ctx, shardInfo); err != nil {
		// Clean up on failure
		err = multierr.Combine(err, shard.Close(), os.RemoveAll(shardPath))
		return 0, metadata.ErrMetadata
	}

	// Add to local cache
	sm.mu.Lock()
	sm.shards[shardID] = shard
	sm.mu.Unlock()

	return shardID, nil
}

// GetShard gets a shard by ID, loading it if necessary
func (sm *ShardManager) GetShard(ctx context.Context, shardID uint64) (*Shard, error) {
	if sm.closed.Load() {
		return nil, ErrShardManagerClosed
	}

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
		if errors.Is(err, metadata.ErrNotFound) {
			return nil, ErrShardNotFound
		}
		return nil, err
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(shardInfo.VolumeID, shardID)
	if err != nil {
		return nil, err
	}

	// Check if shard directory exists
	if _, err := os.Stat(shardPath); os.IsNotExist(err) {
		return nil, ErrShardNotFound
	}

	// Determine if this shard should be read-only
	readOnly := shardInfo.Status == metadata.ShardStatusReadOnly

	// Check if this node is primary for the volume
	if !sm.volumeManager.IsPrimaryForVolume(shardInfo.VolumeID) {
		readOnly = true
	}

	// Open the shard
	shard, err := NewShard(shardInfo.VolumeID, shardPath, shardID, shardInfo.ClientID, readOnly)
	if err != nil {
		return nil, err
	}

	sm.shards[shardID] = shard

	return shard, nil
}

// CloseShard closes a shard and marks it as read-only
func (sm *ShardManager) CloseShard(ctx context.Context, shardID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed.Load() {
		return nil
	}
	shard, exists := sm.shards[shardID]
	if !exists {
		return ErrShardNotFound
	}

	// Close the shard (marks it as read-only)
	if err := shard.Close(); err != nil {
		return err
	}

	// Update metadata
	if err := sm.metadataStore.UpdateShardStatus(ctx, shardID, metadata.ShardStatusReadOnly); err != nil {
		return metadata.ErrMetadata
	}

	return nil
}

// DeleteShard deletes a shard
func (sm *ShardManager) DeleteShard(ctx context.Context, shardID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed.Load() {
		return nil
	}
	// Get shard metadata first
	shardInfo, err := sm.metadataStore.GetShard(ctx, shardID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return ErrShardNotFound
		}
		return err
	}

	// Close and remove from cache if loaded
	if shard, exists := sm.shards[shardID]; exists {
		if err := shard.Close(); err != nil {
			return err
		}
		delete(sm.shards, shardID)
	}

	// Get the shard path
	shardPath, err := sm.volumeManager.GetShardPath(shardInfo.VolumeID, shardID)
	if err != nil {
		return err
	}

	// Remove shard directory
	if err := os.RemoveAll(shardPath); err != nil {
		return ErrIO
	}

	// Update metadata
	if err := sm.metadataStore.UpdateShardStatus(ctx, shardID, metadata.ShardStatusDeleted); err != nil {
		return metadata.ErrMetadata
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

	if err := shard.Close(); err != nil {
		return err
	}

	delete(sm.shards, shardID)

	return nil
}

// Close closes all shards
func (sm *ShardManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed.Load() {
		return nil
	}
	sm.closed.Store(true)
	var err error
	for _, shard := range sm.shards {
		err = multierr.Append(err, shard.Close())
	}
	return err
}

// LoadAllShards loads all shards from mounted volumes
func (sm *ShardManager) LoadAllShards(ctx context.Context) error {
	mountedVolumes := sm.volumeManager.ListMountedVolumes()

	for _, vol := range mountedVolumes {
		// List all shard directories in this volume
		entries, err := os.ReadDir(vol.MountPath)
		if err != nil {
			return ErrIO
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
				slog.Error("Failed to load shard", "shardID", shardID, "error", err)
			}
		}
	}

	return nil
}
