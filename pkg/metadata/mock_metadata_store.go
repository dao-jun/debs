package metadata

import (
	"context"
)

var _ MetadataStore = (*MockMetadataStore)(nil)

// MockMetadataStore is a simple in-memory metadata store for development/testing
type MockMetadataStore struct {
	shards      map[uint64]*ShardInfo
	volumes     map[string]*VolumeInfo
	nodes       map[string]*NodeInfo
	nextShardID uint64
}

func NewMockMetadataStore() *MockMetadataStore {
	return &MockMetadataStore{
		shards:      make(map[uint64]*ShardInfo),
		volumes:     make(map[string]*VolumeInfo),
		nodes:       make(map[string]*NodeInfo),
		nextShardID: 1,
	}
}

func (m *MockMetadataStore) RegisterNode(ctx context.Context, info NodeInfo) MetadataError {
	m.nodes[info.NodeID] = &info
	return nil
}

func (m *MockMetadataStore) UnregisterNode(ctx context.Context, nodeID string) MetadataError {
	_, exists := m.nodes[nodeID]
	if !exists {
		return ErrNotFound
	}
	delete(m.nodes, nodeID)
	return nil
}

func (m *MockMetadataStore) GenerateShardID(ctx context.Context) (uint64, MetadataError) {
	id := m.nextShardID
	m.nextShardID++
	return id, nil
}

func (m *MockMetadataStore) CreateShard(ctx context.Context, shard *ShardInfo) MetadataError {
	m.shards[shard.ShardID] = shard
	return nil
}

func (m *MockMetadataStore) GetShard(ctx context.Context, shardID uint64) (*ShardInfo, MetadataError) {
	shard, exists := m.shards[shardID]
	if !exists {
		return nil, ErrNotFound
	}
	return shard, nil
}

func (m *MockMetadataStore) UpdateShardStatus(ctx context.Context, shardID uint64, status ShardStatus) MetadataError {
	shardInfo, exists := m.shards[shardID]
	if !exists {
		return ErrNotFound
	}
	shardInfo.Status = status
	return nil
}

func (m *MockMetadataStore) DeleteShard(ctx context.Context, shardID uint64) MetadataError {
	delete(m.shards, shardID)
	return nil
}

func (m *MockMetadataStore) RegisterVolume(ctx context.Context, volume *VolumeInfo) MetadataError {
	m.volumes[volume.VolumeID] = volume
	return nil
}

func (m *MockMetadataStore) GetVolume(ctx context.Context, volumeID string) (*VolumeInfo, MetadataError) {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return nil, ErrNotFound
	}
	return volume, nil
}

func (m *MockMetadataStore) UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) MetadataError {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return ErrNotFound
	}
	volume.NodeID = newPrimaryNodeID
	volume.IsPrimary = true
	return nil
}

func (m *MockMetadataStore) ListVolumesByNode(ctx context.Context, nodeID string) ([]*VolumeInfo, MetadataError) {
	var volumes []*VolumeInfo
	for _, v := range m.volumes {
		if v.NodeID == nodeID {
			volumes = append(volumes, v)
		}
	}
	return volumes, nil
}

func (m *MockMetadataStore) AddBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return ErrNotFound
	}
	volume.BackupNodes = append(volume.BackupNodes, nodeID)
	return nil
}

func (m *MockMetadataStore) RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError {
	volume, exists := m.volumes[volumeID]
	if !exists {
		return ErrNotFound
	}
	for i, n := range volume.BackupNodes {
		if n == nodeID {
			volume.BackupNodes = append(volume.BackupNodes[:i], volume.BackupNodes[i+1:]...)
			break
		}
	}
	return nil
}

func (m *MockMetadataStore) UnregisterVolume(ctx context.Context, volumeID string) MetadataError {
	delete(m.volumes, volumeID)
	return nil
}
