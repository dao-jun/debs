package metadata

import (
	"context"
	"testing"
	"time"
)

var _ MetadataStore = (*TestMockMetadataStore)(nil)

// MockMetadataStore is a mock implementation for testing
type TestMockMetadataStore struct {
	failCount      int
	currentAttempt int
}

func (m *TestMockMetadataStore) RegisterNode(ctx context.Context, info NodeInfo) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) UnregisterNode(ctx context.Context, nodeID string) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) GenerateShardID(ctx context.Context) (uint64, MetadataError) {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return 0, ErrMetadata
	}
	return 12345, nil
}

func (m *TestMockMetadataStore) CreateShard(ctx context.Context, shard *ShardInfo) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) GetShard(ctx context.Context, shardID uint64) (*ShardInfo, MetadataError) {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return nil, ErrMetadata
	}
	return &ShardInfo{ShardID: shardID}, nil
}

func (m *TestMockMetadataStore) DeleteShard(ctx context.Context, shardID uint64) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) RegisterVolume(ctx context.Context, volume *VolumeInfo) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) GetVolume(ctx context.Context, volumeID string) (*VolumeInfo, MetadataError) {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return nil, ErrMetadata
	}
	return &VolumeInfo{VolumeID: volumeID}, nil
}

func (m *TestMockMetadataStore) UpdateVolumePrimary(ctx context.Context, volumeID string, newPrimaryNodeID string) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) ListVolumesByNode(ctx context.Context, nodeID string) ([]*VolumeInfo, MetadataError) {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return nil, ErrMetadata
	}
	return []*VolumeInfo{{VolumeID: "vol-1"}}, nil
}

func (m *TestMockMetadataStore) AddBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) RemoveBackupNode(ctx context.Context, volumeID string, nodeID string) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func (m *TestMockMetadataStore) UnregisterVolume(ctx context.Context, volumeID string) MetadataError {
	m.currentAttempt++
	if m.currentAttempt <= m.failCount {
		return ErrMetadata
	}
	return nil
}

func TestRetryWrapperSuccess(t *testing.T) {
	// Test that operations succeed on first attempt
	mock := &TestMockMetadataStore{failCount: 0}
	config := &RetryConfig{
		MaxElapsedTime:  1 * time.Second,
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		Multiplier:      2.0,
	}
	wrapper := NewRetryWrapper(mock, config)
	ctx := context.Background()

	// Test GenerateShardID
	shardID, err := wrapper.GenerateShardID(ctx)
	if err != nil {
		t.Errorf("GenerateShardID failed: %v", err)
	}
	if shardID != 12345 {
		t.Errorf("Expected shard ID 12345, got %d", shardID)
	}
}

func TestRetryWrapperRetrySuccess(t *testing.T) {
	// Test that operations succeed after retries
	mock := &TestMockMetadataStore{failCount: 2}
	config := &RetryConfig{
		MaxElapsedTime:  2 * time.Second,
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		Multiplier:      2.0,
	}
	wrapper := NewRetryWrapper(mock, config)
	ctx := context.Background()

	// Test CreateShard - should succeed after 2 failures
	shard := &ShardInfo{ShardID: 1, VolumeID: "vol-1"}
	err := wrapper.CreateShard(ctx, shard)
	if err != nil {
		t.Errorf("CreateShard failed after retries: %v", err)
	}
	if mock.currentAttempt != 3 {
		t.Errorf("Expected 3 attempts, got %d", mock.currentAttempt)
	}
}

func TestRetryWrapperFailure(t *testing.T) {
	// Test that operations fail after max retries
	mock := &TestMockMetadataStore{failCount: 100} // Always fail
	config := &RetryConfig{
		MaxElapsedTime:  500 * time.Millisecond, // Short timeout for test
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     50 * time.Millisecond,
		Multiplier:      2.0,
	}
	wrapper := NewRetryWrapper(mock, config)
	ctx := context.Background()

	// Test GetShard - should fail after max elapsed time
	_, err := wrapper.GetShard(ctx, 1)
	if err == nil {
		t.Error("GetShard should have failed after max retries")
	}
}

func TestRetryWrapperAllMethods(t *testing.T) {
	// Test all methods succeed with retries
	config := &RetryConfig{
		MaxElapsedTime:  1 * time.Second,
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		Multiplier:      2.0,
	}
	ctx := context.Background()

	tests := []struct {
		name      string
		failCount int
		operation func(*RetryWrapper) error
	}{
		{
			name:      "DeleteShard",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.DeleteShard(ctx, 1)
			},
		},
		{
			name:      "RegisterVolume",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.RegisterVolume(ctx, &VolumeInfo{VolumeID: "vol-1"})
			},
		},
		{
			name:      "UpdateVolumePrimary",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.UpdateVolumePrimary(ctx, "vol-1", "node-1")
			},
		},
		{
			name:      "AddBackupNode",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.AddBackupNode(ctx, "vol-1", "node-2")
			},
		},
		{
			name:      "RemoveBackupNode",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.RemoveBackupNode(ctx, "vol-1", "node-2")
			},
		},
		{
			name:      "UnregisterVolume",
			failCount: 1,
			operation: func(w *RetryWrapper) error {
				return w.UnregisterVolume(ctx, "vol-1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &TestMockMetadataStore{failCount: tt.failCount}
			wrapper := NewRetryWrapper(mock, config)

			err := tt.operation(wrapper)
			if err != nil {
				t.Errorf("%s failed: %v", tt.name, err)
			}
			if mock.currentAttempt != tt.failCount+1 {
				t.Errorf("Expected %d attempts, got %d", tt.failCount+1, mock.currentAttempt)
			}
		})
	}
}

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()
	if config.MaxElapsedTime != 10*time.Second {
		t.Errorf("Expected MaxElapsedTime 10s, got %v", config.MaxElapsedTime)
	}
	if config.InitialInterval != 100*time.Millisecond {
		t.Errorf("Expected InitialInterval 100ms, got %v", config.InitialInterval)
	}
	if config.MaxInterval != 2*time.Second {
		t.Errorf("Expected MaxInterval 2s, got %v", config.MaxInterval)
	}
	if config.Multiplier != 1.5 {
		t.Errorf("Expected Multiplier 1.5, got %v", config.Multiplier)
	}
}

func TestRetryWrapperWithNilConfig(t *testing.T) {
	// Test that nil config uses defaults
	mock := &TestMockMetadataStore{failCount: 0}
	wrapper := NewRetryWrapper(mock, nil)

	if wrapper.config.MaxElapsedTime != 10*time.Second {
		t.Errorf("Expected default MaxElapsedTime, got %v", wrapper.config.MaxElapsedTime)
	}

	ctx := context.Background()
	_, err := wrapper.GenerateShardID(ctx)
	if err != nil {
		t.Errorf("GenerateShardID failed: %v", err)
	}
}
