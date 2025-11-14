package shard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewShard(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard("test-volume", shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer func(shard *Shard) {
		_ = shard.Close()
	}(shard)

	// Verify shard properties
	if shard.GetID() != 1 {
		t.Errorf("Expected shard ID 1, got %d", shard.GetID())
	}

	if shard.GetClientID() != "client-1" {
		t.Errorf("Expected client ID 'client-1', got '%s'", shard.GetClientID())
	}

	if shard.IsReadOnly() {
		t.Error("Expected shard to be writable, but it's read-only")
	}
}

func TestShardPutGet(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	clientId := "client-1"
	// Create a new shard
	shard, err := NewShard("test-volume", shardPath, 1, clientId, false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer func(shard *Shard) {
		_ = shard.Close()
	}(shard)

	// Test Put and Get
	entryID := uint32(100)
	value := []byte("test value")
	indexes := map[string][]byte{
		"key1": []byte("index1"),
		"key2": []byte("index2"),
	}

	// Put value
	if err := shard.Put(clientId, entryID, value, indexes); err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	// Get value
	retrievedValue, err := shard.Get(entryID)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	// Verify value
	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value '%s', got '%s'", string(value), string(retrievedValue))
	}
}

func TestShardReadOnly(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	clientId := "client-1"

	// Create a new shard
	shard, err := NewShard("test-volume", shardPath, 1, clientId, false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer func(shard *Shard) {
		_ = shard.Close()
	}(shard)

	// Put a value
	entryID := uint32(1)
	value := []byte("test value")
	if err := shard.Put(clientId, entryID, value, nil); err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	// Close the shard (make it read-only)
	if err := shard.Close(); err != nil {
		t.Fatalf("Failed to close shard: %v", err)
	}

	// Verify it's read-only
	if !shard.IsReadOnly() {
		t.Error("Expected shard to be read-only after close")
	}

	// Try to put another value (should fail)
	if err := shard.Put(clientId, 2, []byte("another value"), nil); err == nil {
		t.Error("Expected error when putting to read-only shard")
	}

	// Get should not still work
	retrievedValue, err := shard.Get(entryID)
	if err == nil {
		t.Errorf("Failed to get entry from read-only shard: %v", err)
	}

	if err != nil {
		return
	}
	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value '%s', got '%s'", string(value), string(retrievedValue))
	}
}

func TestShardNotFound(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard("test-volume", shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer func(shard *Shard) {
		_ = shard.Close()
	}(shard)

	// Try to get non-existent entry
	_, err = shard.Get(999)
	if err != ErrEntryNotFound {
		t.Errorf("Expected ErrEntryNotFound, got %v", err)
	}
}
