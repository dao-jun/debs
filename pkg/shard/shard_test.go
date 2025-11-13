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
	defer os.RemoveAll(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard(shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Delete()

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
	defer os.RemoveAll(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard(shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Delete()

	// Test Put and Get
	entryID := uint32(100)
	value := []byte("test value")
	indexes := map[string][]byte{
		"key1": []byte("index1"),
		"key2": []byte("index2"),
	}

	// Put value
	if err := shard.Put(entryID, value, indexes); err != nil {
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

func TestShardBatchGet(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard(shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Delete()

	// Put multiple entries
	entries := map[uint32][]byte{
		1: []byte("value1"),
		2: []byte("value2"),
		3: []byte("value3"),
	}

	for entryID, value := range entries {
		if err := shard.Put(entryID, value, nil); err != nil {
			t.Fatalf("Failed to put entry %d: %v", entryID, err)
		}
	}

	// Batch get
	entryIDs := []uint32{1, 2, 3, 999} // 999 doesn't exist
	results, err := shard.BatchGet(entryIDs)
	if err != nil {
		t.Fatalf("Failed to batch get: %v", err)
	}

	// Verify results
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	for entryID, expectedValue := range entries {
		if value, exists := results[entryID]; !exists {
			t.Errorf("Entry %d not found in results", entryID)
		} else if string(value) != string(expectedValue) {
			t.Errorf("Entry %d: expected '%s', got '%s'", entryID, string(expectedValue), string(value))
		}
	}

	// Verify that non-existent entry is not in results
	if _, exists := results[999]; exists {
		t.Error("Non-existent entry 999 should not be in results")
	}
}

func TestShardReadOnly(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "shard-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard(shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Delete()

	// Put a value
	entryID := uint32(1)
	value := []byte("test value")
	if err := shard.Put(entryID, value, nil); err != nil {
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
	if err := shard.Put(2, []byte("another value"), nil); err == nil {
		t.Error("Expected error when putting to read-only shard")
	}

	// Get should still work
	retrievedValue, err := shard.Get(entryID)
	if err != nil {
		t.Fatalf("Failed to get entry from read-only shard: %v", err)
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
	defer os.RemoveAll(tempDir)

	shardPath := filepath.Join(tempDir, "shard-1")

	// Create a new shard
	shard, err := NewShard(shardPath, 1, "client-1", false)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}
	defer shard.Delete()

	// Try to get non-existent entry
	_, err = shard.Get(999)
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}
