package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
)

var Fenced = errors.New("shard is fenced")
var IOError = errors.New("I/O error")

// Shard represents a single shard stored in Pebble
type Shard struct {
	mu       sync.RWMutex
	id       uint64
	clientID string
	db       *pebble.DB
	path     string
	readOnly bool
	volume   string
}

// NewShard creates a new shard or opens an existing one
func NewShard(volumeId string, path string, shardID uint64, clientID string, readOnly bool) (*Shard, error) {
	opts := &pebble.Options{
		// Configure Pebble for optimal performance
		MemTableSize:             64 << 20, // 64 MB
		MaxConcurrentCompactions: func() int { return 3 },
	}

	if readOnly {
		opts.ReadOnly = true
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", path, err)
	}

	s := &Shard{
		id:       shardID,
		clientID: clientID,
		db:       db,
		path:     path,
		readOnly: readOnly,
		volume:   volumeId,
	}

	return s, nil
}

// Put stores a value with indexes
func (s *Shard) Put(clientId string, entryID uint32, value []byte, indexes map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readOnly || !strings.EqualFold(clientId, s.clientID) {
		return Fenced
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Store the main value
	key := makeEntryKey(entryID)
	if err := batch.Set(key, value, pebble.Sync); err != nil {
		return IOError
	}

	// Store indexes
	for indexKey, indexValue := range indexes {
		idxKey := makeIndexKey(indexKey, entryID)
		if err := batch.Set(idxKey, indexValue, pebble.NoSync); err != nil {
			return IOError
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return IOError
	}

	return nil
}

// Get retrieves a value by entry ID
func (s *Shard) Get(entryID uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := makeEntryKey(entryID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, IOError
	}
	defer closer.Close()

	// Copy the value because it's only valid until closer.Close()
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// BatchGet retrieves multiple values by entry IDs
func (s *Shard) BatchGet(entryIDs []uint32) (map[uint32][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make(map[uint32][]byte)

	for _, entryID := range entryIDs {
		key := makeEntryKey(entryID)
		value, closer, err := s.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // Skip not found entries
			}
			return nil, fmt.Errorf("failed to get entry %d: %w", entryID, err)
		}

		// Copy the value
		result := make([]byte, len(value))
		copy(result, value)
		closer.Close()

		results[entryID] = result
	}

	return results, nil
}

// Close closes the shard and marks it as read-only
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return IOError
		}
		s.db = nil
	}

	// Reopen in read-only mode
	opts := &pebble.Options{
		ReadOnly: true,
	}

	db, err := pebble.Open(s.path, opts)
	if err != nil {
		return IOError
	}

	s.db = db
	s.readOnly = true

	return nil
}

// Delete deletes the shard data
func (s *Shard) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return IOError
		}
		s.db = nil
	}
	return nil
}

// IsReadOnly returns whether the shard is read-only
func (s *Shard) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readOnly
}

// GetID returns the shard ID
func (s *Shard) GetID() uint64 {
	return s.id
}

// GetClientID returns the client ID that owns this shard
func (s *Shard) GetClientID() string {
	return s.clientID
}

func (s *Shard) GetVolumeId() string {
	return s.volume
}

// makeEntryKey creates a key for an entry
// Format: "e:" + entryID (4 bytes)
func makeEntryKey(entryID uint32) []byte {
	key := make([]byte, 5)
	key[0] = 'e'
	binary.BigEndian.PutUint32(key[1:], entryID)
	return key
}

// makeIndexKey creates a key for an index
// Format: "i:" + indexKey + ":" + entryID (4 bytes)
func makeIndexKey(indexKey string, entryID uint32) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte('i')
	buf.WriteString(indexKey)
	buf.WriteByte(':')
	entryIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(entryIDBytes, entryID)
	buf.Write(entryIDBytes)
	return buf.Bytes()
}

// ErrNotFound is returned when an entry is not found
var ErrNotFound = fmt.Errorf("entry not found")
