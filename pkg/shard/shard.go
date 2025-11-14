package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

var ErrFenced = errors.New("shard is fenced")
var ErrIO = errors.New("I/O error")

// ErrEntryNotFound is returned when an entry is not found
var ErrEntryNotFound = errors.New("entry not found")
var ErrShardNotFound = errors.New("shard not found")
var ErrWriteFailed = errors.New("write failed")
var ErrReadFailed = errors.New("read failed")
var ErrNotShardLeader = errors.New("not shard leader")

// Shard represents a single shard stored in Pebble
type Shard struct {
	mu       sync.RWMutex
	id       uint64
	clientID string
	db       *pebble.DB
	path     string
	readOnly bool
	volume   string
	closed   atomic.Bool
}

// NewShard creates a new shard or opens an existing one
func NewShard(volumeId string, path string, shardID uint64, clientID string, readOnly bool) (*Shard, error) {
	var db *pebble.DB = nil
	var err error = nil
	if !readOnly {
		db, err = pebble.Open(path, &pebble.Options{
			DisableWAL: true,
			// Configure Pebble for optimal performance
			MemTableSize:             64 << 20, // 64 MB
			MaxConcurrentCompactions: func() int { return 3 },
		})
		if err != nil {
			return nil, ErrIO
		}
	}

	s := &Shard{
		id:       shardID,
		clientID: clientID,
		db:       db,
		path:     path,
		readOnly: readOnly,
		volume:   volumeId,
		closed:   atomic.Bool{},
	}
	return s, err
}

func (s *Shard) openDbIfNeeded() error {
	if s.db != nil || !s.readOnly {
		return nil
	}
	opts := &pebble.Options{
		ReadOnly: true,
		Cache:    GetPebbleCache(),
	}
	db, err := pebble.Open(s.path, opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Put stores a value with indexes
func (s *Shard) Put(clientId string, entryID uint32, value []byte, indexes map[string][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return ErrFenced
	}
	if s.readOnly || !strings.EqualFold(clientId, s.clientID) {
		return ErrFenced
	}

	batch := s.db.NewBatch()
	defer func(batch *pebble.Batch) {
		if err := batch.Close(); err != nil {
			slog.Error("failed to close batch", "error", err)
		}
	}(batch)

	// Store the main value
	key := makeEntryKey(entryID)
	if err := batch.Set(key, value, pebble.Sync); err != nil {
		return ErrWriteFailed
	}

	// Store indexes
	for indexKey, indexValue := range indexes {
		idxKey := makeIndexKey(indexKey, entryID)
		if err := batch.Set(idxKey, indexValue, pebble.NoSync); err != nil {
			return ErrWriteFailed
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return ErrWriteFailed
	}

	return nil
}

// Get retrieves a value by entry ID
func (s *Shard) Get(entryID uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed.Load() {
		return nil, ErrFenced
	}
	if err := s.openDbIfNeeded(); err != nil {
		return nil, ErrReadFailed
	}
	key := makeEntryKey(entryID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrEntryNotFound
		}
		return nil, ErrReadFailed
	}
	defer func(closer io.Closer) {
		if err := closer.Close(); err != nil {
			slog.Error("failed to close closer", "error", err)
		}
	}(closer)

	// Copy the value because it's only valid until closer.Close()
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// Close closes the shard and marks it as read-only
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil
	}
	s.closed.Store(true)
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return ErrIO
		}
		s.db = nil
	}
	s.readOnly = true

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
