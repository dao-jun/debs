package volume

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debs/debs/pkg/cloud"
	"github.com/debs/debs/pkg/metadata"
	"golang.org/x/sys/unix"
)

var ErrVolumeNotMounted = errors.New("volume is not mounted")
var ErrVolumeAlreadyMounted = errors.New("volume is already mounted")
var ErrVolumeNotFound = errors.New("volume not found")
var ErrAttachVolume = errors.New("failed to attach volume")
var ErrDetachVolume = errors.New("failed to detach volume")
var ErrEnsureFilesystem = errors.New("failed to ensure filesystem")
var ErrNotMounted = errors.New("not mounted")
var ErrVolumeStats = errors.New("failed to get volume stats")
var ErrNoEnoughVolume = errors.New("not enough volume")
var ErrVmNotStarted = errors.New("vm not started")

func IsVolumeError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrVolumeNotMounted) ||
		errors.Is(err, ErrVolumeAlreadyMounted) ||
		errors.Is(err, ErrVolumeNotFound) ||
		errors.Is(err, ErrAttachVolume) ||
		errors.Is(err, ErrDetachVolume) ||
		errors.Is(err, ErrEnsureFilesystem) ||
		errors.Is(err, ErrNotMounted) ||
		errors.Is(err, ErrVolumeStats) ||
		errors.Is(err, ErrNoEnoughVolume) ||
		errors.Is(err, ErrVmNotStarted)
}

// VolumeManager manages EBS volumes on the local node
type VolumeManager struct {
	mu             sync.RWMutex
	provider       cloud.VolumeProvider
	metadataStore  metadata.MetadataStore
	nodeID         string
	mountedVolumes map[string]*MountedVolume // volumeID -> MountedVolume
	started        atomic.Bool
}

// MountedVolume represents a volume mounted on this node
type MountedVolume struct {
	VolumeID  string
	MountPath string
	IsPrimary bool
	MountTime time.Time
}

// NewVolumeManager creates a new VolumeManager
func NewVolumeManager(
	provider cloud.VolumeProvider,
	metadataStore metadata.MetadataStore,
	nodeID string,
) *VolumeManager {
	return &VolumeManager{
		provider:       provider,
		metadataStore:  metadataStore,
		nodeID:         nodeID,
		mountedVolumes: make(map[string]*MountedVolume),
		started:        atomic.Bool{},
	}
}

func (vm *VolumeManager) Start(ctx context.Context) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.started.Load() {
		return nil
	}

	volumes, err := vm.metadataStore.ListVolumesByNode(ctx, vm.nodeID)
	if err != nil {
		return err
	}
	for _, volume := range volumes {
		vm.mountedVolumes[volume.VolumeID] = &MountedVolume{
			VolumeID:  volume.VolumeID,
			MountPath: volume.MountPath,
			IsPrimary: volume.PrimaryNode == vm.nodeID,
		}
	}
	vm.started.Store(true)
	return nil
}

// AddMountedVolume is an admin interface to save a mounted volume.
func (vm *VolumeManager) AddMountedVolume(ctx context.Context, v *MountedVolume, nodes []string) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if !vm.started.Load() {
		return ErrVmNotStarted
	}

	var volumeID = v.VolumeID
	_, err := vm.metadataStore.GetVolume(ctx, volumeID)
	if !errors.Is(err, metadata.ErrNotFound) {
		return ErrVolumeAlreadyMounted
	}

	err = vm.metadataStore.RegisterVolume(ctx, &metadata.VolumeInfo{
		VolumeID:    volumeID,
		MountPath:   v.MountPath,
		Nodes:       nodes,
		PrimaryNode: vm.nodeID,
	})
	if err == nil {
		vm.mountedVolumes[volumeID] = v
	}

	return err
}

// ListMountedVolumes returns all mounted volumes
func (vm *VolumeManager) ListMountedVolumes() []*MountedVolume {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if !vm.started.Load() {
		return []*MountedVolume{}
	}

	volumes := make([]*MountedVolume, 0, len(vm.mountedVolumes))
	for _, v := range vm.mountedVolumes {
		volumes = append(volumes, v)
	}

	return volumes
}

// PromoteToPrimary promotes a backup node to primary for a volume
// This is used during failover when the primary node goes down
func (vm *VolumeManager) PromoteToPrimary(ctx context.Context, volumeID string) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if !vm.started.Load() {
		return ErrVmNotStarted
	}
	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return ErrNotMounted
	}

	if mounted.IsPrimary {
		return nil // Already primary
	}

	// Remount as read-write
	cmd := exec.Command("mount", "-o", "remount,rw", mounted.MountPath)
	if _, err := cmd.CombinedOutput(); err != nil {
		return ErrAttachVolume
	}

	mounted.IsPrimary = true

	// Update metadata
	if err := vm.metadataStore.UpdateVolumePrimary(ctx, volumeID, vm.nodeID); err != nil {
		return err
	}

	return nil
}

// GetShardPath returns the filesystem path for a shard
func (vm *VolumeManager) GetShardPath(volumeID string, shardID uint64) (string, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if !vm.started.Load() {
		return "", ErrVmNotStarted
	}
	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return "", ErrVolumeNotMounted
	}

	return filepath.Join(mounted.MountPath, fmt.Sprintf("shard-%d", shardID)), nil
}

// IsPrimaryForVolume checks if this node is the primary for a volume
func (vm *VolumeManager) IsPrimaryForVolume(volumeID string) bool {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if !vm.started.Load() {
		return false
	}
	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return false
	}

	return mounted.IsPrimary
}

// VolumeStats contains statistics about a volume
type VolumeStats struct {
	VolumeID       string
	TotalBytes     uint64
	AvailableBytes uint64
	UsedBytes      uint64
}

// GetVolumeStats returns disk space statistics for a volume
func (vm *VolumeManager) GetVolumeStats(volumeID string) (*VolumeStats, error) {
	vm.mu.RLock()
	mounted, exists := vm.mountedVolumes[volumeID]
	vm.mu.RUnlock()

	if !exists {
		return nil, ErrVolumeNotMounted
	}

	// Use syscall to get filesystem statistics
	var stat unix.Statfs_t
	if err := unix.Statfs(mounted.MountPath, &stat); err != nil {
		return nil, ErrVolumeStats
	}

	// Calculate space in bytes
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	availableBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - (stat.Bfree * uint64(stat.Bsize))

	return &VolumeStats{
		VolumeID:       volumeID,
		TotalBytes:     totalBytes,
		AvailableBytes: availableBytes,
		UsedBytes:      usedBytes,
	}, nil
}
