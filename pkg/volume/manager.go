package volume

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/debs/debs/pkg/cloud"
	"github.com/debs/debs/pkg/metadata"
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

// VolumeManager manages EBS volumes on the local node
type VolumeManager struct {
	mu             sync.RWMutex
	provider       cloud.VolumeProvider
	metadataStore  metadata.MetadataStore
	nodeID         string
	instanceID     string
	mountedVolumes map[string]*MountedVolume // volumeID -> MountedVolume
}

// MountedVolume represents a volume mounted on this node
type MountedVolume struct {
	VolumeID  string
	MountPath string
	Device    string
	IsPrimary bool
	MountTime time.Time
}

// NewVolumeManager creates a new VolumeManager
func NewVolumeManager(
	provider cloud.VolumeProvider,
	metadataStore metadata.MetadataStore,
	nodeID string,
	instanceID string,
) *VolumeManager {
	return &VolumeManager{
		provider:       provider,
		metadataStore:  metadataStore,
		nodeID:         nodeID,
		instanceID:     instanceID,
		mountedVolumes: make(map[string]*MountedVolume),
	}
}

// MountVolume mounts an EBS volume to the node
// The volume is mounted at /{volumeID}
func (vm *VolumeManager) MountVolume(ctx context.Context, volumeID string, device string, isPrimary bool) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Check if already mounted
	if _, exists := vm.mountedVolumes[volumeID]; exists {
		return ErrVolumeAlreadyMounted
	}

	// Attach the volume to this instance
	err := vm.provider.AttachVolume(ctx, volumeID, vm.instanceID, device)
	if err != nil {
		return ErrAttachVolume
	}

	// Wait for the volume to be attached
	err = vm.provider.WaitForVolumeAttached(ctx, volumeID, vm.instanceID)
	if err != nil {
		return ErrAttachVolume
	}

	// Wait for device to appear in the system
	if err := vm.waitForDevice(device, 30*time.Second); err != nil {
		return err
	}

	// Create mount point
	mountPath := fmt.Sprintf("/%s", volumeID)
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return ErrAttachVolume
	}

	// Check if the device has a filesystem, if not create one
	if err := vm.ensureFilesystem(device); err != nil {
		return ErrEnsureFilesystem
	}

	// Mount the volume
	mountOptions := "rw,noatime"
	if !isPrimary {
		// Backup nodes mount as read-only
		mountOptions = "ro,noatime"
	}

	cmd := exec.Command("mount", "-o", mountOptions, device, mountPath)
	if _, err := cmd.CombinedOutput(); err != nil {
		return ErrAttachVolume
	}

	// Record the mounted volume
	vm.mountedVolumes[volumeID] = &MountedVolume{
		VolumeID:  volumeID,
		MountPath: mountPath,
		Device:    device,
		IsPrimary: isPrimary,
		MountTime: time.Now(),
	}

	// Update metadata
	volumeInfo, err := vm.metadataStore.GetVolume(ctx, volumeID)
	if err != nil {
		// If volume doesn't exist in metadata, register it
		volumeInfo = &metadata.VolumeInfo{
			VolumeID:    volumeID,
			NodeID:      vm.nodeID,
			IsPrimary:   isPrimary,
			MountPath:   mountPath,
			BackupNodes: []string{},
		}
		if err = vm.metadataStore.RegisterVolume(ctx, volumeInfo); err != nil {
			return metadata.ErrMetadata
		}
	} else {
		// Add this node as a backup node if not primary
		if !isPrimary {
			if err := vm.metadataStore.AddBackupNode(ctx, volumeID, vm.nodeID); err != nil {
				return metadata.ErrMetadata
			}
		}
	}

	return nil
}

// UnmountVolume unmounts an EBS volume from the node
func (vm *VolumeManager) UnmountVolume(ctx context.Context, volumeID string, force bool) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return ErrVolumeNotMounted
	}

	// Unmount the volume
	cmd := exec.Command("umount", mounted.MountPath)
	if _, err := cmd.CombinedOutput(); err != nil {
		if !force {
			return ErrDetachVolume
		}
		// Force unmount
		cmd = exec.Command("umount", "-f", mounted.MountPath)
		if _, err := cmd.CombinedOutput(); err != nil {
			return ErrDetachVolume
		}
	}

	// Detach the volume
	err := vm.provider.DetachVolume(ctx, volumeID, vm.instanceID, force)
	if err != nil {
		return ErrDetachVolume
	}

	// Wait for the volume to be detached
	if !force {
		err = vm.provider.WaitForVolumeDetached(ctx, volumeID, vm.instanceID)
		if err != nil {
			return ErrDetachVolume
		}
	}

	// Remove from mounted volumes
	delete(vm.mountedVolumes, volumeID)

	// Update metadata
	if mounted.IsPrimary {
		// If this was the primary node, we should update the metadata to reflect that
		// In a real system, a leader election or external controller should handle failover
	} else {
		// Remove from backup nodes
		if err := vm.metadataStore.RemoveBackupNode(ctx, volumeID, vm.nodeID); err != nil {
			return metadata.ErrMetadata
		}
	}

	return nil
}

// GetMountedVolume returns information about a mounted volume
func (vm *VolumeManager) GetMountedVolume(volumeID string) (*MountedVolume, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return nil, ErrNotMounted
	}

	return mounted, nil
}

// ListMountedVolumes returns all mounted volumes
func (vm *VolumeManager) ListMountedVolumes() []*MountedVolume {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

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

	mounted, exists := vm.mountedVolumes[volumeID]
	if !exists {
		return fmt.Errorf("volume %s is not mounted", volumeID)
	}

	if mounted.IsPrimary {
		return nil // Already primary
	}

	// Remount as read-write
	cmd := exec.Command("mount", "-o", "remount,rw", mounted.MountPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to remount volume as read-write: %s, %w", string(output), err)
	}

	mounted.IsPrimary = true

	// Update metadata
	if err := vm.metadataStore.UpdateVolumePrimary(ctx, volumeID, vm.nodeID); err != nil {
		return fmt.Errorf("failed to update volume primary in metadata: %w", err)
	}

	return nil
}

// waitForDevice waits for a device to appear in the system
func (vm *VolumeManager) waitForDevice(device string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(device); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ErrAttachVolume
}

// ensureFilesystem checks if a filesystem exists on the device, and creates one if not
func (vm *VolumeManager) ensureFilesystem(device string) error {
	// Check if filesystem exists
	cmd := exec.Command("blkid", device)
	if err := cmd.Run(); err == nil {
		// Filesystem exists
		return nil
	}

	// Create ext4 filesystem
	cmd = exec.Command("mkfs.ext4", "-F", device)
	if _, err := cmd.CombinedOutput(); err != nil {
		return ErrEnsureFilesystem
	}

	return nil
}

// GetVolumeForShard returns the volume ID that contains the given shard
func (vm *VolumeManager) GetVolumeForShard(ctx context.Context, shardID uint64) (string, error) {
	shardInfo, err := vm.metadataStore.GetShard(ctx, shardID)
	if err != nil {
		return "", metadata.ErrMetadata
	}

	return shardInfo.VolumeID, nil
}

// GetShardPath returns the filesystem path for a shard
func (vm *VolumeManager) GetShardPath(volumeID string, shardID uint64) (string, error) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

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
