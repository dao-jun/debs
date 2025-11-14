package cloud

import (
	"context"
)

// VolumeState represents the state of an EBS volume
type VolumeState string

const (
	VolumeStateCreating  VolumeState = "creating"
	VolumeStateAvailable VolumeState = "available"
	VolumeStateInUse     VolumeState = "in-use"
	VolumeStateDeleting  VolumeState = "deleting"
	VolumeStateDeleted   VolumeState = "deleted"
	VolumeStateError     VolumeState = "error"
)

// Volume represents a cloud volume (EBS)
type Volume struct {
	VolumeID           string
	SizeGB             int64
	State              VolumeState
	AvailabilityZone   string
	VolumeType         string
	MultiAttachEnabled bool
	Attachments        []VolumeAttachment
}

// VolumeAttachment represents a volume attachment to an instance
type VolumeAttachment struct {
	VolumeID string
	NodeId   string
	Device   string
	State    string
}

// VolumeProvider is the interface for cloud volume operations
// This abstracts operations on cloud provider block storage (AWS EBS, Aliyun EBS, etc.)
type VolumeProvider interface {
	// CreateVolume creates a new volume with multi-attach enabled
	CreateVolume(ctx context.Context, sizeGB int64, availabilityZone string) (*Volume, error)

	// DeleteVolume deletes a volume
	DeleteVolume(ctx context.Context, volumeID string) error

	// DescribeVolume gets information about a volume
	DescribeVolume(ctx context.Context, volumeID string) (*Volume, error)

	// AttachVolume attaches a volume to an instance
	AttachVolume(ctx context.Context, volumeID string, nodeId string, device string) error

	// DetachVolume detaches a volume from an instance
	DetachVolume(ctx context.Context, volumeID string, nodeId string, force bool) error

	// WaitForVolumeAvailable waits until the volume is in available state
	WaitForVolumeAvailable(ctx context.Context, volumeID string) error

	// WaitForVolumeAttached waits until the volume is attached to an instance
	WaitForVolumeAttached(ctx context.Context, volumeID string, nodeId string) error

	// WaitForVolumeDetached waits until the volume is detached from an instance
	WaitForVolumeDetached(ctx context.Context, volumeID string, nodeId string) error

	// ListVolumes lists all volumes
	ListVolumes(ctx context.Context) ([]*Volume, error)

	// EnableMultiAttach enables multi-attach for a volume (if not already enabled)
	EnableMultiAttach(ctx context.Context, volumeID string) error
}
