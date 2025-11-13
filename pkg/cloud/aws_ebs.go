package cloud

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// AWSEBSProvider implements VolumeProvider for AWS EBS
type AWSEBSProvider struct {
	client *ec2.Client
	region string
}

// NewAWSEBSProvider creates a new AWS EBS provider
func NewAWSEBSProvider(ctx context.Context, region string) (*AWSEBSProvider, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &AWSEBSProvider{
		client: ec2.NewFromConfig(cfg),
		region: region,
	}, nil
}

// CreateVolume creates a new EBS volume with multi-attach enabled
func (p *AWSEBSProvider) CreateVolume(ctx context.Context, sizeGB int64, availabilityZone string) (*Volume, error) {
	input := &ec2.CreateVolumeInput{
		AvailabilityZone:   aws.String(availabilityZone),
		Size:               aws.Int32(int32(sizeGB)),
		VolumeType:         types.VolumeTypeIo2, // io2 supports multi-attach
		MultiAttachEnabled: aws.Bool(true),
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeVolume,
				Tags: []types.Tag{
					{
						Key:   aws.String("Name"),
						Value: aws.String("storage-service-volume"),
					},
					{
						Key:   aws.String("MultiAttach"),
						Value: aws.String("enabled"),
					},
				},
			},
		},
	}

	output, err := p.client.CreateVolume(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create volume: %w", err)
	}

	return &Volume{
		VolumeID:           aws.ToString(output.VolumeId),
		SizeGB:             int64(aws.ToInt32(output.Size)),
		State:              VolumeState(output.State),
		AvailabilityZone:   aws.ToString(output.AvailabilityZone),
		VolumeType:         string(output.VolumeType),
		MultiAttachEnabled: aws.ToBool(output.MultiAttachEnabled),
	}, nil
}

// DeleteVolume deletes an EBS volume
func (p *AWSEBSProvider) DeleteVolume(ctx context.Context, volumeID string) error {
	input := &ec2.DeleteVolumeInput{
		VolumeId: aws.String(volumeID),
	}

	_, err := p.client.DeleteVolume(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete volume %s: %w", volumeID, err)
	}

	return nil
}

// DescribeVolume gets information about an EBS volume
func (p *AWSEBSProvider) DescribeVolume(ctx context.Context, volumeID string) (*Volume, error) {
	input := &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	}

	output, err := p.client.DescribeVolumes(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe volume %s: %w", volumeID, err)
	}

	if len(output.Volumes) == 0 {
		return nil, fmt.Errorf("volume %s not found", volumeID)
	}

	vol := output.Volumes[0]
	volume := &Volume{
		VolumeID:           aws.ToString(vol.VolumeId),
		SizeGB:             int64(aws.ToInt32(vol.Size)),
		State:              VolumeState(vol.State),
		AvailabilityZone:   aws.ToString(vol.AvailabilityZone),
		VolumeType:         string(vol.VolumeType),
		MultiAttachEnabled: aws.ToBool(vol.MultiAttachEnabled),
		Attachments:        make([]VolumeAttachment, 0, len(vol.Attachments)),
	}

	for _, att := range vol.Attachments {
		volume.Attachments = append(volume.Attachments, VolumeAttachment{
			VolumeID:   aws.ToString(att.VolumeId),
			InstanceID: aws.ToString(att.InstanceId),
			Device:     aws.ToString(att.Device),
			State:      string(att.State),
		})
	}

	return volume, nil
}

// AttachVolume attaches an EBS volume to an EC2 instance
func (p *AWSEBSProvider) AttachVolume(ctx context.Context, volumeID string, instanceID string, device string) error {
	input := &ec2.AttachVolumeInput{
		VolumeId:   aws.String(volumeID),
		InstanceId: aws.String(instanceID),
		Device:     aws.String(device),
	}

	_, err := p.client.AttachVolume(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to attach volume %s to instance %s: %w", volumeID, instanceID, err)
	}

	return nil
}

// DetachVolume detaches an EBS volume from an EC2 instance
func (p *AWSEBSProvider) DetachVolume(ctx context.Context, volumeID string, instanceID string, force bool) error {
	input := &ec2.DetachVolumeInput{
		VolumeId:   aws.String(volumeID),
		InstanceId: aws.String(instanceID),
		Force:      aws.Bool(force),
	}

	_, err := p.client.DetachVolume(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to detach volume %s from instance %s: %w", volumeID, instanceID, err)
	}

	return nil
}

// WaitForVolumeAvailable waits until the volume is in available state
func (p *AWSEBSProvider) WaitForVolumeAvailable(ctx context.Context, volumeID string) error {
	waiter := ec2.NewVolumeAvailableWaiter(p.client)

	input := &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	}

	err := waiter.Wait(ctx, input, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for volume %s to become available: %w", volumeID, err)
	}

	return nil
}

// WaitForVolumeAttached waits until the volume is attached to an instance
func (p *AWSEBSProvider) WaitForVolumeAttached(ctx context.Context, volumeID string, instanceID string) error {
	waiter := ec2.NewVolumeInUseWaiter(p.client)

	input := &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	}

	err := waiter.Wait(ctx, input, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for volume %s to be attached: %w", volumeID, err)
	}

	return nil
}

// WaitForVolumeDetached waits until the volume is detached from an instance
func (p *AWSEBSProvider) WaitForVolumeDetached(ctx context.Context, volumeID string, instanceID string) error {
	waiter := ec2.NewVolumeAvailableWaiter(p.client)

	input := &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeID},
	}

	err := waiter.Wait(ctx, input, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for volume %s to be detached: %w", volumeID, err)
	}

	return nil
}

// ListVolumes lists all EBS volumes
func (p *AWSEBSProvider) ListVolumes(ctx context.Context) ([]*Volume, error) {
	input := &ec2.DescribeVolumesInput{}

	output, err := p.client.DescribeVolumes(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list volumes: %w", err)
	}

	volumes := make([]*Volume, 0, len(output.Volumes))
	for _, vol := range output.Volumes {
		volume := &Volume{
			VolumeID:           aws.ToString(vol.VolumeId),
			SizeGB:             int64(aws.ToInt32(vol.Size)),
			State:              VolumeState(vol.State),
			AvailabilityZone:   aws.ToString(vol.AvailabilityZone),
			VolumeType:         string(vol.VolumeType),
			MultiAttachEnabled: aws.ToBool(vol.MultiAttachEnabled),
			Attachments:        make([]VolumeAttachment, 0, len(vol.Attachments)),
		}

		for _, att := range vol.Attachments {
			volume.Attachments = append(volume.Attachments, VolumeAttachment{
				VolumeID:   aws.ToString(att.VolumeId),
				InstanceID: aws.ToString(att.InstanceId),
				Device:     aws.ToString(att.Device),
				State:      string(att.State),
			})
		}

		volumes = append(volumes, volume)
	}

	return volumes, nil
}

// EnableMultiAttach enables multi-attach for a volume
// Note: For AWS EBS, multi-attach must be enabled at creation time and cannot be modified later
func (p *AWSEBSProvider) EnableMultiAttach(ctx context.Context, volumeID string) error {
	// Check if volume already has multi-attach enabled
	volume, err := p.DescribeVolume(ctx, volumeID)
	if err != nil {
		return err
	}

	if volume.MultiAttachEnabled {
		return nil // Already enabled
	}

	return fmt.Errorf("multi-attach cannot be enabled on existing volume %s; it must be set at creation time", volumeID)
}
