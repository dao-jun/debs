# Deployment Guide

This guide covers deploying the storage service on AWS EC2 with EBS multi-attach support.

## Prerequisites

### AWS Requirements

1. **EC2 Instances**
    - Instance type that supports EBS multi-attach (e.g., r5, r5d, m5, m5d, c5, c5d families)
    - Instances must be in the same Availability Zone
    - Instances should have IAM role with EC2 and EBS permissions

2. **IAM Permissions**

   The EC2 instance role needs the following permissions:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "ec2:CreateVolume",
           "ec2:DeleteVolume",
           "ec2:DescribeVolumes",
           "ec2:AttachVolume",
           "ec2:DetachVolume",
           "ec2:ModifyVolume",
           "ec2:DescribeInstances"
         ],
         "Resource": "*"
       }
     ]
   }
   ```

3. **Network**
    - Security group allowing gRPC traffic (port 50051)
    - Proper VPC and subnet configuration

4. **Metadata Store**
    - etcd cluster (recommended)
    - Or ZooKeeper cluster
    - Or Consul cluster

## Installation

### 1. Build the Binary

On your build machine:

```bash
cd storage-service
make build
```

Or download pre-built binaries from releases.

### 2. Deploy to EC2 Instances

Copy the binary to your EC2 instances:

```bash
scp bin/storage-server ec2-user@<instance-ip>:~/
```

### 3. Create Configuration

Create `/etc/storage-service/config.yaml` on each node:

```yaml
server:
  address: ":50051"

cloud:
  provider: "aws"
  region: "us-west-2"
  availability_zone: "us-west-2a"

metadata:
  type: "etcd"
  endpoints:
    - "etcd-node-1:2379"
    - "etcd-node-2:2379"
    - "etcd-node-3:2379"
  namespace: "/storage-service"

node:
  node_id: "node-1"  # Unique for each node
  instance_id: ""    # Auto-detected from EC2 metadata if empty
```

### 4. Create Systemd Service

Create `/etc/systemd/system/storage-service.service`:

```ini
[Unit]
Description=Storage Service
After=network.target

[Service]
Type=simple
User=storage
Group=storage
WorkingDirectory=/opt/storage-service
ExecStart=/opt/storage-service/bin/storage-server
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal
Environment="AWS_REGION=us-west-2"

[Install]
WantedBy=multi-user.target
```

### 5. Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable storage-service
sudo systemctl start storage-service
sudo systemctl status storage-service
```

## Multi-Attach Volume Setup

### Creating a Multi-Attach Volume

Using AWS CLI:

```bash
aws ec2 create-volume \
    --volume-type io2 \
    --size 100 \
    --availability-zone us-west-2a \
    --multi-attach-enabled \
    --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=storage-service-vol-1}]'
```

### Attaching Volume to Primary Node

```bash
# Get volume ID from the create-volume output
VOLUME_ID=vol-xxxxxxxxxxxxx

# Attach to primary instance
aws ec2 attach-volume \
    --volume-id $VOLUME_ID \
    --instance-id i-primary-instance \
    --device /dev/sdf
```

### Attaching Volume to Backup Nodes

```bash
# Attach to backup instances
aws ec2 attach-volume \
    --volume-id $VOLUME_ID \
    --instance-id i-backup-instance-1 \
    --device /dev/sdf

aws ec2 attach-volume \
    --volume-id $VOLUME_ID \
    --instance-id i-backup-instance-2 \
    --device /dev/sdf
```

### Manual Volume Mounting

On the primary node:

```bash
# Create mount point
sudo mkdir -p /$VOLUME_ID

# Format volume (only on first use)
sudo mkfs.ext4 /dev/sdf

# Mount as read-write
sudo mount -o rw,noatime /dev/sdf /$VOLUME_ID
```

On backup nodes:

```bash
# Create mount point
sudo mkdir -p /$VOLUME_ID

# Mount as read-only
sudo mount -o ro,noatime /dev/sdf /$VOLUME_ID
```

## High Availability Setup

### Architecture

```
┌─────────────────┐
│  Load Balancer  │
└────────┬────────┘
         │
    ┌────┴─────┐
    │          │
┌───▼──┐   ┌───▼──┐   ┌──────┐
│Node 1│   │Node 2│   │Node 3│
│(Pri) │   │(Bkp) │   │(Bkp) │
└──┬───┘   └──┬───┘   └──┬───┘
   │          │          │
   └──────────┴──────────┘
              │
       ┌──────▼──────┐
       │  EBS Volume │
       │ Multi-Attach│
       └─────────────┘
```

### Failover Process

1. **Detection**: External monitoring detects primary node failure
2. **Promotion**: Backup node is promoted to primary
3. **Remount**: Volume is remounted as read-write on new primary
4. **Update**: Metadata is updated to reflect new primary
5. **Resume**: Service continues with minimal downtime

### Implementing Failover

You can use tools like:
- AWS Auto Scaling with health checks
- Kubernetes with StatefulSets
- Custom monitoring with failover scripts

Example failover script:

```bash
#!/bin/bash
# failover.sh - Promote backup node to primary

VOLUME_ID=$1
OLD_MOUNT_PATH=/$VOLUME_ID

# Remount as read-write
sudo mount -o remount,rw $OLD_MOUNT_PATH

# Update metadata (using etcdctl example)
etcdctl put /storage-service/volumes/$VOLUME_ID/primary $(hostname)

# Restart service to pick up new role
sudo systemctl restart storage-service
```

## Monitoring

### Metrics to Monitor

1. **Node Health**
    - CPU usage
    - Memory usage
    - Disk I/O
    - Network I/O

2. **Volume Health**
    - Volume status
    - Attachment status
    - IOPS usage
    - Throughput

3. **Service Metrics**
    - Request latency
    - Request rate
    - Error rate
    - Active shards

### Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'storage-service'
    static_configs:
      - targets:
        - 'node-1:50051'
        - 'node-2:50051'
        - 'node-3:50051'
```

## Troubleshooting

### Volume Not Attaching

Check if:
- Volume and instance are in the same AZ
- Instance type supports multi-attach
- Volume is io2 type with multi-attach enabled

### Mount Fails

Check if:
- Device exists: `ls -l /dev/sdf`
- Filesystem exists: `sudo blkid /dev/sdf`
- Permissions are correct
- Volume is attached: `aws ec2 describe-volumes --volume-ids $VOLUME_ID`

### Service Won't Start

Check logs:
```bash
sudo journalctl -u storage-service -f
```

Common issues:
- Missing permissions
- Cannot connect to metadata store
- Port already in use

## Backup and Recovery

### Snapshot Strategy

Create regular snapshots of EBS volumes:

```bash
aws ec2 create-snapshot \
    --volume-id $VOLUME_ID \
    --description "Storage service backup $(date +%Y%m%d)"
```

### Recovery Process

1. Create volume from snapshot
2. Enable multi-attach
3. Attach to nodes
4. Mount volumes
5. Update metadata
6. Restart services

## Performance Tuning

### EBS Volume Configuration

- Use io2 volumes for best performance
- Provision appropriate IOPS based on workload
- Monitor IOPS usage and adjust as needed

### Instance Configuration

- Use instances with enhanced networking
- Enable EBS optimization
- Use instance types with high network bandwidth

### Pebble Configuration

Adjust Pebble settings in code for your workload:
- MemTableSize
- MaxConcurrentCompactions
- Cache size
- Bloom filter settings

## Security

### Best Practices

1. **Encryption**
    - Enable EBS encryption
    - Use KMS for key management
    - Encrypt data at rest

2. **Access Control**
    - Use IAM roles, not access keys
    - Follow principle of least privilege
    - Regularly rotate credentials

3. **Network Security**
    - Use security groups to restrict access
    - Enable VPC flow logs
    - Use private subnets when possible

4. **Audit**
    - Enable CloudTrail logging
    - Monitor API calls
    - Set up alerts for suspicious activity

## Cost Optimization

1. **Volume Sizing**
    - Right-size volumes based on usage
    - Delete unused volumes
    - Use lifecycle policies for snapshots

2. **Instance Selection**
    - Use Spot instances for non-critical workloads
    - Consider Reserved Instances for long-term usage
    - Use appropriate instance sizes

3. **IOPS Provisioning**
    - Monitor IOPS usage
    - Adjust provisioned IOPS as needed
    - Use GP3 for development environments