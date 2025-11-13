# Architecture Overview

## System Design

The storage service is a distributed storage system designed to leverage cloud-native EBS volumes with multi-attach capabilities. This design eliminates the need for implementing custom replication mechanisms while providing high availability and fault tolerance.

## Core Concepts

### Shard

A **Shard** is a fundamental unit of data storage:
- Represents a segment of data (e.g., 2GB)
- Stored using Pebble (Go port of RocksDB)
- Has a globally unique, monotonically increasing ID
- Belongs to exactly one client
- Can be in one of three states: active, read-only, or deleted
- Located at: `/{volumeId}/shard-{shardId}/`

### Volume

A **Volume** is an EBS volume with multi-attach enabled:
- Contains multiple shards
- Mounted at: `/{volumeId}/`
- Can be attached to multiple nodes simultaneously
- Has one primary node (read-write) and multiple backup nodes (read-only)
- Uses io2 volume type for multi-attach support

### Node

A **Node** is an EC2 instance running the storage service:
- Has a unique node ID
- Can be primary or backup for multiple volumes
- Manages shards on locally mounted volumes
- Communicates via gRPC

## Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
└────────────────────────┬────────────────────────────────────┘
                         │ gRPC
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    Storage Service                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              gRPC Server (pkg/server)                │  │
│  │  - Put/BatchPut  - Get/BatchGet                      │  │
│  │  - NewShard      - CloseShard     - DeleteShard      │  │
│  └───────────┬──────────────────────────────────────────┘  │
│              │                                              │
│  ┌───────────▼───────────────┐  ┌─────────────────────┐   │
│  │  ShardManager (pkg/shard) │  │ VolumeManager       │   │
│  │  - Create/Open/Close      │◄─┤ (pkg/volume)        │   │
│  │  - Put/Get operations     │  │ - Mount/Unmount     │   │
│  │  - Shard lifecycle        │  │ - Failover support  │   │
│  └──────────┬────────────────┘  └──────────┬──────────┘   │
│             │                               │              │
│  ┌──────────▼────────────┐     ┌───────────▼──────────┐   │
│  │  Shard (pkg/shard)    │     │  Cloud Provider      │   │
│  │  - Pebble DB          │     │  (pkg/cloud)         │   │
│  │  - Key-Value storage  │     │  - AWS EBS (io2)     │   │
│  │  - Index support      │     │  - Volume operations │   │
│  └───────────────────────┘     └──────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Metadata Store (pkg/metadata)                │  │
│  │  - Shard → Volume mapping                            │  │
│  │  - Volume → Node mapping                             │  │
│  │  - Shard ID generation                               │  │
│  │  Interface only - implement with etcd/ZK/Consul     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Write Path (Put Operation)

1. Client sends PutRequest via gRPC
2. Server receives request, validates clientId
3. ShardManager retrieves or loads the shard
4. Shard verifies client ownership
5. Data is written to Pebble with indexes
6. Pebble persists to disk with WAL
7. Response sent to client

```
Client → gRPC Server → ShardManager → Shard → Pebble → Disk
```

### Read Path (Get Operation)

1. Client sends GetRequest via gRPC
2. Server receives request
3. ShardManager retrieves or loads the shard
4. Shard reads from Pebble
5. Data returned through the stack
6. Response sent to client

```
Client ← gRPC Server ← ShardManager ← Shard ← Pebble ← Disk
```

### Shard Creation Flow

1. Client requests NewShard
2. Server generates new ShardID via metadata store
3. ShardManager selects a volume (primary node only)
4. Creates shard directory: `/{volumeId}/shard-{shardId}/`
5. Initializes Pebble database
6. Registers shard in metadata store
7. Returns ShardID to client

```
Client → Server → Metadata (generate ID) → ShardManager → Volume → Disk
                ↓
          Metadata (register shard)
```

## Multi-Attach Architecture

### Normal Operation

```
┌──────────┐        ┌──────────┐        ┌──────────┐
│  Node A  │        │  Node B  │        │  Node C  │
│ (Primary)│        │ (Backup) │        │ (Backup) │
└────┬─────┘        └────┬─────┘        └────┬─────┘
     │                   │                    │
     │ RW                │ RO                 │ RO
     │                   │                    │
     └───────────────────┴────────────────────┘
                         │
                  ┌──────▼──────┐
                  │ EBS Volume  │
                  │ (Multi-Att) │
                  └─────────────┘
```

- **Primary Node (Node A)**: Mounts volume as read-write, serves all write requests
- **Backup Nodes (B, C)**: Mount volume as read-only, can serve read requests
- All nodes have identical data view

### Failover Scenario

When primary node fails:

1. **Detection**: Monitoring system detects primary node failure
2. **Election**: New primary is selected from backup nodes
3. **Promotion**: Selected node remounts volume as read-write
4. **Update**: Metadata store updated with new primary
5. **Resume**: Service continues with new primary

```
Before:                    After:
┌──────────┐               ┌──────────┐
│  Node A  │  ✗            │  Node B  │
│ (Primary)│  FAILED       │ (Primary)│ ← Promoted
└────┬─────┘               └────┬─────┘
     │                           │
┌────▼─────┐               ┌────▼─────┐
│  Node B  │               │  Node C  │
│ (Backup) │               │ (Backup) │
└──────────┘               └──────────┘
```

## Key Design Decisions

### 1. Pebble for Storage Engine

**Why Pebble?**
- Go-native (no CGO required)
- LSM-tree architecture for write-heavy workloads
- Built-in WAL for durability
- Efficient compaction
- Battle-tested (used in CockroachDB)

### 2. Interface-Based Abstractions

**Metadata Store Interface:**
- Allows integration with any consensus system
- No vendor lock-in
- Easy to test with mocks

**Cloud Provider Interface:**
- Support for multiple cloud providers
- Easy to add new providers (Aliyun, GCP, etc.)
- Consistent API across providers

### 3. Multi-Attach Over Replication

**Benefits:**
- No custom replication protocol needed
- EBS handles data durability (3x replication)
- Lower operational complexity
- Native cloud integration
- Simplified failover

**Tradeoffs:**
- Requires EBS multi-attach support
- All nodes must be in same AZ
- io2 volumes required (more expensive)
- Limited to supported instance types

### 4. Shard-Based Data Organization

**Advantages:**
- Granular data management
- Easy to implement retention policies
- Simple to implement shard migration
- Client isolation
- Predictable performance

**Considerations:**
- Shard size limits (e.g., 2GB)
- Metadata overhead
- Shard lifecycle management

## Scalability

### Horizontal Scaling

1. **Add more volumes**: Create new EBS volumes for new shards
2. **Add more nodes**: Attach volumes to new nodes as backup
3. **Load balancing**: Distribute shards across volumes

### Vertical Scaling

1. **Increase volume size**: Expand EBS volumes
2. **Provision more IOPS**: Adjust io2 IOPS
3. **Upgrade instance types**: Use larger EC2 instances

## Consistency Model

### Write Consistency

- **Within a shard**: Linearizable (single writer)
- **Across shards**: No guarantees (different shards independent)
- **During failover**: Short unavailability window

### Read Consistency

- **From primary**: Always latest data
- **From backup**: Potentially stale (depends on cache)
- **After shard close**: Consistent read-only view

## Performance Characteristics

### Write Performance

- Limited by:
    - Pebble WAL fsync latency
    - EBS IOPS limits
    - Network latency
- Optimizations:
    - Batch operations
    - Async compaction
    - Write buffering

### Read Performance

- Limited by:
    - Pebble read path
    - EBS throughput
    - Cache hit rate
- Optimizations:
    - Block cache
    - Read from backup nodes
    - Bloom filters for non-existent keys

## Fault Tolerance

### Node Failures

- **Primary node fails**: Backup promoted, service continues
- **Backup node fails**: No impact, can add replacement
- **Multiple backups fail**: Primary still operational

### Network Failures

- **Network partition**: Primary continues, backups isolated
- **Client disconnection**: Automatic retry with exponential backoff

### Storage Failures

- **EBS volume failure**: Extremely rare (AWS SLA: 99.999%)
- **Recovery**: Restore from snapshot
- **Prevention**: Regular snapshots

## Security Considerations

1. **Data Encryption**: EBS encryption at rest
2. **Network Security**: TLS for gRPC (optional, add later)
3. **Access Control**: IAM roles for AWS API access
4. **Audit**: CloudTrail for API logging
5. **Client Authentication**: ClientID verification

## Future Enhancements

1. **Compression**: Add compression for stored data
2. **Encryption**: Application-level encryption
3. **Metrics**: Prometheus metrics export
4. **Tracing**: OpenTelemetry integration
5. **Admin API**: Management operations
6. **Shard splitting**: Automatic shard size management
7. **Multi-region**: Cross-region replication
8. **Smart caching**: ML-based cache prediction