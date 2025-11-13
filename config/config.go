package config

// Config holds the configuration for the storage service
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Cloud    CloudConfig    `yaml:"cloud"`
	Metadata MetadataConfig `yaml:"metadata"`
	Node     NodeConfig     `yaml:"node"`
}

// ServerConfig holds the gRPC server configuration
type ServerConfig struct {
	Address string `yaml:"address"` // e.g., ":50051"
}

// CloudConfig holds the cloud provider configuration
type CloudConfig struct {
	Provider         string `yaml:"provider"` // "aws" or "aliyun"
	Region           string `yaml:"region"`
	AvailabilityZone string `yaml:"availability_zone"`
}

// MetadataConfig holds the metadata store configuration
type MetadataConfig struct {
	Type      string            `yaml:"type"` // "etcd", "zookeeper", etc.
	Endpoints []string          `yaml:"endpoints"`
	Namespace string            `yaml:"namespace"`
	Options   map[string]string `yaml:"options"`
}

// NodeConfig holds the node configuration
type NodeConfig struct {
	NodeID     string `yaml:"node_id"`     // Unique identifier for this node
	InstanceID string `yaml:"instance_id"` // Cloud instance ID (e.g., EC2 instance ID)
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Address: ":50051",
		},
		Cloud: CloudConfig{
			Provider:         "aws",
			Region:           "us-west-2",
			AvailabilityZone: "us-west-2a",
		},
		Metadata: MetadataConfig{
			Type:      "etcd",
			Endpoints: []string{"localhost:2379"},
			Namespace: "/storage-service",
			Options:   make(map[string]string),
		},
		Node: NodeConfig{
			NodeID:     "node-1",
			InstanceID: "",
		},
	}
}
