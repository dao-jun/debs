.PHONY: all build clean proto test install-tools help

# Variables
BINARY_NAME=debs
BINARY_PATH=bin/$(BINARY_NAME)
PROTO_DIR=proto
GOPATH_BIN=$(shell go env GOPATH)/bin

# Default target
all: build

# Build the binary
build: proto
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	@go build -o $(BINARY_PATH) cmd/server/main.go
	@echo "Build complete: $(BINARY_PATH)"

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       $(PROTO_DIR)/storage.proto
	@echo "Protobuf code generated"

# Install necessary tools
install-tools:
	@echo "Installing protobuf tools..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed"

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -cover -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html
	@echo "Clean complete"

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "Format complete"

# Run linter
lint:
	@echo "Running linter..."
	@golangci-lint run ./... || echo "golangci-lint not installed, skipping..."

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	@go mod tidy
	@echo "Tidy complete"

# Run the server
run: build
	@echo "Starting server..."
	@$(BINARY_PATH)

# Show help
help:
	@echo "Available targets:"
	@echo "  all            - Build the project (default)"
	@echo "  build          - Build the binary"
	@echo "  proto          - Generate protobuf code"
	@echo "  install-tools  - Install required development tools"
	@echo "  test           - Run tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  clean          - Remove build artifacts"
	@echo "  fmt            - Format Go code"
	@echo "  lint           - Run linter"
	@echo "  tidy           - Tidy Go modules"
	@echo "  run            - Build and run the server"
	@echo "  help           - Show this help message"