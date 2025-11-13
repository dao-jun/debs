package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/debs/debs/proto"
)

func main() {
	// Connect to the storage service
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)

	client := pb.NewStorageServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Example 1: Create a new shard
	fmt.Println("=== Example 1: Create a new shard ===")
	newShardResp, err := client.NewShard(ctx, &pb.NewShardRequest{
		ClientId: "example-client",
	})
	if err != nil {
		log.Fatalf("Failed to create shard: %v", err)
	}
	fmt.Printf("Created shard with ID: %d\n", newShardResp.ShardId)
	shardID := newShardResp.ShardId

	// Example 2: Put a single entry
	fmt.Println("\n=== Example 2: Put a single entry ===")
	putResp, err := client.Put(ctx, &pb.PutRequest{
		ClientId: "example-client",
		ShardId:  shardID,
		EntryId:  1,
		Value:    []byte("Hello, World!"),
		Indexes: []*pb.Index{
			{Key: "category", Value: []byte("greeting")},
			{Key: "language", Value: []byte("en")},
		},
	})
	if err != nil {
		log.Fatalf("Failed to put entry: %v", err)
	}
	fmt.Printf("Put entry result: %s\n", putResp.Code)

	// Example 3: Get the entry
	fmt.Println("\n=== Example 3: Get the entry ===")
	getResp, err := client.Get(ctx, &pb.GetRequest{
		ShardId: shardID,
		EntryId: 1,
	})
	if err != nil {
		log.Fatalf("Failed to get entry: %v", err)
	}
	fmt.Printf("Got entry: %s (code: %s)\n", string(getResp.Value), getResp.Code)

	// Example 4: Batch put multiple entries
	fmt.Println("\n=== Example 4: Batch put multiple entries ===")
	batchPutResp, err := client.BatchPut(ctx, &pb.BatchPutRequest{
		ClientId: "example-client",
		Requests: []*pb.PutRequest{
			{
				ShardId: shardID,
				EntryId: 2,
				Value:   []byte("Entry 2"),
			},
			{
				ShardId: shardID,
				EntryId: 3,
				Value:   []byte("Entry 3"),
			},
			{
				ShardId: shardID,
				EntryId: 4,
				Value:   []byte("Entry 4"),
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to batch put: %v", err)
	}
	fmt.Printf("Batch put completed, %d responses\n", len(batchPutResp.Responses))

	// Example 5: Batch get multiple entries
	fmt.Println("\n=== Example 5: Batch get multiple entries ===")
	stream, err := client.BatchGet(ctx, &pb.BatchGetRequest{
		ShardId:  shardID,
		EntryIds: []uint32{1, 2, 3, 4, 999}, // 999 doesn't exist
	})
	if err != nil {
		log.Fatalf("Failed to batch get: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if resp.Code == pb.Code_OK {
			fmt.Printf("Entry %d: %s\n", resp.EntryId, string(resp.Value))
		} else {
			fmt.Printf("Entry %d: not found\n", resp.EntryId)
		}
	}

	// Example 6: Close the shard (make it read-only)
	fmt.Println("\n=== Example 6: Close the shard ===")
	closeResp, err := client.CloseShard(ctx, &pb.CloseShardRequest{
		ShardId: shardID,
	})
	if err != nil {
		log.Fatalf("Failed to close shard: %v", err)
	}
	fmt.Printf("Close shard result: %s\n", closeResp.Code)

	// Example 7: Try to put to a closed shard (should fail or return an error)
	fmt.Println("\n=== Example 7: Try to put to a closed shard ===")
	putResp, err = client.Put(ctx, &pb.PutRequest{
		ClientId: "example-client",
		ShardId:  shardID,
		EntryId:  5,
		Value:    []byte("This should fail"),
	})
	if err != nil {
		fmt.Printf("Put to closed shard failed (expected): %v\n", err)
	} else {
		fmt.Printf("Put to closed shard result: %s\n", putResp.Code)
	}

	// Example 8: Can still read from closed shard
	fmt.Println("\n=== Example 8: Read from closed shard ===")
	getResp, err = client.Get(ctx, &pb.GetRequest{
		ShardId: shardID,
		EntryId: 1,
	})
	if err != nil {
		log.Fatalf("Failed to get from closed shard: %v", err)
	}
	fmt.Printf("Got entry from closed shard: %s (code: %s)\n", string(getResp.Value), getResp.Code)

	fmt.Println("\n=== All examples completed ===")
}
