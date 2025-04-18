package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {

	// Parse command line arguments
	startTime := time.Now()
	defer func() {
		fmt.Printf("Total time taken: %v\n", time.Since(startTime))
	}()

	endpoints := flag.String("endpoints", "127.0.0.1:1379,127.0.0.1:2379,127.0.0.1:3379,127.0.0.1:4379,127.0.0.1:5379", "Comma-separated list of etcd endpoints")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent workers")
	numRequests := flag.Int("requests", 40000, "Total number of requests to send")
	valueSize := flag.Int("value-size", 100, "Size of random values")
	keyPrefix := flag.String("key-prefix", "loadtest", "Prefix for all test keys")
	timeout := flag.Int("timeout", 5, "Request timeout in seconds")
	flag.Parse()

	// Parse endpoints using standard library
	endpointList := strings.Split(*endpoints, ",")

	// Print test parameters
	fmt.Printf("Starting load test with %d concurrent workers and %d total requests\n",
		*concurrency, *numRequests)
	fmt.Printf("Using etcd endpoints: %v\n", endpointList)

	// Create a client for each endpoint
	clients := make([]*clientv3.Client, len(endpointList))
	for i, endpoint := range endpointList {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Duration(*timeout) * time.Second,
		})
		if err != nil {
			log.Fatalf("Failed to create client for %s: %v", endpoint, err)
		}
		defer client.Close()
		clients[i] = client
	}

	// Set up context with timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the load test
	var wg sync.WaitGroup
	requestsPerWorker := *numRequests / *concurrency
	remainder := *numRequests % *concurrency

	// Start workers
	for i := 0; i < *concurrency; i++ {
		requests := requestsPerWorker
		if i < remainder {
			requests++
		}

		startID := i * requestsPerWorker
		if i > 0 {
			startID += min(i, remainder)
		}

		wg.Add(1)
		go func(workerID int, startID int, numRequests int) {
			defer wg.Done()
			runWorker(ctx, workerID, clients, startID, numRequests, *keyPrefix, *valueSize, time.Duration(*timeout)*time.Second)
		}(i, startID, requests)
	}

	wg.Wait()
	fmt.Println("Load test completed")
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// runWorker is a goroutine that performs PUT requests
func runWorker(ctx context.Context, workerID int, clients []*clientv3.Client, startID int, numRequests int,
	keyPrefix string, valueSize int, timeout time.Duration) {

	// Create a random generator with a unique seed per worker
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	for i := 0; i < numRequests; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			// Continue processing
		}

		// Select a random client (endpoint)
		client := clients[r.Intn(len(clients))]

		// Generate unique key and random value
		key := fmt.Sprintf("%s/%d", keyPrefix, startID+i)
		value := randomString(r, valueSize)

		// Perform PUT with timeout
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := client.Put(requestCtx, key, value)
		cancel()

		if err != nil {
			log.Printf("Worker %d: PUT failed: %v", workerID, err)
		}
	}
}

// randomString generates a random string of the specified length
func randomString(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}
