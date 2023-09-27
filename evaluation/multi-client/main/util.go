package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func mustCreateClient(endpoint ...string) *clientv3.Client {
	return mustCreateClientWithTimeout(1*time.Minute, endpoint...)
}

func mustCreateClientWithTimeout(timeout time.Duration, endpoint ...string) *clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoint, DialKeepAliveTimeout: timeout})
	if err != nil {
		panic(fmt.Sprintf("create client for endpoint %v failed: %v", endpoint, err))
	}
	return cli
}

func mustRandBytes(n int) []byte {
	rb := make([]byte, n)
	_, err := rand.Read(rb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate value: %v\n", err)
		os.Exit(1)
	}
	return rb
}