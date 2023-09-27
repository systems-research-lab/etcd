package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"crypto/rand"

	clientv3 "go.etcd.io/etcd/client/v3"
	yaml "gopkg.in/yaml.v3"
)

type config struct {
	Type   string
	Folder string

	Clusters [][]string
	Before   uint64
	After    uint64
	Warmup   uint64
	Cooldown uint64
	Threads  uint64
	Load     uint64

	Repetition uint64

	User           string
	PasswordEnvVar string
	EtcdServerDir  string
	EtcdctlPath    string
	EtcdutlPath    string
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "config file")

	flag.Parse()

	fmt.Println("Read config " + configFile)

	var cfg config
	if data, err := os.ReadFile(configFile); err == nil {
		err = yaml.Unmarshal(data, &cfg)
		if err != nil {
			panic("unmarshal config file failed: " + err.Error())
		}
	} else {
		panic("read config file failed: " + err.Error())
	}

	switch cfg.Type {
	case "split-performance":
		splitPerformance(cfg)
	case "split-impact":
		splitImpact(cfg)
	case "split-load":
		splitLoad(cfg)
	case "merge":
		merge(cfg)
	case "bench-split":
		benchmarkSplit(cfg)
	case "bench-merge":
		benchmarkMerge(cfg)
	default:
		panic(fmt.Sprintf("unknown type: %v", cfg.Type))
	}
}

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