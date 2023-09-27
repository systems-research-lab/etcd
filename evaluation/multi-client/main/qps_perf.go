package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const ClientSshUser = "ubuntu"
const ClientStartCmd = "go run qps_perf_client.go util.go"
const ClientBasePath = "~/etcd/evaluation/multi-client/main"
const ClientTimeout = 5 // in minutes

type qpsReport struct {
	Start int64 `json:"startUnixMicro"`
}

type config struct {
	Clients    []string
	Clusters   [][]string
	Before     uint64
	After      uint64
	Warmup     uint64
	Cooldown   uint64
	Threads    uint64
	Repetition uint64
	Version    string
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "config file")
	flag.Parse()

	var cfg config
	if data, err := os.ReadFile(configFile); err == nil {
		err = yaml.Unmarshal(data, &cfg)
		if err != nil {
			panic("unmarshal config file failed: " + err.Error())
		}
	} else {
		panic("read config file failed: " + err.Error())
	}

	run(cfg)
}

func run(cfg config) {
	startCh := make(chan struct{})
	errCh := make(chan error)
	for idx := range cfg.Clients {
		go func(idx int) {
			<-startCh
			errCh <- startClient(cfg, uint64(idx))
		}(idx)
	}
	close(startCh)

	log.Printf("Start clients.\n")
	startTime := time.Now()
	for idx := range cfg.Clients {
		if err := <-errCh; err != nil {
			log.Panicf("Client %v failed: %v\n", idx, err)
		}
	}

	// download results
	time.Sleep(10 * time.Second)
	log.Printf("Downloading results from clients.\n")
	if err := downloadResults(cfg); err != nil {
		log.Panicln("Download results error: %v", err)
	}

	data, err := json.Marshal(qpsReport{Start: startTime.UnixMicro()})
	if err != nil {
		log.Panicln("Marshal report error: " + err.Error())
	}

	if err := os.WriteFile(fmt.Sprintf("./qps-%v-%v-%v.json", cfg.Threads, cfg.Repetition, cfg.Version),
		data, 0666); err != nil {
		log.Panicln(fmt.Sprintf("write report json failed: %v", err))
	}

	log.Printf("QPS finished.\n")
}

func startClient(cfg config, idx uint64) error {
	client := cfg.Clients[idx]
	cluster := strings.Join(cfg.Clusters[0], ",")

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(ClientTimeout)*time.Minute)
	defer cancel()

	log.Printf("Start client %v..\n", idx)
	cmd := exec.CommandContext(ctx,
		"ssh", ClientSshUser+"@"+client,
		fmt.Sprintf("ulimit -n 65535 && cd %v && %v", ClientBasePath, ClientStartCmd),
		"--id", strconv.FormatUint(idx, 10),
		"--cluster", cluster,
		"--warmup", strconv.FormatUint(cfg.Warmup, 10),
		"--cooldown", strconv.FormatUint(cfg.Cooldown, 10),
		"--measure", strconv.FormatUint(cfg.Before+cfg.After, 10),
		"--thread", strconv.FormatUint(cfg.Threads, 10),
		"--repetition", strconv.FormatUint(cfg.Repetition, 10),
		"--version", cfg.Version)

	var cmdout bytes.Buffer
	var cmderr bytes.Buffer
	cmd.Stdout = &cmdout
	cmd.Stderr = &cmderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run client %v failed:\nstdout:%v\nstderr:%v\nerr:%v\n", idx, cmdout.String(), cmderr.String(), err)
	}

	return nil
}

func MinMax(array []int64) (int64, int64) {
	var max int64 = array[0]
	var min int64 = array[0]
	for _, value := range array {
		if max < value {
			max = value
		}
		if min > value {
			min = value
		}
	}
	return min, max
}

func downloadResults(cfg config) error {
	sizes := make([]int64, 0, len(cfg.Clients))
	for idx, clientIp := range cfg.Clients {
		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Minute)
		defer cancel()

		fn := fmt.Sprintf(fmt.Sprintf("qps-client-%v-%v-%v-%v.json", idx, cfg.Threads, cfg.Repetition, cfg.Version))
		cmd := exec.CommandContext(ctx, "scp", fmt.Sprintf("%v@%v:%v/%v", ClientSshUser, clientIp, ClientBasePath, fn), ".")
		if err := cmd.Run(); err != nil {
			return err
		}

		fi, err := os.Stat(fn)
		if err != nil {
			return err
		}
		sizes = append(sizes, fi.Size())
	}
	log.Printf("Results sizes: %v\n", sizes)

	min, max := MinMax(sizes)
	if max > int64(float64(min)*1.5) {
		return fmt.Errorf("Sizes vary too much: %v %v\n", max, min)
	}

	return nil
}
