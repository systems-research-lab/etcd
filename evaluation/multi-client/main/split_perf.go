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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
)

const ClientSshUser = "ubuntu"
const ClientBasePath = "~/etcd/evaluation/multi-client/main"
const ClientStartCmd = "go run split_perf_client.go util.go"
const ClientTimeout = 2 // in minutes

type splitReport struct {
	Start int64 `json:"startUnixMicro"`
	Issue int64 `json:"issueUnixMicro"`
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
	log.Printf("Get split info...\n")
	splitCli, clusterIds := getSplitInfo(cfg)

	startCh := make(chan struct{})
	errCh := make(chan error)
	for idx := range cfg.Clients {
		go func(idx int) {
			<-startCh
			errCh <- startClient(cfg, uint64(idx))
		}(idx)
	}
	close(startCh)

	log.Printf("Start.\n")
	startTime := time.Now()
	time.Sleep(time.Second * time.Duration(cfg.Warmup+cfg.Before))

	issueTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if _, err := splitCli.MemberSplit(ctx, getSplitMemberList(clusterIds), false, false); err != nil {
		// if (cfg.Threads <= 256) {
		// 	log.Panicf(fmt.Sprintf("split failed: %v", err))
		// } // else check leaders later directly
	}

	time.Sleep(time.Second * time.Duration(cfg.After+cfg.Cooldown))

	for idx := range cfg.Clients {
		if err := <-errCh; err != nil {
			log.Panicf("Client %v failed: %v\n", idx, err)
		}
	}

	// check multiple leaders
	leaderEps := make([]string, 0, len(cfg.Clusters))
	for _, clr := range cfg.Clusters {
		for _, ep := range clr {
			cli := mustCreateClient(ep)
			resp, err := cli.Status(context.TODO(), ep)
			if err != nil {
				panic(fmt.Sprintf("get status for endpoint %v failed: %v", ep, err.Error()))
			}
			if err = cli.Close(); err != nil {
				panic(err)
			}
	
			if resp.Header.MemberId == resp.Leader {
				leaderEps = append(leaderEps, ep)
				break
			}
		}
	}
	if len(leaderEps) != len(cfg.Clusters) {
		log.Panicf("Split failed, leaders: %v", leaderEps)
	}

	// download results
	time.Sleep(10 * time.Second)
	log.Printf("Downloading results from clients.\n")
	if err := downloadResults(cfg); err != nil {
		log.Panicln("Download results error: %v", err)
	}

	data, err := json.Marshal(splitReport{Start: startTime.UnixMicro(), Issue: issueTime.UnixMicro()})
	if err != nil {
		log.Panicln("Marshal report error: " + err.Error())
	}

	if err := os.WriteFile(fmt.Sprintf("./split-%v-%v-%v.json", len(cfg.Clusters), cfg.Threads, cfg.Repetition),
		data, 0666); err != nil {
		log.Panicln(fmt.Sprintf("write report json failed: %v", err))
	}

	log.Printf("Split finished.\n")
}

func startClient(cfg config, idx uint64) error {
	client := cfg.Clients[idx]
	cluster := strings.Join(cfg.Clusters[idx], ",")

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(ClientTimeout)*time.Minute)
	defer cancel()

	log.Printf("Start client %v..\n", idx)
	cmd := exec.CommandContext(ctx,
		"ssh", ClientSshUser+"@"+client,
		fmt.Sprintf("cd %v && %v", ClientBasePath, ClientStartCmd),
		"--id", strconv.FormatUint(idx, 10),
		"--cluster", cluster,
		"--warmup", strconv.FormatUint(cfg.Warmup, 10),
		"--cooldown", strconv.FormatUint(cfg.Cooldown, 10),
		"--measure", strconv.FormatUint(cfg.Before+cfg.After, 10),
		"--split", strconv.FormatInt(int64(len(cfg.Clusters)), 10),
		"--thread", strconv.FormatUint(cfg.Threads, 10),
		"--repetition", strconv.FormatUint(cfg.Repetition, 10))

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
		ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(ClientTimeout)*time.Minute)
		defer cancel()

		fn := fmt.Sprintf(fmt.Sprintf("split-client-%v-%v-%v-%v.json", idx, len(cfg.Clusters), cfg.Threads, cfg.Repetition))
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
	if (max > int64(float64(min) * 2)) {
		return fmt.Errorf("Sizes vary too much: %v %v\n", max, min)
	}

	return nil
}

func getSplitInfo(cfg config) (*clientv3.Client, [][]uint64) {
	// get members' id and find the leader before split
	clusterIds := make([][]uint64, len(cfg.Clusters))
	leaderId := uint64(0)
	leaderEp := ""
	leaderClrIdx := -1
	for idx, clr := range cfg.Clusters {
		clusterIds[idx] = make([]uint64, 0, len(clr))
		for _, ep := range clr {
			cli := mustCreateClient(ep)
			resp, err := cli.Status(context.TODO(), ep)
			if err != nil {
				panic(fmt.Sprintf("get status for endpoint %v failed: %v", ep, err.Error()))
			}
			if leaderId != 0 && leaderId != resp.Leader {
				panic(fmt.Sprintf("leader not same: %v and %v", leaderId, resp.Leader))
			}

			leaderId = resp.Leader
			if resp.Header.MemberId == leaderId {
				leaderEp = ep
				leaderClrIdx = idx
			}

			clusterIds[idx] = append(clusterIds[idx], resp.Header.MemberId)
			if err = cli.Close(); err != nil {
				panic(err)
			}
		}
	}
	if leaderEp == "" || leaderClrIdx == -1 {
		log.Panicf("leader not found.")
	} else {
		log.Printf("found leader %v at endpoint %v\n", leaderId, leaderEp)
	}

	return mustCreateClient(leaderEp), clusterIds
}

func getSplitMemberList(clusters [][]uint64) []etcdserverpb.MemberList {
	clrs := make([]etcdserverpb.MemberList, 0)
	for _, clr := range clusters {
		mems := make([]etcdserverpb.Member, 0)
		for _, id := range clr {
			mems = append(mems, etcdserverpb.Member{ID: id})
		}
		clrs = append(clrs, etcdserverpb.MemberList{Members: mems})
	}
	return clrs
}
