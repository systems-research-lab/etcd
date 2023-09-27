package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/melbahja/goph"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MaxTxnOps = 100
)

type benchMergeReport struct {
	ScriptGet     int64 `json:"scriptGet"`
	ScriptPut     int64 `json:"scriptPut"`
	ScriptCleanUp int64 `json:"scriptCleanUp"`
	ScriptJoin    int64 `json:"scriptJoin"`
	ScriptRestart int64 `json:"scriptRestart"`

	Merge int64 `json:"merge"`
}

func benchmarkMerge(cfg config) {
	log.Printf("measure replicate...")
	getKey, putKey, cleanUp, join, restart := replicateBench(cfg)
	// <-time.After(5 * time.Second)
	// No longer needed cause the data wil be got from load test
	// log.Printf("measure merge...")
	// mergeCost := mergeBench(cfg)
	mergeCost := time.Second * 0
	log.Printf("get: %v, put: %v, clean up: %v, join: %v, restart: %v\nmerge: %v\n",
		getKey.Milliseconds(), putKey.Milliseconds(), cleanUp.Milliseconds(), join.Milliseconds(), restart.Milliseconds(),
		mergeCost.Milliseconds())

	data, err := json.Marshal(benchMergeReport{
		ScriptGet:     getKey.Milliseconds(),
		ScriptPut:     putKey.Milliseconds(),
		ScriptCleanUp: cleanUp.Milliseconds(),
		ScriptJoin:    join.Milliseconds(),
		ScriptRestart: restart.Milliseconds(),
		Merge:         mergeCost.Milliseconds(),
	})
	if err != nil {
		log.Panicf("marshal benchmark report failed: %v\n", err)
	}
	if err = os.WriteFile(fmt.Sprintf("%v/bench-merge--%v-%v-%v.json", cfg.Folder, len(cfg.Clusters), cfg.Load, cfg.Repetition),
		data, 0666); err != nil {
		panic(fmt.Sprintf("write report json failed: %v", err))
	}
}

func replicateBench(cfg config) (getKey, putKey, cleanUp, join, restart time.Duration) {
	// connect to hosts
	sshClis := make([]map[string]*goph.Client, len(cfg.Clusters))
	for idx, clr := range cfg.Clusters {
		sshClis[idx] = make(map[string]*goph.Client)
		for _, ep := range clr {
			auth, err := goph.Key("/home/ubuntu/.ssh/id_rsa", "")
			if err != nil {
				log.Fatal(err)
			}
			host := strings.Split(strings.Replace(ep, "http://", "", 1), ":")[0]
			cli, err := goph.NewUnknown("ubuntu", host, auth)
			if err != nil {
				log.Panicf("connect to host %v failed: %v\n", host, err)
			}
			sshClis[idx][ep] = cli
			defer cli.Close()
		}
	}

	// start etcd servers
	wg := sync.WaitGroup{}
	for clrIdx, clr := range cfg.Clusters {
		etcdConfigs := parseEtcdConfigs(getClusterUrl(clr, clrIdx*3+1))
		for host, cli := range sshClis[clrIdx] {
			wg.Add(1)
			go func(cli *goph.Client, host string) {
				mustStartEtcd(cli, cfg.EtcdServerDir, etcdConfigs[host])
				// log.Printf("start etcd server on host %v\n", host)
				wg.Done()
			}(cli, host)
		}
	}
	wg.Wait()

	// prepare some load
	log.Printf("prepare load...")
	wg = sync.WaitGroup{}
	for i := 0; i < len(cfg.Clusters); i++ {
		wg.Add(1)
		go func(i int) {
			prepareLoad(cfg.Clusters[i][0], cfg.Load, cfg.Load*uint64(i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	log.Printf("ready to start...")
	time.After(5 * time.Second)
	start := time.Now()

	// retrieve keys from other clusters and put to the first one
	kvs := make([][2][]byte, 0, cfg.Load)
	for clrIdx, _ := range cfg.Clusters {
		// retrieve keys
		getCli := mustCreateClientWithTimeout(time.Minute, cfg.Clusters[clrIdx]...)

		startIdx := int64(int(cfg.Load) * clrIdx)
		endIdx := startIdx + int64(cfg.Load)
		endKey := strconv.FormatInt(endIdx, 10)
		for offset := startIdx; ; {
			resp, err := getCli.Do(context.TODO(), clientv3.OpGet(
				strconv.FormatInt(offset, 10), clientv3.WithRange(endKey)))
			if err != nil {
				log.Panicf("get keys failed: %v\n", err)
			}
			for _, kv := range resp.Get().Kvs {
				kvs = append(kvs, [2][]byte{kv.Key, kv.Value})
			}

			if resp.Get().More {
				offset += resp.Get().Count
			} else {
				break
			}
		}
		getCli.Close()
	}
	getKeyTime := time.Now()
	log.Printf("get %v keys...", len(kvs))

	// put keys
	log.Printf("put keys...")

	numThread := 64
	offset := 0
	wg = sync.WaitGroup{}
	for offset < len(kvs) {
		wg.Add(1)
		size := len(kvs) / numThread + 1
		go func(offset int, size int) {
			putCli := mustCreateClient(cfg.Clusters[0]...)
			for j := offset; j < offset+size && j < len(kvs); j++ {
				if _, err := putCli.Do(context.TODO(), clientv3.OpPut(string(kvs[j][0]), string(kvs[j][1]))); err != nil {
					log.Printf("put error: %v", err)
				}
			}
			putCli.Close()
			wg.Done()
		}(offset, size)
		offset += size
	}
	wg.Wait()
	putKeyTime := time.Now()

	// stop other clusters and clean up
	log.Printf("stopping..")
	for clrIdx := 1; clrIdx < len(cfg.Clusters); clrIdx++ {
		etcdConfigs := parseEtcdConfigs(getClusterUrl(cfg.Clusters[clrIdx], clrIdx*3+1))
		cleanWithWait(sshClis[clrIdx], etcdConfigs, [][]string{cfg.Clusters[clrIdx]}, cfg.EtcdServerDir, false)
	}
	cleanUpTime := time.Now()

	// add other clusters to the first one
	log.Printf("add members...")
	addPeerAddr := make([]string, 0, 0)
	for _, clr := range cfg.Clusters[1:] {
		addPeerAddr = append(addPeerAddr, clr...)
	}
	if _, err := mustCreateClient(cfg.Clusters[0]...).MemberJoint(context.TODO(), addPeerAddr, nil); err != nil {
		log.Panicf("add members failed: %v\n", err)
	}
	joinTime := time.Now()

	// restart other clusters
	log.Printf("restarting...")
	joinClusterEndpoints := make([]string, 0, 0)
	for _, clr := range cfg.Clusters[1:] {
		joinClusterEndpoints = append(joinClusterEndpoints, clr...)
	}
	joinClusterUrl := getClusterUrl(joinClusterEndpoints, 1)
	for clrIdx := 1; clrIdx < len(cfg.Clusters); clrIdx++ {
		wg = sync.WaitGroup{}
		for host, cli := range sshClis[clrIdx] {
			wg.Add(1)
			clr := cfg.Clusters[clrIdx]
			etcdConfigs := parseEtcdConfigs(getClusterUrl(clr, clrIdx*3+1))
			go func(cli *goph.Client, host string) {
				joinConfig := etcdConfigs[host]
				joinConfig.initialCluster = joinClusterUrl
				joinConfig.initialClusterState = "existing"
				mustStartEtcd(cli, cfg.EtcdServerDir, joinConfig)
				// log.Printf("start etcd server on host %v\n", host)
				wg.Done()
			}(cli, host)
		}
		wg.Wait()
	}
	restartTime := time.Now()

	log.Printf("cleaning up...")
	for clrIdx, clr := range cfg.Clusters {
		etcdConfigs := parseEtcdConfigs(getClusterUrl(clr, clrIdx*3+1))
		clean(sshClis[clrIdx], etcdConfigs, [][]string{cfg.Clusters[clrIdx]}, cfg.EtcdServerDir)
	}

	return getKeyTime.Sub(start), putKeyTime.Sub(getKeyTime), cleanUpTime.Sub(putKeyTime),
		joinTime.Sub(cleanUpTime), restartTime.Sub(joinTime)
}

func mergeBench(cfg config) time.Duration {
	// connect to hosts
	sshClis := make([]map[string]*goph.Client, len(cfg.Clusters))
	for idx, clr := range cfg.Clusters {
		sshClis[idx] = make(map[string]*goph.Client)
		for _, ep := range clr {
			auth, err := goph.Key("/home/ubuntu/.ssh/id_rsa", "")
			if err != nil {
				log.Fatal(err)
			}
			host := strings.Split(strings.Replace(ep, "http://", "", 1), ":")[0]
			cli, err := goph.NewUnknown("ubuntu", host, auth)
			if err != nil {
				log.Panicf("connect to host %v failed: %v\n", host, err)
			}
			sshClis[idx][ep] = cli
			defer cli.Close()
		}
	}

	// start etcd servers
	wg := sync.WaitGroup{}
	for clrIdx, clr := range cfg.Clusters {
		etcdConfigs := parseEtcdConfigs(getClusterUrl(clr, clrIdx*3+1))
		for host, cli := range sshClis[clrIdx] {
			wg.Add(1)
			go func(cli *goph.Client, host string) {
				mustStartEtcd(cli, cfg.EtcdServerDir, etcdConfigs[host])
				// log.Printf("start etcd server on host %v\n", host)
				wg.Done()
			}(cli, host)
		}
	}
	wg.Wait()

	// prepare some load
	log.Printf("prepare load...")
	wg = sync.WaitGroup{}
	for i := 0; i < len(cfg.Clusters); i++ {
		wg.Add(1)
		go func(i int) {
			prepareLoad(cfg.Clusters[i][0], cfg.Load, cfg.Load*uint64(i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	urls := make([]string, 0, len(cfg.Clusters))
	for _, clr := range cfg.Clusters[1:] {
		urls = append(urls, clr[0])
	}
	mlist := getMergeMemberList(urls)

	log.Printf("ready to start...")
	time.After(10 * time.Second)

	cli := findLeader(cfg.Clusters[0])
	defer cli.Close()

	log.Printf("merging...")
	// start := time.Now()
	_, err := cli.MemberMerge(context.TODO(), mlist)
	if err != nil {
		log.Panicf("merge failed: %v\n", err)
	}
	// cost := time.Since(start)

	// fetch merge measurement from server
	log.Printf("fetch measurements...")
	serverCost := int64(math.MaxInt64)
	for _, clr := range cfg.Clusters {
		for _, ep := range clr {
			m := getMergeMeasure(ep)
			if m.MergeTxStart < 0 || m.MergeTxCommit < 0 || m.MergeEnter < 0 {
				log.Printf("invalid measure: %v", m)
			}
			if m.LeaderElect > 0 {
				if m.LeaderElect-m.MergeTxIssue < serverCost {
					serverCost = m.LeaderElect - m.MergeTxStart
				}
			}
		}
	}
	if serverCost > int64(math.MaxInt64)/2 {
		log.Panicf("Leader after merge not found.")
	}

	for clrIdx, clr := range cfg.Clusters {
		etcdConfigs := parseEtcdConfigs(getClusterUrl(clr, clrIdx*3+1))
		clean(sshClis[clrIdx], etcdConfigs, [][]string{cfg.Clusters[clrIdx]}, cfg.EtcdServerDir)
	}

	return time.Microsecond * time.Duration(serverCost)
}

func findLeader(cluster []string) *clientv3.Client {
	for _, ep := range cluster {
		cli := mustCreateClientWithTimeout(30*time.Second, ep)
		resp, err := cli.Status(context.TODO(), ep)
		if err != nil {
			panic(fmt.Sprintf("get status for endpoint %v failed: %v", ep, err.Error()))
		}
		if resp.Header.MemberId == resp.Leader {
			return cli
		}
		if err = cli.Close(); err != nil {
			panic(err)
		}
	}

	panic("leader not found")
}
