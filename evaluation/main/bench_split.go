package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/melbahja/goph"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	DefaultTickMS       = 50
	DefaultElectionMs   = 1000
	DefaultSnapshotName = "snapshot.db"
)

type etcdConfig struct {
	port int

	name string

	tickMs     int
	electionMs int

	listenClientUrls    string
	advertiseClientUrls string

	initialAdvertisePeerUrls string
	listenPeerUrls           string
	initialCluster           string
	initialClusterState      string
}

func (e etcdConfig) String() string {
	return "--data-dir=" + "data.etcd." + e.name +
		" --name=" + e.name +
		" --heartbeat-interval=" + strconv.Itoa(e.tickMs) +
		" --election-timeout=" + strconv.Itoa(e.electionMs) +
		" --listen-client-urls=" + e.listenClientUrls +
		" --advertise-client-urls=" + e.advertiseClientUrls +
		" --initial-advertise-peer-urls=" + e.initialAdvertisePeerUrls +
		" --listen-peer-urls=" + e.listenPeerUrls +
		" --initial-cluster=" + e.initialCluster +
		" --initial-cluster-state=" + e.initialClusterState +
		" --pre-vote=false" +
		" --log-level=panic"
}

func parseEtcdConfigs(clusterUrl string) map[string]etcdConfig {
	if strings.Trim(clusterUrl, " ") == "" {
		panic("empty cluster url")
	}

	configs := make(map[string]etcdConfig, 0)
	for _, node := range strings.Split(clusterUrl, ",") {
		tokens := strings.Split(node, "=")

		name := tokens[0]
		peerUrl := tokens[1]

		tokens = strings.Split(strings.Replace(peerUrl, "http://", "", 1), ":")
		host := tokens[0]
		port, err := strconv.Atoi(tokens[1])
		if err != nil {
			panic("convert port failed: " + err.Error())
		}

		clientUrl := fmt.Sprintf("http://%v:%v", host, port-1)
		configs[peerUrl] = etcdConfig{
			port:                     port,
			name:                     name,
			tickMs:                   DefaultTickMS,
			electionMs:               DefaultElectionMs,
			listenClientUrls:         clientUrl,
			advertiseClientUrls:      clientUrl,
			initialAdvertisePeerUrls: peerUrl,
			listenPeerUrls:           peerUrl,
			initialCluster:           clusterUrl,
			initialClusterState:      "new",
		}
	}

	return configs
}

type benchSplitReport struct {
	ScriptSnapshot int64 `json:"scriptSnapshot"`
	ScriptRemove   int64 `json:"scriptRemove"`
	ScriptRestart  int64 `json:"scriptRestart"`
	ScriptRestore  int64 `json:"scriptRestore"`

	Split int64 `json:"split"`
}

func benchmarkSplit(cfg config) {
	log.Printf("measure restore...")
	snapshotCost, removeCost, restoreTime, restartCost := restoreBench(cfg)
	log.Printf("measure split...")
	splitCost := splitBench(cfg)
	log.Printf("snapshot: %v, remove: %v, restore: %v, restart: %v\nsplit: %v\n",
		snapshotCost.Milliseconds(), removeCost.Milliseconds(), restoreTime.Milliseconds(), restartCost.Milliseconds(),
		splitCost.Milliseconds())

	if splitCost.Milliseconds() > int64(200) {
		log.Panicf("Split cost too high (%v ms), outlier data.\n", splitCost.Milliseconds())
	}

	data, err := json.Marshal(benchSplitReport{
		ScriptSnapshot: snapshotCost.Milliseconds(),
		ScriptRemove:   removeCost.Milliseconds(),
		ScriptRestore:  restoreTime.Milliseconds(),
		ScriptRestart:  restartCost.Milliseconds(),
		Split:          splitCost.Milliseconds()})
	if err != nil {
		log.Panicf("marshal benchmark report failed: %v\n", err)
	}
	if err = os.WriteFile(fmt.Sprintf("%v/bench-split-%v-%v-%v.json", cfg.Folder, len(cfg.Clusters), cfg.Load, cfg.Repetition),
		data, 0666); err != nil {
		panic(fmt.Sprintf("write report json failed: %v", err))
	}
}

func restoreBench(cfg config) (snapshot, remove, restore, restart time.Duration) {
	// connect to hosts
	sshClis := make(map[string]*goph.Client)
	for _, clr := range cfg.Clusters {
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
			sshClis[ep] = cli
		}
	}

	// start etcd servers
	wg := sync.WaitGroup{}
	endpoints := make([]string, 0, 0)
	for _, clr := range cfg.Clusters {
		endpoints = append(endpoints, clr...)
	}
	etcdConfigs := parseEtcdConfigs(getClusterUrl(endpoints, 1))
	for host, cli := range sshClis {
		wg.Add(1)
		go func(cli *goph.Client, host string) {
			mustStartEtcd(cli, cfg.EtcdServerDir, etcdConfigs[host])
			log.Printf("start etcd server on host %v\n", host)
			wg.Done()
		}(cli, host)
	}
	wg.Wait()

	// prepare some load
	log.Printf("prepare load...")
	prepareLoad(cfg.Clusters[0][0], cfg.Load, 0)

	log.Printf("ready to start...")
	time.After(1 * time.Second)

	snapshotPath := cfg.EtcdServerDir
	clusterUrl := getClusterUrl(endpoints[len(cfg.Clusters[0]):], len(cfg.Clusters[0])+1)
	start := time.Now()

	// create snapshot
	wg = sync.WaitGroup{}
	for _, clr := range cfg.Clusters[1:] {
		for _, ep := range clr {
			wg.Add(1)
			go func(ep string) {
				createSnapshot(sshClis[ep], ep, cfg.EtcdctlPath, snapshotPath)
				wg.Done()
			}(ep)
		}
	}
	wg.Wait()
	snapshotTime := time.Now()

	// remove nodes
	etcdCli := mustCreateClient(cfg.Clusters[0][0])
	_, err := etcdCli.MemberJoint(context.TODO(), nil, getMemberIds(endpoints[len(cfg.Clusters[0]):]))
	if err != nil {
		log.Panicf("remove members failed: %v\n", err)
	}
	removedTime := time.Now()

	// restore from snapshot
	wg = sync.WaitGroup{}
	for _, clr := range cfg.Clusters[1:] {
		for _, ep := range clr {
			wg.Add(1)
			go func(ep string) {
				restoreSnapshot(sshClis[ep], etcdConfigs[ep], clusterUrl, cfg.EtcdServerDir, cfg.EtcdutlPath, snapshotPath)
				wg.Done()
			}(ep)
		}
	}
	wg.Wait()
	restoreTime := time.Now()

	// restart etcd
	wg = sync.WaitGroup{}
	for _, clr := range cfg.Clusters[1:] {
		for _, ep := range clr {
			wg.Add(1)
			go func(ep string) {
				restartEtcd(sshClis[ep], etcdConfigs[ep], cfg.EtcdServerDir)
				wg.Done()
			}(ep)
		}
	}
	wg.Wait()
	restartTime := time.Now()

	clean(sshClis, etcdConfigs, cfg.Clusters, cfg.EtcdServerDir)
	return snapshotTime.Sub(start), removedTime.Sub(snapshotTime), restoreTime.Sub(removedTime), restartTime.Sub(restoreTime)
}

func createSnapshot(sshCli *goph.Client, endpoint, etcdctlPath, snapshotPath string) {
	out, err := sshCli.Run(fmt.Sprintf("ETCDCTL_API=3 %v --endpoints %v snapshot save %v/%v",
		etcdctlPath, endpoint, snapshotPath, DefaultSnapshotName))
	if err != nil {
		log.Panicf("create snapshot failed: %s\n", out)
	}
}

func restoreSnapshot(sshCli *goph.Client, config etcdConfig, clusterUrl, etcdserverDir, etcdutlPath, snapshotPath string) {
	args := "--name " + config.name +
		" --data-dir " + "data.etcd." + config.name + "-restore" +
		" --initial-cluster " + clusterUrl +
		" --initial-advertise-peer-urls " + config.initialAdvertisePeerUrls
	out, err := sshCli.Run(fmt.Sprintf(
		"cd %v && %v snapshot restore %v/%v %v",
		etcdserverDir, etcdutlPath, snapshotPath, DefaultSnapshotName, args))
	if err != nil {
		log.Panicf("restore snapshot failed: %s\n", out)
	}
}

func restartEtcd(sshCli *goph.Client, config etcdConfig, etcdServerDir string) {
	args := "--name=" + config.name +
		" --data-dir=" + "data.etcd." + config.name + "-restore" +
		" --listen-client-urls=" + config.listenClientUrls +
		" --advertise-client-urls=" + config.advertiseClientUrls +
		" --listen-peer-urls=" + config.listenPeerUrls +
		" --heartbeat-interval=" + strconv.Itoa(config.tickMs) +
		" --election-timeout=" + strconv.Itoa(config.electionMs) +
		" --pre-vote=false" +
		" --log-level=panic"
	cmd, err := sshCli.Command(fmt.Sprintf("cd %v && nohup ./server %v > etcd.%v.out 2>&1 &",
		etcdServerDir, args, config.name))
	if err != nil {
		log.Panicf("create command failed: %v", err)
	}
	if cmd.Start() != nil {
		log.Panicf("restart etcd failed: %v\n", err)
	}
}

func splitBench(cfg config) time.Duration {
	// connect to hosts
	sshClis := make(map[string]*goph.Client)
	for _, clr := range cfg.Clusters {
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
			sshClis[ep] = cli
		}
	}

	// start etcd servers
	wg := sync.WaitGroup{}
	endpoints := make([]string, 0, 0)
	for _, clr := range cfg.Clusters {
		endpoints = append(endpoints, clr...)
	}
	etcdConfigs := parseEtcdConfigs(getClusterUrl(endpoints, 1))
	for host, cli := range sshClis {
		wg.Add(1)
		go func(cli *goph.Client, host string) {
			mustStartEtcd(cli, cfg.EtcdServerDir, etcdConfigs[host])
			log.Printf("start etcd server on host %v\n", host)
			wg.Done()
		}(cli, host)
	}
	wg.Wait()

	// prepare some load
	log.Printf("prepare load...")
	prepareLoad(cfg.Clusters[0][0], cfg.Load, 0)

	cli := mustCreateClient(cfg.Clusters[0][0])
	resp, err := cli.MemberList(context.TODO())
	if err != nil {
		log.Panicf("list members failed: %v\n", err)
	}

	mlist := make([]etcdserverpb.MemberList, len(cfg.Clusters))
	for _, member := range resp.Members {
		name, err := strconv.ParseUint(member.Name, 10, 64)
		if err != nil {
			log.Panicf("convert name %v failed: %v\n", member.Name, err)
		}
		// assumption used for the following calculation:
		// 	each cluster has 3 nodes and the first node's name starts by 1
		idx := (name - 1) / 3
		mlist[idx].Members = append(mlist[idx].Members, etcdserverpb.Member{ID: member.ID})
	}

	log.Printf("ready to start...")
	time.After(1 * time.Second)
	start := time.Now()

	_, err = cli.MemberSplit(context.TODO(), mlist, false, false)
	if err != nil {
		log.Panicf("split members failed: %v\n", err)
	}

	// record time
	cost := time.Since(start)
	clean(sshClis, etcdConfigs, cfg.Clusters, cfg.EtcdServerDir)
	log.Printf("restore split cost: %v", cost.Milliseconds())

	return cost
}

func mustStartEtcd(cli *goph.Client, etcdServerDir string, etcdConfig etcdConfig) {
	cmd, err := cli.Command(fmt.Sprintf("cd %v && nohup ./server %v > etcd.%v.out 2>&1 &",
		etcdServerDir, etcdConfig.String(), etcdConfig.name))
	if err != nil {
		log.Panicf("create command for %v failed: %v\n", cli.Config.Addr, err)
	}
	if cmd.Start() != nil {
		log.Panicf("start etcd server on %v failed: %v\n", cli.Config.Addr, err)
	}
}

func prepareLoad(ep string, load uint64, offset uint64) {
	cli := mustCreateClientWithTimeout(10*time.Minute, ep)
	defer cli.Close()

	wg := sync.WaitGroup{}
	numThreads := uint64(8)
	for i := 0; i < int(numThreads); i++ {
		wg.Add(1)
		go func(tidx int, start, end uint64) {
			for ; start <= end; start++ {
				_, err := cli.Do(context.TODO(),
					clientv3.OpPut(fmt.Sprintf("%v", start), string(mustRandBytes(256))))
				if err != nil {
					// log.Printf("put request #%v failed: %v\n", start, err)
					start--
					continue
				}
			}
			wg.Done()
		}(i, offset+uint64(i)*(load/numThreads), offset+uint64(i+1)*(load/numThreads)-1)
	}
	wg.Wait()
}

func clean(sshClis map[string]*goph.Client, etcdConfigs map[string]etcdConfig, clusters [][]string, etcdServerDir string) {
	cleanWithWait(sshClis, etcdConfigs, clusters, etcdServerDir, true)
}

func cleanWithWait(sshClis map[string]*goph.Client, etcdConfigs map[string]etcdConfig,
	clusters [][]string, etcdServerDir string, wait bool) {
	// clean up
	log.Println("clean up...")
	wg := sync.WaitGroup{}
	for _, clr := range clusters {
		for _, ep := range clr {
			wg.Add(1)
			go func(ep string) {
				if wait {
					<-time.After(time.Second)
				}
				out, err := sshClis[ep].Run(fmt.Sprintf("killall -9 server"))
				if err != nil {
					log.Panicf("stop server on %v failed: %s", ep, out)
				}

				for {
					out, err = sshClis[ep].Run(fmt.Sprintf("lsof -t -i:%v", etcdConfigs[ep].port))
					if err != nil {
						break
					}
				}

				if wait {
					<-time.After(time.Second)
				}
				if out, err = sshClis[ep].Run(fmt.Sprintf(
					"cd %v && rm -rf data.etcd.%v etcd.%v.out *-restore %v",
					etcdServerDir, etcdConfigs[ep].name, etcdConfigs[ep].name, DefaultSnapshotName)); err != nil {
					log.Panicf("clean up failed: %s", out)
				}

				wg.Done()
			}(ep)
		}
	}
	wg.Wait()
}

func getMemberIds(urls []string) []uint64 {
	ids := make([]uint64, 0)
	cli := mustCreateClient(urls[0])
	for _, ep := range urls {
		resp, err := cli.Status(context.TODO(), ep)
		if err != nil {
			log.Panicf("get staus failed: %v\n", err)
		}
		ids = append(ids, resp.Header.MemberId)
	}
	return ids
}

func getClusterUrl(endpoints []string, startIdx int) string {
	nodes := make([]string, 0)
	for _, node := range endpoints {
		nodes = append(nodes, strconv.Itoa(startIdx)+"="+node)
		startIdx++
	}
	return strings.Join(nodes, ",")
}
