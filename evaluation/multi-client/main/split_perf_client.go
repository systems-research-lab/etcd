package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type query struct {
	Start   int64 `json:"start"`   // unix microsecond timestamp
	Latency int64 `json:"latency"` // in microsecond
}

func main() {
	var id int64 // client id
	var cluster string
	var warmupTime int64
	var cooldownTime int64
	var measureTime int64
	var split int64
	var thread int64
	var repetition int64
	flag.Int64Var(&id, "id", -1, "")
	flag.StringVar(&cluster, "cluster", "", "") // e.g. http://10.200.125.51:22380,http://10.200.125.52:22380,http://10.200.125.53:22380
	flag.Int64Var(&warmupTime, "warmup", -1, "")
	flag.Int64Var(&cooldownTime, "cooldown", -1, "")
	flag.Int64Var(&measureTime, "measure", -1, "")
	flag.Int64Var(&split, "split", -1, "")
	flag.Int64Var(&thread, "thread", -1, "")
	flag.Int64Var(&repetition, "repetition", -1, "")
	flag.Parse()

	if id == -1 || cluster == "" ||
		warmupTime == -1 || cooldownTime == -1 || measureTime == -1 ||
		split == -1 || thread == -1 || repetition == -1 {
		panic("Wrong arguments.")
	}

	start(id, cluster, split, warmupTime, cooldownTime, measureTime, thread, repetition)
}

type splitClientReport struct {
	Start   int64   `json:"startUnixMicro"`
	Queries []query `json:"queries"`
}

func start(id int64, cluster string, split, warmupTime, cooldownTime, measureTime, thread, repetition int64) {
	startTime, queries := sendRequests(id, cluster, warmupTime+cooldownTime+measureTime, thread)
	data, err := json.Marshal(splitClientReport{startTime.UnixMicro(), queries})
	if err != nil {
		panic("Marshal report error: " + err.Error())
	}

	if err = os.WriteFile(fmt.Sprintf("./split-client-%v-%v-%v-%v.json", id, split, thread, repetition),
		data, 0666); err != nil {
		panic(fmt.Sprintf("write report json failed: %v", err))
	}

	log.Printf("Client %d finished %v queries.\n", id, len(queries))
}

func sendRequests(cid int64, cluster string, runTime, thread int64) (time.Time, []query) {
	startCh := make(chan struct{})
	queryCh := make(chan []query)
	for i := int64(0); i < thread; i++ {
		go func(tid int64) {
			clients := make([]*clientv3.Client, 0, 3)
			for _, ep := range strings.Split(cluster, ",") {
				clients = append(clients, mustCreateClient(ep))
			}

			<-startCh
			startTime := time.Now()
			queries := make([]query, 0)
			for rid := 0; int64(time.Since(startTime).Seconds()) < runTime; rid++ {
				t := time.Now()

				// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if _, err := clients[rid%len(clients)].Do(context.TODO(),
					// clientv3.OpPut(fmt.Sprintf("thread-%v-%v-%v", cid, tid, rid), strconv.Itoa(rid))); err != nil {
					clientv3.OpPut(fmt.Sprintf("thread-%v-%v-%v", cid, tid, rid), string(mustRandBytes(512)))); err != nil {
					// cancel()
					log.Printf("thread %v sending request %v error: %v\n", tid, rid, err)
					continue
				}
				// cancel()
				queries = append(queries, query{t.UnixMicro(), time.Since(t).Microseconds()})
			}

			queryCh <- queries
		}(i)
	}

	startTime := time.Now()
	close(startCh)

	queries := make([]query, 0)
	for i := int64(0); i < thread; i++ {
		queries = append(queries, <-queryCh...)
	}

	return startTime, queries
}
