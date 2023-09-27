ssh 192.168.0.242 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.30 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.239 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.124 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.154 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.156 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.197 mkdir -p ~/etcd/evaluation/multi-client/main
ssh 192.168.0.122 mkdir -p ~/etcd/evaluation/multi-client/main

scp split_perf_client.go util.go 192.168.0.242:~/etcd/evaluation/multi-client/main
scp split_perf_client.go util.go 192.168.0.30:~/etcd/evaluation/multi-client/main
scp split_perf_client.go util.go 192.168.0.239:~/etcd/evaluation/multi-client/main

scp merge_perf_client.go util.go 192.168.0.242:~/etcd/evaluation/multi-client/main
scp merge_perf_client.go util.go 192.168.0.30:~/etcd/evaluation/multi-client/main
scp merge_perf_client.go util.go 192.168.0.239:~/etcd/evaluation/multi-client/main

scp qps_perf_client.go util.go 192.168.0.242:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.30:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.239:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.124:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.154:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.156:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.197:~/etcd/evaluation/multi-client/main
scp qps_perf_client.go util.go 192.168.0.122:~/etcd/evaluation/multi-client/main
