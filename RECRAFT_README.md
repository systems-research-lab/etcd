# ETCD Cluster Setup Guide

## Setting Up Your Environment

### Add Go Binaries to PATH
```bash
export PATH=$PATH:$(go env GOPATH)/bin
```

## Building the Binaries

### Build `etcd` and `etcdctl`
At the root of the repository, run:
```bash
make build
```

### Build the Server Binary
Navigate to the `server` directory and run:
```bash
go build -o server
```

## Starting a Cluster

### Start a 3-Node Cluster
Navigate to the `deploy` directory (which contains the `fabfile`) and run:
```bash
fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380
```

## Managing Clusters

### Split a Cluster into Subclusters
First, get member IDs using:
```bash
etcdctl member list
```

Then split the cluster:
```bash
etcdctl --endpoints=<endpoints> member split <member_ids_group1> <member_ids_group2>
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member split 7ac641502b72a71a,b71f75320dc06a6c,d07d5325fff892c1 b7bacd4212cc9323,a100ada638d79265
```

### Merge Clusters
```bash
etcdctl member merge <cluster_member_url1>,<cluster_member_url2>
```
**Example:**
```bash
etcdctl member merge http://127.0.0.1:2380,http://127.0.0.1:4380
```

## Adding and Removing Nodes

### Using Recraft Consensus

**Add a Node:**
```bash
etcdctl member joint --add <node_url> --mode recraft
```
**Example:**
```bash
etcdctl member joint --add http://127.0.0.1:4380 --mode recraft
```

**Add Multiple Nodes:**
```bash
etcdctl member joint --add <node_url1>,<node_url2> --mode recraft
```
**Example:**
```bash
etcdctl member joint --add http://127.0.0.1:4380,http://127.0.0.1:5380 --mode recraft
```

**Leave Joint Consensus:**
```bash
etcdctl member leave joint
```

**Remove a Node:**
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id> --mode recraft
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380 member joint --remove d07d5325fff892c1 --mode recraft
```

**Remove Multiple Nodes:**
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id1>,<member_id2> --mode recraft
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member joint --remove b7bacd4212cc9323,a100ada638d79265 --mode recraft
```

### Using Raft Consensus

**Add a Node:**
```bash
etcdctl member joint --add <node_url>
```
**Example:**
```bash
etcdctl member joint --add http://127.0.0.1:4380
```

**Add Multiple Nodes:**
```bash
etcdctl member joint --add <node_url1>,<node_url2>
```
**Example:**
```bash
etcdctl member joint --add http://127.0.0.1:4380,http://127.0.0.1:5380
```

**Remove a Node:**
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id>
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380 member joint --remove d07d5325fff892c1
```

**Remove Multiple Nodes:**
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id1>,<member_id2>
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member joint --remove b7bacd4212cc9323,a100ada638d79265
```

**Leave Joint Consensus:**
```bash
etcdctl member leave joint
```
---
This guide covers the essential commands for setting up, splitting, merging, and managing nodes in an ETCD cluster using both **Recraft** and **Raft** consensus models. Customize the endpoints and member IDs to match your environment.