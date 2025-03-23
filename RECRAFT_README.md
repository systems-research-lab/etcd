# ETCD Cluster Setup Guide

## Pre-requisites

### Install Python
Ensure Python 3.13 or higher is installed. You can check your Python version with:
```bash
python3 --version
```
If Python is not installed, download it from [python.org](https://www.python.org/downloads/) or use your system's package manager.

### Set Up a Virtual Environment
Create a virtual environment to isolate dependencies:
```bash
python3 -m venv .venv
```
Activate the virtual environment:
- On macOS/Linux:
    ```bash
    source .venv/bin/activate
    ```
- On Windows:
    ```bash
    .venv\Scripts\activate
    ```

### Install Required Python Packages
Install `fabric` and `requests` using `pip`:
```bash
pip3 install fabric requests
```

Verify the installation:
```bash
pip3 show fabric requests
```

## Setting Up Your Environment

### Install gobin
```bash
go install github.com/myitcv/gobin@latest
```

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

### Set `LOCAL_ETCD_DIR` Environment Variable
Set the `LOCAL_ETCD_DIR` environment variable to the root of the repository:
```bash
export LOCAL_ETCD_DIR=$(pwd)
```
This ensures that scripts and tools referencing this variable can locate the repository's root directory.

### Add `etcd` and `etcdctl` to PATH
At the root of the repository, run:
```bash
export PATH=$PATH:$LOCAL_ETC_DIR/bin
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
First, get member IDs(HEX IDs) using:
```bash
etcdctl --write-out=table member list
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

### Testing Split and Merge Functionality

You can test the split and merge functionality using the provided script at the root:

```bash
./test_sm.sh
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
**Script:**
You can test the functionality of adding a single node using the Recraft consensus with the provided script located at the root:
```bash
./test_scm_add_single.sh
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
**Script:**
You can test the functionality of adding multiple nodes using the Recraft consensus with an explicit leave joint command by using the provided script located at the root:
```bash
./test_scm_add_multiple_explicit_leave.sh
```
You can test the functionality of adding multiple nodes using the Recraft consensus with an implicit leave joint command by using the provided script located at the root:

```bash
./test_scm_add_multiple_implicit_leave.sh
```

**Remove a Node:**

First, get member IDs(HEX IDs) using:
```bash
etcdctl --write-out=table member list
```
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id> --mode recraft
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380 member joint --remove d07d5325fff892c1 --mode recraft
```
**Script:**
You can test the functionality of removing a single node using the Recraft consensus with the provided script located at the root:
```bash
./test_scm_remove_single.sh
```

**Remove Multiple Nodes:**
First, get member IDs(HEX IDs) using:
```bash
etcdctl --write-out=table member list
```
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id1>,<member_id2> --mode recraft
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member joint --remove b7bacd4212cc9323,a100ada638d79265 --mode recraft
```
**Leave Joint Consensus:**
```bash
etcdctl member leave joint
```
**Script:**
You can test the functionality of removing multiple nodes using the Recraft consensus with an explicit leave joint command by using the provided script located at the root:
```bash
./test_scm_remove_multiple_explicit_leave1.sh
```
```bash
./test_scm_remove_multiple_explicit_leave2.sh
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
**Script:**
You can test the functionality of adding a single node using the Raft joint consensus with the provided script located at the root:
```bash
./test_joint_add_single.sh
```

**Add Multiple Nodes:**
```bash
etcdctl member joint --add <node_url1>,<node_url2>
```
**Example:**
```bash
etcdctl member joint --add http://127.0.0.1:4380,http://127.0.0.1:5380
```
**Script:**
You can test the functionality of adding multiple nodes using the Raft joint consensus by using the provided script located at the root:
```bash
./test_joint_add_multiple.sh
```

**Remove a Node:**

First, get member IDs(HEX IDs) using:
```bash
etcdctl --write-out=table member list
```
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id>
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380 member joint --remove d07d5325fff892c1
```
**Script:**
You can test the functionality of removing a single node using the Raft joint consensus with the provided script located at the root:
```bash
./test_joint_remove_single.sh
```

**Remove Multiple Nodes:**
First, get member IDs(HEX IDs) using:
```bash
etcdctl --write-out=table member list
```
```bash
etcdctl --endpoints=<endpoints> member joint --remove <member_id1>,<member_id2>
```
**Example:**
```bash
etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member joint --remove b7bacd4212cc9323,a100ada638d79265
```
**Script:**
You can test the functionality of removing multiple nodes using the Raft joint consensus by using the provided script located at the root:
```bash
./test_joint_remove_multiple.sh
```

**Leave Joint Consensus:**
```bash
etcdctl member leave joint
```
---
This guide covers the essential commands for setting up, splitting, merging, and managing nodes in an ETCD cluster using both **Recraft** and **Raft** consensus models. Customize the endpoints and member IDs to match your environment.