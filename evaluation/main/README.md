## Instructions

### Update Configuration File

In the configuration file, make the following modifications to tailor it to your setup:

1. **Node IPs**: Update the `clusters` section with the IP addresses of your etcd nodes. Each cluster should be represented as a list of node URLs. For example:
  ```yaml
  clusters:
    - [http://<node1-ip>:2380, http://<node2-ip>:2380, http://<node3-ip>:2380]
    - [http://<node4-ip>:2380, http://<node5-ip>:2380, http://<node6-ip>:2380]
  ```

2. **Binary Paths**: Modify the paths for the etcd binaries to match your environment:
  - `etcdserverdir`: Path to the etcd server directory.
  - `etcdctlpath`: Path to the etcdctl command-line tool.
  - `etcdutlpath`: Path to the etcdutl utility.

  Example:
  ```yaml
  etcdserverdir: /path/to/etcd/server
  etcdctlpath: /path/to/etcdctl
  etcdutlpath: /path/to/etcdutl
  ```

3. **Operation Type**: Set the `type` field to the desired operation. Available options include:
  - `split-performance`: Measure performance improvement by split.
  - `split-impact`: Measure performance impact by split.
  - `split-load`: Measure the load to split.
  - `merge`: Measure merge performance.
  - `bench-split`: Perform a split benchmark.
  - `bench-merge`: Perform a merge benchmark.

  Example:
  ```yaml
  type: bench-merge
  ```

4. **Additional Parameters**:
  - `folder`: Specify the directory for storing reports.
  - `warmup` and `cooldown`: Set the warmup and cooldown durations.
  - `threads`: Configure the number of threads (set to `0` for default).
  - `load`: Specify the number of key-value pairs to prepare on each cluster.
  - `repetition`: Set the number of repetitions for the benchmark.

  Example:
  ```yaml
  folder: ../report
  before: 5
  after: 5
  warmup: 3
  cooldown: 3
  threads: 0
  load: 1000
  repetition: 1
  ```

Ensure all paths and IPs reflect your actual setup to avoid configuration issues.

### Run the Operation

To execute the operation, use the following command:

```bash
./main
```

For batch execution with multiple thread counts and repetitions, use the following command:

```bash
python batch_run.py
```

### Update SSH Key Path and User
1. **In `bench-merge.go`:**
    - In the `replicateBench` function:
      - Replace the SSH key path on **line 67**.
      - Replace the user (`ubuntu`) on **line 72**.

2. **In `bench-split.go`:**
    - In the `splitBench` and `restoreBench` functions:
      - Replace the SSH key path and user to match your setup.