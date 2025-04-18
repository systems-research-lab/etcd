# 🔧 etcd Reconfiguration Tool

This Go script provides a unified interface to trigger advanced etcd cluster operations such as **member split**, **merge**, **joint consensus (add/remove)**, and **leave joint consensus**, making it ideal for performance evaluation or testing scenarios.

---

## 🚀 Starting an etcd Cluster

Before using the script, you need to start your etcd cluster.

To start a **3-node etcd cluster**, run:

```bash
cd deploy
fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380
```

To start a **5-node cluster**, extend the command accordingly:

```bash
cd deploy
fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:6380,5=http://127.0.0.1:7380
```

---

## 🛠️ Running the Script

```bash
go run main.go config.yml
```

---

## 🧾 Configuration: `config.yml`

The script reads all configurations from a single `config.yml` file. Below are the required fields and how to modify them for each operation.

---

### ✅ Common Field

```yaml
endpoints:
  - "http://127.0.0.1:1380"
  - "http://127.0.0.1:2380"
  - "http://127.0.0.1:3380"
  - "http://127.0.0.1:6380"
  - "http://127.0.0.1:7380"
```

List of all active etcd server endpoints in your cluster.

---

### ⚙️ Operation Modes

---

### 1. 🔀 **Member Split**

> Splits the cluster into subgroups for simulation or testing.

```yaml
operation: split
subClusters:
  - ["http://127.0.0.1:1380", "http://127.0.0.1:2380", "http://127.0.0.1:3380"]
  - ["http://127.0.0.1:6380", "http://127.0.0.1:7380"]
```

- Requires `subClusters` — a list of endpoint groups that represent each partition.

---

### 2. 🔁 **Member Merge**

> Merges subclusters into a single cluster.

```yaml
operation: merge
subClusters:
  - ["http://127.0.0.1:1380", "http://127.0.0.1:2380", "http://127.0.0.1:3380"]
  - ["http://127.0.0.1:6380", "http://127.0.0.1:7380"]
```

- Also requires `subClusters` — each list should represent an individual subcluster.

---

### 3. ➕ **Joint Consensus (Add Members)**

> Triggers a joint consensus operation to **add new members**.

#### a. Raft-based joint add:

```yaml
operation: joint_add
mode: raft
addPeers:
  - "http://127.0.0.1:4380"
```

#### b. Recraft-based joint add:

```yaml
operation: joint_add
mode: recraft
addPeers:
  - "http://127.0.0.1:5380"
```

- Use `mode: raft` or `mode: recraft` depending on the joint consensus protocol.
- List the `addPeers` as the new endpoints to be added.

---

### 4. ➖ **Joint Consensus (Remove Members)**

> Triggers a joint consensus operation to **remove existing members** from the cluster.

#### a. Raft-based joint remove:

```yaml
operation: joint_remove
mode: raft
removePeers:
  - "1a99b4b14094011e"
```

#### b. Recraft-based joint remove:

```yaml
operation: joint_remove
mode: recraft
removePeers:
  - "1a99b4b14094011e"
```

- Use `mode: raft` or `mode: recraft` to specify the consensus protocol being used.
- Provide the list of `removePeers` as member IDs (in hex format) that should be removed.
- To get member IDs:

```bash
etcdctl --endpoints=<endpoint_url> member list
```

```bash
etcdctl --endpoints=http://127.0.0.1:2380 member list
```

---

### 5. 🔚 **Leave Joint Consensus**

> Exits from a joint consensus phase after addition/removal is complete.

```yaml
operation: leave_joint
```

- No additional parameters needed.
- Must be called after joint consensus changes are committed.

---

---

## 👨‍💻 Example

To split a 5-node cluster into two:

```yaml
operation: split
subClusters:
  - ["http://127.0.0.1:1380", "http://127.0.0.1:2380", "http://127.0.0.1:3380"]
  - ["http://127.0.0.1:6380", "http://127.0.0.1:7380"]
```

Then merge them back:

```yaml
operation: merge
subClusters:
  - ["http://127.0.0.1:1380", "http://127.0.0.1:2380", "http://127.0.0.1:3380"]
  - ["http://127.0.0.1:6380", "http://127.0.0.1:7380"]
```

---