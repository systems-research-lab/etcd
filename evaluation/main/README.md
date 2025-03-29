In the config file, modify the node IPs and binary paths (etcdserverdir, etcdctlpath, etcdutlpath) to reflect your setup.

## Instructions

### Modify Configuration
Update the `type` in the configuration to specify the operation of interest:
- `split-performance`
- `split-impact`
- `split-load`
- `merge`
- `bench-split`
- `bench-merge`

### Build the Code
Run the following command to build the code:
```bash
go build -o main
```

### Run the Operation
Execute the operation using:
```bash
./main
```

### Update SSH Key Path and User
1. **In `bench-merge.go`:**
    - In the `replicateBench` function:
      - Replace the SSH key path on **line 67**.
      - Replace the user (`ubuntu`) on **line 72**.

2. **In `bench-split.go`:**
    - In the `splitBench` and `restoreBench` functions:
      - Replace the SSH key path and user to match your setup.