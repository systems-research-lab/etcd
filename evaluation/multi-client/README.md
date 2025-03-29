In the config file, replace the client IPs and cluster node IPs to match your seup.

# Instructions

1. Modify the type to reflect the operation of interest (`merge`, `split`, `qps`).

2. In `merge_perf.go`, `split_perf.go`, and `qps_perf.go`, update the following variables to match your setup:
    - `ClientSshUser`
    - `ClientBasePath`

3. Run the operation using the command below:
    ```bash
    python repete_run.py
    ```