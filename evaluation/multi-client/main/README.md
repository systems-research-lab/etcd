# Instructions

1. Update the configuration file with the following details:

    - Set the `type` field to the operation of interest. Allowed values are `merge`, `split`, or `qps`. For example:
      ```yaml
      type: merge
      ```

    - Specify the `clients` involved in the operation. Add the IP addresses of the clients you want to include. For example:
      ```yaml
      clients:
        - 192.168.0.242
        - 192.168.0.30
        - 192.168.0.239
      ```

    - Define the `clusters` participating in the operation. Each cluster should be defined on a single line, and you can modify the node IPs as needed. For example:
      ```yaml
      clusters:
        - [http://192.168.0.32:2380, http://192.168.0.40:2380, http://192.168.0.86:2380]
        - [http://192.168.0.124:2380, http://192.168.0.154:2380, http://192.168.0.156:2380]
        - [http://192.168.0.197:2380, http://192.168.0.122:2380, http://192.168.0.158:2380]
      ```

    - Adjust the timing parameters (`before`, `after`, `warmup`, `cooldown`) and other settings (`threads`, `repetition`) to match your requirements. For example:
      ```yaml
      before: 30
      after: 60
      warmup: 10
      cooldown: 5
      threads: 128
      repetition: 1
      ```

2. In `merge_perf.go`, `split_perf.go`, and `qps_perf.go`, update the following variables to match your setup:
    - `ClientSshUser`
    - `ClientBasePath`

3. Run the operation using the command below:
    ```bash
    python repete_run.py
    ```