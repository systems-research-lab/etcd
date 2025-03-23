# Gracefully stop 'server' and 'etcd' processes
echo "Stopping server and etcd processes..."
killall -9 server || echo "Server process not found."
killall -9 etcd || echo "ETCD process not found."

sleep 5 && echo "Server and etcd processes stopped."

# Delete etcd-related data and logs from the server and deploy directory
echo "Cleaning up etcd data and log files..."
rm -rf $LOCAL_ETCD_DIR/deploy/data.etcd.* 2>/dev/null || echo "No data.etcd.* directories found."
rm -f $LOCAL_ETCD_DIR/deploy/etcd.*.out 2>/dev/null || echo "No etcd.*.out files found."
rm -f $LOCAL_ETCD_DIR/deploy/nohup.out 2>/dev/null || echo "nohup.out file not found."
rm -rf $LOCAL_ETCD_DIR/server/data.etcd.* 2>/dev/null || echo "No data.etcd.* directories found."
rm -f $LOCAL_ETCD_DIR/server/etcd.*.out 2>/dev/null || echo "No etcd.*.out files found."