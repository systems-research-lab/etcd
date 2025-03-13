# Gracefully stop 'server' and 'etcd' processes
echo "Stopping server and etcd processes..."
killall -9 server || echo "Server process not found."
killall -9 etcd || echo "ETCD process not found."

sleep 5 && echo "Server and etcd processes stopped."

# Delete etcd-related data and logs from the server directory (one level above deploy/)
echo "Cleaning up etcd data and log files..."
rm -rf deploy/data.etcd.* 2>/dev/null || echo "No data.etcd.* directories found."
# rm -f etcd.*.out 2>/dev/null || echo "No etcd.*.out files found."
rm -f deploy/nohup.out 2>/dev/null || echo "No etcd.*.out files found."
rm -rf server/data.etcd.* 2>/dev/null || echo "No data.etcd.* directories found."
# rm -f ../server/etcd.*.out 2>/dev/null || echo "No etcd.*.out files found."