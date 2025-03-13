#!/bin/bash

# Navigate to the deploy directory
echo "Navigating to the deploy directory..."
cd deploy || { echo "Failed to navigate to deploy directory"; exit 1; }

# Start the Fabric script in the background
echo "Starting the Fabric script..."
nohup fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380 &
sleep 5

# List etcd members
echo "Listing initial etcd members..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list &
sleep 2

# Add a key-value pair to etcd
echo "Adding key-value pair (a, b) to etcd..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 put a b &
sleep 2

# Retrieve the value for key "a"
echo "Retrieving value for key 'a'..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get a &
sleep 3

# Join a new etcd member
echo "Joining a new etcd member..."
../bin/etcdctl member joint --add http://127.0.0.1:4380 --mode recraft &
sleep 5

# List updated etcd members
echo "Listing updated etcd members..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list &
sleep 5

# Start etcd node 4
echo "Starting etcd node 4..."
nohup ../bin/etcd --data-dir=data.etcd.4 \
     --name="4" \
     --initial-advertise-peer-urls=http://127.0.0.1:4380 \
     --listen-peer-urls=http://127.0.0.1:4380 \
     --advertise-client-urls=http://127.0.0.1:4379 \
     --initial-cluster="1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380" \
     --initial-cluster-state=existing \
     --listen-client-urls=http://127.0.0.1:4379 \
     --log-level=debug > etcd.4.out 2>&1 &
sleep 10

# List etcd members after adding node 4
echo "Listing etcd members after adding node 4..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 --write-out=table member list &
sleep 3

# Retrieve value for key "a" from node 4
echo "Retrieving value for key 'a' from node 4..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get a &
sleep 2

# echo "Cleaning up cluster..."
# ../clean_up.sh
# sleep 10

echo "Script execution completed."

# ./bin/etcdctl --endpoints=http://127.0.0.1:4380 put c d
# ./bin/etcdctl member leave joint