#!/bin/bash

echo "Changing directory to deploy"
cd deploy

echo "Starting a cluster of 3 nodes..."
fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380
sleep 3

echo "Listing cluster members from node 1 (1380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list
echo "Listing cluster members from node 2 (2380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:2380 --write-out=table member list
echo "Listing cluster members from node 3 (3380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:3380 --write-out=table member list
sleep 10

echo "Removing a member with ID b71f75320dc06a6c..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380 member remove b71f75320dc06a6c
sleep 10

echo "Listing cluster members after removal on node 1 (1380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list
echo "Listing cluster members after removal on node 2 (2380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:2380 --write-out=table member list
echo "Listing cluster members after removal on node 3 (3380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:3380 --write-out=table member list

echo "Writing data to the cluster: Putting key 'a' with value 'b' on node 1..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 put a b
echo "Reading key 'a' from node 3..."
../bin/etcdctl --endpoints=http://127.0.0.1:3380 get a

echo "Adding a new member (node 4) to the cluster..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:3380 member add 4 --peer-urls=http://127.0.0.1:4380
sleep 2

echo "Starting etcd for new member 4..."
nohup ../bin/etcd --data-dir=data.etcd.4 \
     --name="4" \
     --initial-advertise-peer-urls=http://127.0.0.1:4380 \
     --listen-peer-urls=http://127.0.0.1:4380 \
     --advertise-client-urls=http://127.0.0.1:4379 \
     --initial-cluster="1=http://127.0.0.1:1380,3=http://127.0.0.1:3380,4=http://127.0.0.1:4380" \
     --initial-cluster-state=existing \
     --listen-client-urls=http://127.0.0.1:4379 \
     --log-level=debug > etcd.4.out 2>&1 &
sleep 5

echo "Writing data to new member 4: Putting key 'c' with value 'd'..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 put c d

echo "Reading key 'a' from node 1..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get a
echo "Reading key 'a' from node 3..."
../bin/etcdctl --endpoints=http://127.0.0.1:3380 get a

echo "Listing cluster members from node 1 (1380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list
echo "Listing cluster members from node 3 (3380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:3380 --write-out=table member list
echo "Listing cluster members from node 4 (4380)..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 --write-out=table member list

echo "Cleaning up cluster..."
../clean_up.sh
sleep 5

echo "Cluster operations completed successfully."