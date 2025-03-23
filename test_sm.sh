#!/bin/bash

echo "Changing to deploy directory"
cd deploy

echo "Starting etcd nodes (1,2,3)..."
fab start 1=http://127.0.0.1:1380,2=http://127.0.0.1:2380,3=http://127.0.0.1:3380
sleep 10

echo "Starting etcd nodes (4,5)..."
fab start 4=http://127.0.0.1:4380,5=http://127.0.0.1:5380
sleep 10

echo "Listing members on endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list

echo "Listing members on endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 --write-out=table member list
sleep 3

echo "Putting key 'a' with value 'b' on endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 put a b

echo "Putting key 'c' with value 'd' on endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 put c d
sleep 2

echo "Getting key 'a' from endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get a

echo "Getting key 'c' from endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get c
sleep 2

echo "Merging members from endpoints 2380 and 4380..."
../bin/etcdctl member merge http://127.0.0.1:2380,http://127.0.0.1:4380
sleep 15

echo "Verifying membership on endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 --write-out=table member list

echo "Verifying membership on endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 --write-out=table member list
sleep 3

echo "Getting key 'c' from endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get c

echo "Getting key 'a' from endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get a
sleep 2

echo "Splitting the cluster..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380,http://127.0.0.1:2380,http://127.0.0.1:3380,http://127.0.0.1:4380,http://127.0.0.1:5380 member split 7ac641502b72a71a,b71f75320dc06a6c,d07d5325fff892c1 b7bacd4212cc9323,a100ada638d79265
sleep 10

echo "Putting key 'e' with value 'f' on endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 put e f
sleep 2

echo "Putting key 'g' with value 'h' on endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 put g h
sleep 2

echo "Getting key 'g' from endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get g

echo "Getting key 'e' from endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get e
sleep 2

echo "Final validation: Getting key 'e' and 'c' from endpoint 1380..."
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get e
../bin/etcdctl --endpoints=http://127.0.0.1:1380 get c

echo "Final validation: Getting key 'g' and 'a' from endpoint 4380..."
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get g
../bin/etcdctl --endpoints=http://127.0.0.1:4380 get a

echo "Cleaning up cluster..."
../clean_up.sh
sleep 5

echo "Script execution completed successfully."