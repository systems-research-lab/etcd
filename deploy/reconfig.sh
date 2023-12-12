#!/bin/bash
start=`date +%s%N`
~/etcd/etcdctl/./etcdctl --endpoints=192.168.0.36:2380,192.168.0.239:2380,192.168.0.128:2380 member joint 4 5 --add http://192.168.0.183:2380,http://192.168.0.121:2380
end=`date +%s%N`
echo EnterJoint time was `expr $end - $start` nanoseconds.

./join.sh

startL=`date +%s%N`
~/etcd/etcdctl/./etcdctl --endpoints=192.168.0.36:2380,192.168.0.239:2380,192.168.0.128:2380,192.168.0.183:2380,192.168.0.121:2380 member split 3 --leave 
endL=`date +%s%N`
echo LeaveJoint time was `expr $endL - $startL` nanoseconds.


echo Reconfiguration time was `expr $end - $start + $endL - $startL`