#!/bin/bash
start=`date +%s%N`
./etcdctl --endpoints=192.168.0.36:2380,192.168.0.239:2380,192.168.0.128:2380 put key value
end=`date +%s%N`
echo Execution time was `expr $end - $start` nanoseconds.