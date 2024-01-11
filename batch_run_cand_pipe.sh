#!/bin/bash

start=$1
end=$2
for f in `seq $start $end` ; do
    for scan in `ls -d /data/craco/craco/SB0$f/scans/*/* ` ; do
	TS_SOCKET=/tmp/candpipe-ts tsp ./run_candpipe_and_summarise.sh $scan
    done
done
