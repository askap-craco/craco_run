#!/bin/bash

scan=$1
scanroot=$(echo $scan | sed s%/data/craco/%%)
echo "scan" $scan $scanroot
refresh 2>&1 > /dev/null

mpi_run_beam.sh $scan `which mpi_do_candpipe.sh`
files=$(ls /CRACO/DATA*/$scanroot/results/clustering_output/*uniq*.csv)
summarise_cands $files
sleep 1m

