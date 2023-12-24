#!/bin/bash

scan=$1
scanroot=$(echo $scan | sed s%/data/craco/%%)
echo "scan" $scan $scanroot
refresh 2>&1 > /dev/null
files=$(ls /CRACO/DATA*/$scanroot/results/clustering_output/*uniq*.csv)
summarise_cands $files
seep 1m
