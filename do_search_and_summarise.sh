#!/bin/bash

scan=$1
shellpath=$2
runname=$3

#source /home/craftop/.conda/.activate_conda.sh
#conda activate craco


# does mpirun for all beams and waits until all beams have finished before returning
$shellpath

scanroot=$(echo $scan | sed s%/data/craco/%%)
refresh 2>&1 >/dev/null
files=$(ls /CRACO/DATA*/$scanroot/$runname/clustering_output/*uniq*.csv)
summarise_cands $files
