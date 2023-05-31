#!/bin/bash

obssbid=$1
calsbid=$2
runname=$3

leading_zero_fill ()
{
    # print the number as a string with a given number of leading zeros
    printf "%0$1d\\n" "$2"
}


### cd to a proper direcotry...
cd /data/seren-01/big/craco/wan342/craco_run
echo "current directory $PWD"


if ! (/data/seren-01/big/craco/wan342/craco_run/prepare_craco.py -o $obssbid -c $calsbid -r $runname); then
    echo "Exception was raised during prepare_craco run... aborted..."
    exit
fi

obssbidpad=$(leading_zero_fill 6 "$obssbid")
scriptdir=/data/seren-01/big/craco/SB$obssbidpad/run/scripts

for runscript in $scriptdir/*.sh
do 
    # reset mpicard...
    echo "reset mpi cards..."
    tsp mpiresetcards.sh
    echo "rerun preparation scripts to remove dead cards..."
    tsp /data/seren-01/big/craco/wan342/craco_run/prepare_craco.py -o $obssbid -c $calsbid -r $runname
    echo "running script for $runscript"
    tsp $runscript
done