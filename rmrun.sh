#!/bin/bash

# remove a pipeline run folder...

sbid=$1
runname=$2

if [ -z "$sbid" ]
then
    echo you should specify an sbid to delete... aborting
    exit
fi

if [ -z "$runname" ]
then
    echo you should specify an runname to remove... aborting...
    exit
fi

for file in /data/seren-??/big/craco/$sbid/scans/??/*/$runname
do
    echo deleting pipeline results from $file ....
    rm -r $file
done
