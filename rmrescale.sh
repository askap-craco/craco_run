#!/bin/bash
# function for remove all rescale folder from each pipeline run
sbid=$1

if [ -z "$sbid" ]
then
    echo you should specify an sbid to delete... aborting
    exit
fi

for file in /data/seren-??/big/craco/$sbid/scans/??/*/*/rescale
do
    echo deleting rescale files from $file ....
    rm -r $file
done

# rm -r /data/seren-??/big/craco/$sbid/scans/??/*/*/rescale