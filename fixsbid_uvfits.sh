#!/bin/bash

### fix uvfits files for a given sbid
sbid=$1

for file in /data/seren-??/big/craco/$sbid/scans/??/*/*/b??.uvfits
do
    fixuvfits $file
done