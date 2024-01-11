#!/bin/bash

# Deletes uvfits files in the givieng local directory on all nodes in the $HOSTFILE
# input should be a list of directories like /data/craco/craco/SB012345
# use it like:
# ls -d /data/craco/craco/SB055699 | xargs ./delete_sbid.sh

for f in $@ ; do
    keepfile=$f/KEEP
    if [[ -f $keepfile ]] ; then
       echo "SBDIR $f has keepfile. Not deleting. Keepfile has"
       cat $keepfile
    else
	echo "Deleting UVFITS file in $f hostfile=$HOSTFILE"
	mpirun -hostfile $HOSTFILE --map-by ppr:1:node `which find` $f -name 'cas*.fil' -delete
    fi
done
