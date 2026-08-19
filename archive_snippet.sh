#!/bin/bash
#### scripts to archive snippet...
uvfitspath=$1
comments=$2

if [ -z "$uvfitspath" ]; then
    echo "please provide uvfitspath to be submitted..."
    exit 64
fi

if [ -z "$comments" ]; then
    comments="archive snippet"
fi


# initiate craco environment
source /home/craftop/.conda/.activate_conda.sh
conda activate craco

# fix files...
chmod a+w $uvfitspath
`which fixuvfits` $uvfitspath
chmod a-w $uvfitspath

pathid=$(echo "$uvfitspath" | cut -d'/' -f5- | rev | cut -d'/' -f2- | rev)
parentdir=$(dirname $uvfitspath)
echo $comments >> $parentdir/ARCHIVE

acacia_dir="acacia:craco-snippet"

echo "archiving uvfits snippet to acacia... identifier - $pathid"
rclone copy -PL $uvfitspath $acacia_dir/$pathid
rclone copy -PL $parentdir/candidate.txt $acacia_dir/$pathid
rclone copy -PL $parentdir/ARCHIVE $acacia_dir/$pathid

echo "adding writing permission..."
chmod a+w $uvfitspath