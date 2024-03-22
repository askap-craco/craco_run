#!/bin/bash
### script to archive data
sbid=$1
scan=$2
beam=$3
node=$4
# dry_run="--dry-run"
dry_run=

### format sbid
sbid_format=$(printf "%06d" $sbid)
node_format=$(printf "%02d" $node)
beam_format=$(printf "%02d" $beam)
head_dir="/CRACO/DATA_00/craco/SB${sbid_format}"
data_dir="/CRACO/DATA_${node_format}/craco/SB${sbid_format}"

acacia_dir="acacia:archive1/SB${sbid_format}"

echo "making file under scan to mark the beginning of the archive"
touch $head_dir/scans/$scan/ARCHIVE_START

echo "remove write access for uvfits file..."
# ls $data_dir/scans/$scan/b${beam_format}.uvfits*
chmod a-w $data_dir/scans/$scan/b${beam_format}.uvfits*

echo "arching uvfits file to acacia..."
rclone copy $data_dir $acacia_dir --include "scans/$scan/**/*b${beam_format}*" --include "scans/$scan/*b${beam_format}*" --copy-links $dry_run

echo "archiving metadata files to acacia..."
rclone copy $head_dir $acacia_dir --include "*b09*"  --include "*.json.gz" --include "*.antflag.json" --copy-links $dry_run

echo "archiving running scripts..."
rclone copy $head_dir $acacia_dir --include "scans/$scan/*.log" --include "scans/$scan/**/*.log" $dry_run

echo "making file under scan to mark the beginning of the archive"
touch $head_dir/scans/$scan/ARCHIVE_END



