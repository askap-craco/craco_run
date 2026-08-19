#!/bin/bash

# Deletes uvfits files in the givieng local directory on all nodes in the $HOSTFILE
# input should be a list of directories like /data/craco/craco/SB012345
# use it like:
# ls -d /data/craco/craco/SB055699 | xargs ./delete_sbid.sh

source /home/craftop/.conda/.activate_conda.sh
conda activate craco
echo `which python`
sblist=$(find /CRACO/DATA_01/craco/ -maxdepth 1 -mtime -7 ! -mtime -1 -name "SB0*")
short_sblist=()

slack_message_log_file=/CRACO/SOFTWARE/craco/craftop/logs/slack_messager.log
echo "" >> $slack_message_log_file
echo `date` >> $slack_message_log_file

tmpfile=/tmp/daily_deletion_list.txt
echo "" > $tmpfile
for filepath in "${sblist[@]}"; do
  x=$(echo "$filepath"| awk -F/ '{print $NF}')
  short_sblist+=("$x")
  echo "$x\n" >> $tmpfile
done

echo "" >> $tmpfile

echo $short_sblist
echo tmpfile= $tmpfile

echo 'Executing - python /CRACO/SOFTWARE/craco/craftop/softwares/craco_run/slack_messager.py "Deleting the following SBIDs" -att $tmpfile'
python /CRACO/SOFTWARE/craco/craftop/softwares/craco_run/slack_messager.py "Deleting the following SBIDs" -att $tmpfile 2>&1 >> $slack_message_log_file

echo "Slack message sent"
echo "Now starting to delete stuff"


for sb in $short_sblist; do 
  echo "checking $sb"
  echo "Executing python /CRACO/SOFTWARE/craco/craftop/softwares/craco_run/delete_sbid.py $sb"
  python /CRACO/SOFTWARE/craco/craftop/softwares/craco_run/delete_sbid.py $sb 
done

