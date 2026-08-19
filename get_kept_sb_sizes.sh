#!/bin/bash

for i in {ls /CRACO/DATA_00/craco/SB0*/KEEP }
do 
	sb=`echo $i | awk -F/ '{print $5}'`
	echo $sb 
	if [[ "${sb}" ]]
	then 
		cat /CRACO/DATA_00/craco/$sb/KEEP
		du -sch /CRACO/DATA_??/craco/$sb
	fi
done | tee  ~/keeps_with_size.tmp
