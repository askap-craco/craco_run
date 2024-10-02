#!/bin/bash
# Run injections
# Use input SBID if supplied.
if [ $# -eq 0 ]
then
	echo No SBID given, will try to find a suitable recent one.
	cd /CRACO/DATA_01/craco/
	# Get recent SBIDs that are larger than 5GB.
	recent_sbids=$(du -sh -t 5G $(ls | grep SB0 | sort -r | head -n 30) | awk '{print $2}')
	# Find an SBID where the inj directory does not yet exist.
	for sbid in $recent_sbids
	do
		if [ ! -d ${sbid}/scans/??/*/inj_r4 ]
		then
			break
		fi
		echo Skipping $sbid
	done
	# Strip "SB0". 
	sbid=${sbid//"SB0"/}
else
	sbid=$1
fi

cd /CRACO/SOFTWARE/craco/craftop/softwares/craco_run

# Find the calibration SBID
cal_sbid=$(basename "$(realpath /CRACO/DATA_00/craco/SB0${sbid}/cal)")
# Strip the characters "SB0"
cal_sbid=${cal_sbid//"SB0"/}
echo Running injections on $sbid with calibration $cal_sbid.
command="./prepare_skadi.py -obs $sbid -cal $cal_sbid -run inj_r4 -inj /CRACO/DATA_00/craco/injection/randomized_4_scattering.yml --nqueues 2"
echo $command
$command
