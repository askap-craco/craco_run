#!/bin/bash
# Run injections. You can either give an SBID e.g. "./inject.sh 66251" or a number 
# "./inject -n 5" to inject in 5 recent SBIDs > 5GB that have not yet had injections.
# Attempting both gets the program stuck somehow.

# Search for a -n flag and get the number
narg=1  # in case it is not given
run="inj_s4a"
yaml="simple_4.1_fixu.yml"  #"randomized_4_scattering.yml"  #"simple_3_seed.yml"   #"simple_2_high_dm.yml"  #"simple_1_rand_wdmsp_shape_flat.yml"  #"width_above3_phase_0_snr30.yml"  #"inj_params7_pc_width.yml"    #"randomized_4_scattering.yml"
#sarg=100 # number of sbids to search in the past
while getopts "n:r:y:" opt; do
        case $opt in
                n)
                        narg=$OPTARG
			echo Got n = $narg
                        ;;
		r)
			run=$OPTARG
			echo Got run=$run
			;;
		y)
			yaml=$OPTARG
			echo Got yaml=$yaml
			;;
		?)
			echo "Invalid option: -${OPTARG}."
			exit 1
			;;
        esac
	
done
shift $(( OPTIND - 1 ))
#echo $@ $#
echo "n is $narg"   #, s is $sarg"

# Use input SBID if supplied.
if [ $# -eq 0 ]
then
	echo No SBID given, will try to find a suitable recent one.
	cd /CRACO/DATA_01/craco/
	# Get recent SBIDs that are larger than 5GB.
	#recent_sbids=$(du -sh -t 5G $(ls | grep SB0 | sort -r | head -n $sarg) | awk '{print $2}')
	recent_sbids=$(find ./SB0*/scans/??/*/b00.uvfits -size +5G | cut -d/ -f2 | sort -ur)
	# Find SBIDs where the inj directory does not yet exist.
	sbids=()
	nappended=0
	for sbid in ${recent_sbids:8} # Slice out the first SBID.
	do
		if [ ! -d ${sbid}/scans/??/*/inj_r4 ]
		then
			# Strip "SB0" and append.
			echo adding $sbid
			sbids+=(${sbid//"SB0"/})
			echo ${sbids[@]}
			((nappended++))
		else
			echo Skipping $sbid
		fi
		echo $nappended $narg
		if [ $nappended -eq $narg ]
		then
			break
		fi
	done
else
	sbids=$@
fi

echo Will run on ${sbids[@]} 2>&1 | tee -a /data/craco/craco/jah011/injected_in.log

cd /CRACO/SOFTWARE/craco/craftop/softwares/craco_run
for sbid in ${sbids[@]}
do
	# Find the calibration SBID
	cal_sbid=$(basename "$(realpath /CRACO/DATA_00/craco/SB0${sbid}/cal)")
	# Strip the characters "SB0"
	cal_sbid=${cal_sbid//"SB0"/}
	if [ $cal_sbid -gt 10000 ]
	then
		echo Running injections on $sbid with calibration $cal_sbid.
		command="./prepare_skadi.py -obs $sbid -cal $cal_sbid -run $run -inj /CRACO/DATA_00/craco/injection/${yaml}" # -add \"--simulate-data\""
		echo Running $command  2>&1 | tee -a /data/craco/craco/jah011/injected_in.log
		$command
	else
		echo $cal_sbid is not a valid calibration SBID
	fi
done
