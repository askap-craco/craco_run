#!/bin/bash
# retreive what calibration solution that is used for a given sbid

sbid=$1

ls -l /CRACO/DATA_00/craco/SB0$sbid/cal
