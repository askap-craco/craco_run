#!/bin/bash

# activate craco environment
echo "activating conda environment for craco..."
source /home/craftop/.conda/.remove_conda.sh
source /home/craftop/.conda/.activate_conda.sh
conda activate craco

`which craco_archiver_monitor.py`