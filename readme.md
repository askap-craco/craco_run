### CRACO MpiPipeline Preparation Scripts

This script is used for making preparation for CRACO MPI offline pipeline run. The script is designed for seren. 
This script will
1. Get the metadata file from tethys for a given SBID
2. Find bad antennas based on the metadata file
3. Link calibration folder to the observation which is going to perform search on
4. Make mpipipeline bash script to run the whole process
5. Use `task-spooler` to queue the whole process

If you want to change the parameters, for example, which additional antennas you want to flag, the number of the samples you want to perform the search on, etc.
You need to modify them in `craco_cfg.py` file.

If you want to run the pipeline for one specific schedule block, use `run_sbid.sh` bash script 

`>>> ./run_sbid.sh <the schedule block to run on> <the calibration schedule block> <the run name>`

For example, if I want to execute mpipipeline on SB49721 with the calibration file derived from SB49719,
and put all results under `results` folder, then we can use

`>>> ./run_sbid.sh 49721 49719 results`

If you wish to run the pipeline without calibration applied, use `none` instead, i.e.,

`>>> ./run_sbid.sh 49721 none results`

```
SB0?????
 ├── cal # symbolic link to the calibration directory
 ├── SB?????.json.gz # metadata file
 ├── run # directory to store scripts and pipeline output
 │    ├── scripts # folder to save bash scripts for the final mpipipeline run
 │    └── out # folder to save piupeline output files
 └── scans
      ├── 00/20??????...
      │    ├── ccap*.fits # ccapfits files 
      │    ├── results # directory for storing the final result
      ├── 01/20??????...
      ...

```

### Notes

#### Script directory and virtual environment

The scripts are stored at `/data/big/craco/wan342/craco_run` on seren. Usually, the default environment for `craftop` will work fine. But if you want to run a more flexible craco (i.e., you can change code yourself, not ask Keith to do :)), `deactivate` the default environment, and use `conda activate pipe` to activate this `conda` environment.

#### How to perform calibration

For details, please refer to the readme file at `https://github.com/askap-craco/craco_calib/tree/pipe`. 

#### `prepare_craco.py` scripts

The core part of this bash script is `prepare_craco.py` script, the usage is almost the same as `run_sbid.sh`

`>>> ./prepare_craco.py -o 49721 -c 49719 -r results`

This assumes that you will perform the search on SB49721 with the calibration file derived from SB49719.
And save all the output files under `results` folder

After running the script, the data structure of the SB49721 (your observation) will be 


