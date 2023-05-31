### CRACO MpiPipeline Preparation Scripts

This script is used for making preparation for CRACO MPI offline pipeline run. The script is designed for seren. 
This script will
1. Get the metadata file from tethys for a given SBID
2. Find bad antennas based on the metadata file
3. Link calibration folder to the observation which is going to perform search on
4. Make mpipipeline bash script to run the whole process
5. (TODO) Use `ts` to queue the jobs

If you want to change the parameters, for example, which additional antennas you want to flag, the number of the samples you want to perform the search on, etc.
You need to modify them in `craco_cfg.py` file.

If you are happy with the parameters, use the following code to run the process

`>>> ./prepare_craco.py -o 49721 -c 49719`

This assumes that you will perform the search on SB49721 with the calibration file derived from SB49719

After running the script, the data structure of the SB49721 (your observation) will be 

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
