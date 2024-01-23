# configuration file for running on skadi
NDM                 =       "auto"                 # number of DM trials
THRESHOLD           =       6                   # threshold for search pipeline
FLAGANT             =       "25-30"             # antenna to be flagged 
UPDATE_UV_BLOCKS    =       12                  # update uv coordinates every Nxnt (256 by default)


# Fixed flagging parameters
FREQ_FLAG_FILE      =       "/home/craftop/share/fixed_freq_flags.txt"

# flagger related parameters
AUTOFLAGGER         =       True                # turn on/off the auto IQRM flagger
DFLAG_FRADIUS       =       180                  # suggested by Vivek, 64
DFLAG_THRESH        =       5                   # threshold for flagging bad channels
DFLAG_TBLK          =       256                  # caluclate statistics over how many samples
UVUPDATE_BLOCK      =       12                  # update uvw every 12 block

# parameter that does not change a lot
PC_FILTERBANK       =       "pc.fil"
SEARCHPIPE_PATH     =       "`which mpi_do_search_pipeline.sh`"


# task spooler related
# PIPE_TS_ONFINISH    =       "/CRACO/SOFTWARE/craco/craftop/softwares/craco_run/ts_piperun_call.py"
# CAL_TS_ONFINISH     =       "/CRACO/SOFTWARE/craco/craftop/softwares/craco_run/ts_calibration_call.py"
# PIPE_RUN_TS_SOCKET  =       "/data/craco/craco/tmpdir/queues"

####### the following for testing locally only
PIPE_TS_ONFINISH    =       "/Users/zwang/Documents/Curtin/craco_run/ts_piperun_call.py"
CAL_TS_ONFINISH     =       "/Users/zwang/Documents/Curtin/craco_run/ts_calibration_call.py"
PIPE_RUN_TS_SOCKET  =       "/Users/zwang/Documents/Curtin/tmpdir/queues"
