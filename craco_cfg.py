# search related parameter
MAX_NCARDS          =       30
NCARDS_PER_HOST     =       3
ND                  =       40
N                   =       10000000000 #1000000000 # number of samples used to perform the search
MAXBEAM             =       20 # number of beam to run simultaneously
THRESHOLD           =       6 # number of threshold to be used in the search pipeline
DEAD                =       None # "seren-02:0-1" #"seren-01:0-1,seren-04:0-1,seren-07:1,seren-08:1" # "seren-01:0-1,seren-04:1,seren-07:1"
FLAGANT             =       "24-30" # additional antenna to be flagged, should be a string (should be 1-based)
SUBTRACT            =       64 # subtract every 32 samples
FLAGCHAN            =       "60-84" #"47-55" for VAST observation # channel number to be flagged
UPDATE_UV_BLOCKS    =       768 # can be 0, or anything above 256 (included)
SAVE_UVFITS         =       False # whether to save the uvfits files or not

# flagger related parameters
AUTOFLAGGER         =       True # turn on/off the auto IQRM flagger
DFLAG_FRADIUS        =       64 # suggested by Vivek, 64
DFLAG_THRESH        =       5 # threshold for flagging bad channels
DFLAG_TBLK          =       16 # caluclate statistics over how many samples

# some usually not changing parameter here
FCM                 =       "/home/ban115/20220714.fcm"
BLOCK               =       None # decided by ccapfits file automatically, usually it should be "2-4"
CARD                =       None # same as the above, usually it should be "1-12"
PC_FILTERBANK       =       "pc.fil"
# MPIPIPESH_PATH      =       "/data/big/craco/wan342/craco_run/mpifiles/mpipipeline.sh"
MPIPIPESH_PATH      =       "mpipipeline.sh" # change it accordingly
