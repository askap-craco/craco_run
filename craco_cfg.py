# search related parameter
MAX_NCARDS          =       30
NCARDS_PER_HOST     =       3
ND                  =       40
N                   =       100 # number of samples used to perform the search
MAXBEAM             =       20 # number of beam to run simultaneously
THRESHOLD           =       6 # number of threshold to be used in the search pipeline
DEAD                =       None
FLAGANT             =       "24-30" # additional antenna to be flagged, should be a string (should be 1-based)

# some usually not changing parameter here
FCM                 =       "/home/ban115/20220714.fcm"
BLOCK               =       None # decided by ccapfits file automatically, usually it should be "2-4"
CARD                =       None # same as the above, usually it should be "1-12"
PC_FILTERBANK       =       "pc.fil"
MPIPIPESH_PATH      =       "./mpipipeline.sh" # change it accordingly