#!/bin/bash

mpirun --hostfile mpi_seren.txt --map-by ppr:2:node -x PATH -x XILINX_XRT /data/seren-01/big/craco/wan342/craco_run/examcards.sh