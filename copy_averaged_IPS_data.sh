#!/bin/bash

cd /CRACO

sbid=$1
srcdirs="DATA_??/craco/$sbid"
files="scans/??/2*/b??_ex_0_-1_tx_8.uvfits"
destserver="venice.atnf.csiro.au"
destdir=$2

cmd0="rsync -avL --relative --progress DATA_00/craco/$sbid $destserver:$destdir/"
echo $cmd0
$cmd0

cmd="rsync -avL --relative --progress $srcdirs/$files $destserver:$destdir/"
echo $cmd
$cmd

cd -
