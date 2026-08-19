#!/bin/bash

cd /CRACO

sbid=$1
srcdirs="DATA_??/craco/$sbid"
destdir=$2

cmd="rsync -avL --relative --progress --bwlimit=100000K $srcdirs seren-01.atnf.csiro.au:$destdir/"
echo $cmd
$cmd

cd -
