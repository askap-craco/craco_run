#!/usr/bin/env python
"""
Keeps an SBID

Copyright (C) CSIRO 2022
"""
import pylab
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import os
import sys
import logging
from craco.datadirs import DataDirs, SchedDir
import subprocess

log = logging.getLogger(__name__)

__author__ = "Keith Bannister <keith.bannister@csiro.au>"

def _main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Script description', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', action='store_true', help='Be verbose')
    parser.add_argument('--only_scan', type=str, help='Only keep this scan (tstart)', default=None)
    parser.add_argument(dest='files', nargs='*', type=int)
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    if values.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    dirs = DataDirs()

    name = input(f'Please type your name: ')
    if len(name) == '':
        print('no name. Quitting')
        return 0

    for sbid in values.files:
        keep_sbid(sbid, name, values)

        
def keep_sbid(sbid, name, values):
    sched = SchedDir(sbid)
    nscans = len(sched.scans)
    sizes = sched.get_size()
    total_size = sum(sizes.values())

    print(f'SBID {sbid} contains {nscans} scan(s) and total size {total_size}')

    msg = input('Please type the reason why you want to keep  this SBID: ')
    if len(msg) == '':
        print('No message - quitting without keeping this scan')
        return 0

    keepfile = os.path.join(sched.sched_head_dir, 'KEEP')
    with open(keepfile, 'w') as f:
        f.write('talkto:' +  name + '\n' + msg + '\n')
        
    #hostfile=os.path.join(sched.sched_head_dir
    hostfile = os.environ['HOSTFILE']
    if values.only_scan:
        list_of_tstarts = [i.split("/")[-1] for i in sched.scans]
        assert values.only_scan in list_of_tstarts, f"The requested scan {values.only_scan} not found in SBID. The list of scans - {sched.scans}"
        print(f'Only making scan - {values.only_scan} readonly')
        local_scan_file = f'/data/craco/craco/SB{sbid:06d}/scans/??/{values.only_scan}'

    else:   
        local_scan_file = f'/data/craco/craco/SB{sbid:06d}'

    cmd = f'mpirun -hostfile {hostfile} -map-by ppr:1:node find {local_scan_file} -type f -exec chmod a-w {{}} \; '
    print('Making uvfits files read only with ', cmd)
    retcode = subprocess.call(cmd, shell=True)
    print('Setting uvfits as read only was successful')


if __name__ == '__main__':
    _main()
