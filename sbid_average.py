#!/usr/bin/env python
### wrapper to run calibration, linking everything in one script

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import os
import glob
import subprocess

from craco.datadirs import SchedDir, ScanDir

# from prepare_skadi import MetaManager
import craco_cfg as cfg

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

def get_all_scans(sbid):
    scanpattern = f"/data/craco/craco/{sbid}/scans/??/??????????????"
    scans = sorted(glob.glob(scanpattern))
    #allscans = [scan for scan in scans if filter_scan(scan)]
    return allscans

def filter_scan(sbid, scan):
    """
    based on the scan, check whether there are 36 uvfits file...
    """
    scan = "/".join(scan.split("/")[-2:])
    scandir = ScanDir(sbid=sbid, scan=scan)
    uvfits_exists = scandir.uvfits_paths_exists
    if len(uvfits_exists) < 36: 
        log.warning(f"there are less than 36 uvfits file in {scan}")
        return False # need all beams exists

    uvfits_size = np.array([os.path.getsize(path) / (1024 ** 3) for path in uvfits_exists])
    if np.any(uvfits_size < 3): 
        log.warning(f"the file size is too small for {scan}...")
        return False # if the size is too small aborted..
    return True

def run(args):
    sbid = _format_sbid(args.sbid)
    allscans = get_all_scans(sbid)
    for scan in allscans:
        log.info("Averaging uvfits files in scan - {scan}")
        cmd = f"""mpi_run_beam.sh {scan} `which mpi_run_uvfits_average.sh` --tx {args.tx}"""
        
        # if self.values.dryrun:
        if self.values.dryrun:
            log.info(f"please run  - {cmd}")
        else:
            log.info(f"Running cmd - {cmd}")
            os.system(cmd)
    
def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="Run uvfits_average", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-sbid",type=str, help="schedule block ID to average", required = True)
    parser.add_argument("-tx",type=int, help="Scrunching factor", required = True)
    parser.add_argument("-dryrun", '--dryrun', help="whether to run it or not", default=False, action='store_true')

    args = parser.parse_args()
    run(args)

if __name__ == "__main__":
    main()
