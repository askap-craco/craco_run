#!/usr/bin/env python
### wrapper to run calibration, linking everything in one script

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import os
import glob
import numpy as np
import subprocess

from craco.datadirs import SchedDir, ScanDir

from prepare_skadi import MetaManager
from metaflag import MetaAntFlagger
import craco_cfg as cfg

from auto_sched import push_sbid_calibration

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

class CalibManager:
    """
    manage calibration (and also get metadata)
    """
    def __init__(self, values):
        self.calsbid = _format_sbid(values.calsbid)
        self.scheddir = SchedDir(sbid=self.calsbid)
        self.__get_all_scans()

        self.values = values # again... as a backup

    ### find scans...
    def __get_all_scans(self, ):
        scanpattern = f"/data/craco/craco/{self.calsbid}/scans/??/??????????????"
        scans = sorted(glob.glob(scanpattern))
        self.allscans = [scan for scan in scans if self.__filter_scan(scan)]
        # self.allscans = sorted(glob.glob(scanpattern))

    def __filter_scan(self, scan):
        """
        based on the scan, check whether there are 36 uvfits file...
        """
        scan = "/".join(scan.split("/")[-2:])
        scandir = ScanDir(sbid=self.calsbid, scan=scan)
        uvfits_exists = scandir.uvfits_paths_exists
        if len(uvfits_exists) < 36: 
            log.warning(f"there are less than 36 uvfits file in {scan}")
            return False # need all beams exists

        uvfits_size = np.array([os.path.getsize(path) / (1024 ** 3) for path in uvfits_exists])
        if np.any(uvfits_size < 3): 
            log.warning(f"the file size is too small for {scan}...")
            return False # if the size is too small aborted..
        return True

    def _get_meta(self):
        metamanager = MetaManager(self.values.calsbid)
        metamanager._get_tethys_metadata()
        metamanager._get_flagger_info()
        log.info("loading flag information from metadata...")
        self.startmjds = metamanager.metaantflag.startmjds

    def _select_scan(self, random=False):
        if random:
            raise NotImplementedError("random scan not supported...")
        
        if len(self.allscans) > 10:
            print(f"Using scan - {self.allscans[10]}")
            return self.allscans[10]

        else:
            return self.allscans[0]

    def copy_solution(self):
        copycal_path = "/CRACO/SOFTWARE/craco/wan342/Software/craco_run/copycal.py"
        cpcmd = f"{copycal_path} -cal {self.values.calsbid}"

        environment = {
            "TS_SOCKET": "/data/craco/craco/tmpdir/queues/cal",
            "TS_ONFINISH": f"{cfg.CAL_TS_ONFINISH}",
            "TMPDIR": "/data/craco/craco/tmpdir",
        }
        ecopy = os.environ.copy()
        ecopy.update(environment)

        subprocess.run(
            [f"tsp {cpcmd}"], shell=True, capture_output=True,
            text=True, env=ecopy
        )

    def run(self):
        calscan = self._select_scan()
        self._get_meta()

        ### try to push it to calibration database
        try:
            push_sbid_calibration(
                sbid=self.values.calsbid, prepare=True, plot=False
            )
        except Exception as error:
            log.info(f"failed to push to database... error message - {error}")

        ### load startmjd to use for calibration
        shortscan = "/".join(calscan.split("/")[-2:])
        try: startmjd = eval(self.startmjds[shortscan])
        except: startmjd = 0

        cmd = f"""mpi_run_beam.sh {calscan} `which mpi_do_calibrate.sh` --start-mjd {startmjd}"""
        
        # if self.values.dryrun:
        if False:
            log.info(f"please run  - {cmd}")
        else:
            log.info(f"queuing up calibration - {cmd}")
            ### use subprocess instead here
            environment = {
                "TS_SOCKET": "/data/craco/craco/tmpdir/queues/cal",
                # "TS_ONFINISH": f"{cfg.CAL_TS_ONFINISH}",
                "TMPDIR": "/data/craco/craco/tmpdir",
            }
            ecopy = os.environ.copy()
            ecopy.update(environment)

            subprocess.run(
                [f"tsp {cmd}"], shell=True, capture_output=True,
                text=True, env=ecopy
            )

        self.copy_solution()   

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="calibration process...", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-cal", "--calsbid", type=str, help="calibration schedule block", )
    parser.add_argument("-dryrun", type=bool, help="whether to run it or not", default=False)

    values = parser.parse_args()       

    calmanager = CalibManager(values)
    calmanager.run()


if __name__ == "__main__":
    main()
