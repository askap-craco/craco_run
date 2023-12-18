#!/usr/bin/env python
### wrapper to run calibration, linking everything in one script

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import os
import glob

from prepare_skadi import MetaManger

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
        self.__get_all_scans()

        self.values = values # again... as a backup

    ### find scans...
    def __get_all_scans(self, ):
        scanpattern = f"/data/craco/craco/{self.calsbid}/scans/??/??????????????"
        self.allscans = sorted(glob.glob(scanpattern))

    def _get_meta(self):
        metamanager = MetaManger(self.values.calsbid)
        metamanager._get_tethys_metadata()

    def _select_scan(self, random=False):
        if random:
            raise NotImplementedError("random scan not supported...")
        return self.allscans[0]

    def copy_solution(self):
        copycal_path = "/CRACO/SOFTWARE/craco/wan342/Software/craco_run/copycal.py"
        cpcmd = f"{copycal_path} -cal {self.values.calsbid}"
        os.system(f"tsp {cpcmd}")

    def run(self):
        calscan = self._select_scan()
        self._get_meta()
        cmd = f"""mpi_run_beam.sh {calscan} `which mpi_do_calibrate.sh`"""
        # if self.values.dryrun:
        if False:
            log.info(f"please run  - {cmd}")
        else:
            log.info(f"queuing up calibration - {cmd}")
            os.system(f"tsp {cmd}")     

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
