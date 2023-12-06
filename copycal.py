#!/usr/bin/env python

import os
import glob

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

class CalSolCopier:
    def __init__(self, calsbid):
        self.calsbid = _format_sbid(calsbid)
        self.make_cal_dir()

    def make_cal_dir(self):
        if os.path.exists(self.caldir):
            log.info("existing calibration directory found... delete it...")
            os.system(f"rm -r {self.caldir}")
        os.makedirs(self.caldir)

    @property
    def caldir(self):
        return f"/CRACO/DATA_00/craco/calibration/{self.calsbid}"

    def run(self):
        """
        copy all available calibration to head node...
        """
        for node in range(1, 19):
            caldir_node = f"/CRACO/DATA_{node:0>2}/craco/calibration/{self.calsbid}"
            cpcmd = f"cp -r {caldir_node}/* {self.caldir}"
            log.info(f"copying calibration solution from node{node}...")
            os.system(cpcmd)

        ### check the number of solution
        nsol = len(glob.glob(f"{self.caldir}/??"))
        if nsol != 36:
            log.warning(f"solution not completed... {nsol} beams found...")
        else:
            log.info("all calibration solution copied successfully...")

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="copy given sbid solution to head node...", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-cal", "--calsbid", type=str, help="calibration schedule block", )
    values = parser.parse_args()

    copier = CalSolCopier(values.calsbid)
    copier.run()

if __name__ == "__main__":
    main()
        