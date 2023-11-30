#!/usr/bin/env python
### make all preparation for running pipeline on skadi

import glob
import os

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

####### this part is used to make symbolic link to the calibration #######
'''
On each skadi node, you need to find the calibration solution under /CRACO/DATA_??/craco/calibration/SB0<calsbid>/<beam>/...
                    ^^^ we may put it all under skadi_00 later...
We want to put symbolic link under /CRACO/DATA_??/craco/SB0<obssbid>/cal
'''

class CalLinker:
    """
    class CalLinker is used to make symbolic link for calibration files

    constructor
    +++++++++++++++++++
    Params:
        obssbid: int
        calsbid: int
    """
    def __init__(self, obssbid, calsbid):
        self.obssbid = _format_sbid(obssbid, padding=True)
        self.calsbid = _format_sbid(calsbid, padding=True)

    def _get_runcal_dir(self, node):
        return f"/CRACO/DATA_{node:0>2}/craco/{self.obssbid}/cal"

    def _get_cal_dir(self):
        return f"/CRACO/DATA_00/craco/calibration/{self.calsbid}"

    def _clean_cal_node(self, node):
        """
        clean up calibration directory if there is something exists
        """
        runcal_dir = self._get_runcal_dir(node)
        if os.path.exists(runcal_dir):
            log.info(f"find existing calibration link in {runcal_dir}... removing...")
            rmcmd = f"rm -r {runcal_dir}"
            os.system(rmcmd)

    def clean_cal(self):
        log.info(f"checking existing calibration link for {self.obssbid}...")
        for node in range(0, 19):
            self._clean_cal_node(node)

    def link_cal(self):
        cal_dir = self._get_cal_dir()
        log.info(f"check if calibration solution existing under {cal_dir}...")
        if not os.path.exists(cal_dir):
            log.critical(f"no calibration found for {self.calsbid}... aborted")
            raise ValueError(f"no calibration solution found for {self.calsbid}...")

        for node in range(0, 19):
            # make the link in the headnode to keep track everything
            log.debug(f"linking calibration solution for node{node}...")
            lncmd = f"ln -s {cal_dir} {self._get_runcal_dir(node)}"
            log.debug(f"executing {lncmd}...")
            os.system(lncmd)

if __name__ == "__main__":
    obssbid = 55063
    calsbid = 55064

    callink = CalLinker(obssbid, calsbid)
    callink.clean_cal()
    callink.link_cal()

    