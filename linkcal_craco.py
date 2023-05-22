#!/usr/bin/env python
# give two sbid and make a symbolic of the calibration
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

def find_cal(sbid, scan=None):
    """
    find the path of the calibration solution of a given sbid
    """
    sbid = _format_sbid(sbid) # reformat it to be SB049120 etc.
    if scan is None: scan = "00"
    calpath = f"/data/seren-01/big/craco/calibration/{sbid}/scans/{scan}"
    callst = sorted(glob.glob(f"{calpath}/*"))
    assert len(callst) > 0, f"no calibration solution found for {sbid}..."
    log.info(f"{len(callst)} calibration solution found... will use the first one...")
    return callst[0]

def linkcal(obssbid, calsbid, calscan=None):
    """
    make symbolic link to sbid for calibration
    """
    calpath = find_cal(calsbid, scan=calscan)

    obssbid = _format_sbid(obssbid)
    obspath = f"/data/seren-01/big/craco/{obssbid}"
    if not os.path.exists(obspath):
        log.info(f"no observation file for {obssbid}... aborted")
        raise FileNotFoundError(f"No observation folder found for {obssbid}")
    
    if os.path.exists(f"{obspath}/cal"):
        log.info(f"A symbolic found under {obssbid}... will remove it and create a new one...")
        os.system(f"rm {obspath}/cal")

    lncmd = f"ln -s {calpath} {obspath}/cal"
    log.info(f"executing {lncmd}")
    os.system(lncmd)

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="link calibration solution from a given sbid for another sbid", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-o", "--obs", type=str, help="SBID of the observation", default=None)
    parser.add_argument("-c", "--cal", type=str, help="SBID of the calibration", default=None)

    values = parser.parse_args()
    linkcal(values.obs, values.cal, calscan=None)

if __name__ == "__main__":
    main()
