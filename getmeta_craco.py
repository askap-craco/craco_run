#!/usr/bin/env python
# this function is similar to original getmeta.sh script
# we will do a little bit formatting here
import os
import logging
import glob

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

def getmeta(sbid):
    """
    There are too many sbid format...
    SB049120, SB49120, 49120
    """
    if sbid[:2] == "SB": sbid = int(sbid[2:])
    _sbid_p = _format_sbid(sbid, padding=True)
    _sbid = _format_sbid(sbid, padding=False)
    if not os.path.exists(f"/data/seren-01/big/craco/{_sbid_p}"):
        raise FileNotFoundError(f"no directory found on seren-01 for {_sbid}")

    scpcmd = f'''scp "tethys:/data/TETHYS_1/craftop/metadata_save/{_sbid}.json.gz" /data/seren-01/big/craco/{_sbid_p}/'''
    log.info(f"executing command - {scpcmd}")
    os.system(scpcmd)

    # check if metadata is there
    if not os.path.isfile(f"/data/seren-01/big/craco/{_sbid_p}/{_sbid}.json.gz"):
        log.info(f"copy metadata file failed... no json.gz file found on seren-01...")
        raise ValueError("metadata file copying failed... please check...")

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="copy metadata file from tethys to seren-01, similar function as getmeta.sh", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-s", "--sbid", type=str, help="SBID to work on", default=None)

    values = parser.parse_args()
    getmeta(values.sbid)

if __name__ == "__main__":
    main()



