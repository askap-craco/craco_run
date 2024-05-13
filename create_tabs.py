import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import os, argparse
import glob
import subprocess
import numpy as np

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

def subprocess_execute(cmd):
            environment = {
                "TS_SOCKET": "/data/craco/craco/tmpdir/queues/tab",
                "TMPDIR": "/data/craco/craco/tmpdir",
            }
            ecopy = os.environ.copy()
            ecopy.update(environment)
            if args.dry:
                print(f'subprocess.run([f"tsp {cmd}"], shell=True, capture_output=True,text=True, env=ecopy)')
            else:
                 #pass
                 subprocess.run([f"tsp {cmd}"], shell=True, capture_output=True,text=True, env=ecopy)

def create_tab(scanpath, beam, calpath, metapath, target_str, no_norm, nt, output_prefix):
    new_dir = scanpath + "/tab"
    os.system("mkdir -p " + new_dir)
    uvpath = scanpath + f"/b{beam:02g}.uvfits"
    if not os.path.exists(uvpath):
        raise ValueError("Cant find the uvfits file - ", uvpath)
    if not os.path.exists(calpath):
        raise ValueError("Cant find the calibration file - ", calpath)
    if not os.path.exists(metapath):
        raise ValueError("Can't find the metadatafile - ", metapath)

    if no_norm:
        norm_str = ""
    else:
        norm_str = " -norm"

    scanid = scanpath.split("/")[-1]
    sbid = args.sbid
    outname_postfix = f"SBID_{sbid}_scanid_{scanid}_beamid_{beam:02g}.fil"
    cmd = f'tab_filterbank -uv {uvpath} -c {calpath} -mf {metapath} -t "{target_str}" {norm_str} -nt {nt} {scanpath}/tab/{output_prefix}_{outname_postfix}'
    subprocess_execute(cmd)


def run(args):
    sbid = _format_sbid(args.sbid)
    scheddir = SchedDir(args.sbid)
    # allscans = get_all_scans(sbid)
    allscans = scheddir.scans
    for scan in allscans:
        scandir = ScanDir(args.sbid, scan=scan)
        uvfitspath = scandir.beam_uvfits_path(args.beam)
        scanpath = "/".join(uvfitspath.split("/")[:-1])
        calpath = scheddir.beam_cal_path(args.beam)
        metapath = scheddir.metafile
        create_tab(scanpath, args.beam, calpath, metapath, args.target, args.no_norm, args.nt, args.outname)

if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument("-sbid", type=int, help="SBID", required=True)
    a.add_argument("-beam", type=int, help="Beam", required=True)
    a.add_argument("-target", type=str, help="Target coords (enclosed in quotes)", required=True)
    a.add_argument("-no_norm", action='store_true', help="Don't apply renormalisation (def:false)", default=False)
    a.add_argument("-nt", type=int, help="Block nt in samples (def:256)", default=256)
    a.add_argument("-outname", type=str, help="Prefix for the output file name (will be appended by _beamxx.fil)", required=True)
    a.add_argument("-dry", action='store_true', help="Dry run only", default=False)
    args = a.parse_args()
    run(args)