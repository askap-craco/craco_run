#!/usr/bin/env python
# make preparation for craco mpirun

"""
if you want to change the configuration of the pipeline run
please change the values in `craco_cfg.py`

after doing that, in the command line, use
>>> ./prepare_craco.py -o 49721 -c 49719 -r results
to make all preparation for mpipipeline run
"""

"""
basic idea is to write a configuration file or bash script
and save basic parameter information there
of course, before writing these configurations, 
we will perform preparations for getting metadata, flagged antennas etc.

some parameter we consider to input
1) input and output related arguments
    obssbid, calsbid, flagant
2) pipeline related arguments
    max-ncards (30), ncards-per-host (3),
    nd (40, what does this mean btw?), N (100 here for testing...)
    maxbeam (20, maximum number of beam to run simutaneously...)
    threshold (6)
"""

### preparation
"""
1) Check if SBID existing; 2) get metadata file
3) flag antenna based on the metadata
4) link calibration file
5) check if all things existed
"""
import os
import re
import sys
import glob

from craft.cmdline import strrange

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from getmeta_craco import getmeta
from meta2flagant import find_bad_ant, dump_bad_ant
from linkcal_craco import linkcal

import craco_cfg as cfg

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

def _check_sbid(sbid):
    sbid = _format_sbid(sbid) # SB049120
    if not os.path.exists(f"/data/seren-01/big/craco/{sbid}"):
        raise FileNotFoundError(f"No Ccapfits Files Found for {sbid}...")

def run_prepare(obssbid, calsbid):
    # format sbid
    _obssbid = _format_sbid(obssbid)
    if calsbid is not None:
        _calsbid = _format_sbid(calsbid)
    _obssbid_s = _format_sbid(obssbid, padding=False)

    # a range of different directory and path...
    caldir = f"/data/seren-01/big/craco/{_obssbid}/cal"
    meta = f"/data/seren-01/big/craco/{_obssbid}/{_obssbid_s}.json.gz"
    flagpath = f"/data/seren-01/big/craco/{_obssbid}/flagant_meta.txt"

    _check_sbid(obssbid)
    log.info(f"starting preparation for schedule block {obssbid}...")

    getmeta(obssbid)
    badant = find_bad_ant(meta)
    dump_bad_ant(flagpath, badant)

    if calsbid is not None:
        linkcal(obssbid, calsbid)

    ### check if all files are already there...
    if not os.path.exists(meta):
        raise FileNotFoundError(f"No metadata file found for {_obssbid}...")
    if not os.path.exists(flagpath):
        raise FileNotFoundError(f"No flagged antenna file found for {_obssbid}...")
    if not os.path.exists(caldir) and calsbid is not None:
        raise FileNotFoundError(f"No calibration files found for {_obssbid}...")

    log.info("data transferring, linking done successfully... starting to make run.sh file...")

def _find_scans(obssbid):
    _obssbid = _format_sbid(obssbid)
    # take seren-01 as an example...
    scanfmt = f"/data/seren-01/big/craco/{_obssbid}/scans/*/*"
    log.info(f"searching for all scans in {_obssbid}")
    scanlst = glob.glob(scanfmt)
    log.info(f"{len(scanlst)} scans found for {_obssbid}")

    ### get a list of scan and timestamp here
    allscans = []
    for scanpath in scanlst:
        pathlst = scanpath.split("/")
        scan = pathlst[-2]
        ts = pathlst[-1]
        allscans.append([scan, ts])

    return allscans

def _extract_block_card(ccappath):
    """
    extract block and card from ccappath
    """
    ccapfits = ccappath.split("/")[-1]
    block = int(ccapfits[6:8])
    card = int(ccapfits[10:12])
    return block, card

def _intlst_to_range(intlst):
    """
    convert an integer list to a strrange to be readable...
    """
    intlst = sorted(list(intlst))

    if len(intlst) == 0: return ""

    splitlst = []
    for i in range(len(intlst)):
        if i == 0: tmp = [intlst[0]]
        elif intlst[i] - intlst[i-1] > 1:
            splitlst.append(tmp)
            tmp = [intlst[i]]
        else: tmp.append(intlst[i])
            
    splitlst.append(tmp)

    strrange_ = []
    for slst in splitlst:
        if len(slst) == 1: strrange_.append(f"{str(slst[0])}")
        else: strrange_.append(f"{str(slst[0])}-{slst[-1]}")
    return ",".join(strrange_)

def _find_block_card_strrange(obssbid, scan, ts):
    # /data/seren-??/big/craco/SB049120/scans/00/20230406114010
    _obssbid = _format_sbid(obssbid)
    ccapfmt = f"/data/seren-??/big/craco/{_obssbid}/scans/{scan}/{ts}/ccap_*.fits"
    ccaplst = glob.glob(ccapfmt)
    log.info(f"{len(ccaplst)} ccapfits files found for {_obssbid}/{scan}/{ts}")
    assert len(ccaplst) > 0, f"No ccapfits file found for {_obssbid}/{scan}/{ts}..."

    blocklst = []; cardlst = []
    for ccappath in ccaplst:
        block, card = _extract_block_card(ccappath)
        if block not in blocklst: blocklst.append(block)
        if card not in cardlst: cardlst.append(card)

    block_strrange = _intlst_to_range(blocklst)
    card_strrange = _intlst_to_range(cardlst)

    return block_strrange, card_strrange

def _split_beam_run(maxbeam, allbeam=36):
    beamrange = []; startbeam = 0
    while startbeam < allbeam:
        endbeam = min(allbeam-1, startbeam + maxbeam - 1)
        if startbeam == endbeam: beamrange.append(f"{startbeam}")
        else: beamrange.append(f"{startbeam}-{endbeam}")
        startbeam += maxbeam
    return beamrange

def _load_flag_ant(obssbid):
    _obssbid = _format_sbid(obssbid)
    flagpath = f"/data/seren-01/big/craco/{_obssbid}/flagant_meta.txt"

    with open(flagpath) as fp:
        flaglst = eval(fp.read())

    ### remove antenna > 30...
    flaglst = [i for i in flaglst if i <= 30]

    if cfg.FLAGANT is None:
        return _intlst_to_range(flaglst)
    flaglst.extend(strrange(cfg.FLAGANT))
    flaglst = list(set(flaglst))
    return _intlst_to_range(flaglst)

def _load_dead_card(resetlog="/data/big/craco/wan342/craco_run/resetlogs"):
    if cfg.DEAD is not None:
        log.warning("DEAD CARDS specified in cfg file... will use that...")
        return cfg.DEAD
    dead_cards = []
    node_dict = {0: "86", 1: "3b"}
    for icard in range(1, 11): # seren-01 to seren-10
        for inode in range(2):
            fname = f"{resetlog}/seren-{icard:0>2}_0000:{node_dict[inode]}:00.1.log"
            with open(fname) as fp:
                logstring = fp.read()
            pattern = r" JTAG ID Code\s+:\s+(.*?)\s+\n"
            jtag_status = re.findall(pattern, logstring)

            if "0x0" in jtag_status or len(jtag_status) == 0:
                dead_cards.append(f"seren-{icard:0>2}:{inode}")
    if dead_cards: return ",".join(dead_cards)
    return None
    


def _make_scan_run(obssbid, calsbid, scan, ts, runname="results"):
    _obssbid = _format_sbid(obssbid)
    _obssbid_s = _format_sbid(obssbid, padding=False)

    # a range of different directory and path...
    indir = f"/data/big/craco/{_obssbid}/scans/{scan}/{ts}/"
    caldir = f"/data/seren-01/big/craco/{_obssbid}/cal"
    meta = f"/data/seren-01/big/craco/{_obssbid}/{_obssbid_s}.json.gz"
    flagpath = f"/data/seren-01/big/craco/{_obssbid}/flagant_meta.txt"
    runpath = f"/data/seren-01/big/craco/{_obssbid}/run"
    runscriptpath = f"{runpath}/scripts"
    runoutpath = f"{runpath}/out"

    if not os.path.exists(runscriptpath):
        os.makedirs(runscriptpath)
    if not os.path.exists(runoutpath):
        os.makedirs(runoutpath)

    # work out block card range
    block_strrange, card_strrange = _find_block_card_strrange(obssbid, scan, ts)
    if cfg.BLOCK is not None: 
        log.warning("use user defined block range to overwrite...")
        block_strrange = cfg.BLOCK
    if cfg.CARD is not None:
        log.warning("use user defined card range to overwrite...")
        card_strrange = cfg.CARD

    log.info(f"block will be used - {block_strrange}...")
    log.info(f"card will be used - {card_strrange}...")

    flagant_strrange = _load_flag_ant(obssbid)

    if flagant_strrange == "":
        log.info("no antenna will be flagged...")
    else:
        log.info(f"antenna will be flagged - {flagant_strrange}...")

    _bashcmd = f"""#!/bin/bash
indir={indir}
caldir={caldir}
meta={meta}
fcm={cfg.FCM}

outdir=$indir/{runname}
"""

    # check if there is any dead card
    # dead_card = _load_dead_card()
    # if dead_card is not None:
    #     _bashcmd += f"""dead={dead_card}\n"""

    if flagant_strrange != "":
        _bashcmd += f"""flagant={flagant_strrange}\n"""

    _bashcmd += f"""block={block_strrange}
card={card_strrange}
"""

    if cfg.FLAGCHAN is not None:
        _bashcmd += f"""flagchan={cfg.FLAGCHAN}\n"""

    _bashcmd = f"""{_bashcmd}
{cfg.MPIPIPESH_PATH} --cardcap-dir $indir --outdir $outdir --phase-center-filterbank {cfg.PC_FILTERBANK} """

    if calsbid is not None:
        _bashcmd += f"""--calibration $caldir """

    _bashcmd += f"""--fcm $fcm --metadata $meta --xclbin $XCLBIN --pol-sum --block $block --card $card """
    _bashcmd += f"""--max-ncards {cfg.MAX_NCARDS} --ncards-per-host {cfg.NCARDS_PER_HOST} --nd {cfg.ND} -N {cfg.N} --threshold {cfg.THRESHOLD} --subtract {cfg.SUBTRACT} """

    if flagant_strrange != "":
        _bashcmd += f"""--flag-ants $flagant """
    # if dead_card is not None:
    #     _bashcmd += f"""--dead-cards $dead """

    if cfg.FLAGCHAN is not None:
        _bashcmd += f"""--flag-chans $flagchan """
    
    ### add uvw update
    _bashcmd += f"""--update-uv-blocks {cfg.UPDATE_UV_BLOCKS} """
    
    for irun, beam_strrange in enumerate(_split_beam_run(cfg.MAXBEAM)):
        bashcmd = _bashcmd + f"""--search-beams {beam_strrange} """
        if cfg.SAVE_UVFITS:
            bashcmd += f"""--save-uvfits-beams {beam_strrange} """
        bashcmd += f"""2>&1 | tee {runoutpath}/piperun.{scan}.{ts}.{irun}.{runname}.out\n"""

        runshfname = f"{runscriptpath}/runpipe.{scan}.{ts}.{irun}.sh"
        log.info(f"write bash file to {runshfname}")
        with open(runshfname, "w") as fp:
            fp.write(bashcmd)

        # change mode of the function
        log.info("change the run bash script to executable...")
        os.system(f"chmod +x {runshfname}")
        


def make_run(obssbid, calsbid, runname="results"):
    """
    the basic structure at the moment

    SB0xxxxx
     |- cal # symbolic for calibration directory
     |- SBxxxxx.json.gz # metadata file
     |- scans # ccapfits files for scans
     |- run
         |- scripts # directory to store bash scripts
         |- out # pipeline run output files
    """
    _obssbid = _format_sbid(obssbid)
    runpath = f"/data/seren-01/big/craco/{_obssbid}/run"
    runscriptpath = f"{runpath}/scripts"

    oldscripts = glob.glob(f"{runscriptpath}/*.sh")
    if len(oldscripts) > 0:
        log.info("previous run scripts found in the directory... remove them...")
        os.system(f"rm {runscriptpath}/*.sh")

    allscans = _find_scans(obssbid) #[["00", "2023xxxx"], ["01", "2023xxxx"]]
    for scan, ts in allscans:
        log.info(f"making bash script for {_obssbid}/{scan}/{ts}...")
        _make_scan_run(obssbid, calsbid, scan, ts, runname=runname)

def make_tsp_run(
    obssbid, calsbid, results="results",

):
    """
    make tsp file to do all run in once...
    """
    pass


def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="make preparation for mpipipeline run", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-o", "--obs", type=str, help="SBID of the observation", default=None)
    parser.add_argument("-c", "--cal", type=str, help="SBID of the calibration", default=None)
    parser.add_argument("-r", "--run", type=str, help="run name, `results` by default", default="results")

    values = parser.parse_args()

    if values.cal is not None:
        if values.cal.lower() == "none":
            run_prepare(values.obs, None)
            make_run(values.obs, None, values.run)

        else:
            run_prepare(values.obs, values.cal)
            make_run(values.obs, values.cal, values.run)
    else:
        run_prepare(values.obs, None)
        make_run(values.obs, None, values.run)

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as error:
        log.info("Error found when executing `prepare_craco.py`... please run it alone to check...")
        log.error("Error message...", exc_info=error)
        sys.exit(1)
        
    

