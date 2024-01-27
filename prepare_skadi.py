#!/usr/bin/env python
### make all preparation for running pipeline on skadi

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import glob
import os
import re
from datetime import datetime

from craft.cmdline import strrange
from craco.datadirs import SchedDir, ScanDir

from metaflag import MetaAntFlagger, MetaManager
import craco_cfg as cfg
import subprocess

from auto_sched import push_sbid_execution, update_table_single_entry

def _format_sbid(sbid, padding=True):
    "perform formatting for the sbid"
    if isinstance(sbid, int): sbid = str(sbid)
    if sbid.isdigit(): # if sbid are digit
        if padding: return "SB{:0>6}".format(sbid)
        return f"SB{sbid}"
    return sbid

def get_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")

def intlst_to_strrange(intlst):
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

def make_dir(path):
    if not os.path.exists(path):
        log.info(f"making new directories... {path}")
        os.makedirs(path)

def get_nchan_from_uvfits_header(fname, hdr_size = 16384):
    with open(fname, 'rb') as o:
        raw_h = o.read(hdr_size)
    pattern = "NAXIS4\s*=\s*(\d+.\d+)\s*"
    matches = re.findall(pattern, str(raw_h))
    if len(matches) !=1:
        raise RuntimeError(f"Something went wrong when looking for nchan in the header, I used this regex - {pattern}")

    nchan = int(float(matches[0]))
    if nchan < 300: return nchan
    raise RuntimeError(f"I found a really unexpected value of nchan from uvfits header - {nchan}")


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
            log.info(f"executing {lncmd}...")
            os.system(lncmd)
    
    def run(self):
        self.clean_cal()
        self.link_cal()


class ExecuteManager:
    """
    managing pipeline running - we will still use bash file so that we can keep track
    """
    ### can we include calibration here as well?
    def __init__(self, values):
        self.callinker = CalLinker(values.obssbid, values.calsbid)
        self.metamanage = MetaManager(values.obssbid)
        self.obssbid = _format_sbid(values.obssbid, padding=True)
        self.runname = values.runname

        ### bashscript directory
        self.shelldir = f"/CRACO/DATA_00/craco/{self.obssbid}/runscript"
        make_dir(self.shelldir)
        
        ### save all values as a backup
        self.values = values

    ### some properties
    @property
    def metapath(self):
        return f"/CRACO/DATA_00/craco/{self.obssbid}/SB{self.values.obssbid}.json.gz"

    @property
    def calpath(self):
        return f"/data/craco/craco/{self.obssbid}/cal"

    ### get all basic information...
    def _get_obs_info(self,):
        # get $indir $flagant $metadata
        self.__get_all_scans() #these are all $indir s
        self.__get_flag_ant()
        self.__get_start_mjd()

    def __get_all_scans(self, ):
        scanpattern = f"/data/craco/craco/{self.obssbid}/scans/??/??????????????"
        self.allscans = glob.glob(scanpattern)

    def __get_flag_ant(self, ):
        # get flagant from metadata
        metaflag = self.metamanage.metaantflag.badants # list
        flagant = [ia for ia in metaflag if ia <= 30]

        cfgflag = strrange(cfg.FLAGANT) #list as well
        flagant.extend(cfgflag)

        self.flagant = intlst_to_strrange(set(flagant))

    def __get_start_mjd(self, ):
        # self.startmjd = self.metamanage.metaantflag.trange[0]
        self.startmjds = self.metamanage.metaantflag.startmjds # it is a dictionary

    def __get_scan_nchan(self, scan):
        scandir = ScanDir(sbid=self.obssbid, scan=scan)
        uvfitspath = scandir.uvfits_paths[0]
        try:
            nchan = get_nchan_from_uvfits_header(uvfitspath)
            return nchan
        except:
            return 288

    def format_scanrun_name(self, scan, ):
        trun = get_timestamp()
        scanlst = scan.split("/")
        scannum = scanlst[-2]
        scanstart = scanlst[-1][-6:]
        return f"{self.obssbid}.{scannum}.{scanstart}.{self.runname}.c{trun}"

    ### write everything to a bash file
    def write_bash_scan(self, scan, dryrun=False):
        scanfname = self.format_scanrun_name(scan)
        shfname = f"run.{scanfname}.sh"

        ### get a scan to retreive information from self.startmjds
        shortscan = "/".join(scan.split("/")[-2:])
        try: startmjd = eval(self.startmjds[shortscan])
        except: startmjd = 0

        if startmjd is None: startmjd = 0

        bashf = f"""#!/bin/bash

indir={scan}
caldir={self.calpath}
meta={self.metapath}
phase_center_filterbank={cfg.PC_FILTERBANK}
runname={self.runname}

outdir=$indir/$runname
"""
        runcmd = f"""mpi_run_beam.sh $indir $cmd -psf --update-uv-blocks $uvupdate --calibration $caldir --outdir $outdir --metadata $meta --xclbin $XCLBIN --ndm $ndm --phase-center-filterbank $phase_center_filterbank --start-mjd $startmjd """

        if self.flagant:
            bashf += f"""flagant={self.flagant}\n"""
            runcmd += f"""--flag-ants $flagant """

        ### check ndm
        if isinstance(cfg.NDM, int) or isinstance(cfg.NDM, float):
            ndm = cfg.NDM
        else:
            nchan = self.__get_scan_nchan(shortscan)
            ndm = nchan - 30 # hard coded here for now!

        bashf += f"""cmd={cfg.SEARCHPIPE_PATH}
ndm={ndm}
startmjd={startmjd}
uvupdate={cfg.UVUPDATE_BLOCK}
"""

        if cfg.AUTOFLAGGER:
            bashf += f"""
### flagging
dflag_fradius={cfg.DFLAG_FRADIUS}
dflag_cas_threshold={cfg.DFLAG_THRESH}
dflag_tblk={cfg.DFLAG_TBLK}
freq_flag_file={cfg.FREQ_FLAG_FILE}
"""
            runcmd += f"""--dflag-fradius $dflag_fradius --dflag-cas-threshold $dflag_cas_threshold --dflag-tblk $dflag_tblk --flag-frequency-file $freq_flag_file """

            if self.values.injection:
                assert os.path.exists(self.values.injection), f"Injection file - {self.values.injection} does not exist"
                runcmd += f" --injection-file {self.values.injection} "

            if self.values.addition:
                runcmd += f"{self.values.addition} " # add additional parameter to it.. for example flagging

        bashf += f"""
cmd={cfg.SEARCHPIPE_PATH}

mkdir -p $outdir 2>/dev/null

trun=`date +%m%d%H%M%S`
logpath=$outdir/{scanfname}.$trun.log
"""

        bashf = f"""{bashf}

{runcmd} 2>&1 | tee $logpath  
"""
        if not dryrun: # dryrun won't write anything to the disk
            with open(f"{self.shelldir}/{shfname}", "w") as fp:
                fp.write(bashf)

        return f"{self.shelldir}/{shfname}"

    def run(self, ):
        self.callinker.run()
        self.metamanage.run()
        self._get_obs_info()

        self.shellscripts = []

        nqueues = self.values.nqueues
        environments = []
        commands = []
        nscans = len(self.allscans)
        for iscan, scan in enumerate(self.allscans):
            if nscans > 1: 
                iqueue = iscan % nqueues
            else:
                iqueue = int(self.values.obssbid) % nqueues
            shellpath = self.write_bash_scan(scan, dryrun=self.values.dryrun) # note scan is /data/craco/craco/SB0xxxxx/...
            
            ### todo - decide which queue to use based on the current queue value
            environments.append({
                'TS_SOCKET':f'{cfg.PIPE_RUN_TS_SOCKET}/{iqueue}',
                'TS_ONFINISH': f"{cfg.PIPE_TS_ONFINISH}",
                'START_CARD':str(iqueue*2),
                'RUNNAME':self.runname
            })
            
            searchcmd = f'./do_search_and_summarise.sh {scan} {shellpath} {self.runname}'
            if self.values.dryrun:
                commands.append(f"echo `{searchcmd}`; sleep 10")
            else:
                commands.append(searchcmd)
            
            self.shellscripts.append(shellpath)

        log.info("making bash files executable...")
        for i in self.shellscripts:
            chmodcmd = f"chmod +x {i}"
            os.system(chmodcmd)

        tspcmds = [f"tsp {cmd}" for cmd in commands]

        log.info("executing bash scripts...")
        for tspcmd, environment in zip(tspcmds, environments):
            log.info(f"running {tspcmd} with evironment {environment}")
            if not self.values.dryrun:
                ecopy = os.environ.copy()
                ecopy.update(environment)
                p = subprocess.run([tspcmd], shell=True, capture_output=True, text=True, env=ecopy)
                tsp_jobid = int(p.stdout.strip())

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="write pipeline run bash scripts...", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-obs", "--obssbid", type=int, help="observation schedule block", )
    parser.add_argument("-cal", "--calsbid", type=int, help="calibration schedule block", )
    parser.add_argument("-run", "--runname", type=str, help="runname for the pipeline run", default="results")
    parser.add_argument("-inj", "--injection", type=str, help="injection file to be used", default=None)
    parser.add_argument("-add", "--addition", type=str, help="additional argument passed to search_pipeline", default=None)
    parser.add_argument("-dryrun", '--dryrun', help="whether to run it or not", default=False, action='store_true')
    parser.add_argument('--nqueues', help='Number of queues to send jobs to', default=1, type=int)

    values = parser.parse_args()

    log.info("updating execution database...")
    try: 
        push_sbid_execution(
            sbid=values.obssbid, calsbid=values.calsbid, 
            runname=values.runname, reset=True,
        )
    except Exception as error:
        log.info(f"failed to push new update to database... with the following error message - {error}")
    
    log.info("updating observation database...")
    try:
        update_table_single_entry(
            sbid=values.obssbid, column="tsp", value=True,
            table="observation"
        )
    except Exception as error:
        log.warning(f"failed to update tsp column to True - error: {error}")

    execmanager = ExecuteManager(values)
    execmanager.run()


if __name__ == "__main__":
    main()

    
