#!/usr/bin/env python
### make all preparation for running pipeline on skadi

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

import glob
import os
from datetime import datetime

from craft.cmdline import strrange

from metaflag import MetaAntFlagger
import craco_cfg as cfg

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
    
    def run(self):
        self.clean_cal()
        self.link_cal()

class MetaManger:
    """
    class to manage all metadata files
    """
    def __init__(self, obssbid, frac=0.8):
        self.obssbid = _format_sbid(obssbid, padding=True)
        ### get head node folder for this sbid
        self.workdir = f"/CRACO/DATA_00/craco/{self.obssbid}"
        self.metaname = f"{_format_sbid(obssbid, padding=False)}.json.gz"
        self.badfrac = frac # determine the fraction of bad antenna

    ### get meta data and save it to correct place
    def _get_tethys_metadata(self, overwrite=False):
        if not overwrite:
            if os.path.exists(f"{self.workdir}/{self.metaname}"):
                log.info("metadata exists... stop downloading...")
                return
        else:
            log.warning("overwriting existing metadata...")
        
        ### get meta data from tethys
        scpcmd = f'''scp "tethys:/data/TETHYS_1/craftop/metadata_save/{self.metaname}" {self.workdir}'''
        log.info(f"downloading metadata {self.metaname} from tethys")
        os.system(scpcmd)

    def _get_flagger_info(self, ):
        self.metaantflag = MetaAntFlagger(
            f"{self.workdir}/{self.metaname}", fraction=self.badfrac,
        )

        dumpfname = f"{self.workdir}/{self.obssbid}.antflag.json"
        self.metaantflag.run(dumpfname)

        ### note there are information useful in this self.metaantflag

    def run(self):
        self._get_tethys_metadata(overwrite=False)
        self._get_flagger_info()

class ExecuteManager:
    """
    managing pipeline running - we will still use bash file so that we can keep track
    """
    ### can we include calibration here as well?
    def __init__(self, values):
        self.callinker = CalLinker(values.obssbid, values.calsbid)
        self.metamanage = MetaManger(values.obssbid)
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
        self.startmjd = self.metamanage.metaantflag.trange[0]

    def format_scanrun_name(self, scan, ):
        trun = get_timestamp()
        scanlst = scan.split("/")
        scannum = scanlst[-2]
        scanstart = scanlst[-1][-6:]
        return f"{self.obssbid}.{scannum}.{scanstart}.{self.runname}.c{trun}"

    ### write everything to a bash file
    def write_bash_scan(self, scan):
        scanfname = self.format_scanrun_name(scan)
        shfname = f"run.{scanfname}.sh"

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

        bashf += f"""cmd={cfg.SEARCHPIPE_PATH}
ndm={cfg.NDM}
startmjd={self.startmjd}
uvupdate={cfg.UVUPDATE_BLOCK}
"""

        if cfg.AUTOFLAGGER:
            bashf += f"""
### flagging
dflag_fradius={cfg.DFLAG_FRADIUS}
dflag_cas_threshold={cfg.DFLAG_THRESH}
dflag_tblk={cfg.DFLAG_TBLK}
"""
            runcmd += f"""--dflag-fradius $dflag_fradius --dflag-cas-threshold $dflag_cas_threshold --dflag-tblk $dflag_tblk """

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

        with open(f"{self.shelldir}/{shfname}", "w") as fp:
            fp.write(bashf)

        return f"{self.shelldir}/{shfname}"

    def run(self, ):
        self.callinker.run()
        self.metamanage.run()
        self._get_obs_info()

        self.shellscripts = []
        self.fixuvfitscmd = []
        for scan in self.allscans:
            shellpath = self.write_bash_scan(scan)
            self.shellscripts.append(shellpath)
            self.fixuvfitscmd.append(f"mpi_run_beam.sh {scan} `which mpi_do_fix_uvfits.sh`")

        log.info("making bash files executable...")
        for i in self.shellscripts:
            chmodcmd = f"chmod +x {i}"
            os.system(chmodcmd)

        ### fixuvfits...
        log.info("fixing uvfits...")
        for fixuvfitscmd in self.fixuvfitscmd:
            os.system(f"tsp {fixuvfitscmd}")

        tspcmds = [f"tsp {i}" for i in self.shellscripts]
        # if not self.values.dryrun:
        if True:
            log.info("executing bash scripts...")
            for tspcmd in tspcmds:
                log.info(f"running {tspcmd}")
                os.system(tspcmd)

        else:
            log.info("it will execute the following commands")
            for tspcmd in tspcmds:
                log.info(f"- {tspcmd}")

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="write pipeline run bash scripts...", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-obs", "--obssbid", type=str, help="observation schedule block", )
    parser.add_argument("-cal", "--calsbid", type=str, help="calibration schedule block", )
    parser.add_argument("-run", "--runname", type=str, help="runname for the pipeline run", default="results")
    parser.add_argument("-add", "--addition", type=str, help="additional argument passed to search_pipeline", default=None)
    # parser.add_argument("-dryrun", type=bool, help="whether to run it or not", default=True)

    values = parser.parse_args()

    execmanger = ExecuteManager(values)
    execmanger.run()


if __name__ == "__main__":
    main()

    