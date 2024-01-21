# module and functions for automatically scheduling
from aces.askapdata.schedblock import SB, SchedulingBlock

from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time
from astropy import units

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from craco.datadirs import DataDirs, SchedDir, ScanDir, RunDir, CalDir
from craco import plotbp

from configparser import ConfigParser
import glob
import re
import os

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_config(config="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(config)

    if not parser.has_section(section):
        raise ValueError(f"Section {section} not found in {filename}")
    params = parser.items(section)
    return {k:v for k, v in params}

### load sql
import psycopg2
def get_psql_connet():
    config = load_config()
    return psycopg2.connect(**config)

from sqlalchemy import create_engine 
def get_psql_engine():
    c = load_config()
    engine_str = "postgresql+psycopg2://"
    engine_str += f"""{c["user"]}:{c["password"]}@{c["host"]}:{c["port"]}/{c["database"]}"""
    return create_engine(engine_str)

class InvalidSBIDError(Exception):
    def __init__(self, sbid):
        super().__init__(f"{sbid} is not a valid sbid")

class CracoSchedBlock:
    
    def __init__(self, sbid):
        self.sbid = sbid
        try: self.askap_schedblock = SchedulingBlock(self.sbid)
        except: self.askap_schedblock = None

        ### craco data structure related
        self.scheddir = SchedDir(sbid=sbid)
        
        ### get obsparams and obsvar
        if self.askap_schedblock is not None:
            self.obsparams = self.askap_schedblock.get_parameters()
            self.obsvar = self.askap_schedblock.get_variables()
        
        try: self.get_avail_ant()
        except:
            self.antennas = None
            self.flagants = ["None"]

    # get information from craco data
    @property
    def craco_scans(self):
        return self.scheddir.scans
    
    @property
    def craco_exists(self):
        if len(self.craco_scans) == 0: return False
        return True
    
    @property
    def craco_sched_uvfits_size(self):
        """get uvfits size in total"""
        size = 0 # in the unit of GB
        for scan in self.craco_scans:
            try:
                scandir = ScanDir(sbid=self.sbid, scan=scan)
            except NotImplementedError:
                continue # no rank file found... aborted
            uvfits_exists = scandir.uvfits_paths_exists
            if len(uvfits_exists) == 0: continue
            size += os.path.getsize(uvfits_exists[0]) / 1024 / 1024 / 1024
        return size

    # get various information from aces
    def get_avail_ant(self):
        """get antennas that are available"""
        antennas = self.obsvar["schedblock.antennas"]
        self.antennas = re.findall("(ant\d+)", self.obsvar["schedblock.antennas"])
        ### calculate the antenna that are flagged
        if self.scheddir.flagant is not None:
            log.info("loading antenna flags from metadata...")
            self.flagants = [str(ant) for ant in self.scheddir.flagant]
        else:
            self.flagants = [str(i) for i in range(1, 37) if f"ant{i}" not in self.antennas]

    # get source field direction
    def _get_field_direction(self, src="src1"):
        ### first try common.target.src?.field_direction in obsparams
        if f"common.target.{src}.field_direction" in self.obsparams:
            field_direction_str = self.obsparams[f"common.target.{src}.field_direction"]
        ### then try schedblock.src16.field_direction in obsvar
        elif f"schedblock.{src}.field_direction" in self.obsvar:
            field_direction_str = self.obsvar[f"schedblock.{src}.field_direction"]
        return self.__parse_field_direction(field_direction_str)
            
    def __parse_field_direction(self, field_direction_str):
        """parse field_direction_str"""
        pattern = """\[(.*),(.*),.*\]"""
        matched = re.findall(pattern, field_direction_str)
        assert len(matched) == 1, f"find none or more matched pattern in {field_direction_str}"
        ### then further parse ra and dec value
        ra_str, dec_str = matched[0]
        ra_str = ra_str.replace("'", "").replace('"', "") # replace any possible " or '
        dec_str = dec_str.replace("'", "").replace('"', "")
        
        if (":" in ra_str) and (":" in dec_str):
            field_coord = SkyCoord(ra_str, dec_str, unit=(units.hourangle, units.degree))
        else:
            field_coord = SkyCoord(ra_str, dec_str, unit=(units.degree, units.degree))
            
        return field_coord.ra.value, field_coord.dec.value
    
    def get_scan_source(self):
        """
        retrieve scan and source pair based on the schedulingblock
        """
        refant = self.antennas[0]
        scan_src_match = {}
        sources = []
        for scan in range(100): # assume maximum scan number is 99
            scanstr = f"{scan:0>3}"
            scanantkey = f"schedblock.scan{scanstr}.target.{refant}"
            if scanantkey in self.obsvar: 
                src = self._find_scan_source(scan)
                scan_src_match[scan] = src
                if src not in sources: sources.append(src)
            else: break
        self.scan_src_match = scan_src_match
        self.sources = sources
            
    def _find_scan_source(self, scan):
        # in self.obsvar under schedblock.scan000.target.ant1
        scanstr = f"{scan:0>3}"
        allsrc = [self.obsvar[f"schedblock.scan{scanstr}.target.{ant}"].strip() for ant in self.antennas]
        unisrc = list(set(allsrc))
        assert len(unisrc) == 1, "cannot handle fly's eye mode..."
        return unisrc[0]
        
    def get_sources_coord(self, ):
        """
        get source and direction pair
        """
        self.source_coord = {src:self._get_field_direction(src) for src in self.sources}

    @property
    def corrmode(self):
        """corrlator mode"""
        return self.obsparams["common.target.src%d.corrmode"]        
        
    @property
    def template(self, ):
        return self.askap_schedblock.template
      
    @property
    def spw(self, ):
        try:
            if self.template in ["OdcWeights", "Beamform"]:
                return eval(self.obsvar["schedblock.spectral_windows"])[0]
            return eval(self.obsvar["weights.spectral_windows"])[0]
        except: return [-1, -1]
        # note - schedblock.spectral_windows is the actual hardware measurement sets spw
        # i.e., for zoom mode observation, schedblock.spectral_windows one is narrower
    
    @property
    def central_freq(self, ):
        try: return eval(self.obsparams["common.target.src%d.sky_frequency"])
        except: return -1
        
    @property
    def footprint(self, ):
        return self.askap_schedblock.get_footprint_name()
    
    @property
    def status(self,):
        return self.askap_schedblock._service.getState(self.sbid)
        # return sbstatus.value, sbstatus.name
    
    @property
    def alias(self, ):
        try: return self.askap_schedblock.alias
        except: return ""
    
    @property
    def start_time(self, ):
        try: return Time(self.obsvar["executive.start_time"]).mjd # in mjd
        except: return 0

    @property
    def weight_sched(self, ):
        try: return int(self.obsvar["weights.schedulingblock"])
        except: return -1
    
    @property
    def duration(self, ):
        if self.status.value <= 3: return -1 # before execution
        try: return eval(self.obsvar["executive.duration"])
        except: return -1

    @property
    def weight_reset(self, ):
        if self.template in ["OdcWeights", "Beamform"]: return True
        if self.template == "Bandpass":
            if "dcal" in self.alias: return True
        return False
    
    def rank_calibration(self, field_direction=None, altaz=None):
        """
        get rank for calibration
        0 - cannot be used for calibration
            1) odc or beamform scan 
            2) dcal scan 
            3) without RACS catalogue - Dec > 40 or Dec < -80?
            4) scan that is too short (here we use 120 seconds) 
            5) zoom mode
        1 - Usuable but not ideal
            1) Galactic field - |b| < 5
            2) elevation angle is less than 30d
        2 - Good for calibration
            1) Extragalactic field with RACS catalogue
        3 - Perfect for calibration
            1) bandpass but not dcal scan
        """
        if self.template in ["OdcWeights", "Beamform"]: return 0
        if self.duration <= 120: return 0
        if self.template == "Bandpass":
            if "dcal" in self.alias: return 0
            return 3
        if "zoom" in self.corrmode: return 0
        
        
        ### for other cases, you need to consider field_direction
        if field_direction is None: return -1
        
        coord = SkyCoord(*field_direction, unit=units.degree)
        if coord.dec.value > 40 or coord.dec.value < -80:
            log.info(f"footprint is outside of RACS catalogue... decl - {coord.dec.value}")
            return 0 # source outside RACS catalogue

        ## if the elevation angle is less than 30 degree, return 0
        if altaz is None: return 0
        altazcoord = coord.transform_to(altaz)
        if altazcoord.alt.value < 30: 
            log.info(f"not ideal to use this scan for calibration... elevation in the middle of the scan is {altazcoord.alt.value:.2f}")
            return 1
        ### this will make long scan worse... comment out atm we now using middle scan
        
        coord = coord.galactic
        if abs(coord.b.value) <= 5: 
            log.info(f"the scan is close to the Galactic Plane... b = {coord.b.value}")
            return 1
        return 2
    
    def get_sbid_calib_rank(self, ):
        rank = self.rank_calibration()
        if rank != -1: return rank
        
        self.get_scan_source()
        self.get_sources_coord()
        
        ranks = []
        ### get altaz
        if self.start_time > 0 and self.duration > 0:
            log.info("working out the time in the middle of the observation...")
            midmjd = self.start_time + self.duration / 86400 / 2
            askaploc = AltAz(
                obstime=Time(midmjd, format="mjd"),
                location=EarthLocation.of_site("ASKAP")
            )
        else: askaploc=None
        for src in self.source_coord:
            ranks.append(
                self.rank_calibration(self.source_coord[src], askaploc)
            )
        
        if len(ranks) == 0: return 0
        return min(ranks)
        
    
    def format_sbid_dict(self, ):
        ### format sbid dictionary
        d = dict(sbid=self.sbid)
        d["alias"] = self.alias
        d["corr_mode"] = self.corrmode
        d["craco_record"] = self.craco_exists
        ### spw
        try: spw = self.spw
        except: spw = [-1, -1] # so that you can post to the database
        d["start_freq"], d["end_freq"] = spw
        d["central_freq"] = self.central_freq
        
        d["footprint"] = self.footprint
        d["template"] = self.template
        d["start_time"] = self.start_time
        d["duration"] = self.duration
        
        d["flagant"] = ",".join(self.flagants)
        d["status"] = self.status.value # note - anything larger than 3 is usuable
        
        try: d["calib_rank"] = self.get_sbid_calib_rank()
        except Exception as error: 
            log.warning(f"cannot get the calibration rank... Error message is as followed: {error}")
            d["calib_rank"] = -1
            
        d["craco_size"] = self.craco_sched_uvfits_size
        d["weight_sched"] = self.weight_sched
        d["weight_reset"] = self.weight_reset
            
        return d

### functions to interact with database
def update_craco_sched_status(conn, craco_sched_info):
    sbid = craco_sched_info["sbid"]
    # find whether this sbid exists already
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM observation WHERE SBID = {sbid}")
    res = cur.fetchall()
    if len(res) == 0: # insert
        d = craco_sched_info
        insert_sql = f"""INSERT INTO observation (
    sbid, alias, corr_mode, start_freq, end_freq, 
    central_freq, footprint, template, start_time, 
    duration, flagant, status, calib_rank, craco_record, 
    craco_size, weight_reset, weightsched
)
VALUES (
    {d["sbid"]}, '{d["alias"]}', '{d["corr_mode"]}', {d["start_freq"]}, {d["end_freq"]},
    {d["central_freq"]}, '{d["footprint"]}', '{d["template"]}', {d["start_time"]},
    {d["duration"]}, '{d["flagant"]}', {d["status"]}, {d["calib_rank"]}, {d["craco_record"]}, 
    {d["craco_size"]}, {d["weight_reset"]}, {d["weight_sched"]}
);
"""
        cur.execute(insert_sql)
        conn.commit()
        
    else: # update
        d = craco_sched_info
        update_sql = f"""UPDATE observation 
SET alias='{d["alias"]}', corr_mode='{d["corr_mode"]}', start_freq={d["start_freq"]},
end_freq={d["end_freq"]}, central_freq={d["central_freq"]}, footprint='{d["footprint"]}', 
template='{d["template"]}', start_time={d["start_time"]}, duration={d["duration"]}, 
flagant='{d["flagant"]}', status={d["status"]}, calib_rank={d["calib_rank"]}, 
craco_record={d["craco_record"]}, craco_size={d["craco_size"]}, 
weight_reset={d["weight_reset"]}, weightsched={d["weight_sched"]}
WHERE sbid={sbid}
"""
        cur.execute(update_sql)
        conn.commit()

############# FOR CALIBRATION ##################

def push_sbid_calibration(sbid, prepare=True, plot=True):
    """
    add calibration information to the database
    """
    if int(sbid) == 0: return
    log.info(f"loading calibration for {sbid}")
    
    conn = get_psql_connet()
    cur = conn.cursor()
    
    calcls = CracoCalSol(sbid)
    solnum = calcls.solnum
    if prepare:
        status = 1 # this means running
        valid = False
        goodant, goodbeam = -1, -1
        solnum = -1
    else:
        status = 0
        try:
            valid, goodant, goodbeam = calcls.rank_calsol(plot=plot)
        except Exception as err:
            log.info(f"failed to get calibration quality... - {sbid}")
            log.info(f"error message - {err}")
            valid, goodant, goodbeam = False, 0, 0
            status = 2 # something goes run
        
    
    cur.execute(f"SELECT * FROM calibration WHERE SBID={sbid}")
    res = cur.fetchall()
    if len(res) == 0: # insert
        insert_sql = f"""INSERT INTO calibration (
    sbid, valid, solnum, goodant, goodbeam, status
)
VALUES (
    {sbid}, {valid}, {solnum}, {goodant}, {goodbeam}, {status}
)
"""
        cur.execute(insert_sql)
        conn.commit()
        
    else:
        update_sql = f"""UPDATE calibration
SET valid={valid}, solnum={solnum}, 
goodant={goodant}, goodbeam={goodbeam}, status={status}
WHERE sbid={sbid}
"""
        cur.execute(update_sql)
        conn.commit()
        
    conn.close()


class CracoCalSol:
    def __init__(
        self, sbid, flagfile="/home/craftop/share/fixed_freq_flags.txt"
    ):
        self.sbid = sbid
        self.caldir = CalDir(sbid)
        self.scheddir = SchedDir(sbid)

        ### load flagfreqs
        self.flagfreqs = np.loadtxt(flagfile)

    @property
    def solnum(self):
        npyfiles = glob.glob(f"{self.caldir.cal_head_dir}/??/b??.aver.4pol.smooth.npy")
        return len(npyfiles)

    @property
    def flag_ants(self):
        """
        load from metadata file - instead of database
        """
        flagant = self.scheddir.flagant
        if flagant is not None:
            return flagant
        
        ### else use database
        log.info(f"loading flagant of {self.sbid} from database...")

        engine = get_psql_engine()
        sql_df = pd.read_sql(f"SELECT flagant FROM observation WHERE sbid={sbid}", engine)

        assert len(sql_df) == 1, "no sbid found in observation table..."
        flagant = sql_df["flagant"][0]
        if flagant == "None": 
            raise ValueError("no flag antenna information found... not suitable for calibration...")
        if flagant == "": flagant = []
        else: flagant = [int(i) for i in flagant.split(",")]

        return flagant #both of them are 1-indexed

    @property
    def good_ant(self):
        """
        0-indexed good antenna - note flagant is zero indexed
        """
        flagant = self.flag_ants
        return [i-1 for i in range(1, 31) if i not in flagant]

    def _load_flagfile_chan(self, solfreqs, flag=True):
        arr_lst = [(solfreqs / 1e6 <= freqs[1]) & (solfreqs / 1e6 >= freqs[0]) for freqs in self.flagfreqs]
        freqflag_bool = np.sum(arr_lst, axis=0).astype(bool)
        if flag: return np.where(freqflag_bool)[0]
        return np.where(~freqflag_bool)[0]

    ### calculate the score
    def rank_calsol(
        self, phase_difference_threshold=30, plot=True,
        good_frac_threshold=0.6, bad_frac_threshold=0.4,
    ):
        beams = []; beam_phase_diff = []
        for ibeam in range(36):
            try:
                beamcalsol = CalSolBeam(self.sbid, ibeam)
                unflagchans = self._load_flagfile_chan(beamcalsol.freqs, flag=False)

                phdif = beamcalsol.extract_phase_diff(self.good_ant, unflagchans)
                beam_phase_diff.append(phdif)
                beams.append(ibeam)
            except Exception as error:
                log.info(f"cannot load solution from beam {ibeam} for {self.sbid}...")
                log.info(f"error message - {error}")
                continue

        ### concatnate things
        sbid_phase_diff = np.concatenate(
            [i[None, ...] for i in beam_phase_diff], axis=0
        ) # it should be in a shape of nbeam, nant, nchan
        self.sbid_phase_diff = sbid_phase_diff

        nbeam, nant, nchan = sbid_phase_diff.shape
        beams = np.array(beams)
        if plot: ### plot phase differencec image for all beams
            log.info(f"plotting calibration solution quality control plot for SB{self.sbid}")
            fig = plt.figure(figsize=(12, 8), facecolor="white", dpi=75)
            for i in range(36):
                ax = fig.add_subplot(6, 6, i+1)
                ax.set_title(f"beam{i:0>2}")
                try: index = np.where(beams==i)[0][0]
                except: log.info(f"no solution loaded from beam{i}... will not plot it")
                ax.imshow(
                    sbid_phase_diff[index], vmin=0, vmax=90, 
                    aspect="auto", interpolation="none"
                )
            fig.tight_layout()
            fig.savefig(f"{self.caldir.cal_head_dir}/calsol_qc.png", bbox_inches="tight")
            plt.close()

        ### work out statistics
        sbid_phase_good = sbid_phase_diff < phase_difference_threshold
        sbid_good_frac = sbid_phase_good.mean(axis=-1) # take the mean of frequency

        ### decide the number of the good beam
        good_beam_count = (sbid_good_frac.mean(axis=1) > bad_frac_threshold).sum()
        good_ant_count = (sbid_good_frac.mean(axis=0) > bad_frac_threshold).sum()
        valid_calsol = np.all(sbid_good_frac > good_frac_threshold)

        return valid_calsol, good_ant_count, good_beam_count
        
class CalSolBeam:
    def __init__(self, sbid, beam,):
        self.sbid = sbid
        self.caldir = CalDir(sbid)

        ### all files
        self.binfile = self.caldir.beam_cal_binfile(beam)
        self.freqfile = self.caldir.beam_cal_freqfile(beam)
        self.smoothfile = self.caldir.beam_cal_smoothfile(beam)
        self.__load_bandpass()

    @property
    def freqs(self,):
        try:
            return np.load(self.freqfile)
        except:
            log.warning(f"cannot load frequency file - {self.freqfile}")
            return None

    def __load_bandpass(self,):
        ### load bin bandpass
        log.warning("only XX calibration solution is loaded...")
        bpcls = plotbp.Bandpass.load(self.binfile)
        self.binbp = bpcls.bandpass.copy()[0, ..., 0]
        nant, nchan = self.binbp.shape
        ### load smooth bandpass
        self.smobp = np.load(self.smoothfile)[0, ..., 0]

        ### find reference antenna based on self.binbp
        valid_data_arr = np.array([self.__count_nan(self.binbp[ia]) for ia in range(nant)])
        ira = valid_data_arr.argmin() # reference antenna, 0-indexed
        assert valid_data_arr[ira] != 1., f"no reference antenna found for {self.binfile}"
        log.info(f"use {ira} (0-indexed) as the reference antenna")

        ### workout phase
        self.binph = np.angle(self.binbp / self.binbp[ira], deg=True)
        self.smoph = np.angle(self.smobp / self.smobp[ira], deg=True)

        self.phdif = np.min([
            (self.smoph - self.binph) % 360, (self.binph - self.smoph) % 360
        ], axis=0)

    def __count_nan(self, arr, isnan=True, fraction=True):
        total = np.size(arr)
        nancount = (np.isnan(arr) | np.isinf(arr)).sum()
        if fraction:
            if isnan: return nancount / total
            return 1 - nancount / total
        if isnan: return nancount
        return total - nancount

    def extract_phase_diff(self, goodant=None, unflagchan=None):
        if goodant is None: goodant = np.arange(30)
        ### only select data known to be good
        phdif = self.phdif[goodant]
        if unflagchan is not None:  phdif = phdif[:, unflagchan]
        return phdif


########### FOR execution ###############

def push_sbid_execution(sbid, runname="results", calsbid=None, status=0):
    scheddir = SchedDir(sbid)

    ### get calibration sbid
    if calsbid is None:
        try: calsbid = scheddir.cal_sbid
        except: calsbid = -1

        if isinstance(calsbid, str):
            calsbid = int(calsbid[2:])

    scans = len(scheddir.scans)
    rawfile_count = 0
    clusfile_count = 0
    for scan in scheddir.scans:
        try:
            rundir = RunDir(scheddir.sbid, scan=scan, run=runname)
            rawfile_count += len(rundir.raw_candidate_paths())
            clusfile_count += len(rundir.clust_candidate_paths())
        except Exception as error:
            log.info(f"error in loading run directory - {scheddir.sbid}, {scan}, {runname}")
            continue
            
    ### start to update database
    conn = get_psql_connet()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM execution WHERE SBID={sbid} AND RUNNAME='{runname}'")
    
    res = cur.fetchall()
    if len(res) == 0: # insert
        insert_sql = f"""INSERT INTO execution (
    sbid, calsbid, status, scans, rawfiles, clustfiles, runname
)
VALUES (
    {sbid}, {calsbid}, {status}, {scans}, {rawfile_count}, {clusfile_count}, '{runname}'
)
"""
        cur.execute(insert_sql)
        conn.commit()
        
    else:
        update_sql = f"""UPDATE execution
SET calsbid={calsbid}, status={status}, scans={scans}, 
rawfiles={rawfile_count}, clustfiles={clusfile_count}
WHERE sbid={sbid} AND runname='{runname}'
"""
        cur.execute(update_sql)
        conn.commit()
        
    conn.close()