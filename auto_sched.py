# module and functions for automatically scheduling
from aces.askapdata.schedblock import SB, SchedulingBlock

from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy import units

from craco.datadirs import DataDirs, SchedDir, ScanDir, RunDir

import re
import os

class InvalidSBIDError(Exception):
    def __init__(self, sbid):
        super().__init__(f"{sbid} is not a valid sbid")

class CracoSchedBlock:
    
    def __init__(self, sbid):
        self.sbid = sbid
        try: self.askap_schedblock = SchedulingBlock(self.sbid)
        except: self.askap_schedblock = None
        
        ### get obsparams and obsvar
        if self.askap_schedblock is not None:
            self.obsparams = self.askap_schedblock.get_parameters()
            self.obsvar = self.askap_schedblock.get_variables()
        
        try: self.get_avail_ant()
        except:
            self.antennas = None
            self.flagants = ["None"]

        ### craco data structure related
        self.scheddir = SchedDir(sbid=sbid)

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
    
    def rank_calibration(self, field_direction=None):
        """
        get rank for calibration
        0 - cannot be used for calibration
            1) odc or beamform scan 
            2) dcal scan 
            3) without RACS catalogue - Dec > 40 or Dec < -80?
            4) scan that is too short (here we use 120 seconds) 
            5) zoom mode
        1 - Usuable but not ideal
            Galactic field - |b| < 5
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
            return 0 # source outside RACS catalogue
        
        coord = coord.galactic
        if abs(coord.b.value) <= 5: return 1
        return 2
    
    def get_sbid_calib_rank(self, ):
        rank = self.rank_calibration()
        if rank != -1: return rank
        
        self.get_scan_source()
        self.get_sources_coord()
        
        ranks = []
        for src in self.source_coord:
            ranks.append(self.rank_calibration(self.source_coord[src]))
        
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
        except: d["calib_rank"] = -1
            
        d["craco_size"] = self.craco_sched_uvfits_size
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
    craco_size, weight_reset
)
VALUES (
    {d["sbid"]}, '{d["alias"]}', '{d["corr_mode"]}', {d["start_freq"]}, {d["end_freq"]},
    {d["central_freq"]}, '{d["footprint"]}', '{d["template"]}', {d["start_time"]},
    {d["duration"]}, '{d["flagant"]}', {d["status"]}, {d["calib_rank"]}, {d["craco_record"]}, 
    {d["craco_size"]}, {d["weight_reset"]}
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
craco_record={d["craco_record"]}, craco_size={d["craco_size"]}, weight_reset={d["weight_reset"]}
WHERE sbid={sbid}
"""
        cur.execute(update_sql)
        conn.commit()

def get_solnum(sbid):
    calsol_path = f"/CRACO/DATA_00/craco/calibration/SB0{sbid}"
    npyfiles = glob.glob(f"{calsol_path}/??/b??.aver.4pol.smooth.npy")
    return len(npyfiles)
    

def push_sbid_calibration(sbid):
    """
    sbid	valid 	solnum
    
    TODO - a function to check whether the calibration is good or not
    """
    if int(sbid) == 0: return
    
    ### get results
    engine = get_psql_engine()
    df = pd.read_sql(f"SELECT * FROM observation WHERE sbid={sbid}", engine)
#     engine.close()
    if len(df) == 0: return
    
    obsrow = df.iloc[0]
    
    conn = get_psql_connet()
    cur = conn.cursor()
    solnum = get_solnum(sbid)
    
    cur.execute(f"SELECT * FROM calibration WHERE SBID={sbid}")
    res = cur.fetchall()
    if len(res) == 0: # insert
        insert_sql = f"""INSERT INTO calibration (
    sbid, valid, solnum
)
VALUES (
    {sbid}, False, {solnum}
)
"""
        cur.execute(insert_sql)
        conn.commit()
        
    else:
        update_sql = f"""UPDATE calibration
SET valid=False, solnum={solnum}
WHERE sbid={sbid}
"""
        cur.execute(update_sql)
        conn.commit()
        
    conn.close()


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
        except:
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