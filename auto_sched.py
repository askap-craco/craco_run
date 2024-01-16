# module and functions for automatically scheduling
from aces.askapdata.schedblock import SB, SchedulingBlock

from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy import units

import re

class CracoSchedBlock:
    
    def __init__(self, sbid):
        self.sbid = sbid
        self.askap_schedblock = SchedulingBlock(self.sbid)
        
        ### get obsparams and obsvar
        if self.askap_schedblock is not None:
            self.obsparams = self.askap_schedblock.get_parameters()
            self.obsvar = self.askap_schedblock.get_variables()
        else:
            self.status = "NOTOBSERVED"
        
        try: self.get_avail_ant()
        except:
            self.antennas = None
            self.flagants = None
        
    # get various information
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
            if scanantkey in sched.obsvar: 
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
        if self.template in ["OdcWeights", "Beamform"]:
            return eval(self.obsvar["schedblock.spectral_windows"])[0]
        return eval(self.obsvar["weights.spectral_windows"])[0]
        # note - schedblock.spectral_windows is the actual hardware measurement sets spw
        # i.e., for zoom mode observation, schedblock.spectral_windows one is narrower
    
    @property
    def central_freq(self, ):
        return eval(self.obsparams["common.target.src%d.sky_frequency"])
        
    @property
    def footprint(self, ):
        return self.askap_schedblock.get_footprint_name()
    
    @property
    def status(self,):
        return self.askap_schedblock._service.getState(self.sbid)
        # return sbstatus.value, sbstatus.name
    
    @property
    def alias(self, ):
        return self.askap_schedblock.alias
    
    @property
    def start_time(self, ):
        return Time(self.obsvar["executive.start_time"]).mjd # in mjd
    
    @property
    def duration(self, ):
        return eval(self.obsvar["executive.duration"])
    
    def rank_calibration(self, field_direction=None):
        """
        get rank for calibration
        0 - cannot be used for calibration
            1) Odc or beamform scan 
            2) dcal scan 
            3) without RACS catalogue - Dec > 40 or Dec < -80?
            4) scan that is too short (here we use 120 seconds) 
            5) zoom mode
        1 - Usuable but not ideal
            Galactic field - |b| < 5
        2 - Good for calibration
            1) Extragalactic field with RACS catalogue
        3 - Perfect for calibration
            1) bandpass but not dcal scan v
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
        ### spw
        try: spw = self.spw
        except: spw = [None, None]
        d["start_freq"], d["end_freq"] = spw
        d["central_freq"] = self.central_freq
        
        d["footprint"] = self.footprint
        d["template"] = self.template
        d["start_time"] = self.start_time
        d["duration"] = self.duration
        
        d["flagant"] = ",".join(self.flagants)
        d["status"] = self.status.value # note - anything larger than 3 is usuable
        
        d["calib_rank"] = self.get_sbid_calib_rank()
            
        return d