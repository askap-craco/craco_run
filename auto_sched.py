# module and functions for automatically scheduling
from aces.askapdata.schedblock import SB, SchedulingBlock

from astropy.coordinates import SkyCoord
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
        
    # get various information
    def get_avail_ant(self):
        """get antennas that are available"""
        antennas = self.obsvar["schedblock.antennas"]
        self.antennas = re.findall("(ant\d+)", sched.obsvar["schedblock.antennas"])
        ### calculate the antenna that are flagged
        self.flagants = [i for i in range(1, 37) if f"ant{i}" not in ants]
        
    # get source field direction
    def _get_field_direction(self, src="src1"):
        ### first try common.target.src?.field_direction in obsparams
        if f"common.target.{src}.field_direction" in self.obsparams:
            field_direction_str = self.obsparams[f"common.target.{src}.field_direction"]
        ### then try schedblock.src16.field_direction in obsvar
        elif f"schedblock.{src}.field_direction" in self.obsvar:
            field_direction_str = self.obsvar[f"schedblock.{src}.field_direction"]
            
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
    