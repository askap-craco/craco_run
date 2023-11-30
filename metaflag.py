#!/usr/bin/env python
### functions to get 1) bad antennas; 2) get starting and end time

import numpy as np
import logging
import json

from craco.metadatafile import MetadataFile 

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def find_true_range(bool_array):
    """
    get all start and end indices for consecutive True values
    """
    # Identify the indices where the array changes from False to True or True to False
    change_indices = np.where(np.diff(np.concatenate(([False], bool_array, [False]))))[0]

    # Pair the start and end indices
    ranges = [(start, end) for start, end in zip(change_indices[::2], change_indices[1::2])]

    return ranges

class MetaAntFlagger:
    
    def __init__(self, metafile, fraction=0.8):
        log.info(f"loading metadata file from {metafile}")
        self.meta = MetadataFile(metafile)
        self.antflags = self.meta.antflags
        
        ### get basic information
        self._get_info()
        self.badants = self._find_bad_ant(fraction=fraction)
        log.info(f"finding {len(self.badants)} bad antennas...")
        self._find_good_ranges()
        
        
    def _get_info(self):
        nt, na = self.antflags.shape
        self.nt = nt
        self.na = na
    
    def _find_bad_ant(self, fraction=0.8):
        """
        the antenna will be flagged if 80% of the time, it is bad
        """
        flagantsum = self.antflags.sum(axis=0)
        return np.where(flagantsum >= self.nt * fraction)[0]
    
    def _find_good_ranges(self):
        flagtimesum = self.antflags.sum(axis=1) - len(self.badants)
        good_bool = flagtimesum == 0
        self.good_ranges = find_true_range(good_bool)

        
    ### get range of time
    def get_start_end_time(self):
        if len(self.good_ranges) == 0:
            log.warning("no good range found for this schedule block...")
            return None, None
        ### get the best ranges
        ranges_time = np.array([t[1] - t[0] for t in self.good_ranges])
        best_start, best_end = self.good_ranges[np.argmax(ranges_time)]
        log.info(f"the best range found... {best_start} ~ {best_end}, it lasted for {best_end - best_start} hardware samples...")
        
        return [self.meta.times[best_start].value, self.meta.times[best_end-1].value]

    def get_flag_ant(self):
        return list(self.badants + 1)


    def run(self, dumpfname):
        trange = self.get_start_end_time()
        badants = self.get_flag_ant()

        metainfo = dict(
            trange = trange.__str__(),
            flagants = badants.__str__()
        )
        log.info(f"dumping metadata information to {dumpfname}")
        with open(dumpfname, "w") as fp:
            json.dump(metainfo, fp, indent=4)

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="dump information needs to be used in the pipeline", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-meta", "--meta", type=str, help="Path to the meta data file (.json.gz)", default=None)
    parser.add_argument("-dump", "--dump", type=str, help="Path to save the information", default="./metainfo.json")
    parser.add_argument("-frac", "--frac", type=float, help="Fractional of bad interval to be considered as a bad antenna", default=0.8)

    values = parser.parse_args()

    metaflag = MetaAntFlagger(
        metafile = values.meta,
        fraction = values.frac,
    )
    metaflag.run(values.dump)


if __name__ == "__main__":
    main()
