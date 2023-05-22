#!/usr/bin/env python
# function to get flag antenna from the metadata
import logging
import numpy as np 
from craco.metadatafile import MetadataFile 

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def find_bad_ant(metafile):
    log.info(f"loading metadata file from {metafile}...")
    meta = MetadataFile(metafile)
    
    # all bad-behaved antennas are with u=v=w=0.
    uvwmax = np.max(abs(meta.all_uvw), axis=(0, 2))
    badbool = (uvwmax == 0.).sum(axis=1).astype(bool)
    log.info(f"found {sum(badbool)} bad antennas...")
    assert len(badbool) == 36, f"expect 36 antennas in the metadata, only {len(badbool)} found..."
    return np.arange(36)[badbool] + 1

def dump_bad_ant(outfile, badant):
    badantstr = [str(int(i)) for i in badant]
    log.info(f"save bad antennas information to {outfile}...")
    with open(outfile, "w") as fp:
        fp.write("[{}]".format(",".join(badantstr)))

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="produce a list of antenna need to be flagged based on metadata", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-meta", "--meta", type=str, help="Path to the meta data file (.json.gz)", default=None)
    parser.add_argument("-p", "--path", type=str, help="Path to save the flag antenna file", default=".")

    values = parser.parse_args()

    # main function
    badant = find_bad_ant(values.meta)
    dump_bad_ant(f"{values.path}/flagant.txt", badant)

if __name__ == "__main__":
    main()