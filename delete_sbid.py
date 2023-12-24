import os
import glob
import argparse
import logging
from craft.cmdline import strrange

logging.basicConfig(filename="/CRACO/SOFTWARE/craco/craftop/logs/delete_sbid.log",
                        level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d-%H:%M:%S',
                        )
printer = logging.StreamHandler()

logging.getLogger('').addHandler(printer)
log = logging.getLogger(__name__)

def parse_sbid(sbid):
    if sbid.startswith("SB"):
        assert len(sbid) == 8, f"Malformed SBID - {sbid}"
        return sbid
    elif sbid.startswith("5"):
        assert len(sbid) == 5, f"Malformed SBID - {sbid}"
        return "SB0" + sbid
    elif sbid.startswith("0"):
        assert len(sbid) == 6, f"Malformed SBID - {sbid}"
        return "SB" + sbid

    else:
        raise ValueError(f"Malformed SBID - {sbid}")
        

def delete_uv(path):
    log.debug(f"Deleting {path}")
    cmd = f"rm {path}"
    log.info(f"Running the command - {cmd}")
    if not args.dry:
        os.system(cmd)


def main(args):
    log.info("---------------------------------------------------------------")
    if args.dry:
        log.info("I've been invoked in the dry run mode!")
    sbid = parse_sbid(args.sbid)
    root_regex = f"/CRACO/DATA_??/craco/{sbid}"
    root_paths = glob.glob(root_regex)
    if len(root_paths) == 0:
        log.info(f"No directories exist on any of the SKADI nodes for the requested SBID - {sbid}")
    else:
        for root_path in root_paths:
            node_name = root_path.strip().split("/")[2]
            if node_name == "DATA_00":
                continue
            
            uvfits_regex = os.path.join(root_path, "scans/??/202*/b??.uvfits")
            uvfits_paths = glob.glob(uvfits_regex)

            log.info(f"Found {len(uvfits_paths)} uvfits files on node {node_name}")
            if len(uvfits_paths) == 0:
                log.debug(f"No uvfits files found on {node_name}! The regex I used was: {uvfits_regex}")

            else:
                for uvfits_path in uvfits_paths:
                    beam_no = int(uvfits_path.split("/")[-1].strip(".uvfits").strip("b"))
                    if args.keep_beams is not None:
                        if beam_no in args.keep_beams:
                            log.info(f"I am skipping Beam {beam_no:02g} because I was asked to keep these beams - {args.keep_beams}")
                            pass
                        else:
                            delete_uv(uvfits_path)

                    elif args.delete_beams is not None:
                        if beam_no in args.delete_beams:
                            delete_uv(uvfits_path)
                        else:
                            log.info(f"I am skipping Beam {beam_no:02g} because I was asked to delete only these beams - {args.delete_beams}")
                            pass
                    else:
                        delete_uv(uvfits_path)



if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument("sbid", type=str, help="SBID to delete")
    a.add_argument("-dry", action='store_true', help="Only do a dry run and print what you will delete", default=False)
    
    g = a.add_mutually_exclusive_group()
    g.add_argument("-keep_beams", type=strrange, help="Keep these beams (list)")
    g.add_argument("-delete_beams", type=strrange, help="Delete only these beams (list)")

    args = a.parse_args()

    main(args)
