#!/usr/bin/env python
# this script is used to validate an invalid calibration solution

import auto_sched

def validate_sbid(sbid):
    sbid = int(sbid)
    auto_sched.update_table_single_entry(
        sbid, "valid", True, "calibration",
    )

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(
        description="validate a calibration solution manually", 
        formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-sbid",type=int, help="schedule block ID to convert", required = True)

    args = parser.parse_args()

    validate_sbid(args.sbid)

if __name__ == "__main__":
    main()