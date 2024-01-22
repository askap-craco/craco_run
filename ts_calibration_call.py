#!/usr/bin/env python
import sys
import re

from auto_sched import push_sbid_calibration

def find_calib_info(runcmd):
    pat = "copycal\.py -cal (\d*)"
    match_res = re.findall(pat, runcmd)
    assert len(match_res) == 1, f"found {len(match_res)} pattern in {runcmd}..."
    return int(match_res[0])

### update calib_rank in observation table -2 if calibration is not suitable
# note - perhaps good to implement that in auto_sched, push_sbid_calibration part...

if __name__ == "__main__":
    args = sys.argv
    runcmd = args[-1]
    sbid = find_calib_info(runcmd)
    push_sbid_calibration(
        sbid=sbid, prepare=False, 
        plot=True, updateobs=True
    )

    ### add slack here if possible