#!/usr/bin/env python
import sys
import re
import os

from craco.datadirs import CalDir

from auto_sched import (
    push_sbid_calibration, SlackPostManager,
    query_table_single_column
)

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
    calib_status = query_table_single_column(sbid, "status", "calibration")
    calib_valid = query_table_single_column(sbid, "valid", "calibration")

    caldir = CalDir(sbid)
    qc_fpath = f"{caldir.cal_head_dir}/calsol_qc.png"

    slackbot = SlackPostManager(test=False)
    slackmsg = f"*[CALIB]* finish calibration for SB{sbid} - valid {calib_valid} with status {calib_status}"
    if os.path.exists(qc_fpath):
        slackbot.upload_file(files=qc_fpath, comment=slackmsg)
    else:
        slackbot.post_message(slackmsg + "*no quality contral image found*", mention_team=True)