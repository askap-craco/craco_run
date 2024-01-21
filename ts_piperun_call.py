#!/usr/bin/env python
import sys
import re

from auto_sched import push_sbid_execution

### match sbid from the input command
def find_run_info(runcmd):
    pat = "run\.SB(\d+)\.\d{2}\.\d{6}\.(.*)\.c\d*\.sh"
    match_res = re.findall(pat, runcmd)
    assert len(match_res) == 1, f"found {len(match_res)} pattern in {runcmd}..."
    sbid, runname = match_res[0]
    return int(sbid), runname

if __name__ == "__main__":
    args = sys.argv
    runstatus = int(args[2])
    runcmd = args[-1]
    sbid, runname = find_run_info(runcmd)

    push_sbid_execution(
        sbid=sbid, runname=runname, newstatus=runstatus
    )
