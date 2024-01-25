#!/usr/bin/env python
import sys
import re

from auto_sched import push_sbid_execution, SlackPostManager

### match sbid from the input command
def find_run_info(runcmd):
    pat = "run\.SB(\d+)\.(\d{2})\.(\d{6})\.(.*)\.c\d*\.sh"
    match_res = re.findall(pat, runcmd)
    assert len(match_res) == 1, f"found {len(match_res)} pattern in {runcmd}..."
    sbid, scan, starttime, runname = match_res[0]
    return int(sbid), scan, starttime, runname

if __name__ == "__main__":
    args = sys.argv
    runstatus = int(args[2])
    runcmd = args[-1]
    sbid, scan, starttime, runname = find_run_info(runcmd)

    push_sbid_execution(
        sbid=sbid, runname=runname, newstatus=runstatus
    )

    ### slack notification here
    slackbot = SlackPostManager(test=False)
    if runstatus == 0: mention_team = False
    else: mention_team = True

    slackbot.post_message(
        f"*[PIPERUN]* finish running SB{sbid} scan {scan} starting from {starttime} with status code {runstatus}",
        mention_team = mention_team
    )

