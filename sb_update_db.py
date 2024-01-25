#!/usr/bin/env python

"""
get observation table updated automatically, no argument needed
"""

import os

from auto_sched import (
    run_observation_update, SlackPostManager
)

def run():
    slackbot = SlackPostManager(test=False)
    
    env = os.environ.copy()
    latestsbid = env.get("SB_ID")

    if latestsbid is None:
        log.info("no sbid found in envionemnt variable...")
        return
    try:
        latestsbid = int(latestsbid)
    except Exception as error:
        log.warning(f"cannot convert sbid {latestsbid} to integer...")
        slackbot.chat_postMessage(
            f"*[SBRUNNER]* receive {latestsbid} from sbrunner - fail to  convert it to int"
        )
        return

    slackbot.chat_postMessage(
        f"*[SBRUNNER]* update observation database up to the most recent sbid - {latestsbid}"
    )
    run_observation_update(int(latestsbid))

if __name__ == "__main__":
    run()
        

    
