#!/usr/bin/env python

"""
get observation table updated automatically, 
this is purely based on metadata file existance
"""

import os
import time
import traceback

from auto_sched import (
    run_observation_update, SlackPostManager,
    get_recent_finish_sbid, _get_ice_service,
    get_db_max_sbid, _get_meta_max_sbid
)

import logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run(sleeptime=60):
    slackbot = SlackPostManager(test=False)
    iceservice = _get_ice_service()

    slackbot.post_message(f"*[SBRUNNER]* observation table update has been enabled")

    try:
        while True:
            dbmaxsbid = get_db_max_sbid()
            metasbid = _get_meta_max_sbid()

            if dbmaxsbid is None:
                slackbot.post_message(
                    f"*[SBRUNNER]* cannot get latest sbid from observation table",
                    mention_team=True,
                )
                raise ValueError("cannot load dbmaxsbid from database")

            if metasbid is None:
                slackbot.post_message(
                    f"*[SBRUNNER]* cannot get latest sbid from metadata files",
                    mention_team=True,
                )
                raise ValueError("cannot load recentsbid from metadata files")

            if dbmaxsbid == metasbid:
                log.debug(f"dbmaxsbid={dbmaxsbid} is the same as metasbid={metasbid} - no updated needed")
            else:
                recentsbid = get_recent_finish_sbid(service=iceservice)
                if recentsbid == dbmaxsbid:
                    log.debug(f"SB{metasbid} is recording... wait for it to finish")
                else:
                    slackbot.post_message(
                        f"*[SBRUNNER]* update observation database up to the most recent sbid - {recentsbid}"
                    )
                    run_observation_update(recentsbid, waittime=60) # wait for additional 1 min
                    log.info(f"update successfully up to {recentsbid} successfully")
            time.sleep(sleeptime)
    except KeyboardInterrupt:
        slackbot.post_message(
            f"*[SBRUNNER]* observation table update has been disabled"
        )
    except Exception as error:
        log.error(f"unexpected error - {error}")
        traceback.print_exc()
        slackbot.post_message(f"*[SBRUNNER]* observation table update has been disabled due to unexpected error")

if __name__ == "__main__":
    run()
        

    
