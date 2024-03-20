#!/usr/bin/env python
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

from slack_sdk import WebClient

import subprocess
import time
import glob
import re
import os

### for now we list all variable here
### later we will move it to a separate file
testchannel = "C06C6D3V03S"
obschannel = "C060VEYBSBW"
prochannel = "C05Q11P9GRH"
opchannel = "C06FCTQ6078"
mentionlst = [
    "<@U049R2ZMKAN>", "<@U4P2MNJTY>", "<@U012FPE7D2B>",
    "<@U01MHB4ABEU>", "<@U4MN5BE9X>",
]
# mentionlst = ["<@U049R2ZMKAN>"]

class CracoSlack:

    def __init__(self, channel):
        token = os.environ["SLACK_CRACO_TOKEN"]
        self.client = WebClient(token=token)
        self.channel = channel

    def send_message(self, msg, channel=None):
        if msg is None: return None
        if channel is None: channel = self.channel

        if isinstance(msg, str):
            return self.client.chat_postMessage(
                channel=channel,
                text=msg
            )
        if isinstance(msg, list):
            return self.client.chat_postMessage(
                channel=channel,
                blocks=msg,
            )

    def reply_message(self, msg, thread_ts, channel=None):
        if msg is None: return None
        if channel is None: channel = self.channel
        
        if isinstance(msg, str):
            return self.client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=msg
            )
        if isinstance(msg, list):
            return self.client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                blocks=msg,
            )

    ### more detailed posting
    def format_code(self, code):
        """this will return a dictionary"""
        return {
            "type": "rich_text",
            "elements": [{
                "type": "rich_text_preformatted",
                "elements": [{
                    "type": "text", "text": code
                }]
            }]
        }

    def format_text(self, text):
        return {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": text,
                "emoji": True,
            }
        }

class RunMonitor(CracoSlack):
    def __init__(self, channel):
        super().__init__(channel)

    # get diskspace
    def post_tethys_freedisk(self, threshold=80):
        p = subprocess.run(
            ["""ssh tethys -f "df -h" """], 
            shell=True, capture_output=True, text=True
        )
        freeline = re.findall("(/dev/sdb1.+/data/TETHYS_1)\n", p.stdout)
        if len(freeline) == 0: return

        freeline = freeline[0]

        dfout = self.format_code(freeline)
        r = self.send_message([dfout])

        try:
            used = float(re.findall("\s+(\d+)\%\s+", freeline)[0])
        except:
            return 
        if used > threshold:
            alert = "{} - Urgent! please delete some data off of the tethys disks".format(", ".join(mentionlst))
            self.reply_message(alert, thread_ts=r.data["ts"])

    def post_freedisk(self, threshold=95):
        _ = subprocess.run(
            ["ls /CRACO/DATA_{:0>2}".format(i) for i in range(19)], 
            shell=True, capture_output=True, 
        )
        p = subprocess.run(
            ["df -h /CRACO/DATA*"], 
            shell=True, capture_output=True, text=True
        )
        
        ### get used portion
        used = re.findall("\s(\d+)\%\s", p.stdout)
        used = np.array([int(i) for i in used])
        

        ### get df output
        dfout = self.format_code(p.stdout)
        r = self.send_message([dfout])
        
        if (used > threshold).sum() > 0: 
            alert = "{} - Urgent! please delete some data".format(", ".join(mentionlst))
            self.reply_message(alert, thread_ts=r.data["ts"])

    def _check_queue(self, iqueue=None):
        if iqueue is None:
            p = subprocess.run(
                ["tsp"], shell=True, capture_output=True, text=True
            )
        else:
            env = {'TS_SOCKET': f'/data/craco/craco/tmpdir/queues/{iqueue}'}
            p = subprocess.run(
                ["tsp"], shell=True, capture_output=True, 
                text=True, env=env
            )

        ### find queued in the output
        return len(re.findall("(queued)", p.stdout))
        
    def post_queue(self, nqueue=2, threshold=2):
        if nqueue is None:
            njobq = self._check_queue(iqueue=None)
            msg = f"{njobq} jobs currently queued...\n"
            alljobs = njobq

        else:
            alljobs = 0; msg = ""
            for iqueue in range(nqueue):
                njobq = self._check_queue(iqueue)
                msg += f"{njobq} jobs are currently queued in Queue{iqueue}...\n"
                alljobs += njobq
        if alljobs <= threshold:
            msg += "{} - please queue more new jobs\n".format(", ".join(mentionlst))

        self.send_message(msg)

def main():
    r = RunMonitor(channel=opchannel)
    while True:
        r.post_queue(nqueue=2, threshold=-1) # no warning posted atm
        r.post_freedisk(threshold=110)
        r.post_tethys_freedisk(threshold=90)
        time.sleep(3600) # sleep for one hour

if __name__ == "__main__":
    main()