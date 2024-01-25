#!/usr/bin/env python

from auto_sched import PipeSched

if __name__ == "__main__":
    pipesched = PipeSched(dryrun=False)
    pipesched.run()