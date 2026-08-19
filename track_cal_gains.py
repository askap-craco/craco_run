import numpy as np
from datetime import datetime as DT
import os
import glob
import matplotlib.pyplot as plt
import argparse


def plot_gains(amps, sbids, beam):
    tstarts, tstart_names = get_tstarts(sbids, beam)
    f = plt.figure()
    ax = f.add_subplot(111)
    for i in range(36):
        ax.plot(tstarts, amps[:len(tstarts), i], '.')
    ax.set_xticks(tstarts, tstart_names, rotation=75)
    plt.ylim(0, 20)
    plt.ylabel("Gain amplitude")
    plt.xlabel("Date of cal obs")
    plt.title(f"Calibration gain soln amp vs time (SBIDs) - Beam {beam:02g}")
    plt.show()


def get_tstarts(sbids, beam):
    tstarts = []
    tnow = DT.now()
    tstart_names = []
    for ii, sbid in enumerate(sbids):
       fitspath = f"/CRACO/DATA_00/craco/calibration/{sbid}/{beam:02g}/b{beam:02g}.uvfits"
       if os.path.islink(fitspath):
         full_path = os.readlink(fitspath)
         tstart = full_path.split("/")[7]
         tstart_dt = DT.strptime(tstart,"%Y%m%d%H%M%S")
         tstarts.append((tstart_dt - tnow).total_seconds() / 86400)
         tstart_names.append(tstart[2:8])
       else:
           print(fitspath)
    return tstarts, tstart_names

def get_all_sbids():
    sbs = []
    all_s = glob.glob("/CRACO/DATA_00/craco/calibration/SB*")
    all_s.sort()
    for sb in all_s:
        sbs.append(sb.split("/")[-1])
    return sbs


def get_gains(sbids, beam=0):
    amps = np.zeros((len(sbids), 36))
    for ii, sbid in enumerate(sbids):
        sb = format_sbid(sbid)
        solfile = f"/CRACO/DATA_00/craco/calibration/{sb}/{beam:02g}/b{beam:02g}.aver.4pol.smooth.npy"
        if os.path.exists(solfile):
            sols = np.load(solfile)
            gains = np.abs(sols)[0, :, 0, 0]
            amps[ii, :] = gains
    return amps

def format_sbid(sbid, padding=True, prefix=True):
    """
    format sbid into a desired format
    """
    if isinstance(sbid, str): sbid = int(
        "".join([i for i in sbid if i.isdigit()])
    )

    sbidstr = ""
    if prefix: sbidstr += "SB"
    if padding: sbidstr += f"{sbid:0>6}"
    else: sbidstr += f"{sbid}"

    return sbidstr

def main():
    if args.sbids is None:
        sbids = get_all_sbids()
    else:
        sbids = args.sbids
    gains = get_gains(sbids, args.beam)
    plot_gains(gains, sbids, args.beam)


if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument('-sbids', type=str, nargs = '+', help="List of sbids to plot (def:all)", default=None)
    a.add_argument('-beam', type=int, help="Beam to plot")
     
    args = a.parse_args()
    main()



