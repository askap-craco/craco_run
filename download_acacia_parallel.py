#!/usr/bin/env python3

import argparse
import os
import subprocess
# from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool


def download_file(file, sbid, dryrun=False):
    # print(file) # remove filesize
    file = file.split()[-1]
    print(file)

    data_id = os.path.basename(os.path.dirname(file))
    filename = os.path.basename(file)
    scan_path = os.path.dirname(os.path.dirname(file))

    dest = (
        f"/CRACO/{data_id}/craco/{sbid}/scans/"
        f"{scan_path}/{filename}"
    )

    source = f"acacia:as116/{sbid}/{file}"

    print(f"{file} -> {dest}", flush=True)

    if dryrun:
        return

    os.makedirs(os.path.dirname(dest), exist_ok=True)

    cmd = [
        "rclone",
        "copyto",
        source,
        dest, "-P",
        "--stats", "10s",
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    for line in process.stdout:
        print(f"[{file}] {line}", end="", flush=True)

    returncode = process.wait()

    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-sbid", type=int)
    parser.add_argument("-dryrun", action="store_true")
    parser.add_argument("-j", "--jobs", type=int, default=4)

    args = parser.parse_args()

    sbid = f"SB{args.sbid:06d}"

    print(f"SBID: {sbid}")
    print(f"Dry run: {args.dryrun}")
    print(f"Parallel jobs: {args.jobs}")

    # Get the list of UVFITS files from Acacia
    cmd = [
        "rclone",
        "ls",
        f"acacia:as116/{sbid}",
        "--include", "*.uvfits",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    files = [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    ]

    print(f"Found {len(files)} UVFITS files")

    if args.dryrun:
        for file in files:
            download_file(file, sbid, dryrun=True)
        return

    with Pool(processes=args.jobs) as pool:
        pool.starmap(
            download_file,
            [(file, sbid, args.dryrun) for file in files],
        )

    # with ThreadPoolExecutor(max_workers=args.jobs) as executor:

    #     futures = {
    #         executor.submit(download_file, file, sbid): file
    #         for file in files
    #     }

    #     for future in as_completed(futures):
    #         file = futures[future]

    #         try:
    #             future.result()
    #             print(f"Finished: {file}", flush=True)

    #         except subprocess.CalledProcessError as e:
    #             print(
    #                 f"ERROR downloading {file} "
    #                 f"(exit code {e.returncode})",
    #                 flush=True,
    #             )


if __name__ == "__main__":
    main()