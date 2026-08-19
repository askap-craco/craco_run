#!/usr/bin/env python

import glob
import os
import multiprocessing
import logging

import pandas as pd

from craco.datadirs import SchedDir

class SnippetUvfits:
    def __init__(self, snippetpath):
        # in the form of - /CRACO/DATA_03/craco/SB080890/scans/00/20260105120411/beam02/candidates/iblk24/candidate.uvfits
        self.snippetpath = snippetpath
        self.blkdir = os.path.dirname(snippetpath)
        self.info = self.get_snippet_info()

    @property
    def keep(self):
        if os.path.exists(f"{self.blkdir}/KEEP"): return True
        return False

    def get_snippet_info(self):
        parts = self.snippetpath.split("/")
        self.sbid = int(parts[4][2:])
        self.scanno = parts[6]
        self.scantstart = parts[7]
        self.beam = int(parts[8][4:])
        self.iblk = parts[10][4:]

        return dict(sbid=self.sbid, scanno=self.scanno, scantstart=self.scantstart, beam=self.beam, iblk=self.iblk, keep=self.keep)

class SnippetFinder:
    def __init__(self, sbid, scans=None):
        self.sbid = sbid
        self.scheddir = SchedDir(sbid=sbid)
        self.scans = scans if scans is not None else self.scheddir.scans

    def _find_snippets_for_node(self, node, scans):
        if isinstance(scans, str):
            scans = [scans]
        paths = []
        for scan in scans:
            pattern = f"/CRACO/DATA_{node:0>2}/craco/SB{self.sbid:0>6}/scans/{scan}/beam??/candidates/iblk*/candidate.uvfits"
            paths.extend(glob.glob(pattern))
        return paths

    def find_snippets(self, ncpu=18):
        if ncpu == 1:
            allpaths = []
            for scan in self.scans:
                pattern = f"/CRACO/DATA_??/craco/SB{self.sbid:0>6}/scans/{scan}/beam??/candidates/iblk*/candidate.uvfits"
                allpaths.extend(glob.glob(pattern))
            return allpaths
        args = [(node, self.scans) for node in range(1, 19)]
        with multiprocessing.Pool(processes=ncpu) as pool:
            allpaths = pool.starmap(self._find_snippets_for_node, args)
        return sum(allpaths, [])
    
class SnippetRemover:
    def __init__(self, snippetpaths, dryrun=True):
        self.snippetpaths = snippetpaths
        self.snippetdb = self._get_snippet_db()
        self.dryrun = dryrun
        if self.dryrun:
            logging.info("Dry run - no files will actually be deleted")

    def _get_snippet_db(self):
        dbdicts = []
        for snippet in self.snippetpaths:
            su = SnippetUvfits(snippet)
            suinfo = su.info
            suinfo["path"] = snippet
            dbdicts.append(suinfo)
        return pd.DataFrame(dbdicts)

    def delete_snippet(self):
        if len(self.snippetdb) == 0:
            logging.info("No snippets found for deletion")
            return
        deldf = self.snippetdb[self.snippetdb["keep"] == False]
        logging.info(f"{len(deldf)} marked for deletion...")
        for i, row in deldf.iterrows():
            path = row["path"]
            if not self.dryrun: os.remove(path)
            logging.info(f"Deleted snippet - {path}")


def _delete_snippet_sbid(sbid, dryrun=True):
    logging.info(f"Processing SBID - {sbid}")
    sf = SnippetFinder(sbid=sbid)
    sp = sf.find_snippets(ncpu=18)
    logging.info(f"Found {len(sp)} snippets for SBID {sbid}")
    sr = SnippetRemover(sp, dryrun=dryrun)
    sr.delete_snippet()


def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Delete Snippets for a given SBID', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dryrun', action='store_true', help='Dry run - do not actually delete files')
    # parser.add_argument('-v', '--verbose', action='store_true', help='Be verbose')
    # parser.add_argument('--only_scan', type=str, help='Only keep this scan (tstart)', default=None)
    parser.add_argument(dest='sbids', nargs='*', type=int)
    parser.set_defaults(verbose=False)
    values = parser.parse_args()
    # if values.verbose:
    #     logging.basicConfig(level=logging.DEBUG)
    # else:
    #     logging.basicConfig(level=logging.INFO)
    logging.basicConfig(level=logging.INFO)

    for sbid in values.sbids:
        try: 
            _delete_snippet_sbid(sbid, dryrun=values.dryrun)
        except Exception as error:
            with open("/CRACO/SOFTWARE/craco/craftop/op/snippet_deletion_260430.txt", "a") as f:
                f.write(f"Error processing SBID {sbid} - {error}\n")

if __name__ == "__main__":
    main()