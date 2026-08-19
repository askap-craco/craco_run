import os, glob
import numpy as np

from pathlib import Path

def get_size(folder: str) -> int:
    return sum(p.stat().st_size for p in Path(folder).rglob('*'))

def find_all_sbids():
    all_dirs = glob.glob("/CRACO/DATA_00/craco/SB0*/")

