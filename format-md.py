#!/usr/bin/env python
"""format-md: helper script to create markdown for CRS station image files"""
import argparse
from collections import defaultdict
from itertools import pairwise
from os import walk

import pandas as pd
from pyogrio import read_dataframe

pd.set_option("display.max_columns", None)

PARSER = argparse.ArgumentParser(description="Create CRS markdown file")
PARSER.add_argument(
    "path", type=str, nargs="?", default="image", help="path to images"
)

ARGS, _ = PARSER.parse_known_args()


def list_files(filepath):
    """list_files: list all files under filepath

    :param filepath: 

    """
    files = ()
    for d, _, filenames in walk(filepath):
        files = files + tuple(f"{d}/{f}" for f in filenames)
    return files

def main(imagepath):
    """main: create station.md file from png images under filepath

    :param imagepath: path to image directories

    """
    column_width = 2
    pngfile = sorted(list_files(imagepath))
    station = read_dataframe("station.gpkg", layer="station_point")

    crs_map = station.set_index("CRS")["Name"]
    #write_md("station.md", "# ORR Station Flow Images \n\n", pngfile)

    output = defaultdict(list)

    for png in pngfile:
        crs = png[8:11]
        name = crs_map[crs]
        letter = name[0]
        output[letter].append((crs, name, png))

    print("# ORR Station Flow Images \n\n")
    print(f"|{''.join(['Station|CRS|' * column_width])}")
    print(f'|{"|".join(["----------------------------------"] * (column_width * 2))}|')
    for letter, data in output.items():
        print(f"|{letter}|")
        for k in [data[i:j] for i, j in pairwise(range(0, len(data) + column_width, column_width))]:
            text = "|".join([f"{q}|[{p}]({r})" for p, q, r in k])
            print(f"|{text}|")

if __name__ == "__main__":
    main(ARGS.path)
