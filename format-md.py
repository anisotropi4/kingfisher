#!/usr/bin/env python
"""format-md: helper script to create markdown for CRS station image files"""
import argparse
from collections import defaultdict
from itertools import pairwise
from os import walk

import pandas as pd
from pyogrio import read_dataframe

pd.set_option("display.max_columns", None)


def list_files(filepath):
    """list_files: list all files under filepath

    :param
      filepath:
    """
    files = ()
    for d, _, filenames in walk(filepath):
        files = files + tuple(f"{d}/{f}" for f in filenames)
    return files


def main(imagepath, filename):
    """main: create station.md file from png images under filepath

    :param
       imagepath: path to image directories
       filename:  output filename
    """
    column_width = 2
    pngfile = sorted(list_files(imagepath))
    station = read_dataframe("station.gpkg", layer="station_point")
    crs_map = station.set_index("CRS")["Name"]
    # write_md("station.md", "# ORR Station Flow Images \n\n", pngfile)
    output = defaultdict(list)
    for png in pngfile:
        crs = png[8:11]
        name = crs_map[crs]
        letter = name[0]
        output[letter].append((name, crs, png))
    with open(filename, "w", encoding="utf-8") as fout:
        fout.write("# ORR Station Flow Images \n\n")
        fout.write(f"|{''.join(['Station|CRS|' * column_width])}\n")
        fout.write(f'|{"|".join(["------"] * (column_width * 2))}|\n')
        for letter in sorted(output.keys()):
            fout.write(f"|{letter}|\n")
            r = sorted(output[letter])
            n = len(r) + column_width
            for k in [r[i:j] for i, j in pairwise(range(0, n, column_width))]:
                text = "|".join([f"{p}|[{q}]({r})" for p, q, r in k])
                fout.write(f"|{text}|\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create CRS markdown file")
    parser.add_argument(
        "path", type=str, nargs="?", default="image", help="path to images"
    )
    parser.add_argument(
        "filename", type=str, nargs="?", default="station.md", help="md filename"
    )
    args, _ = parser.parse_known_args()
    main(args.path, args.filename)
