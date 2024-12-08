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


def get_output(imagefile, year):
    """get_output: create output dict

    :param
      pngfile: png image filename

    :return
      dict key first station name letter values tuple of name, CRS and path
    """
    r = defaultdict(list)
    station = read_dataframe("work/odm-station.gpkg", layer="odm_station")
    crs_map = station.set_index("CRS")["Name"]
    for image_path in imagefile:
        crs = image_path[8:11]
        j0 = image_path[12:16]
        j1 = image_path[18:20]
        if j0 == "rail" and year:
            continue
        financial_year = f"{j0}-{j1}"
        name = crs_map[crs]
        letter = name[0]
        r[letter].append((name, crs, financial_year, image_path))
    return r


def main(imagepath, filename, gif, year):
    """main: create station.md file from png images under filepath

    :param
       imagepath: path to image directories
       filename:  output filename
    """
    column_width = 1
    imagefile = sorted(list_files(imagepath))
    if gif:
        imagefile = [i for i in imagefile if ".gif" in i]
    else: 
        imagefile = [i for i in imagefile if ".png" in i]
    if year:
        imagefile = [i for i in imagefile if "-20" in i]
    else:
        imagefile = [i for i in imagefile if "-20" not in i]
    if imagefile:
        # write_md("station.md", "# ORR Station Flow Images \n\n", pngfile)
        output = get_output(imagefile, year)
        with open(filename, "w", encoding="utf-8") as fout:
            fout.write("# ORR Station Flow Images \n\n")
            fout.write(f"|{''.join(['Station Financial Year|CRS|' * column_width])}\n")
            fout.write(f'|{"|".join([":------"] * (column_width * 2))}|\n')
            for letter in sorted(output.keys()):
                fout.write(f"|{letter}|\n")
                r = sorted(output[letter])
                n = len(r) + column_width
                for k in [r[i:j] for i, j in pairwise(range(0, n, column_width))]:
                    if year:
                        text = "|".join([f"{p} {r}|[{q} {r}]({s})" for p, q, r, s in k])
                    else:
                        text = "|".join([f"{p}|[{q}]({s})" for p, q, r, s in k])
                    fout.write(f"|{text}|\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create CRS markdown file")
    parser.add_argument(
        "path", type=str, nargs="?", default="image", help="path to images"
    )
    parser.add_argument(
        "filename", type=str, nargs="?", default="station.md", help="md filename"
    )
    parser.add_argument("--gif", help="file type", action="store_true")
    parser.add_argument("--year", help="financial year", action="store_true")
    args, _ = parser.parse_known_args()
    main(args.path, args.filename, args.gif, args.year)
