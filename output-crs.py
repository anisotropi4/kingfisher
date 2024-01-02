#!/usr/bin/env python3
"""output-crs: generate per station PNG files"""

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyogrio import read_dataframe, write_dataframe
from pyogrio.errors import DataSourceError

pd.set_option("display.max_columns", None)
CRS = "EPSG:27700"


def normalize(v):
    """normalize: scale values based on min and max values

    :param v:

    """
    return v / (v.max() - v.min())


def get_log_lw(line):
    """get_log_lw: log10 line-width

    :param line:

    """
    r = np.log10(line) / 11.0
    # r = r / (r.max() - r.min())
    return (11.95 * r) + 0.05


def get_linear_lw(line):
    """get_linear_lw: linear line-width

    :param line:

    """
    # r = line / (line.max() - line.min())
    r = line / 5.0e6
    return (11.9 * r) + 0.1


def get_gb():
    """get_gb: subset of islands that make up mainland Britain"""
    ine = read_dataframe("ine.gpkg", layer="base").to_crs(CRS)
    ix = (
        """1636,1394,1403,1383,1133,1142,1273,1089,1066,634,1035,1048,1165,1163"""
    ).split(",")
    ix = map(int, ix)
    write_dataframe(ine.loc[ix], "ine.gpkg", layer="mainland")

def main():
    """main"""
    mainland = read_dataframe("ine.gpkg", layer="mainland")
    for filename in sorted(os.listdir("data")):
        crs = filename.replace(".gpkg", "")
        filepath = f"image/{crs[0]}/{crs}-rail.png"
        if os.path.isfile(filepath):
            print(f"{crs} found")
            continue
        try:
            gf = read_dataframe(f"data/{filename}", layer=crs)
        except DataSourceError:
            print(f"ERROR: {crs}")
            continue
        if (gf.type != "LineString").all():
            continue
        print(crs)
        fig, ax = plt.subplots(dpi=300.0, layout="constrained")
        fig.set_figheight(8.0)
        fig.patch.set_facecolor("#d4ebf2")
        mainland.plot(ax=ax, color="white")
        ax.axis("off")
        gf["lw"] = get_linear_lw(gf["journeys"])
        ax.set_title(crs, y=1.0, x=0.0, pad=-12)
        gf.plot(ax=ax, linewidth=gf["lw"], color="orange")
        plt.savefig(filepath, bbox_inches="tight")
        plt.close()


if __name__ == "__main__":
    main()
