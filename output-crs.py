#!/usr/bin/env python3
"""output-crs: generate per station PNG files"""

import os
from functools import partial
from multiprocessing import Pool, Process

import geopandas as gp
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyogrio import read_dataframe, write_dataframe
from pyogrio.errors import DataLayerError, DataSourceError
from shapely import unary_union
from shapely.geometry import Polygon

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
    r = (11.9 * r) + 0.1
    ix = line == 0
    r[ix] = 0.0
    return r


def get_gb():
    """get_gb: subset of islands that make up mainland Britain"""
    ine = read_dataframe("data/ine.gpkg", layer="base").to_crs(CRS)
    ix = (
        """1636,1394,1403,1383,1133,1142,1273,1089,1066,634,1035,1048,1165,1163"""
    ).split(",")
    ix = map(int, ix)
    mainland = ine.loc[ix]
    write_dataframe(mainland, "data/ine.gpkg", layer="mainland")
    beeching = mainland.buffer(9000.0)
    beeching = Polygon(unary_union(beeching))
    beeching = gp.GeoSeries(beeching, crs=CRS)
    write_dataframe(
        beeching.to_frame("geometry"), "work/beeching.gpkg", layer="beeching"
    )
    station = read_dataframe("work/odm-station.gpkg", layer="odm_station")
    beeching_label = (
        """Aberdeen,Barrow-In-Furness,Birmingham,Blackpool,Bournmouth,Brighton,Bristol,Cambridge,"""
        """Cardiff,Carlisle,Derby,Dover,Dundee,Edingburgh,Exeter,Glasgow,Gloucester,Grimsby,"""
        """Harwich,Hull,Inverness,Ipswich,Leeds,Leicester,Lincoln,London,Manchester,"""
        """Middlesborough,Newcastle,Norwich,Oxford,Plymouth,Portsmouth,Reading,Scarborough,"""
        """Sheffield,Shrewsbury,South-End-On-Sea,Southampton,Stafford,Swansea,Thurso,"""
        """Yarmouth,York"""
    ).split(",")
    beeching_crs = (
        """ABD,BTN,BHM,BPN,BMH,BTN,BRI,CBG,CDF,CAR,DBY,DVP,DEE,EDB,EXC,GLC,GCR,GMB,HWC,HUL,INV,"""
        """IPS,LDS,LEI,LCN,CHX,MAN,MBR,NCL,NRW,OXF,PLY,PMS,RDG,SCA,SHF,SHR,SOC,SOU,STA,SWA,THS,"""
        """GYM,YRK"""
    ).split(",")
    map_label = station.set_index("CRS").loc[beeching_crs, ["Name", "geometry"]]
    map_label["label"] = beeching_label
    map_label = map_label.reset_index()
    write_dataframe(map_label, "work/beeching.gpkg", layer="label")


def get_filepath(image, crs, year):
    """get_filepath: vetor SVG or image PNG"""
    if image == "vector":
        return f"vector/{crs[0]}/{crs}-{year}-rail.svg"
    if image == "image":
        return f"image/{crs[0]}/{crs}-{year}-rail.png"
    return f"image/{crs[0]}/{crs}-{year}-rail.png"


def write_image(filename, image="image"):
    """write_image: output image file"""
    crs = filename.replace(".gpkg", "")
    try:
        gf = read_dataframe(f"output/{filename}", layer=crs)
    except (DataSourceError, DataLayerError):
        print(f"ERROR: {crs}")
        return
    financial_year = [i for i in gf.columns if i[:2] == "20"]
    print(crs)
    for year in financial_year:
        filepath = get_filepath(image, crs, year)
        if os.path.isfile(filepath):
            print(f"{crs} {year} found")
            continue
        if (gf.type != "LineString").all():
            continue
        fig, ax = plt.subplots(dpi=300.0, layout="constrained")
        fig.set_figheight(8.0)
        fig.patch.set_facecolor("#d4ebf2")
        mainland.plot(ax=ax, color="white")
        ax.axis("off")
        if gf[year].sum() > 0.0:
            gf["lw"] = get_linear_lw(gf[year])
        else:
            gf["lw"] = 0.0
        ax.set_title(f"{crs} {year[:4]}-{year[6:]}", y=1.0, x=0.0, pad=-12)
        gf.plot(ax=ax, linewidth=gf["lw"], color="orange")
        if ".png" in filepath:
            plt.savefig(filepath, bbox_inches="tight", pil_kwargs={"optimize": True})
        else:
            plt.savefig(filepath, bbox_inches="tight")
        plt.close()


def main():
    """main: execution block"""
    global mainland
    mainland = read_dataframe("data/ine.gpkg", layer="mainland")
    pool = Pool(processes=(4 * os.cpu_count() - 1))
    output_image = partial(write_image)
    r = pool.map(output_image, sorted(os.listdir("output")))
    output_image = partial(write_image, image="vector")
    r = pool.map(output_image, sorted(os.listdir("output")))


if __name__ == "__main__":
    main()
