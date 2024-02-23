#!/usr/bin/env python3
"""odm-station: combine ODM years and create single station point geography"""
import json
import re

import geopandas as gp
import numpy as np
import pandas as pd
from pyogrio import write_dataframe

# lookup = {
#    "CLJ": ("CLPHMJ1", "CLPHMJ2", "CLPHMJC", "CLPHMJM", "CLPHMJW"),
#    "HHB": ("HEYMST"),
#    "LBG": ("LNDNBDC", "LNDNBDE"),
#    "VIC": ("VICTRIC", "VICTRIE"),
#    "VXH": ("VAUXHLM", "VAUXHLW"),
#    "WAT": ("WATRLMN"),
#    "WIJ": ("WLSDJHL", "WLSDNJL"),
# }

CRS = "EPSG:27700"
pd.set_option("display.max_columns", None)

OUTPATH = "work/odm-station.gpkg"


def get_naptan():
    """Download NaPTAN data as CSV
    :returns:
       GeoDataFrame
    """
    # NaPTAN data service
    uri = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
    df = pd.read_csv(uri, low_memory=False).dropna(axis=1, how="all")
    data = df[["Easting", "Northing"]].values
    points = gp.points_from_xy(*data.T, crs="EPSG:27700")
    r = gp.GeoDataFrame(data=df, geometry=points, crs=CRS)
    return r


def get_naptan_station(naptan):
    """get_naptan_station

    :param
       naptan
    """
    fields = (
        "ATCOCode,CommonName,LocalityName,ParentLocalityName,StopType,Status,geometry"
    ).split(",")
    r = naptan[naptan["StopType"].isin(["RLY", "MET"])]
    r = r[fields].dropna(axis=1, how="all").fillna("-")
    r["TIPLOC"] = r["ATCOCode"].str[4:]
    r["Name"] = r["CommonName"].str.replace(" Rail Station", "")
    r["Name"] = r["Name"].str.replace(" Station", "")
    return r


def get_attribute_model():
    """get_attribute_mode: read and scrub ORR station attribute data"""
    df = pd.read_excel(
        "data/station-attributes-for-all-mainline-stations.xlsx",
        header=3,
        sheet_name="6329_station_attributes",
    )
    df.columns = [i.replace("\n", "") for i in df.columns]
    data = df[
        ["Ordnance Survey grid: Easting", "Ordnance Survey grid: Northing"]
    ].values
    points = gp.points_from_xy(*data.T, crs="EPSG:27700")
    column = {
        "Station name": "Name",
        "National Location Code": "NLC",
        "Three Letter Code": "CRS",
        "Network Rail Region": "Region",
    }
    r = gp.GeoDataFrame(data=df[column.keys()], geometry=points, crs=CRS)
    return r.rename(columns=column).set_index("NLC", drop=False)


def get_corpus_model():
    """get_corpus_model:"""
    with open("data/CORPUSExtract.json", "r", encoding="utf-8") as fin:
        data = json.load(fin)
    return pd.json_normalize(data, "TIPLOCDATA")


def read_odm_model(n=18):
    """read_odm_model: read ORR origin-destination financial year data"""
    # lookup = {
    #     6909: "Angel Road",
    #     7932: "British Steel Redcar",
    #     8724: "Sampford Courtenay",
    #     9618: "IBM",
    # }
    j0 = f"{str(n).zfill(2)}"
    j1 = f"{str(n+1).zfill(2)}"
    filename = f"ODM_for_rdm_20{j0}-{j1}.csv.bz2"
    column = (
        """Financial_Year,origin_nlc,origin_station_name,origin_station_group,origin_region,"""
        """destination_nlc,destination_station_name,destination_station_group,"""
        """destination_region,journeys"""
    )
    column = {
        "Financial_Year": "FinancialYear",
        "origin_nlc": "o_nlc",
        "origin_station_name": "o_name",
        "origin_station_group": "o_group",
        "origin_region": "o_region",
        "origin_tlc": "o_CRS",
        "destination_nlc": "d_nlc",
        "destination_station_name": "d_name",
        "destination_station_group": "d_group",
        "destination_region": "d_region",
        "destination_tlc": "d_CRS",
        "journeys": "journeys",
    }
    r = pd.read_csv(f"data/{filename}", low_memory=False).rename(columns=column)
    return r


def get_missing(name, df, column):
    """get_missing: find missing values by matching location names"""
    data = []
    for k, n in name.items():
        m = n.replace(" ", "|")
        ix = df[column].str.findall(m, flags=re.IGNORECASE)
        ix = ix[ix.map(len) > n.count(" ")].index
        r = df.loc[ix]
        r["index"] = k
        r["key"] = n
        if "StopType" in r.columns:
            ix = r["StopType"] == "RLY"
            data.append(r[ix])
        elif "TIPLOC" in r.columns:
            ix = r["TIPLOC"].str.strip() != ""
            data.append(r[ix])
        else:
            data.append(r)
    return pd.concat(data).set_index("index")


def get_odm_model():
    """get_odm_model: combine all ODM data for years 2018-2022"""
    data = []
    for year in range(18, 23):
        data.append(read_odm_model(year))
    r = pd.concat(data).reset_index(drop=True)
    r["FinancialYear"] = r["FinancialYear"].replace(1920, 20192020)
    return r


def get_base_odm_station(odm_model):
    """get_base_odm_station: return base odm_station model"""
    column = {
        "o_nlc": "NLC",
        "o_name": "Name",
        "o_group": "Group",
        "o_region": "Region",
        "o_CRS": "CRS",
    }
    r = odm_model[column.keys()].drop_duplicates().rename(columns=column)
    r = r.sort_values(["NLC", "CRS"]).drop_duplicates("NLC")
    return r.set_index("NLC", drop=False)


def get_missing_crs(odm_station, orr_station, corpus):
    """fix_missing_crs: fix missing CRS values"""
    crs_lookup = {6909: "AGR"}
    r = odm_station.copy()
    missing = r[r["CRS"].isna()]
    ix = missing.index.intersection(orr_station.index)
    r.loc[ix, "CRS"] = orr_station.loc[ix, "CRS"]
    missing = r[r["CRS"].isna()]
    missing = get_missing(missing["Name"], corpus, "NLCDESC")
    r.loc[missing.index, "CRS"] = missing["3ALPHA"]
    missing = r[r["CRS"].isna()]
    # from wikipedia https://en.wikipedia.org/wiki/Angel_Road_railway_station
    r.loc[missing.index, "CRS"] = missing.index.map(crs_lookup)
    return r


def get_missing_geometry(odm_model, naptan_station):
    """get_missing_geometry: match NaPTAN"""
    r = odm_model.copy()
    ix = r.loc[:, "geometry"].isna()
    missing = r.loc[ix, "Name"]
    s = get_missing(missing, naptan_station, "Name")
    r.loc[ix, "geometry"] = s["geometry"]
    return r


def get_naptan_column(odm_model, naptan_station):
    """get_naptan_column: get NaPTAN value"""
    r = odm_model.copy()
    geometry = naptan_station["geometry"]
    (i, j), distance = r.sindex.nearest(geometry, return_distance=True)
    df_map = pd.DataFrame(np.stack([i, j]).T, columns=["i_index", "j_index"], dtype=int)
    df_map.loc[i, "distance"] = distance
    df_map = df_map.sort_values(["j_index", "distance"])
    df_map = df_map.drop_duplicates(subset="j_index").sort_index()
    column = (
        """CommonName,LocalityName,ParentLocalityName,StopType,Status,TIPLOC"""
    ).split(",")
    r[column] = ""
    ii, ij = df_map[["i_index", "j_index"]].values.T
    r.loc[ij, column] = naptan_station.loc[ii, column].values
    return r


def get_odm_station(naptan_station, corpus_model, orr_station, odm_model):
    """get_odm_station: fix up station data"""
    odm_station = get_base_odm_station(odm_model)
    odm_station = get_missing_crs(odm_station, orr_station, corpus_model)
    ix = odm_station.index.intersection(orr_station.index)
    geometry = orr_station.loc[ix, "geometry"]
    r = gp.GeoDataFrame(odm_station, geometry=geometry, crs=CRS).reset_index(drop=True)
    r = get_missing_geometry(r, naptan_station)
    r = get_naptan_column(r, naptan_station)
    r["Group"] = r["Group"].fillna("")
    return r.set_index("NLC", drop=False)


def update_odm_model(odm_model, odm_station):
    """update_odm_model:"""
    for k in ["o", "d"]:
        ii = odm_model[f"{k}_group"].isna()
        ij = odm_model.loc[ii, f"{k}_nlc"]
        odm_model.loc[ii, f"{k}_group"] = odm_station.loc[ij, "Group"].values
        ii = odm_model[f"{k}_CRS"].isna()
        ij = odm_model.loc[ii, f"{k}_nlc"]
        odm_model.loc[ii, f"{k}_CRS"] = odm_station.loc[ij, "CRS"].values
        ii = odm_model[f"{k}_name"].isna()
        ij = odm_model.loc[ii, f"{k}_nlc"]
        odm_model.loc[ii, f"{k}_name"] = odm_station.loc[ij, "Name"].values
    return odm_model


def scrub_odm_model(odm_model):
    """scrub_odm_model: journey rows to financial year columns"""
    financial_year = sorted([str(i) for i in set(odm_model["FinancialYear"])])
    column = (
        """o_nlc,o_name,o_group,o_region,d_nlc,"""
        """d_name,d_group,d_region,o_CRS,d_CRS"""
    ).split(",")
    r = odm_model[column].drop_duplicates().reset_index(drop=True)
    r[financial_year] = 0
    column = ["o_nlc", "d_nlc"]
    odm_model = odm_model.set_index(column, drop=False)
    r = r.set_index(column, drop=False)
    for k in financial_year:
        data = odm_model[odm_model["FinancialYear"] == int(k)]
        r.loc[data.index, k] = data.loc[data.index, "journeys"]
    return r


def main():
    """main: script execution point"""
    naptan_model = get_naptan()
    naptan_station = get_naptan_station(naptan_model).reset_index(drop=True)
    corpus_model = get_corpus_model()
    orr_station = get_attribute_model()
    odm_model = get_odm_model()
    odm_station = get_odm_station(naptan_station, corpus_model, orr_station, odm_model)
    odm_model = update_odm_model(odm_model, odm_station)
    odm_model = scrub_odm_model(odm_model)
    write_dataframe(odm_station, OUTPATH, layer="odm_station")
    write_dataframe(odm_model, OUTPATH, layer="odm_model")


if __name__ == "__main__":
    main()
