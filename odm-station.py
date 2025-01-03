#!/usr/bin/env python3
"""odm-station: combine ODM years and create single station point geography"""
import gzip
import json
import re

import geopandas as gp
import pandas as pd

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

OUTSTATION = "work/odm-station.parquet.gz"
OUTNAPTAN = "work/odm-naptan.parquet.gz"
OUTMODEL = "work/odm-model.parquet.gz"


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


def get_naptan_station(cache=True):
    """get_naptan_station
    :param
       naptan
    """
    if cache:
        try:
            r = gp.read_parquet(OUTNAPTAN)
            return r
        except FileNotFoundError:
            pass
    naptan = get_naptan()
    field = (
        "ATCOCode,CommonName,LocalityName,ParentLocalityName,StopType,Status,geometry"
    ).split(",")
    r = naptan[naptan["StopType"].isin(["RLY", "MET"])]
    r = r[field].dropna(axis=1, how="all").fillna("-")
    r["TIPLOC"] = r["ATCOCode"].str[4:]
    r["Name"] = r["CommonName"].str.replace(" Rail Station", "")
    r["Name"] = r["Name"].str.replace(" Station", "")
    return r


def get_attribute_model():
    """get_attribute_mode: read and scrub ORR station attribute data"""
    df = pd.read_excel(
        "data/table-6329-station-attributes-for-all-mainline-stations.ods",
        # "data/station-attributes-for-all-mainline-stations.xlsx",
        header=3,
        sheet_name="6329_station_attributes",
    )
    df.columns = df.columns.str.replace("\n", "").str.replace(" [r]", "")
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
    r = r.rename(columns=column)
    r["NLC"] = r["NLC"] * 100
    return r.set_index("NLC", drop=False)


def get_corpus_model():
    """get_corpus_model:"""
    with gzip.open("data/CORPUSExtract.json.gz", "r") as fin:
        data = json.load(fin)
    r = pd.json_normalize(data, "TIPLOCDATA").drop_duplicates()
    ix = r["NLC"] == 145700
    if (r.loc[ix, "STANOX"].str.strip() == "").all():
        r.loc[ix, "STANOX"] = "72269"
    return r


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
        """Financial_Year,FinancialYear,origin_nlc,o_nlc,origin_station_name,o_name,"""
        """origin_station_group,o_group,origin_region,o_region,origin_tlc,o_CRS,destination_nlc,"""
        """d_nlc,destination_station_name,d_name,destination_station_group,d_group,"""
        """destination_region,d_region,destination_tlc,d_CRS,journeys,journeys"""
    ).split(",")
    column = dict(zip(column[::2], column[1::2]))
    r = pd.read_csv(f"data/{filename}", low_memory=False).rename(columns=column)
    r[["o_nlc", "d_nlc"]] = r[["o_nlc", "d_nlc"]] * 100
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
    """get_odm_model: combine all ODM data for years 2018-2023"""
    data = []
    for year in range(18, 24):
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


def get_missing_crs(odm_station, corpus):
    """fix_missing_crs: fix missing CRS values"""
    crs_lookup = {690900: "AGR"}
    r = odm_station.copy()
    missing = r[r["CRS"].isna()]
    missing = get_missing(missing["Name"], corpus, "NLCDESC")
    r.loc[missing.index, "CRS"] = missing["3ALPHA"]
    missing = r[r["CRS"].isna()]
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


def get_corpus_column(odm_model, corpus_model):
    """get_corpus_column: get CORPUS value"""
    r = odm_model.copy().set_index("CRS")
    s = corpus_model[corpus_model["3ALPHA"].str.strip() != ""].set_index("3ALPHA")
    ix = s.index.intersection(r.index)
    column = ["STANOX", "TIPLOC", "UIC", "NLCDESC"]
    r.loc[ix, column] = s.loc[ix, column]
    return r.reset_index()


def get_naptan_column(odm_model, naptan_station):
    """get_naptan_column: get NaPTAN value"""
    r = odm_model.copy().set_index("TIPLOC")
    column = "CommonName,LocalityName,ParentLocalityName,StopType,Status".split(",")
    r[column] = ""
    s = naptan_station.set_index("TIPLOC")
    ix = s.index.intersection(r.index)
    r.loc[ix, column] = s.loc[ix, column]
    # geometry = naptan_station["geometry"]
    # (i, j), distance = r.sindex.nearest(geometry, return_distance=True)
    # df_map = pd.DataFrame(np.stack([i, j]).T, columns=["i_index", "j_index"], dtype=int)
    # df_map.loc[i, "distance"] = distance
    # df_map = df_map.sort_values(["j_index", "distance"])
    # df_map = df_map.drop_duplicates(subset="j_index").sort_index()
    # ii, ij = df_map[["i_index", "j_index"]].values.T
    # r.loc[ij, column] = naptan_station.loc[ii, column].values
    return r.reset_index()


def get_odm_station(naptan_station, corpus_model, orr_station, odm_model):
    """get_odm_station: fix up station data"""
    odm_station = get_base_odm_station(odm_model)
    odm_station = get_missing_crs(odm_station, corpus_model)
    ix = odm_station.index.intersection(orr_station.index)
    geometry = orr_station.loc[ix, "geometry"]
    r = gp.GeoDataFrame(odm_station, geometry=geometry, crs=CRS).reset_index(drop=True)
    r = get_missing_geometry(r, naptan_station)
    r = get_corpus_column(r, corpus_model)
    r = get_naptan_column(r, naptan_station)
    r["Group"] = r["Group"].fillna("")
    ix = r["NLC"] == 690900
    column = ["TIPLOC", "STANOX", "UIC", "NLCDESC"]
    r.loc[ix, column] = "ANGELRD,51924,69090,ANGEL ROAD".split(",")
    column = ["FinancialYear", "o_nlc", "journeys"]
    s = odm_model[column].rename(columns={"o_nlc": "NLC"})
    s = s.groupby(["NLC", "FinancialYear"]).sum().reset_index()
    s = s.pivot(index="NLC", columns="FinancialYear", values="journeys")
    s = s.fillna(-9999999).astype(int)
    s.columns = s.columns.astype(str)
    r = r.set_index("NLC", drop=False)
    r[s.columns] = s
    return r.fillna("")


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
    naptan_station = get_naptan_station().reset_index(drop=True)
    corpus_model = get_corpus_model()
    orr_station = get_attribute_model()
    odm_model = get_odm_model()
    odm_station = get_odm_station(naptan_station, corpus_model, orr_station, odm_model)
    odm_model = update_odm_model(odm_model, odm_station)
    odm_model = scrub_odm_model(odm_model)
    # write_dataframe(naptan_station, OUTPATH, layer="naptan")
    # write_dataframe(odm_station, OUTPATH, layer="odm_station")
    naptan_station.to_parquet(OUTNAPTAN, compression="gzip")
    odm_station.to_parquet(OUTSTATION, compression="gzip")
    odm_model.to_parquet(OUTMODEL, compression="gzip")


if __name__ == "__main__":
    main()
