#!/usr/bin/env python3

import io
import json
import os
from itertools import pairwise

import fiona
import geopandas as gp
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
import requests
from pyogrio import read_dataframe, write_dataframe
from pyogrio.errors import DataLayerError, DataSourceError
from shapely import STRtree, distance, get_coordinates, line_merge, snap
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import nearest_points, split

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

OUTPATH = "work/odm-path.gpkg"


def get_databuffer(uri, encoding="utf-8"):
    """Download data from URI and returns as an StringIO buffer

    :param uri: param encoding:  (Default value = "utf-8")
    :param encoding:  (Default value = "utf-8")

    """
    r = requests.get(uri, timeout=10)
    return io.StringIO(str(r.content, encoding))


def get_naptan():
    """Download NaPTAN data as CSV
    :returns:
       GeoDataFrame
    """
    # NaPTAN data service
    URI = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
    this_buffer = get_databuffer(URI)
    df = pd.read_csv(this_buffer, low_memory=False).dropna(axis=1, how="all")
    data = df[["Easting", "Northing"]].values
    points = gp.points_from_xy(*data.T, crs="EPSG:27700")
    r = gp.GeoDataFrame(data=df, geometry=points)
    return r


def combine_line(line):
    """combine_line: return LineString GeoSeries combining lines with intersecting endpoints

    :param
       line
    :returns:
       LineString GeoSeries
    """
    r = MultiLineString(line.values)
    return gp.GeoSeries(line_merge(r).geoms, crs=CRS)


def get_station(naptan):
    """get_station

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
    return r


def get_crs(corpus_model):
    """get_crs:

    :param
       corpus_model
    """
    ix = corpus_model["3ALPHA"].str.strip() != ""
    r = corpus_model[ix].copy()
    return r


def get_osmnx_lookup(name_list, tags={"railway": "station"}):
    """get_osmnx_lookup:
    :param
      name_list
    """
    data = []
    for i in name_list:
        j = ox.features.features_from_address(i, tags=tags)
        data.append(j)
    r = pd.concat(data).reset_index()
    return r.to_crs(CRS)


def get_missing_station():
    """get_missing_station:
    :param
      GeoDataFrame()
    """
    ox_lookup = {
        "BOWSTRT": "Bow Street",
        "SOHAM": "Soham",
        "MAGHNTH": "Maghull North",
        "WHLAND": "Whitland",
    }
    r = pd.DataFrame.from_dict(ox_lookup, orient="index", columns=["Name"])
    r["CommonName"] = r["Name"] + " Rail Station"
    r[["StopType", "Status"]] = ["RLY", "active"]
    r = r.reset_index(names="TIPLOC")
    column = "geometry"
    data = get_osmnx_lookup(ox_lookup.values())
    r[column] = data[column]
    r["ATCOCode"] = "9100" + r["TIPLOC"]
    column = "LocalityName,ParentLocalityName".split(",")
    r[column] = "-"
    return gp.GeoDataFrame(r)


def get_station_crs(naptan_model, crs_model):
    """

    :param
      naptan_model
      crs_model:
    """
    lookup = {
        "CLPHMJC": "CLJ",
        "HEYMST": "HHB",
        "LNDNBDE": "LBG",
        "VICTRIE": "VIC",
        "VAUXHLW": "VXH",
        "WATRLMN": "WAT",
        "WLSDNJL": "WIJ",
    }
    r = get_station(naptan_model)
    s = get_missing_station()
    r = pd.concat([s, r])
    r = r.drop_duplicates(subset="TIPLOC")
    r = r.join(crs_model.set_index("TIPLOC"), on="TIPLOC").fillna("")
    r["CRS"] = r["3ALPHA"]
    r = r.set_index("TIPLOC")
    r.loc[lookup.keys(), "CRS"] = list(lookup.values())
    return r.sort_index().reset_index()


def get_end(geometry):
    """get_end: return numpy array of geometry LineString end-points

    :param geometry: geometry LineString
    :returns: end-point numpy arrays

    """
    r = get_coordinates(geometry)
    return np.vstack((r[0, :], r[-1, :]))


def get_source_target(line):
    """get_source_target: return edge and node GeoDataFrames from LineString with unique
    node Point and edge source and target

    :param line: LineString GeoDataFrame
    :returns: GeoDataFrames
        edge, node

    """
    edge = line.copy()
    r = edge["geometry"].map(get_end)
    r = np.stack(r)
    node = gp.GeoSeries(map(Point, r.reshape(-1, 2)), crs=CRS).to_frame("geometry")
    count = node.groupby("geometry").size().rename("count")
    node = node.drop_duplicates("geometry").set_index("geometry", drop=False)
    node = node.join(count).reset_index(drop=True).reset_index(names="node")
    ix = node.set_index("geometry")["node"]
    edge = edge.reset_index(names="edge")
    edge["source"] = ix.loc[map(Point, r[:, 0])].values
    edge["target"] = ix.loc[map(Point, r[:, 1])].values
    return edge, node


def get_split(v, separation=1.0e-6):
    """

    :param v: param separation:  (Default value = 1.0e-6)
    :param separation:  (Default value = 1.0e-6)

    """
    line, point = v["line"], v["point"]
    return list(split(snap(line, point, separation), point).geoms)


def split_network(network_model, point):
    """

    :param network_model: param point:
    :param point:

    """
    line = network_model.reset_index(drop=True).rename("line")
    point = point.rename("point")
    r = pd.concat([line, point], axis=1).apply(get_split, axis=1).explode()
    r = gp.GeoSeries(r, crs=CRS)
    return r


def get_ryde_portsmouth_wightlink(network_model):
    tags = {"name": "Wightlink: Ryde - Portsmouth Fastcat (passenger)"}
    r = ox.features.features_from_place("Portsmouth", tags=tags)
    r = r["geometry"].to_crs(CRS).reset_index(drop=True)
    i, _ = STRtree(r).query(network_model["geometry"], predicate="dwithin", distance=85)
    point = nearest_points(r.values, network_model["geometry"].iloc[i].values)
    s = gp.GeoSeries([LineString(k) for k in zip(*point)], crs=CRS)
    r = pd.concat([r, s]).reset_index(drop=True)
    return r


def set_simple_model(rhye_fix=True):
    """set_simple_model"""
    try:
        r = read_dataframe(OUTPATH, layer="simple_model")
        return r["geometry"]
    except (DataSourceError, DataLayerError):
        try:
            network_model = read_dataframe(OUTPATH, layer="network-model")
        except (DataSourceError, DataLayerError):
            network_model = read_dataframe(
                "data/network-model.gpkg", layer="network-model"
            )
            network_model = network_model.to_crs("EPSG:27700")
            write_dataframe(network_model, OUTPATH, layer="network-model")
    if rhye_fix:
        r = get_ryde_portsmouth_wightlink(network_model)
        r = pd.concat([network_model["geometry"], r])
    else:
        r = network_model["geometry"]
    r = combine_line(r)
    write_dataframe(r.to_frame("geometry"), OUTPATH, layer="simple_model")
    return r


def get_odm_model():
    """ """
    filepath = "data/ODM-2021-22-ALL-journeys-slim.gpkg"
    return read_dataframe(filepath, layer="ODM-2021-22-ALL-journeys-slim")


def set_naptan_model():
    """ """
    try:
        r = read_dataframe(OUTPATH, layer="naptan")
    except (DataSourceError, DataLayerError):
        r = get_naptan()
        write_dataframe(r, OUTPATH, layer="naptan")
    return r


def get_corpus_model():
    """ """
    with open("data/CORPUSExtract.json", "r", encoding="utf-8") as fin:
        data = json.load(fin)
    return pd.json_normalize(data, "TIPLOCDATA")


def get_crs_code(model):
    """

    :param model:

    """
    r = pd.concat([model.iloc[:, 0], model.iloc[:, 1]])
    return r.drop_duplicates().sort_values().reset_index(drop=True)


def get_active_station(station, crs_code):
    """

    :param station: param crs_code:
    :param crs_code:

    """
    ix = station["CRS"].isin(crs_code)
    return station[ix].reset_index(drop=True)


def set_station(naptan_model, odm_model):
    """

    :param naptan_model: param odm_model:
    :param odm_model:

    """
    corpus_model = get_corpus_model()
    try:
        r = read_dataframe(OUTPATH, layer="station")
    except (DataSourceError, DataLayerError):
        crs_model = get_crs(corpus_model)
        r = get_station_crs(naptan_model, crs_model)
        write_dataframe(r, OUTPATH, layer="station")
    try:
        r = read_dataframe(OUTPATH, layer="active_station")
    except (DataSourceError, DataLayerError):
        crs_code = get_crs_code(odm_model[["o_TLC", "d_TLC"]])
        r = get_active_station(r, crs_code)
        write_dataframe(r, OUTPATH, layer="active_station")
    return r


def get_station_point(track_model, station):
    """

    :param track_model: param station:
    :param station:

    """
    r = station.copy()
    s = r["geometry"]
    i, j = track_model.sindex.nearest(s)
    point, _ = nearest_points(track_model.iloc[j].values, s.iloc[i].values)
    r["geometry"] = point
    return r


def set_station_point(simple_model, active_station):
    """

    :param simple_model: param active_station:
    :param active_station:

    """
    try:
        r = read_dataframe(OUTPATH, "station_point")
    except (DataSourceError, DataLayerError):
        r = get_station_point(simple_model, active_station)
        write_dataframe(r, OUTPATH, "station_point")
    return r


def get_station_network(track_model, point):
    """

    :param track_model: param point:
    :param point:

    """
    i, j = track_model.sindex.nearest(point)
    r = split_network(track_model.iloc[j], point)
    ix = np.unique(j)
    r = pd.concat([r, track_model.drop(ix)])
    return r.reset_index(drop=True)


def set_edge_node(simple_model, station_point):
    """

    :param simple_model: param station_point:
    :param station_point:

    """
    try:
        edge = read_dataframe(OUTPATH, layer="edge")
        column = ["source", "target"]
        ix = np.sort(edge[column].values)
        edge.index = pd.MultiIndex.from_arrays(ix.T)
        node = read_dataframe(OUTPATH, layer="node")
        return edge, node
    except (DataSourceError, DataLayerError):
        pass
    try:
        path_model = read_dataframe(OUTPATH, layer="path_model")
    except (DataSourceError, DataLayerError):
        path_model = get_station_network(simple_model, station_point["geometry"])
        path_model = path_model.to_frame("geometry")
        write_dataframe(path_model, OUTPATH, layer="path_model")
    edge, node = get_source_target(path_model)
    edge["length"] = edge.length / 1.0e3
    write_dataframe(node, OUTPATH, layer="node")
    column = ["source", "target"]
    ix = np.sort(edge[column].values)
    edge.index = pd.MultiIndex.from_arrays(ix.T)
    write_dataframe(edge, OUTPATH, layer="edge")
    return edge, node


def get_distance(length, lookup):
    """

    :param distance: param lookup:
    :param lookup:

    """
    r = pd.Series(length).rename_axis("target")
    r = r.to_frame("distance")
    source = r.index[0]
    ix = r.index >= source
    r = r.loc[ix]
    r["source"] = source
    r["target"] = r.index
    ix = r.index.intersection(lookup)
    r = r.loc[ix].sort_index()
    return r.reset_index(drop=True)


def get_full_model(nx_path, station_point):
    """get_full_model:

    :param
      nx_path
      station_point:
      station_point:
    """
    column = ["source", "target"]
    node_crs = station_point.set_index("node")["CRS"]
    data = []
    max_node = node_crs.index.max()
    for i, length in nx.all_pairs_dijkstra_path_length(nx_path, weight="length"):
        if i > max_node:
            break
        if i in node_crs.index:
            print(f"{str(i).zfill(4)}")
            s = get_distance(length, node_crs.index)
            data.append(s)
    r = pd.concat(data)
    r = r.set_index(column).reset_index()
    r["source_crs"] = node_crs[r["source"]].values
    r["target_crs"] = node_crs[r["target"]].values
    return r


def set_full_model(nx_path, station_point):
    """

    :param nx_path: param station_point:
    :param station_point:

    """
    try:
        r = read_dataframe(OUTPATH, layer="full_model")
    except (DataSourceError, DataLayerError):
        r = get_full_model(nx_path, station_point)
        write_dataframe(r, OUTPATH, layer="full_model")
    r = r.set_index(["source", "target"], drop=False)
    return r


def update_station_point(station_point, node):
    """

    :param station_point: param node:
    :param node:

    """
    if "node" in station_point.columns:
        return station_point
    _, j = node.sindex.nearest(station_point["geometry"])
    station_point["node"] = node["node"].iloc[j].to_numpy()
    write_dataframe(station_point, OUTPATH, layer="station_point")
    return station_point


def get_crow_distance(station_model, station_point):
    """

    :param station_model: param station_point:
    :param station_point:

    """
    column = ["source", "target"]
    r = station_model[column].copy()
    node_map = station_point.set_index("node")["geometry"]
    s = np.stack([node_map[r["source"]], node_map[r["target"]]])
    s = pd.DataFrame(s.T, columns=column, index=r.index)
    r["km-crow"] = s.apply(lambda v: distance(*v), axis=1) / 1.0e3
    r["km-crow"] = r["km-crow"].round(2)
    return r.set_index(column)


def get_distance_model(station_model, station_point, track_model):
    """

    :param station_model: param station_point:
    :param track_model:
    :param station_point:

    """
    column = ["source", "target"]
    r = station_model.copy()
    crs_map = station_point.set_index("CRS")["node"]
    r["source"] = crs_map.loc[r.iloc[:, 0]].values
    r["target"] = crs_map.loc[r.iloc[:, 1]].values
    ix = np.sort(r[column].values)
    r = r.set_index(column, drop=False)
    return r


def set_distance_model(odm_model, station_point, edge, full_model):
    """

    :param odm_model: param station_point:
    :param edge: param full_model:
    :param station_point:
    :param full_model:

    """
    try:
        r = read_dataframe(OUTPATH, layer="distance_model")
        r = r.set_index(["source", "target"], drop=False)
        return r
    except (DataSourceError, DataLayerError):
        column = (
            """o_TLC,d_TLC,Financial_Year,journeys,o_STATION,o_RGNCTR,"""
            """o_OWNER,d_STATION,d_RGNCTR,d_OWNER"""
        ).split(",")
        r = get_distance_model(odm_model[column], station_point, edge)
        r["km-crow"] = get_crow_distance(r, station_point)
        s = full_model["distance"].round(2)
        ix = s.index.intersection(r.index)
        r.loc[ix, "km-distance"] = s[ix].values
        ix = s.index.intersection(r.index.swaplevel())
        r.loc[ix.swaplevel(), "km-distance"] = s[ix].values
        r = r.fillna(-1.0)
        write_dataframe(r, OUTPATH, layer="distance_model")
    return r


def get_node_edge(node):
    """

    :param node:

    """
    s = node["node"]
    r = pd.concat([s.rename("source"), s.rename("target"), node["geometry"]], axis=1)
    r[["length", "edge"]] = 0.0, -1
    r = r.set_index(["source", "target"], drop=False)
    return r


def get_undirected_edge_model(edge, node):
    """

    :param edge: param node:
    :param node:

    """
    r = edge.copy()
    r[["target", "source"]] = r[["source", "target"]]
    r.index = r.index.swaplevel()
    s = get_node_edge(node)
    r = pd.concat([edge, r, s]).sort_index()
    r["journeys"] = 0
    r = r.sort_values("length")
    r = r.drop_duplicates(subset=["source", "target"], keep="last")
    r = r.sort_index()
    r["edge"] = range(r.shape[0])
    return r


def get_directed_edge_model(edge, node):
    """

    :param edge: param node:
    :param node:

    """
    r = edge.copy()
    r[["target", "source"]] = r[["source", "target"]]
    r.index = r.index.swaplevel()
    r["geometry"] = r.reverse()
    s = get_node_edge(node)
    r = pd.concat([edge, r, s]).sort_index()
    r["journeys"] = 0
    r = r.sort_values("length")
    r = r.drop_duplicates(subset=["source", "target"], keep="last")
    r = r.sort_index()
    r["edge"] = range(r.shape[0])
    return r


def get_path(nx_path, n, journey_model):
    """

    :param nx_path: param n:
    :param journey_model:
    :param n:

    """
    path = nx.single_source_dijkstra_path(nx_path, n, weight="length")
    EMPTY = pd.DataFrame([], columns=["source", "target", "segment", "journeys"])
    r = pd.Series(path, name="segment")
    source = r.iloc[0][0]
    try:
        lookup = journey_model.loc[source, :].index
    except KeyError:
        return EMPTY
    # set path for source node
    r.iloc[0] = [source] * 2
    r = r.map(pairwise).map(list).to_frame()
    # ix = r.index >= source
    # r = r[ix]
    r["source"] = source
    r["target"] = r.index
    ix = r.index.intersection(lookup)
    r = r.loc[ix].sort_index()
    if r.empty:
        return EMPTY
    r = r.set_index(["source", "target"])
    r["journeys"] = journey_model
    r = r.explode("segment").reset_index()
    r.index = pd.MultiIndex.from_tuples(r["segment"])
    return r.sort_index()


def fn_crs_data(path="output"):
    """fn_crs_data:"""

    def set_crs_point():
        """ """
        filepath = f"{path}/all_point.gpkg"
        if os.path.isfile(filepath):
            return set(fiona.listlayers(filepath))
        return set()

    def get_crs_file(crs):
        """

        :param crs:

        """
        filepath = f"{path}/{crs}.gpkg"
        r = read_dataframe(filepath, layer=crs)
        if crs in POINT_CRS:
            s = read_dataframe(filepath, layer=crs)
            r = pd.concat([r, s])
        r = r.set_index(["source", "target"], drop=False)
        return r

    POINT_CRS = set_crs_point()
    return get_crs_file


# Ryde Pier Head-Portsmouth Harbour


def get_j2_model(journey):
    """get_j2_model
    :param
      journey
    """
    r = journey.copy()
    ix = pd.MultiIndex.from_arrays(np.sort(r[["source", "target"]]).T, names=["u", "v"])
    r.index = ix
    r = r.sort_index()
    r["j2"] = r["journeys"].groupby(["u", "v"]).sum() / 2
    r["w2"] = r["j2"] * 7.0 / 365.0
    r = r[~r.index.duplicated()]
    return r


def initialize_data(odm_model):
    """initialize_data:
    :param
      odm_model
    """
    naptan_model = set_naptan_model()
    simple_model = set_simple_model()
    active_station = set_station(naptan_model, odm_model)
    station_point = set_station_point(simple_model, active_station)
    edge, node = set_edge_node(simple_model, station_point)
    station_point = update_station_point(station_point, node)
    return station_point, edge, node


def main():
    """main: script execution point"""
    odm_model = get_odm_model()
    station_point, edge, node = initialize_data(odm_model)
    nx_path = nx.from_pandas_edgelist(edge, edge_attr="length")
    journey = get_undirected_edge_model(edge, node)
    edge_node_model = get_directed_edge_model(edge, node)
    full_model = set_full_model(nx_path, station_point)
    distance_model = set_distance_model(odm_model, station_point, edge, full_model)
    crs_map = station_point.set_index("CRS")[["Name", "node"]]
    get_crs_file = fn_crs_data()
    for crs in sorted(crs_map.index):
        print(crs)
        filepath = f"output/{crs}.gpkg"
        if os.path.isfile(filepath):
            r = get_crs_file(crs)
            journey.loc[r.index, "journeys"] += r["journeys"]
            continue
        n = crs_map.loc[crs, "node"]
        r = get_path(nx_path, n, distance_model["journeys"])
        if r.empty:
            print(f"ERROR: {crs}\t{crs_map.loc[crs, 'Name']}")
            continue
        r = r[["segment", "journeys"]].groupby("segment").sum()
        journey.loc[r.index, "journeys"] += r["journeys"]
        s = edge_node_model.loc[r.index].copy()
        s["journeys"] = r
        ix = s.type == "LineString"
        write_dataframe(s[ix], filepath, layer=crs)
        ix = s.type == "Point"
        if ix.any():
            filepath = "output/all_point.gpkg"
            write_dataframe(s[ix], filepath, layer=crs)
    write_dataframe(journey, "journeys-all.gpkg", layer="journeys")
    ix = journey["journeys"] > 0
    write_dataframe(journey[ix], "journeys-all.gpkg", layer="journey_model")
    j2_model = get_j2_model(journey)
    write_dataframe(j2_model, "journeys-all.gpkg", layer="j2")
    ix = j2_model["j2"] > 0.0
    write_dataframe(j2_model[ix], "journeys-all.gpkg", layer="j2_model")


if __name__ == "__main__":
    main()
