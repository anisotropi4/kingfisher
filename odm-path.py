#!/usr/bin/env python3

"""odm-path: use ORR financial year passenger ticket data calculate station and aggregated flow"""
import os
from calendar import isleap
from itertools import pairwise

import fiona
import geopandas as gp
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
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
STATIONPATH = "work/station-path.gpkg"


def combine_line(line):
    """combine_line: return LineString GeoSeries combining lines with intersecting endpoints

    :param
       line
    :returns:
       LineString GeoSeries
    """
    r = MultiLineString(line.values)
    return gp.GeoSeries(line_merge(r).geoms, crs=CRS)


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
    """split_network:

    :param network_model:
    :param point:

    """
    line = network_model.reset_index(drop=True).rename("line")
    point = point.rename("point")
    r = pd.concat([line, point], axis=1).apply(get_split, axis=1).explode()
    r = gp.GeoSeries(r, crs=CRS)
    return r


def get_ryde_portsmouth_wightlink(network_model):
    """get_ryde_portsmouth_wightlink: cross the seven seas to Ryde"""
    tags = {"name": "Wightlink: Ryde - Portsmouth Fastcat (passenger)"}
    r = ox.features.features_from_place("Portsmouth", tags=tags)
    r = r["geometry"].to_crs(CRS).reset_index(drop=True)
    i, _ = STRtree(r).query(network_model["geometry"], predicate="dwithin", distance=85)
    point = nearest_points(r.values, network_model["geometry"].iloc[i].values)
    s = gp.GeoSeries([LineString(k) for k in zip(*point)], crs=CRS)
    r = pd.concat([r, s]).reset_index(drop=True)
    return r


def set_simple_model(rhye_fix=True):
    """set_simple_model: read centre-line track-model to LineString

    :param rhye_fix: add a random Isle of Wight ferry path

    """
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
    """get_odm_model2: read odm_model generated using ORR and other data"""
    filepath = "work/odm-station.gpkg"
    r = read_dataframe(filepath, layer="odm_model")
    return r, [i for i in r.columns if i[:2] == "20"]


def get_station():
    """get_odm_model2: read odm_model generated using ORR and other data"""
    filepath = "work/odm-station.gpkg"
    return read_dataframe(filepath, layer="odm_station")


def get_station_point(track_model, station):
    """

    :param track_model
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
        r = read_dataframe(STATIONPATH, "station_point")
    except (DataSourceError, DataLayerError):
        r = get_station_point(simple_model, active_station)
        write_dataframe(r, STATIONPATH, "station_point")
    return r


def get_station_network(track_model, point):
    """

    :param track_model: param point:
    :param point:

    """
    _, j = track_model.sindex.nearest(point)
    r = split_network(track_model.iloc[j], point)
    ix = np.unique(j)
    r = pd.concat([r, track_model.drop(ix)])
    return r.reset_index(drop=True)


def set_edge_node(simple_model, station_point):
    """

    :param simple_model:
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
    """get_full_model: calculate shortest path model

    :param
      nx_path
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
    """set_full_model: read or calculate path-model if not cached

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

    :param station_point
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


def get_distance_model(station_model, station_point):
    """get_distance_model:

    :param station_model:
    :param station_point:

    """
    column = ["source", "target"]
    r = station_model.copy()
    crs_map = station_point.set_index("CRS")["node"]
    r["source"] = crs_map.loc[r.iloc[:, 0]].values
    r["target"] = crs_map.loc[r.iloc[:, 1]].values
    r = r.set_index(column, drop=False)
    return r


def set_distance_model(odm_model, station_point, full_model):
    """set_distance_model:

    :param odm_model:
    :param station_point:
    :param edge:
    :param full_model:

    """
    try:
        r = read_dataframe(OUTPATH, layer="distance_model")
        r = r.set_index(["source", "target"], drop=False)
        return r
    except (DataSourceError, DataLayerError):
        column = (
            """o_CRS,d_CRS,20182019,20192020,20202021,20212022,"""
            """o_name,o_region,d_name,d_region,o_nlc,d_nlc"""
        ).split(",")
        r = get_distance_model(odm_model[column], station_point)
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


def get_undirected_edge_model(edge, node, financial_year):
    """

    :param edge: param node:
    :param node:

    """
    r = edge.copy()
    r[["target", "source"]] = r[["source", "target"]]
    r.index = r.index.swaplevel()
    s = get_node_edge(node)
    r = pd.concat([edge, r, s]).sort_index()
    r[financial_year] = 0
    r = r.sort_values("length")
    r = r.drop_duplicates(subset=["source", "target"], keep="last")
    r = r.sort_index()
    r["edge"] = range(r.shape[0])
    return r


def get_directed_edge_model(edge, node, financial_year):
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
    r[financial_year] = 0
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
    financial_year = journey_model.columns.to_list()
    column = ["source", "target", "segment"] + financial_year
    path = nx.single_source_dijkstra_path(nx_path, n, weight="length")
    empty = pd.DataFrame([], columns=column)
    r = pd.Series(path, name="segment")
    source = r.iloc[0][0]
    try:
        lookup = journey_model.loc[source, :].index
    except KeyError:
        return empty
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
        return empty
    r = r.set_index(["source", "target"])
    r[financial_year] = journey_model
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
        if crs in _point_crs:
            s = read_dataframe(filepath, layer=crs)
            r = pd.concat([r, s])
        r = r.set_index(["source", "target"], drop=False)
        return r

    _point_crs = set_crs_point()
    return get_crs_file


# Ryde Pier Head-Portsmouth Harbour


def get_yearlength(financial_year):
    """get_yearlength: return number of days in financial year

    :param financial_year: list of financial year as str

    """
    r = [366.0 if isleap(int(i[4:])) else 365.0 for i in financial_year]
    return np.asarray(r)


def get_j2_model(journey, financial_year):
    """get_j2_model
    :param
      journey: DataFrame containing journey
      financial_year: list containing financial year keys
    """
    r = journey.copy()
    ix = pd.MultiIndex.from_arrays(np.sort(r[["source", "target"]]).T, names=["u", "v"])
    r.index = ix
    r = r.sort_index()
    column = [f"j2_{i}" for i in financial_year]
    r[column] = (r[financial_year].groupby(["u", "v"]).sum() / 2).astype(int)
    year_length = get_yearlength(financial_year)
    r[[i.replace("j", "w") for i in column]] = r[column] * 7.0 / year_length
    r = r[~r.index.duplicated()]
    return r


def initialize_data(odm_model, financial_year):
    """initialize_data:
    :param
      odm_model
    """
    simple_model = set_simple_model()
    active_station = get_station()
    station_point = set_station_point(simple_model, active_station)
    edge, node = set_edge_node(simple_model, station_point)
    station_point = update_station_point(station_point, node)
    nx_path = nx.from_pandas_edgelist(edge, edge_attr="length")
    journey = get_undirected_edge_model(edge, node, financial_year)
    edge_node_model = get_directed_edge_model(edge, node, financial_year)
    full_model = set_full_model(nx_path, station_point)
    distance_model = set_distance_model(odm_model, station_point, full_model)
    crs_map = station_point.set_index("CRS")[["Name", "node"]]
    return crs_map, journey, nx_path, distance_model, edge_node_model


def set_combined_data(journey, financial_year):
    """set_combined_data:

    :param journey: GeoDataFrame with passenger data
    :param financial_year: list of financial years str

    """
    write_dataframe(journey, "journeys-all.gpkg", layer="journeys")
    ix = (journey[financial_year] > 0).any(axis=1)
    write_dataframe(journey[ix], "journeys-all.gpkg", layer="journey_model")
    j2_model = get_j2_model(journey, financial_year)
    write_dataframe(j2_model, "journeys-all.gpkg", layer="j2")
    ix = (j2_model[financial_year] > 0).any(axis=1)
    write_dataframe(j2_model[ix], "journeys-all.gpkg", layer="j2_model")


def main():
    """main: script execution point"""
    odm_model, financial_year = get_odm_model()
    crs_map, journey, nx_path, distance_model, edge_node_model = initialize_data(
        odm_model, financial_year
    )
    get_crs_file = fn_crs_data()
    for crs in sorted(crs_map.index):
        print(crs)
        filepath = f"output/{crs}.gpkg"
        if os.path.isfile(filepath):
            r = get_crs_file(crs)
            journey.loc[r.index, financial_year] += r[financial_year]
            continue
        n = crs_map.loc[crs, "node"]
        r = get_path(nx_path, n, distance_model[financial_year])
        if r.empty:
            print(f"ERROR: {crs}\t{crs_map.loc[crs, 'Name']}")
            continue
        column = ["segment"] + financial_year
        r = r[column].groupby("segment").sum()
        journey.loc[r.index, financial_year] += r[financial_year]
        s = edge_node_model.loc[r.index].copy()
        s[financial_year] = r
        ix = s.type == "LineString"
        write_dataframe(s[ix], filepath, layer=crs)
        ix = s.type == "Point"
        if ix.any():
            filepath = "output/all_point.gpkg"
            write_dataframe(s[ix], filepath, layer=crs)
    set_combined_data(journey, financial_year)


if __name__ == "__main__":
    main()
