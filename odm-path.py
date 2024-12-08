#!/usr/bin/env python3

"""odm-path: use ORR financial year passenger ticket data calculate station and aggregated flow"""
import datetime as dt
import os
from calendar import isleap
from functools import partial
from itertools import pairwise, starmap
from multiprocessing import Manager
from multiprocessing.pool import Pool

import geopandas as gp
import numpy as np
import osmnx as ox
import pandas as pd
from pyogrio import read_dataframe, write_dataframe
from pyogrio.errors import DataLayerError, DataSourceError
from scipy import sparse
from scipy.sparse.csgraph import connected_components, shortest_path
from scipy.spatial.distance import euclidean
from shapely import STRtree, get_coordinates, line_merge, snap
from shapely.geometry import LineString, MultiLineString, MultiPoint
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


def snap_point_line(point, line):
    """snap_point_line:"""
    s = get_geometry_series(point)
    m = get_geometry_series(line)

    i, j = m.sindex.nearest(s)
    p, _ = nearest_points(m.iloc[j].values, s.iloc[i].values)
    r = gp.GeoSeries(p, crs=CRS).drop_duplicates()
    return r.reset_index(drop=True)


def get_geometry_series(gf):
    """get_geometry_series: get "geometry" column as GeoSeries"""
    r = gf.copy()
    try:
        return r["geometry"].reset_index(drop=True)
    except KeyError:
        pass
    return r.reset_index(drop=True)


def get_nx_geometry(path, edge, station):
    """get_nx_geometry: return combined LineString for a given set of paths"""
    column = ["source", "target"]
    s = edge.set_index(column)
    r = path.map(pairwise).map(list).explode()
    r = r.map(sorted).map(tuple).drop_duplicates()
    r = merge_line(s.loc[r, "geometry"]).to_frame("geometry")
    r = split_network(r, station)
    return r


def get_source_target(line):
    """get_source_target: return edge and node GeoDataFrames from LineString with unique
    node Point and edge source and target
    :param line: LineString GeoDataFrame
    :returns: GeoDataFrames
        edge, node
    """
    r = line.map(get_coordinates).explode()
    ix = r.index.duplicated(keep="last") & r.index.duplicated(keep="first")
    r = gp.points_from_xy(*np.stack(r[~ix]).reshape(-1, 2).T)
    node = pd.Series(r).to_frame("geometry")
    node = node.groupby("geometry").size().rename("count").reset_index()
    node["node"] = node.index
    node = gp.GeoDataFrame(node, crs=CRS)

    edge = line.copy()
    edge = edge.rename_axis("edge").reset_index()

    r = np.asarray(r).reshape(-1, 2)
    i, j = node["geometry"].sindex.nearest(r[:, 0], return_all=False)
    edge["source"] = -1
    edge.iloc[i, -1] = j

    i, j = node["geometry"].sindex.nearest(r[:, 1], return_all=False)
    edge["target"] = -1
    edge.iloc[i, -1] = j

    column = ["source", "target"]
    ix = edge["source"] > edge["target"]
    edge.loc[ix, column] = edge.loc[ix, column[::-1]].values
    edge.loc[ix, "geometry"] = edge.loc[ix, "geometry"].reverse()
    edge = edge.drop_duplicates(subset=column).reset_index(drop=True)
    edge["edge"] = edge.index
    return node, edge


def get_station_edge_node(nx_model, nx_station):
    """get_station_edge_node: get aggregated node, edge network for network split at rail_station"""
    node, edge = get_source_target(nx_model["geometry"].reset_index(drop=True))
    node[["CRS", "NLC"]] = "", 0
    i, j = node.sindex.nearest(nx_station["geometry"])
    node.iloc[j, -2:] = nx_station.iloc[i][["CRS", "NLC"]]
    edge["length"] = edge.length
    edge[["source_CRS", "source_NLC"]] = node.loc[edge["source"], ["CRS", "NLC"]].values
    edge[["target_CRS", "target_NLC"]] = node.loc[edge["target"], ["CRS", "NLC"]].values
    return edge, node


def get_split(v, separation=1.0e-6):
    """

    :param v: param separation:  (Default value = 1.0e-6)
    :param separation:  (Default value = 1.0e-6)

    """
    line, point = v["line"], v["point"]
    return list(split(snap(line, point, separation), point).geoms)


def merge_line(line, directed=False):
    """clean_line: return LineString GeoSeries combining lines and joining intersecting endpoints

    :param
       line
    :returns:
       LineString GeoSeries
    """
    r = line.copy()
    try:
        r = r["geometry"]
    except KeyError:
        pass
    return gp.GeoSeries(line_merge(MultiLineString(r.values), directed).geoms, crs=CRS)


def split_network(network_model, point):
    """split_network:

    :param network_model:
    :param point:

    """
    # write_dataframe(r.to_frame("geometry"), "dump.gpkg", layer="snap_point")
    m = get_geometry_series(network_model)
    m = merge_line(m)
    m = m.reset_index(name="geometry").rename(columns={"index": "line_id"})
    s = snap_point_line(point, m)
    tree = STRtree(s.values)
    i, j = tree.query(m["geometry"].values, predicate="dwithin", distance=1.0)
    s = pd.Series(s.iloc[j].values, index=i)
    s = s.groupby(level=0).apply(np.asarray).map(MultiPoint)
    s = s.rename_axis("line_id").reset_index(name="point")
    s["line"] = m.loc[s["line_id"], "geometry"].values
    r = s.apply(get_split, axis=1)
    r.index = s["line_id"]
    r = r.reset_index(name="geometry")
    r = pd.concat([r, m]).drop_duplicates(subset="line_id")
    r = r.explode("geometry")
    return gp.GeoDataFrame(r.reset_index(drop=True), crs=CRS)


# Ryde Pier Head-Portsmouth Harbour


def get_ryde_portsmouth_wightlink(network_model):
    """get_ryde_portsmouth_wightlink: cross the seven seas to Ryde"""
    tag = {"name": "Wightlink: Ryde - Portsmouth Fastcat (passenger)"}
    ferry = ox.features.features_from_place("Portsmouth", tags=tag).reset_index()
    r = ferry["geometry"].to_crs(CRS).reset_index(drop=True)
    i, _ = STRtree(r).query(network_model["geometry"], predicate="dwithin", distance=85)
    point = nearest_points(r.values, network_model["geometry"].iloc[i].values)
    s = gp.GeoSeries([LineString(k) for k in zip(*point)], crs=CRS)
    r = pd.concat([r, s]).reset_index(drop=True).to_frame("geometry")
    column = ["ASSET_ID", "ELR", "TRACK_ID", "OWNER"]
    r[column] = "\t\t\tNETWORK RAIL:0%,THIRD PARTY:100%,UNCLASSIFIED:0%".split("\t")
    column = ["START", "END", "VERSION"]
    r[column] = 0.0, 0.0, 1.00
    r["EXTRACTED"] = dt.datetime.today().date()
    r.loc[ferry.index, "ASSET_ID"] = ferry["id"]
    return r


def get_mersey_network(network_model, active_station):
    """get_mersey_point: add extra OSM value for disembarcation point"""
    ix = network_model["ASSET_ID"].isin(["9001051907"])
    s = network_model[ix]
    ix = active_station["CRS"].isin(["OMS"])
    r = active_station[ix].reset_index(drop=True)
    s = snap_point_line(r["geometry"], s["geometry"])
    r["geometry"] = s
    key = ["Name", "CRS", "CommonName", "LocalityName", "Status"]
    r.loc[:, key] += "*"
    return r


def get_csr_array(edge):
    """get_csr_array: return compressed sparse array"""
    column = ["source", "target"]
    imax = np.max(edge[column]) + 1
    data = (
        edge["source"].astype(np.int32),
        edge["target"].astype(np.int32),
    )
    r = sparse.csr_array(
        (
            edge["length"].values,
            data,
        ),
        shape=(imax, imax),
    )
    return r


def set_simple_model(active_station, rhye_fix=True):
    """set_simple_model: read centre-line track-model to LineString

    :param rhye_fix: add a random Isle of Wight ferry path

    """
    try:
        r = read_dataframe(OUTPATH, layer="simple-model")
        s = read_dataframe(OUTPATH, layer="rail-station")
        return r, s
    except (DataSourceError, DataLayerError):
        try:
            network_model = read_dataframe(OUTPATH, layer="network-model")
        except (DataSourceError, DataLayerError):
            network_model = read_dataframe(
                "data/network-model.gpkg", layer="TrackCentreLine"
            )
        network_model = network_model.to_crs(CRS)
    mersey_station = get_mersey_network(network_model, active_station)
    s = pd.concat([active_station, mersey_station]).reset_index(drop=True)
    ix = network_model["ASSET_ID"] == "9001037759"
    r = network_model[~ix]
    if rhye_fix:
        r = get_ryde_portsmouth_wightlink(network_model)
        r = pd.concat([network_model, r]).reset_index(drop=True)
    write_dataframe(r, OUTPATH, layer="network-model")
    r = split_network(r, s)
    write_dataframe(r, OUTPATH, layer="simple-model")
    write_dataframe(s, OUTPATH, layer="rail-station")
    return r, s


def get_odm_model():
    """get_odm_model2: read odm_model generated using ORR and other data"""
    filepath = "work/odm-station.gpkg"
    return read_dataframe(filepath, layer="odm_model")


def get_station():
    """get_odm_model: read odm_model generated using ORR and other data"""
    filepath = "work/odm-station.gpkg"
    return read_dataframe(filepath, layer="odm_station")


def get_crow_distance(edge, node):
    """get_crow_distance:

    :param edge:
    :param node:

    """
    column = ["o_CRS", "d_CRS"]
    node_map = node.set_index("CRS")["geometry"]
    s = node_map.loc[edge[column].values.ravel()].get_coordinates().values
    # s = pd.Series(, index=r.index) / 1.0e3
    # s = gp.GeoSeries(map(LineString, s), index=edge.index, crs=CRS).length
    s = np.fromiter(starmap(euclidean, s.reshape(-1, 2, 2)), dtype=float)
    return pd.Series((s / 1.0e3).round(2), index=edge.index, name="crow-km")


def set_distance_model(odm_model, nx_list, node, d):
    """set_distance_model:

    :param odm_model:
    :param station_point:
    :param edge:
    :param full_model:

    """
    column = (
        """o_CRS,d_CRS,20182019,20192020,20202021,20212022,20222023,20232024,"""
        """o_name,o_region,d_name,d_region,o_nlc,d_nlc,crow-km,distance-km"""
    ).split(",")
    try:
        r = read_dataframe(OUTPATH, layer="distance_model")
        r = r.set_index(["o_CRS", "d_CRS"], drop=False)
    except (DataSourceError, DataLayerError):
        r = odm_model.set_index(["o_CRS", "d_CRS"], drop=False)
        r["crow-km"] = get_crow_distance(r, node)
        s = get_crs_edge_distance(d, node, nx_list)
        r["distance-km"] = s.loc[r.index] / 1.0e3
        write_dataframe(r[column], OUTPATH, layer="distance_model")
    return r[column]


def set_point_model(odm_model, node):
    """set_point_model:"""
    column = (
        """node,length,source_CRS,source_NLC,geometry,20182019,20192020,20202021,20212022,"""
        """20222023,20232024"""
    ).split(",")
    r = odm_model[odm_model["o_CRS"] == odm_model["d_CRS"]].copy()
    node_map = node.set_index("CRS")[["node", "geometry"]]
    r[["node", "geometry"]] = node_map.loc[r["o_CRS"]].values
    r["length"] = 0.0
    r = r.rename(columns={"o_CRS": "source_CRS", "o_nlc": "source_NLC"})
    filepath = "output/all_point.gpkg"
    r = gp.GeoDataFrame(r[column], crs=CRS)
    r.to_file(filepath, layer="point")


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
    r = journey.copy().reset_index()
    column = [f"j2_{i}" for i in financial_year]
    r[column] = r[financial_year]
    r[financial_year] = 2 * r[financial_year]
    year_length = get_yearlength(financial_year)
    column = [f"w2_{i}" for i in financial_year]
    r[column] = r[financial_year] * 7.0 / year_length
    r = r[~r.index.duplicated()]
    return r


def get_edge_list_path(edge, node):
    """get_edge_node_segment:"""
    column = ["source", "target"]
    s = edge.set_index(column, drop=False)
    edge_csr = get_csr_array(s)
    n, node["connection"] = connected_components(edge_csr)
    print(f"\t{n} component")
    nx_list = node.loc[node["CRS"] != "", "node"].values
    d, nx_path = shortest_path(
        csgraph=edge_csr, indices=nx_list, directed=False, return_predecessors=True
    )
    return nx_list, nx_path, d


def get_crs_edge_distance(d, node, nx_list):
    """get_crs_edge_distance:"""
    dmask = np.zeros(d.shape[1], dtype=bool)
    dmask[nx_list] = True
    dmask = np.repeat(dmask, d.shape[0]).reshape(-1, d.shape[0]).T
    r = d[dmask].reshape(d.shape[0], -1).ravel()
    ix = np.asarray(np.meshgrid(nx_list, nx_list)).T.reshape(-1, 2)
    r = pd.Series(r, index=pd.MultiIndex.from_arrays(ix.T))
    node_map = node.set_index("node").loc[nx_list, "CRS"]
    r.index = pd.MultiIndex.from_arrays(node_map[ix.ravel()].values.reshape(-1, 2).T)
    return r


def nx_chain_len(path, nx_list):
    """nx_chain: get sorted list of nodes in path if in nx_list"""
    if path is None:
        return 0
    return len(set(path).intersection(nx_list))


def get_all_cs_segment(odm_path, nx_list):
    """get_all_cs_segment: get all shortest node path lists"""
    data = []
    for i, j in enumerate(nx_list):
        if i % 64 == 0:
            print(str(i).rjust(8), str(j).rjust(8))
        r = get_source_cs_segment(j, odm_path, nx_list)
        if r.empty:
            continue
        data.append(r)
    r = pd.concat(data)
    get_nx_chain_len = partial(nx_chain_len, nx_list=nx_list)
    r["count"] = r.map(get_nx_chain_len)
    r = r[r["count"] == 2]
    r["source"] = r["path"].str[0]
    r["target"] = r["path"].str[-1]
    column = ["source", "target"]
    ix = r[column].apply(sorted, axis=1).map(tuple)
    r.index = pd.MultiIndex.from_tuples(ix, names=["source", "target"])
    return r


def get_source_cs_segment(source, odm_path, nx_list):
    """get_source_cs_path: get all shortest node path list if in node_list"""
    base_list = np.asarray(nx_list)
    base_list = base_list[base_list >= source]
    data = np.asarray(base_list)
    ix = np.where(source == np.asarray(nx_list))[0][0]
    path = odm_path[ix][base_list]
    mask = path == -9999
    j = 0
    while not mask.all():
        data = np.vstack([data, path])
        k = np.where(np.isin(path, nx_list))[0]
        path[k] = -9999
        path[mask] = -9999
        mask = path == -9999
        i = np.where(mask)[0]
        path[i] = i
        path = odm_path[ix][path]
        path[i] = -9999
        j = j + 1
    s = data.T
    r = pd.Series([i[i != -9999][::-1] for i in s], index=base_list)
    r = r[r.str[0] == source]
    r = r.to_frame("path")
    return r


def set_simple_node_edge(rail_model, active_station):
    """set_simple_edge_node:"""
    try:
        node = read_dataframe(OUTPATH, layer="simple_node")
        edge = read_dataframe(OUTPATH, layer="simple_edge")
        return node, edge
    except (DataSourceError, DataLayerError):
        pass
    rail_model, station_point = set_simple_model(active_station)
    edge, node = get_station_edge_node(rail_model, station_point)
    edge_list, edge_path, _ = get_edge_list_path(edge, node)
    edge_segment = get_all_cs_segment(edge_path, edge_list)
    # column = ["source", "target"]
    edge_model = get_nx_geometry(edge_segment["path"], edge, station_point)
    edge, node = get_station_edge_node(edge_model, station_point)
    write_dataframe(node, OUTPATH, layer="simple_node")
    write_dataframe(edge, OUTPATH, layer="simple_edge")
    return node, edge


def set_combined_data(journey, financial_year):
    """set_combined_data:

    :param journey: GeoDataFrame with passenger data
    :param financial_year: list of financial years str

    """
    # write_dataframe(journey, "journeys-all.gpkg", layer="journey")
    # ix = (journey[financial_year] > 0).any(axis=1)
    # write_dataframe(journey[ix], "journeys-all.gpkg", layer="journey_model")
    r = journey.copy()
    r[financial_year] = 2 * r[financial_year]
    write_dataframe(journey, "journeys-all.gpkg", layer="journey")
    j2_model = get_j2_model(journey, financial_year)
    write_dataframe(j2_model, "journeys-all.gpkg", layer="j2")
    ix = (j2_model[financial_year] > 0).any(axis=1)
    write_dataframe(j2_model[ix], "journeys-all.gpkg", layer="j2_model")


def source_cs_path(source, odm_path, nx_list):
    """get_source_cs_path3: get all shortest node path list if in node_list"""
    base_list = np.asarray(nx_list)
    # base_list = base_list[base_list >= source]
    data = np.asarray(base_list)
    ix = np.where(source == np.asarray(nx_list))[0][0]
    path = odm_path[ix][base_list]
    mask = path == -9999
    while not mask.all():
        data = np.vstack([data, path])
        path[mask] = -9999
        mask = path == -9999
        i = np.where(mask)[0]
        path[i] = i
        path = odm_path[ix][path]
        path[i] = -9999
    r = pd.Series([i[i != -9999][::-1] for i in data.T], index=base_list)
    r = r.to_frame("path")
    if r.empty:
        r = pd.DataFrame([], columns=["path", "count", "source", "target"])
        r.index = pd.MultiIndex.from_arrays([[]] * 2)
        return r
    get_nx_chain_len = partial(nx_chain_len, nx_list=nx_list)
    r["count"] = r.map(get_nx_chain_len)
    r = r[r["count"] > 1]
    r["source"] = source
    r["target"] = r["path"].str[-1]
    column = ["source", "target"]
    ix = r[column].apply(sorted, axis=1).map(tuple)
    r.index = pd.MultiIndex.from_tuples(ix, names=["source", "target"])
    return r.sort_index()


def crs_model(source, ns):
    """crs_model": calculate shortest path segments and aggregated passenger flow"""
    financial_year = ns.journey_model.columns
    crs = ns.node.loc[source, "CRS"]
    print(f"{crs.rjust(4)}{str(source).rjust(8)}\tpid {str(os.getpid()).rjust(8)}")
    r = source_cs_path(source, ns.nx_path, ns.nx_list)
    r["o_NLC"] = ns.node.loc[r["source"], "NLC"].values
    r["d_NLC"] = ns.node.loc[r["target"], "NLC"].values
    r = r.set_index(["o_NLC", "d_NLC"])
    r["path"] = r["path"].map(pairwise).map(list)
    r[financial_year] = 0
    ix = r.index.intersection(ns.journey_model.index)
    r.loc[ix, financial_year] = ns.journey_model.loc[ix]
    r = r.explode("path")
    ix = r["path"].map(sorted).map(tuple)
    r.index = pd.MultiIndex.from_tuples(ix, names=["source", "target"])
    r = r[financial_year].groupby(level=[0, 1]).sum()
    # write_crs(crs, r, ns.journey, ns.node)
    return source, r


def write_crs(source, model, ns):
    """write_crs:"""
    crs = ns.node.loc[source, "CRS"]
    financial_year = ns.journey_model.columns
    print(f"write {crs}\tpid {str(os.getpid()).rjust(8)}")
    r = ns.journey.copy()
    financial_year = get_financial_year(r.columns)
    r[financial_year] = 0
    ix = model.index.intersection(r.index)
    r.loc[ix, financial_year] = model.loc[ix]
    filepath = f"output/{crs}.gpkg"
    r.loc[model.index].to_file(filepath, layer=crs, engine="pyogrio")
    return model


def get_financial_year(columns):
    """get_financial_year: list with names of FY columns"""
    return [i for i in columns if i[:2] == "20"]


def initialize_model(namespace):
    """initialize_model:"""
    odm_model = get_odm_model()
    financial_year = get_financial_year(odm_model.columns)
    active_station = get_station()
    node, edge = set_simple_node_edge(odm_model, active_station)
    nx_list, nx_path, d = get_edge_list_path(edge, node)
    _ = set_distance_model(odm_model, nx_list, node, d)
    set_point_model(odm_model, node)
    journey = edge.set_index(["source", "target"]).copy()
    journey[financial_year] = 0
    journey_model = odm_model.set_index(["o_nlc", "d_nlc"])[financial_year]
    namespace.nx_path = nx_path
    namespace.nx_list = nx_list
    namespace.node = node
    namespace.journey_model = journey_model
    namespace.journey = journey
    return nx_list, journey.copy()


def main():
    """main: script execution point"""
    print("initialize model")
    ns = Manager().Namespace()
    nx_list, journey = initialize_model(ns)
    print("get model")
    financial_year = get_financial_year(journey.columns)
    nthread = os.cpu_count() - 1
    chunksize = int(np.ceil(len(nx_list) / nthread))
    get_crs_model = partial(crs_model, ns=ns)
    write_model = partial(write_crs, ns=ns)
    with Pool(processes=nthread) as pool:
        r = pool.imap_unordered(get_crs_model, nx_list, chunksize)
        r = pool.starmap(write_model, r)
        s = pd.concat(r).groupby(level=[0, 1]).sum()
        journey.loc[s.index, financial_year] += s
    set_combined_data(journey, financial_year)
    print("finish model")


if __name__ == "__main__":
    main()
