import pandas as pd
import geopandas as gpd
import numpy as np
from numpy import exp, loadtxt, pi, sqrt
import math
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import swiftclient
import json
from io import StringIO
import folium
from folium import plugins
import geopy
from shapely.geometry import Polygon
from geopy.distance import VincentyDistance
import sys

sys.settrace

config_path = '../config.json'
with open(config_path) as json_data_file:
    config = json.load(json_data_file)
    
def swift_con(config):
    user=config['swift_storage']['user']
    key=config['swift_storage']['key']
    auth_url=config['swift_storage']['auth_url']
    tenant_name=config['swift_storage']['tenant_name']
    auth_version=config['swift_storage']['auth_version']
    options = config['swift_storage']['options']
    return swiftclient.Connection(user=user,
                                  key=key,
                                  authurl=auth_url,
                                  os_options=options,
                                  tenant_name=tenant_name,
                                  auth_version=auth_version)

conn = swift_con(config)

csv = conn.get_object("oco2", "/datasets/oco-2/peaks-detected/result_for_oco2_1808.csv")[1]
peak_fc = pd.read_csv(StringIO(str(csv, 'utf-8')),index_col=0)

from datetime import datetime
def to_date(a):
    return datetime.strptime(str(a), '%Y%m%d%H%M%S%f')

peak_fc['date'] = peak_fc['sounding_id'].apply(to_date)
peak_fc['sigma'] = peak_fc['sigma'].apply(abs)
peak_fc['amplitude'] = peak_fc['amplitude'].apply(abs)

peak_fc = gpd.GeoDataFrame(peak_fc, geometry=gpd.points_from_xy(peak_fc.longitude, peak_fc.latitude)).copy()
peak_fc.crs = {'init': 'epsg:4326'}

# Charlotte & Raphaele's method
path_peaks = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/output/peaks_out_1808.csv"
peak_cr = pd.read_csv(path_peaks, sep=",")

peak_cr = gpd.GeoDataFrame(peak_cr, geometry=gpd.points_from_xy(peak_cr.longitude, peak_cr.latitude)).copy()
peak_cr.crs = {'init': 'epsg:4326'}

path_cities = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/cities_v1.csv"
cities = pd.read_csv(path_cities, sep=",", index_col=0)

cities = gpd.GeoDataFrame(cities, geometry=gpd.points_from_xy(cities.longitude, cities.latitude))
cities.crs = {'init': 'epsg:4326'}

path_plants = "https://raw.githubusercontent.com/dataforgoodfr/batch7_satellite_ges/master/dataset/CO2_emissions_centrale.csv"
plants = pd.read_csv(path_plants, sep=",", index_col=0)

plants = gpd.GeoDataFrame(plants, geometry=gpd.points_from_xy(plants.longitude, plants.latitude))
plants.crs = {'init': 'epsg:4326'}

def get_direction_from_uv(u, v):
    ''' Retrieve the heading of a vector'''
    direction = 180/math.pi * math.atan2(u,v)+180
    return direction

def get_wind_norm_from_uv(u, v):
    ''' Retrieve the magitude of a vector'''
    return math.sqrt(pow(u,2)+pow(v,2))

def get_new_coord(lat, lon, d, b):
    ''' Calculate the arrival point of a vector, given a starting point, a distance and a direcion'''
    origin = geopy.Point(lat, lon)
    point = VincentyDistance(kilometers=d).destination(origin, b)
    return [point[1], point[0]]

def capture_zone(lat, lon, u, v, angle=50):
    ''' Calculates the capture zone around a point, given the point, a wind vector and a angme to shape the zone'''
    wind_heading = get_direction_from_uv(u, v)
    wind_norm = get_wind_norm_from_uv(u,v)*3.6

    # BACK LINE
    # 1st point (back - 6h wind)
    point_1 = get_new_coord(lat, lon, wind_norm*6, wind_heading+180)
    # 2nd point (back - 6h wind - 50°)
    point_2 = get_new_coord(lat, lon, wind_norm*6, wind_heading+180-angle)
    # 3rd point (back - 6h wind - 50°)
    point_3 = get_new_coord(lat, lon, wind_norm*6, wind_heading+180+angle)

    # FRONT LINE
    # 4th point (front - 24h wind - 20°)
    point_4 = get_new_coord(lat, lon, wind_norm*24, wind_heading+angle)
    # 5th point (front - 24h wind - 20°)
    point_5 = get_new_coord(lat, lon, wind_norm*24, wind_heading-angle)
    # 6th point (front - 24h wind)
    point_6 = get_new_coord(lat, lon, wind_norm*24, wind_heading)

    points = [point_1, point_2, point_4, point_6, point_5, point_3, point_1]
    return points

def capture_df(row):
    ''' Apply the capture zone function to a dataset row'''
    return Polygon(capture_zone(row["latitude"], row["longitude"], row['windspeed_u'], row['windspeed_v']))

def join_and_count(peaks, cities, plants):
    ''' Spacially join 3 datasets (Polygon, Point and Point)'''
    #intersect cities
    peaks_intersect_invent  = gpd.sjoin(peaks,cities.loc[:, ['Population (CDP)','geometry']], how='left', op='intersects').rename(columns={"index_right": "index_cities"})
    peaks_intersect_ag = peaks_intersect_invent.groupby([peaks_intersect_invent.index, 'sounding_id'])["index_cities"].apply(list).reset_index()
    peaks_intersect_ag["number_cities"] = peaks_intersect_ag["index_cities"].apply(lambda x: np.count_nonzero(~np.isnan(x)))
    peaks_meters_cities = peaks.merge(peaks_intersect_ag, how='left', on='sounding_id')
    
    #intersect plants
    peaks_intersect_plants  = gpd.sjoin(peaks,plants.loc[:, [ 'estimated_generation_gwh', 'geometry']], how='left', op='intersects').rename(columns={"index_right": "index_plants"})
    peaks_intersect_ag = peaks_intersect_plants.groupby([peaks_intersect_plants.index, 'sounding_id'])["index_plants"].apply(list).reset_index()
    peaks_intersect_ag["number_plants"] = peaks_intersect_ag["index_plants"].apply(lambda x: np.count_nonzero(~np.isnan(x)))
    peaks_meters_plants = peaks.merge(peaks_intersect_ag, how='left', on='sounding_id')

    # join
    peaks_and_sources = peaks_meters_cities.merge(peaks_intersect_ag, how='left', on='sounding_id')
    peaks_and_sources = peaks_and_sources.drop(columns=['level_0_x', 'level_0_y'])
    return peaks_and_sources

peaks_fc = peak_fc.loc[(peak_fc["longitude"] < 175) & (peak_fc["longitude"] > -175), :]
#peaks_fc['geometry'] = peaks_fc.apply(capture_df, axis=1)
#peaks_fc_and_sources = join_and_count(peaks_fc, cities, plants)
