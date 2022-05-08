import pandas as pd
import geopandas as gpd
import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster
import numpy as np
import webbrowser
from scipy import stats
import math

peaks = pd.read_csv("dataset/output/peaks_out_1808.csv")
peaks = gpd.GeoDataFrame(peaks, geometry=gpd.points_from_xy(peaks.longitude, peaks.latitude))
peaks.crs = {'init': 'epsg:4326'}

df = pd.read_csv("dataset/CO2_emissions_Edgar_2018_v3.csv")
df["CO2 classification"].value_counts()
co2 = df.loc[df["CO2 classification"] == 4, ['latitude', 'longitude', 'CO2 emissions']].copy()
co2 = co2.rename(columns={"CO2 emissions": "co2em"})
co2 = gpd.GeoDataFrame(co2, geometry=gpd.points_from_xy(co2.longitude, co2.latitude))
co2.crs = {'init': 'epsg:4326'}

centrales = pd.read_csv("dataset/CO2_emissions_centrale.csv")
centrales = centrales.loc[:, ["latitude", "primary_fuel", "longitude", "tCO2_emitted_in_2017"]].copy()
centrales = gpd.GeoDataFrame(centrales, geometry=gpd.points_from_xy(centrales.longitude, centrales.latitude))
centrales.crs = {'init': 'epsg:4326'}

cities = pd.read_csv("dataset/cities_v1.csv")
cities = cities.loc[:, ["latitude", "longitude", 'Population (CDP)']].copy()
cities = gpd.GeoDataFrame(cities, geometry=gpd.points_from_xy(cities.longitude, cities.latitude))
cities.rename(columns={"'Population (CDP)'": "pop"}, inplace=True)
cities.crs = {'init': 'epsg:4326'}

style1 = {'fillColor': '#228B22', 'color': '#228B22'}
style2 = {'fillColor': '#00FFFFFF', 'color': '#00FFFFFF'}


# We use the pseudo mercator to calculate distance in meters (careful bit distorted at poles)

# TODO : buffer radius should depend on windspeed and wind direction


def compute_buffers(peaks, km, y_class="y_class_dbscan_only_gaussian_param"):
    peaks = peaks.copy()
    peaks_meters = peaks.to_crs(epsg=3857)
    # Pour éviter les effets de bords de la carte on enlève ces peaks pour cette visualisation
    peaks_meters = peaks_meters.loc[(peaks_meters["longitude"] < 177) & (peaks_meters["longitude"] > -177), :]
    km_buffer = peaks_meters.loc[:, "geometry"].buffer(km * 1000)
    peaks_meters["buffer"] = km_buffer
    km_buffer_normal = peaks_meters.loc[peaks_meters[y_class] == 1, "geometry"].buffer(km * 1000)
    km_buffer_abnormal = peaks_meters.loc[peaks_meters[y_class] == -1, "geometry"].buffer(km * 1000)
    return peaks_meters, km_buffer_normal, km_buffer_abnormal


def draw_buffer_map(peaks, km, y_class="y_class_dbscan_only_gaussian_param"):
    """ ce n'est qu'une carte qui me permettait de voir les 2 types de peak rapidement,
    il faudrait juste intégrer les buffer dans la carte de Quentin K"""
    _, km_buffer_normal, km_buffer_abnormal = compute_buffers(peaks, km, y_class)
    m = folium.Map(location=[39.9526, -75.1652], zoom_start=3)
    folium.GeoJson(km_buffer_normal.to_crs(epsg=4326), name='normal', style_function=lambda x: style1).add_to(m)
    folium.GeoJson(km_buffer_abnormal.to_crs(epsg=4326), name='abnormal', style_function=lambda x: style2).add_to(m)
    HeatMap(data=co2[['latitude', 'longitude']], radius=3, blur=2, gradient={1: "red"}).add_to(m)

    mc = MarkerCluster()
    for idx, row in centrales.iterrows():
        if not math.isnan(row['longitude']) and not math.isnan(row['latitude']):
            mc.add_child(Marker([row['latitude'], row['longitude']], color="green"))
    m.add_child(mc)

    cit = MarkerCluster()
    for idx, row in cities.iterrows():
        if not math.isnan(row['longitude']) and not math.isnan(row['latitude']):
            mc.add_child(Marker([row['latitude'], row['longitude']], color="orange"))
    m.add_child(cit)
    m.save("dataset/output/cities_peak.html")
    # webbrowser.open("dataset/output/cities_peak.html", new=2)
    return None


draw_buffer_map(peaks, km=100)


def spatial_join_peak_inventory(peaks):
    peaks_meters, _, _ = compute_buffers(peaks, 100)
    peaks_meters = peaks_meters.drop(columns="geometry")
    peaks_meters.rename(columns={"buffer": "geometry"}, inplace=True)

    peaks_meters = peaks_meters.to_crs(epsg=4326)
    peaks_meters.crs = {'init': 'epsg:4326'}

    peaks_intersect_invent = gpd.sjoin(peaks_meters, cities.loc[:, ['Population (CDP)', 'geometry']],
                                       how="left", op='intersects').rename(columns={"index_right": "index_cities"})
    peaks_intersect_invent.info()
    peaks_intersect_ag = \
        peaks_intersect_invent.groupby([peaks_intersect_invent.index, 'sounding_id'])["index_cities"].apply(
            list).reset_index()
    peaks_intersect_ag["number_cities"] = peaks_intersect_ag["index_cities"].apply(
        lambda x: np.count_nonzero(~np.isnan(x)))
    peaks_intersect_ag["number_cities"].describe()
    peaks_intersect_ag = peaks_intersect_ag.drop(columns="level_0")
    peaks_meters_cities = peaks_meters.merge(peaks_intersect_ag, how='left', on='sounding_id')

    peaks_intersect_centrales = gpd.sjoin(peaks_meters_cities, centrales.loc[:, ["geometry", "primary_fuel",
                                                                                 "tCO2_emitted_in_2017"]],
                                          how="left", op='intersects').rename(
        columns={"index_right": "index_centrales"})
    peaks_intersect_centrales.info()
    peaks_intersect_ag = \
        peaks_intersect_centrales.groupby([peaks_intersect_centrales.index, 'sounding_id'])["index_centrales"].apply(
            list).reset_index()
    peaks_intersect_ag["number_centrales"] = peaks_intersect_ag["index_centrales"].apply(
        lambda x: np.count_nonzero(~np.isnan(x)))
    peaks_intersect_ag["number_centrales"].describe()
    peaks_intersect_ag = peaks_intersect_ag.drop(columns="level_0")
    peaks_meters_cities_centrales = peaks_meters_cities.merge(peaks_intersect_ag, how='left', on='sounding_id')
    return peaks_meters_cities_centrales


peaks_meters_cities_centrales = spatial_join_peak_inventory(peaks)


def define_metric_outlier(peaks_meters_cities_centrales, y_class):
    peaks_meters_cities_centrales.loc[peaks_meters_cities_centrales[y_class] == -1,
                                      "number_centrales"].mean()
    peaks_meters_cities_centrales.loc[peaks_meters_cities_centrales[y_class] == 1,
                                      "number_centrales"].mean()
    t, p = stats.ttest_ind(peaks_meters_cities_centrales.loc[peaks_meters_cities_centrales[y_class] == -1,
                                      "number_centrales"],
                          peaks_meters_cities_centrales.loc[peaks_meters_cities_centrales[y_class] == 1,
                                                            "number_centrales"], equal_var=False
                          )
    print(t, p)
    return None