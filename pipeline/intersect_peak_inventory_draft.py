import pandas as pd
import geopandas as gpd
import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster
import webbrowser
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

peaks_meters = peaks.to_crs(epsg=3857)
km_buffer = peaks_meters.geometry.buffer(100 * 1000)
# km_buffer = peaks.geometry.buffer(2)
km_buffer.head()
# km_buffer.plot()
m = folium.Map(location=[39.9526, -75.1652], zoom_start=3)
folium.GeoJson(km_buffer.to_crs(epsg=4326)).add_to(m)
m.save("dataset/output/cities_peak.html")
# peaks_meters.to_crs(epsg=4326).plot()
webbrowser.open("dataset/output/cities_peak.html", new=2)

km_buffer = peaks.loc[peaks["y_class_dbscan_only_gaussian_param"] == 1, "geometry"].buffer(1)
# km_buffer = peaks.geometry.buffer(2)
km_buffer.head()
style1 = {'fillColor': '#228B22', 'color': '#228B22'}
style2 = {'fillColor': '#00FFFFFF', 'color': '#00FFFFFF'}
# km_buffer.plot()
m = folium.Map(location=[39.9526, -75.1652], zoom_start=3)
folium.GeoJson(km_buffer, name='test1', style_function=lambda x: style1).add_to(m)
# m.save("dataset/output/cities_peak.html")
# peaks_meters.to_crs(epsg=4326).plot()
km_buffer2 = peaks.loc[peaks["y_class_dbscan_only_gaussian_param"] == -1, "geometry"].buffer(1)
# km_buffer = peaks.geometry.buffer(2)
km_buffer2.head()
folium.GeoJson(km_buffer2, name='test1', style_function=lambda x: style2).add_to(m)
m.save("dataset/output/cities_peak.html")
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
webbrowser.open("dataset/output/cities_peak.html", new=2)

# Spatial joins, il faudra mettre le buffer en km
peaks_buffer = peaks.to_crs(epsg=2272).copy()
peaks_buffer["geometry"] = peaks_buffer.loc[:, "geometry"].buffer(120 * 5280)  # ~200 km
peaks_buffer = peaks_buffer.to_crs(epsg=4326)
peaks_buffer.crs = {'init': 'epsg:4326'}
peaks_intersect_invent = gpd.sjoin(peaks_buffer, cities.loc[:, ['Population (CDP)', 'geometry']], how="left",
                                   op='intersects').rename(columns={"index_right": "index_cities"})
peaks_intersect_invent.info()
peaks_intersect_invent = gpd.sjoin(peaks_intersect_invent, centrales.loc[:, ["geometry", "primary_fuel",
                                                                             "tCO2_emitted_in_2017"]],
                                   how="left", op='intersects').rename(columns={"index_right": "index_centrales"})
peaks_intersect_invent.info()

for c in ["index_centrales"]:
peaks_intersect_ag = \
    peaks_intersect_invent.groupby([peaks_intersect_invent.index, 'sounding_id', 'latitude', 'longitude', 'orbit',
                                'slope', 'intercept', 'amplitude', 'sigma', 'delta', 'R', 'windspeed_u', 'windspeed_v',
                                'surface_pressure', 'surface_pressure_apriori', 'land_water_indicator',
                                'land_fraction', 'surf_pres', 'y_class_lof', 'outlier_score_lof',
                                'y_class_dbscan', 'y_class_lof_only_gaussian_param',
                                'y_class_dbscan_only_gaussian_param'])["index_cities"].apply(list).reset_index()


# Geodesic buffer around peak : Azimuthal equidistant projection
# https://gis.stackexchange.com/questions/289044/creating-buffer-circle-x-kilometers-from-point-using-python
from functools import partial
import pyproj
from shapely.ops import transform
from shapely.geometry import Point

proj_wgs84 = pyproj.Proj(init='epsg:4326')


def geodesic_point_buffer(lat, lon, km):
    # Azimuthal equidistant projection
    aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)),
        proj_wgs84)
    buf = Point(0, 0).buffer(km * 1000)  # distance in metres
    return transform(project, buf).exterior.coords[:]

# Example
b = geodesic_point_buffer(45.4, -75.7, 100.0)
buffer_around_one_peak = geodesic_point_buffer(peaks.loc[0, "latitude"], peaks.loc[0, "longitude"], 200)
buffer_around_one_peak

from shapely.geometry import Polygon

polygon_geom = Polygon(buffer_around_one_peak)
crs = {'init': 'epsg:4326'}
polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])
print(polygon.geometry)
polygon.plot()


# On fait ceci pour tous les peaks, apres on fera dépend de la force du vent
# pour le moment 200 km
peaks["polygon"] =  peaks.apply(lambda df: geodesic_point_buffer(df["latitude"], df["longitude"], 100),
                                axis = 1)
peaks["polygon"] = peaks["polygon"].apply(lambda x: Polygon(x))
peaks.columns
peaks_with_buffers = peaks.drop(columns="geometry")
peaks_with_buffers = peaks_with_buffers.rename(columns={"polygon": "geometry"})
crs = {'init': 'epsg:4326'}
polygon = gpd.GeoDataFrame(peaks_with_buffers, crs=crs)
print(polygon.geometry)
polygon.plot()

m = folium.Map(location=[39.9526, -75.1652], zoom_start=3)
folium.GeoJson(polygon).add_to(m)
m.save("dataset/output/buffers.html")
# peaks_meters.to_crs(epsg=4326).plot()
webbrowser.open("dataset/output/buffers.html", new=2)
