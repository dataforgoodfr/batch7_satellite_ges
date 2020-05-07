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

peaks_meters = peaks.to_crs(epsg=2272)
km_buffer = peaks_meters.geometry.buffer(10 * 5280)
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
