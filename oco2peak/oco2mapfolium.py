# AUTOGENERATED! DO NOT EDIT! File to edit: notebooks/WIP_OCO2_Map.ipynb (unless otherwise specified).

__all__ = ['inventory_map_only', 'peaks_capture_map', 'stats_invent']

# Cell
import pandas as pd
import geopandas as gpd
import numpy as np
from numpy import exp, loadtxt, pi, sqrt, log
import math
import matplotlib
import matplotlib.pyplot as plt
import swiftclient
import json
from io import StringIO
import folium
from folium import plugins
import geopy
from shapely.geometry import Polygon
from shapely.wkt import loads
from geopy.distance import VincentyDistance

# Cell
def inventory_map_only(plants, plants_coal, cities):
    """
    Create map with inventory only
    :param plants: GeoDataFrame, Dataframe containing all registered plants.
    :param plants_coal: GeoDataFrame, Dataframe containing all registered coal plants.
    :param cities: GeoDataFrame, Dataframe containing all registered big cities.
    :return:
    """
    # Initialize Map
    inventory_map = folium.Map([43, 0], zoom_start=4)
    folium.TileLayer("CartoDB dark_matter", name="Dark mode").add_to(inventory_map)

    # Adding Power plants
    plants_group = folium.FeatureGroup(name="Plants").add_to(inventory_map)
    for index, row in plants.iterrows():
        radius = row['estimated_generation_gwh']/10000
        color="#3186CC" # blue

        tooltip =  "["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        emit = str(round(row['estimated_generation_gwh'],2))
        popup_html="""<h4>"""+tooltip+"""</h4>"""+row['country_long']+"""<p><b>Emission 2018 (est):</b> """+emit+""" GWh</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        plants_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                      row["longitude"]),
                            radius=radius,
                            color=color,
                            # popup=popup,
                            # tooltip=tooltip,
                            fill=True))

    # Adding Coal plants
    plants_coal_group = folium.FeatureGroup(name="Coal Plants").add_to(inventory_map)
    for index, row in plants_coal.iterrows():
        radius = row['Annual CO2 emissions (millions of tonnes) in 2018']/10000
        color="#FF3333" # red

        tooltip =  "["+str(round(row['Latitude'],2))+" ; "+str(round(row['Longitude'],2))+"]"
        emit = str(round(row['Annual CO2 emissions (millions of tonnes) in 2018'],2))
        popup_html="""<h4>"""+tooltip+"""</h4>"""+str(row['Plant'])+"""<p><b>Emission 2018 (est):</b> """+emit+""" GWh</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        plants_coal_group.add_child(folium.CircleMarker(location=(row["Latitude"],
                                      row["Longitude"]),
                            radius=radius,
                            color=color,
                            # popup=popup,
                            # tooltip=tooltip,
                            fill=True))



    # Adding Cities
    cities_group = folium.FeatureGroup(name="Cities").add_to(inventory_map)
    for index, row in cities.iterrows():
        radius = row['Population (CDP)']/2000000
        color="#FEF65B" # yellow

        tooltip =  "["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        pop = str(round(row['Population (CDP)'],0))
        title = "" + str(row['City name']) + ", " + str(row['Country'])
        popup_html="""<h4><b>"""+row["City name"]+"""</b>, """+row["Country"]+"""</h4>"""+"""<p>"""+tooltip+"""</p>"""+"""<p>Population 2017: """+pop+"""</p>"""
        popup_html = """<h4>"""+title+"""</h4><p>"""+tooltip+"""</p>"""+"""<p><b>Population 2017:</b> """+pop+"""</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        cities_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                      row["longitude"]),
                            radius=radius,
                            color=color,
                            # popup=popup,
                            # tooltip=tooltip,
                            fill=True))


    folium.map.LayerControl(collapsed=False).add_to(inventory_map)

    plugins.Fullscreen(
        position='topright',
        title='Expand me',
        title_cancel='Exit me',
        force_separate_button=True
    ).add_to(inventory_map)

    minimap = plugins.MiniMap()
    inventory_map.add_child(minimap)

    #inventory_map.save("inventory_map.html")
    return inventory_map



def peaks_capture_map(peaks, plants, plants_coal, cities):
    """
    Create map with peaks (marker + capture zone) and inventory
    :param peaks: GeoDataFrame, Dataframe containing the peaks we want to display.
    :param plants: GeoDataFrame, Dataframe containing all registered plants.
    :param plants_coal: GeoDataFrame, Dataframe containing all registered coal plants.
    :param cities: GeoDataFrame, Dataframe containing all registered big cities.
    :return:
    """
    # Initialize Map
    peaks_capture = folium.Map([40, -100], zoom_start=4)
    folium.TileLayer("CartoDB dark_matter", name="Dark mode").add_to(peaks_capture)

    # Adding Power plants
    plants_group = folium.FeatureGroup(name="Plants").add_to(peaks_capture)
    for index, row in plants.iterrows():
        color="#17a589" # ligh blue-ish
        radius = row['estimated_generation_gwh']/10000

        tooltip =  "["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        emit = str(round(row['estimated_generation_gwh'],2))
        popup_html="""<h4>"""+tooltip+"""</h4>"""+row['country_long']+"""<p><b>Emission 2018 (est):</b> """+emit+""" GWh</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        plants_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                      row["longitude"]),
                            radius=0.1,
                            color=color,
                            tooltip=tooltip,
                            popup=popup,
                            fill=True))

    # Adding Coal plants
    plants_coal_group = folium.FeatureGroup(name="Coal Plants").add_to(peaks_capture)
    for index, row in plants_coal.iterrows():
        color="#138d75" # blue-ish
        radius = row['Annual CO2 emissions (millions of tonnes) in 2018']/10000

        tooltip =  "["+str(round(row['Latitude'],2))+" ; "+str(round(row['Longitude'],2))+"]"
        emit = str(round(row['Annual CO2 emissions (millions of tonnes) in 2018'],2))
        popup_html="""<h4>"""+tooltip+"""</h4>"""+str(row['Plant'])+"""<p><b>Emission 2018 (est):</b> """+emit+""" GWh</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        plants_coal_group.add_child(folium.CircleMarker(location=(row["Latitude"],
                                      row["Longitude"]),
                            radius=radius,
                            color=color,
                            tooltip=tooltip,
                            popup=popup,
                            fill=True))


    # Adding Cities
    cities_group = folium.FeatureGroup(name="Cities").add_to(peaks_capture)
    for index, row in cities.iterrows():
        color="#7d3c98" # purple
        radius = row['Population (CDP)']/2000000

        tooltip =  "["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        pop = str(round(row['Population (CDP)'],0))
        title = "" + str(row['City name']) + ", " + str(row['Country'])
        popup_html="""<h4><b>"""+row["City name"]+"""</b>, """+row["Country"]+"""</h4>"""+"""<p>"""+tooltip+"""</p>"""+"""<p>Population 2017: """+pop+"""</p>"""
        popup_html = """<h4>"""+title+"""</h4><p>"""+tooltip+"""</p>"""+"""<p><b>Population 2017:</b> """+pop+"""</p>"""
        popup=folium.Popup(popup_html, max_width=450)

        cities_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                      row["longitude"]),
                            radius=radius,
                            color=color,
                            tooltip=tooltip,
                            popup=popup,
                            fill=True))

    # Adding detected peaks
    peaks_group_capture = folium.FeatureGroup(name=" - 50km CirclesCapture Zone").add_to(peaks_capture)
    peaks_group = folium.FeatureGroup(name="Peaks").add_to(peaks_capture)
    for index, row in peaks.iterrows():
        radius = row["amplitude"]/20
        tooltip =  "["+str(round(row['latitude'],2))+" ; "+str(round(row['longitude'],2))+"]"
        color="#a93226" # red
        sounding = str(row['sounding_id'])
        date = str(row['date'])
        orbit = str(row['orbit'])

        popup_html="""<h4>"""+tooltip+"""</h4>"""+date+"""<p>sounding_id: """+sounding+"""</br>orbit: """+orbit+"""</p>"""
        popup_html+='<p><input type="button" value="Show plot"'
        # Injecting JavaScript in popup to fire the Dash Callback
        popup_html+='onclick="\
            let bco_input = parent.document.getElementById(\'input_sounding\'); \
            let lastValue = bco_input.value;'
        popup_html+=f'bco_input.value = \'{sounding}\';'
        popup_html+="let bco_event = new Event('input', { bubbles: true });\
            bco_event.simulated = true;\
            let tracker = bco_input._valueTracker;\
            if (tracker) {\
            tracker.setValue(lastValue);\
            }\
            bco_input.dispatchEvent(bco_event);\
            elt.dispatchEvent(new Event('change'));\
            \"/></p>"

        popup=folium.Popup(popup_html, max_width=450)

        peaks_group_capture.add_child(folium.GeoJson(row['geometry'], name=" - Capture Zone"))

        peaks_group.add_child(folium.CircleMarker(location=(row["latitude"],
                                      row["longitude"]),
                            radius=radius,
                            color=color,
                            tooltip=sounding,
                            popup=popup,
                            fill=True))

    folium.map.LayerControl(collapsed=False).add_to(peaks_capture)

    plugins.Fullscreen(
        position='topright',
        title='Expand me',
        title_cancel='Exit me',
        force_separate_button=True
    ).add_to(peaks_capture)

    minimap = plugins.MiniMap()
    peaks_capture.add_child(minimap)

    #peaks_capture.save("peaks_capture_map.html")
    return peaks_capture


def stats_invent(data, title="INVENTORY STATISTICS"):
    """
    Print statistics on peaks capture
    :param data: GeoDataFrame, Dataframe containing the peaks we want statistics on.
    :param title: str, the title on the printed stats.
    :return:
    """
    df = data[['number_cities', 'number_plants', 'number_coals']]

    print(" --- ", title, " --- ")
    print("Total count: ", df.number_cities.count())
    print("Cities mean and std: ", round(df.number_cities.mean(),2), " x ", round(df.number_cities.std(),2))
    print("Cities non-null count: ", df.number_cities[df.number_cities > 0].count(), " (", round(df.number_cities[df.number_cities > 0].count()/df.number_cities.count()*100,2), "%)")
    print("Plants mean and std: ", round(df.number_plants.mean(),2), " x ", round(df.number_plants.std(),2))
    print("Plants non-null count: ", df.number_plants[df.number_plants > 0].count(), " (", round(df.number_plants[df.number_plants > 0].count()/df.number_plants.count()*100,2), "%)")
    print("Coal Plants mean and std: ", round(df.number_coals.mean(),2), " x ", round(df.number_coals.std(),2))
    print("Coal Plants non-null count: ", df.number_coals[df.number_coals > 0].count(), " (", round(df.number_coals[df.number_coals > 0].count()/df.number_coals.count()*100,2), "%)")

    emitter = (df.number_cities + df.number_plants + df.number_coals) > 0
    print("Peaks without emitter: ", df.number_cities.count() - emitter.sum(), " (", round((df.number_cities.count()-emitter.sum())/df.number_coals.count()*100,2), "%)")
    emitters = (df.number_cities + df.number_plants + df.number_coals) > 5
    print("Peaks less than 5 emitters: ", df.number_cities.count() - emitters.sum(), " (", round((df.number_cities.count()-emitters.sum())/df.number_coals.count()*100,2), "%)")